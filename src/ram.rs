use ChunkerImpl;

/// A chunker that implements the Rapid Asymmetric Maximum (RAM) algorithm.
///
/// This version is the verbose translation of the algorithm given in the paper.
///
/// This algorithm is similar to Asymmetric Extremum (AE), but uses a fixed-sized window before
/// a variable-sized one.
/// The fixed-sized window of size `w` is used to initialize the state with a local maximum `lmax`.
/// A chunk cut point is then emitted from within the variable-sized window for a byte `b` that
/// satisfies `b >= lmax`.
///
/// The authors note that chunk sizes can be unbounded on low-entropy input and advise to use a
/// chunk size limit of `4*w`.
/// That is not implemented here. Consider using `max_size` to enforce a limit.
///
/// Source: Ryan N.S. Widodo, Hyotaek Lim, Mohammed Atiquzzaman: A new content-defined chunking
/// algorithm for data deduplication in cloud storage. Future Generation Computer Systems, Volume
/// 71, 2017, Pages 145-156, ISSN 0167-739X. https://doi.org/10.1016/j.future.2017.02.013.
#[derive(Clone, Debug)]
pub struct RAMChunker {
    window_size: usize,
    state: RAMState,
}

impl RAMChunker {
    /// Creates a chunker using the Rapid Asymmetric Maximum algorithm with the given size for the
    /// fixed window.
    pub fn new(window_size: usize) -> RAMChunker {
        RAMChunker {
            window_size,
            state: Default::default(),
        }
    }
}

impl ChunkerImpl for RAMChunker {
    fn find_boundary(&mut self, data: &[u8]) -> Option<usize> {
        for (i, &b) in data.iter().enumerate() {
            // Calculate position since last chunk boundary, for correct window size calculation.
            let global_pos = i + self.state.pos;

            if b >= self.state.maximum_value {
                if global_pos > self.window_size {
                    return Some(i);
                }

                self.state.maximum_value = b;
            }
        }

        // No cut-point found within the current data block.
        // Advance offset.
        self.state.pos += data.len();
        None
    }

    fn reset(&mut self) {
        self.state.reset()
    }
}

#[derive(Clone, Debug, Default)]
struct RAMState {
    // The current maximum value.
    // This is initialized and reset to zero.
    maximum_value: u8,
    // The position _relative to the last chunk boundary_.
    // Specifically, this resets to zero after a chunk is emitted.
    // We need this to track if we're done filling our window between calls to `find_boundary`,
    // since we're fed blocks of data which may be smaller than the window.
    pos: usize,
}

impl RAMState {
    fn reset(&mut self) {
        self.maximum_value = 0;
        self.pos = 0;
    }
}

/// A chunker that implements the Rapid Asymmetric Maximum (RAM) algorithm.
///
/// This is a potentially optimized version.
///
/// See `RAMChunker` for more information on the algorithm in general.
#[derive(Clone, Debug)]
pub struct MaybeOptimizedRAMChunker {
    // The size of the fixed-size window.
    window_size: usize,
    // The current state.
    state: RAMState,
}

impl MaybeOptimizedRAMChunker {
    /// Creates a chunker using the Rapid Asymmetric Maximum algorithm with the given size for the
    /// fixed window.
    pub fn new(window_size: usize) -> MaybeOptimizedRAMChunker {
        MaybeOptimizedRAMChunker {
            window_size,
            state: Default::default(),
        }
    }
}

impl MaybeOptimizedRAMChunker {
    /// Ensures that the fixed window is filled and a local maximum has been determined.
    /// Returns None if the window is not yet filled and additional buffers of data are required.
    /// Returns Some(pos) for a position within data at which the boundary between the fixed-sized
    /// and variable-sized windows are. Search for a cut-point should commence from this boundary.
    fn ensure_window_filled(&mut self, data: &[u8]) -> Option<usize> {
        // Check if we need to fill our window, and how many bytes we need for that.
        let num_bytes_to_advance = data.len().min(self.window_size - self.state.pos);

        // Our window is full (or the buffer is empty).
        if num_bytes_to_advance == 0 {
            return Some(0);
        }

        // Find the maximum value within the data we're filling with.
        // Unwrap safety: We just made sure the iterator is not empty.
        let max_value = *data[..num_bytes_to_advance].iter().max().unwrap();

        // Update max value and position.
        if max_value >= self.state.maximum_value {
            self.state.maximum_value = max_value
        }
        self.state.pos += num_bytes_to_advance;

        // We're done filling our window from this buffer.
        if self.state.pos == self.window_size {
            return Some(num_bytes_to_advance);
        }

        // We're not done filling our window from this buffer.
        None
    }
}

impl ChunkerImpl for MaybeOptimizedRAMChunker {
    fn find_boundary(&mut self, data: &[u8]) -> Option<usize> {
        let pos_after_window_is_filled = self.ensure_window_filled(data);
        match pos_after_window_is_filled {
            None => {
                // We're still filling our window.
                None
            }
            Some(pos) => {
                // Our window is full, we can start looking for a boundary.
                // We re-slice the data, iterate over it, and search for the first byte that
                // satisfies b >= max_value.
                // We then need to re-add the number of bytes we (potentially) skipped at the
                // beginning of the buffer and return that value.
                let data = &data[pos..];
                data.iter()
                    .enumerate()
                    .find(|(_, &b)| b >= self.state.maximum_value)
                    .map(|(pos_within_slice, _)| pos_within_slice + pos)
            }
        }
    }

    fn reset(&mut self) {
        self.state.reset()
    }
}
