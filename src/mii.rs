use ChunkerImpl;

/// A chunker that implements the Minimal Incremental Interval algorithm.
///
/// This algorithm searches for a sequence of increasing values of a threshold length.
///
/// Source: C. Zhang et al., "MII: A Novel Content Defined Chunking Algorithm for Finding
/// Incremental Data in Data Synchronization," in IEEE Access, vol. 7, pp. 86932-86945, 2019,
/// doi: 10.1109/ACCESS.2019.2926195.
/// PDF: https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=8752387
#[derive(Clone, Debug)]
pub struct MIIChunker {
    interval_length_threshold: usize,
    state: MIIChunkerState,
}

impl MIIChunker {
    /// Creates a new chunker implementing the Minimal Incremental Interval algorithm.
    ///
    /// `interval_length_threshold` configures the inclusive threshold of the interval length after
    /// which a cut-point will be set.
    pub fn new(interval_length_threshold: usize) -> MIIChunker {
        MIIChunker {
            interval_length_threshold,
            state: Default::default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct MIIChunkerState {
    /// The length of the current incrementing run.
    increment_run_length: usize,

    /// The previously ingested byte, if any.
    previous_value: Option<u8>,
}

impl MIIChunkerState {
    fn reset(&mut self) {
        self.increment_run_length = 0;
        self.previous_value = None;
    }
}

impl ChunkerImpl for MIIChunker {
    fn find_boundary(&mut self, data: &[u8]) -> Option<usize> {
        for (i, &b) in data.iter().enumerate() {
            // If we're past the first byte of a new chunk...
            if let Some(previous_value) = self.state.previous_value {
                if b > previous_value {
                    // We have an incrementing interval.
                    self.state.increment_run_length += 1;
                    if self.state.increment_run_length == self.interval_length_threshold {
                        return Some(i);
                    }
                } else {
                    // We don't have an incremental interval anymore.
                    self.state.increment_run_length = 0;
                }
            }

            // Update previous value.
            self.state.previous_value = Some(b);
        }

        // No cut point found in this block
        None
    }

    fn reset(&mut self) {
        self.state.reset()
    }
}
