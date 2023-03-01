use ChunkerImpl;

/// AEChunker implements the Asymmetric Extremum (AE) algorithm.
///
/// This algorithm searches for a local maximum and sets a cut-point at a fixed distance after it,
/// if there is no higher value within that window.
///
/// Source: Y. Zhang et al., "A Fast Asymmetric Extremum Content Defined Chunking Algorithm for Data
/// Deduplication in Backup Storage Systems," in IEEE Transactions on Computers, vol. 66, no. 2,
/// pp. 199-211, 1 Feb. 2017, doi: 10.1109/TC.2016.2595565.
#[derive(Debug, Clone)]
pub struct AEChunker {
    window_size: usize,
    state: AEChunkerState,
}

impl AEChunker {
    /// Creates a new chunker using the Asymmetric Extremum algorithm with the configured size for
    /// the fixed window.
    pub fn new(window_size: usize) -> AEChunker {
        AEChunker {
            window_size,
            state: Default::default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct AEChunkerState {
    /// The maximum value.
    max_value: u8,

    /// The position of the maximum, relative to the last chunk boundary.
    max_position: usize,

    /// The current position relative to the last chunk boundary.
    pos: usize,
}

impl AEChunkerState {
    fn reset(&mut self) {
        self.max_value = 0;
        self.max_position = 0;
        self.pos = 0;
    }
}

impl ChunkerImpl for AEChunker {
    fn find_boundary(&mut self, data: &[u8]) -> Option<usize> {
        for (i, &b) in data.iter().enumerate() {
            // Calculate the position relative to the last chunk boundary, taking into account
            // previously processed blocks of data.
            let global_pos = self.state.pos + i;

            if b <= self.state.max_value {
                // Check if we're within the window.
                if global_pos == self.state.max_position + self.window_size {
                    return Some(i);
                }
            } else {
                self.state.max_value = b;
                self.state.max_position = global_pos;
            }
        }

        // No cut-point found within this block of data.
        self.state.pos += data.len();
        None
    }

    fn reset(&mut self) {
        self.state.reset()
    }
}
