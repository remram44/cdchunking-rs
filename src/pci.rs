use std::collections::VecDeque;
use ChunkerImpl;

/// A chunker implementing the Parity Check of Interval (PCI) algorithm.
///
/// This algorithm checks whether the popcount within a sliding window exceeds a preset threshold.
/// The implementation is generic over the size of the sliding window.
///
/// In contrast to the pseudocode in the paper, the popcount is not recomputed over the entire
/// window for each byte, but rather kept updated whenever the window moves.
/// Additionally, although not mentioned explicitly in the paper, the window size sets a lower
/// bound on the size of chunks produced. This is apparent in the pseudocode of the algorithm, and
/// we implement it as such here.
///
/// Source:C. Zhang, D. Qi, W. Li and J. Guo, "Function of Content Defined Chunking Algorithms in
/// Incremental Synchronization," in IEEE Access, vol. 8, pp. 5316-5330, 2020,
/// doi: 10.1109/ACCESS.2019.2963625.
/// PDF: https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=8949536
#[derive(Debug, Clone)]
pub struct PCIChunker<const W: usize> {
    one_bits_threshold: u32,
    state: PCIChunkerState<W>,
}

impl<const W: usize> PCIChunker<W> {
    /// Create a new chunker implementing the ParityCheck of Interval (PCI) algorithm.
    /// The generic parameter `W` sets the size of the window in bytes.
    /// The parameter `one_bits_threshold` defines the inclusive threshold of one-bits within the
    /// sliding window.
    ///
    /// Contrary to the statements in the original paper, the number of one-bits in a uniformly
    /// distributed byte does _not_ follow a discrete uniform distribution, but rather a binomial one.
    pub fn new(one_bits_threshold: u32) -> PCIChunker<W> {
        PCIChunker {
            one_bits_threshold,
            state: PCIChunkerState::default(),
        }
    }
}

#[derive(Debug, Clone)]
struct PCIChunkerState<const W: usize> {
    /// The sliding window.
    /// This is implemented as a ring buffer.
    window: [u8; W],

    /// The position since the last chunk boundary.
    pos: usize,

    /// The current popcount of the window.
    /// This can be updated for each byte that is ingested by subtracting the popcount of the byte
    /// that is overwritten, and adding the popcount of the byte being added.
    running_popcount: u32,
}

impl<const W: usize> Default for PCIChunkerState<W> {
    fn default() -> Self {
        PCIChunkerState {
            window: [0; W],
            pos: 0,
            running_popcount: 0,
        }
    }
}

impl<const W: usize> PCIChunkerState<W> {
    fn reset(&mut self) {
        self.window = [0; W];
        self.pos = 0;
        self.running_popcount = 0;
    }

    fn is_window_full(&self) -> bool {
        self.pos >= W
    }

    fn ingest_byte_update_popcount(&mut self, b: u8) {
        let pos = self.pos % W;

        // Update running popcount.
        let popcount_to_remove = self.window[pos].count_ones();
        // We should never go negative
        debug_assert!(self.running_popcount >= popcount_to_remove);

        self.running_popcount -= popcount_to_remove;
        self.running_popcount += b.count_ones();

        // Overwrite byte in the window.
        self.window[pos] = b;
        self.pos += 1;
    }

}

impl<const W: usize> ChunkerImpl for PCIChunker<W> {
    fn find_boundary(&mut self, data: &[u8]) -> Option<usize> {
        for (i, &b) in data.iter().enumerate() {
            // Ingest the byte, updating the window and the running popcount.
            self.state.ingest_byte_update_popcount(b);

            // We still need to check if we've read at least window_size bytes, as this sets an
            // implicit bound on the minimum chunk size.
            if self.state.is_window_full() {
                if self.state.running_popcount >= self.one_bits_threshold {
                    return Some(i);
                }
            }
        }

        // No cut-point found within current data block.
        None
    }

    fn reset(&mut self) {
        self.state.reset()
    }
}

/// A chunker implementing the Parity Check of Interval (PCI) algorithm.
///
/// This algorithm checks whether the popcount within a sliding window exceeds a preset threshold.
///
/// In contrast to the pseudocode in the paper, the popcount is not recomputed over the entire
/// window for each byte, but rather kept updated whenever the window moves.
/// Additionally, although not mentioned explicitly in the paper, the window size sets a lower
/// bound on the size of chunks produced. This is apparent in the pseudocode of the algorithm, and
/// we implement it as such here.
///
/// Source:C. Zhang, D. Qi, W. Li and J. Guo, "Function of Content Defined Chunking Algorithms in
/// Incremental Synchronization," in IEEE Access, vol. 8, pp. 5316-5330, 2020,
/// doi: 10.1109/ACCESS.2019.2963625.
/// PDF: https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=8949536
#[derive(Debug, Clone)]
pub struct PCIChunkerNonConst {
    one_bits_threshold: u32,
    state: PCIChunkerStateNonConst,
    window_size: usize,
}

impl PCIChunkerNonConst {
    /// Create a new chunker implementing the ParityCheck of Interval (PCI) algorithm.
    /// The parameter `one_bits_threshold` defines the inclusive threshold of one-bits within the
    /// sliding window.
    /// The `window_size` determines the size of the sliding window, in bytes.
    ///
    /// This implementation keeps a running popcount when processing bytes, and uses a
    /// dynamically-allocated ring buffer.
    pub fn new(window_size: usize, one_bits_threshold: u32) -> PCIChunkerNonConst {
        let mut state = PCIChunkerStateNonConst::default();
        state.reset(window_size);
        PCIChunkerNonConst {
            one_bits_threshold,
            state,
            window_size,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct PCIChunkerStateNonConst {
    /// The sliding window.
    /// This is reset to be filled with zeroes.
    window: VecDeque<u8>,

    /// The position since the last chunk boundary.
    pos: usize,

    /// The current popcount of the window.
    /// This can be updated for each byte that is ingested by subtracting the popcount of the byte
    /// that is overwritten, and adding the popcount of the byte being added.
    running_popcount: u32,
}

impl PCIChunkerStateNonConst {
    fn reset(&mut self, window_size: usize) {
        self.window.clear();

        for _i in 0..window_size {
            self.window.push_back(0);
        }

        self.pos = 0;
        self.running_popcount = 0;
    }

    fn ingest_byte(&mut self, b: u8) -> u32 {
        self.running_popcount -= self.window.pop_front().unwrap().count_ones();
        self.running_popcount += b.count_ones();
        self.window.push_back(b);

        self.pos += 1;

        self.running_popcount
    }
}

impl ChunkerImpl for PCIChunkerNonConst {
    fn find_boundary(&mut self, data: &[u8]) -> Option<usize> {
        for (i, &b) in data.iter().enumerate() {
            let popcount = self.state.ingest_byte(b);
            if self.state.pos >= self.window_size {
                if popcount >= self.one_bits_threshold {
                    return Some(i);
                }
            }
        }

        None
    }

    fn reset(&mut self) {
        self.state.reset(self.window_size)
    }
}
