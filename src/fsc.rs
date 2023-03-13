use ChunkerImpl;

/// A chunker that produces chunks of a fixed size.
///
/// This is fast, but generally bad for deduplication, as it is prone to the boundary-shift problem.
#[derive(Debug, Clone)]
pub struct FixedSizeChunker {
    chunk_size: usize,
    state: FixedSizeChunkerState,
}

#[derive(Debug, Default, Clone, Copy)]
struct FixedSizeChunkerState {
    pos: usize,
}

impl FixedSizeChunkerState {
    fn reset(&mut self) {
        self.pos = 0
    }
}

impl FixedSizeChunker {
    /// Constructs a chunker that produces chunks of a fixed, given size.
    pub fn new(chunk_size: usize) -> FixedSizeChunker {
        FixedSizeChunker {
            chunk_size,
            state: Default::default(),
        }
    }
}

impl ChunkerImpl for FixedSizeChunker {
    fn find_boundary(&mut self, data: &[u8]) -> Option<usize> {
        if self.state.pos + data.len() >= self.chunk_size {
            // Chunk boundary is within this block.
            return Some(self.chunk_size - self.state.pos - 1);
        }

        // Chunk boundary does not lie within this block.
        self.state.pos += data.len();
        None
    }

    fn reset(&mut self) {
        self.state.reset()
    }
}
