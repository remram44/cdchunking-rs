use ChunkerImpl;

/// A hasher that implements the TTTD algorithm.
///
/// Source: Eshghi, Kave et al. "A Framework for Analyzing and Improving Content-Based Chunking Algorithms"
/// Hewlett-Packard Labs Technical Report TR (2005)
/// PDF: https://shiftleft.com/mirrors/www.hpl.hp.com/techreports/2005/HPL-2005-30R1.pdf
#[derive(Debug, Clone)]
pub struct TTTDChunker {
    divisor: u32,
    backup_divisor: u32,
    min_chunk_size: usize,
    max_chunk_size: usize,
    state: TTTDState,
}

impl TTTDChunker {
    /// Creates a new TTTD hasher.
    pub fn new(divisor: u32, backup_divisor: u32, min_chunk_size: usize, max_chunk_size: usize) -> TTTDChunker {
        TTTDChunker {
            divisor,
            backup_divisor,
            min_chunk_size,
            max_chunk_size,
            state: Default::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct TTTDState {
    hash: u32,
    pos: usize,
    backup_pos: usize,
}

impl TTTDState {
    fn reset(&mut self) {
        self.hash = 0;
        self.pos = 0;
        self.backup_pos = 0;
    }

    fn ingest(&mut self, b: u8) {
        self.hash = (self.hash << 1).wrapping_add(TABLE[b as usize]);
        self.pos += 1;
    }

    fn check_hash(&self, divisor: u32, backup_divisor: u32) -> bool {
        if self.hash % backup_divisor == backup_divisor - 1 {
            self.backup_pos = self.pos;
        }
        self.hash % divisor == divisor - 1
    }
}

impl ChunkerImpl for TTTDChunker {
    fn find_boundary(&mut self, data: &[u8]) -> Option<usize> {
        for (i, &b) in data.iter().enumerate() {
            self.state.ingest(b);

            if self.state.pos >= self.min_chunk_size {
                if self.state.pos >= self.max_chunk_size {
                    if self.state.backup_pos > 0 {
                        return Some(self.state.backup_pos);
                    }
                    return Some(i);
                }
                if self.state.check_hash(self.divisor, self.backup_divisor) {
                    return Some(i);
                }
            }
        }

        None
    }

    fn reset(&mut self) {
        self.state.reset()
    }
}