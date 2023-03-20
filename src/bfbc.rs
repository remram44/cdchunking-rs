use ChunkerImpl;

/// A chunker implementing the Bytes Frequency-Based Chunking (BFBC) algorithm.
///
/// This algorithm sets chunk boundaries based on frequently occurring byte pairs.
/// A minimum chunk size is enforced. Afterwards, a sliding window of two bytes is compared to a
/// list of frequently occurring byte pairs in order.
///
/// No pseudocode is given in the paper, this implementation follows the textual description
/// instead.
/// The list of frequent byte pairs is assumed to be small and unsorted and is searched sequentially
/// for each byte after the minimum chunk size has been reached.
///
/// To function properly, prior analysis of the byte pair frequency of the dataset needs to be done.
/// That analysis, and storage of its results, is not implemented here.
/// A simple solution to this would be iterating over the dataset using something like
/// ```
/// use std::fs::File;
/// use std::io::{BufReader, Read};
/// use std::collections::HashMap;
///
/// fn main() -> std::io::Result<()> {
///     let path = "/some/path";
///     let f = File::open(path)?;
///     let mut buf_reader = BufReader::new(f);
///     let mut contents = Vec::new();
///     buf_reader.read_to_end(&mut contents)?;
///
///     let mut pairs_with_frequency = contents
///         .windows(2)
///         .fold(HashMap::new(),|mut acc,w| {
///             let pair = (w[0], w[1]);
///             *acc.entry(pair).or_default() += 1;
///             acc
///         })
///         .collect::<Vec<_>>();
///
///     pairs_with_frequency.sort_by_key(|p| p.1);
///     pairs_with_frequency.reverse();
///
///     Ok(())
/// }
/// ```
///
/// The authors note that a maximum chunk size should be enforced as well.
/// That is not implemented in this algorithm, consider wrapping with `max_size`.
///
/// Source: Saeed, A.S.M. and George, L.E.: Data Deduplication System Based on Content-Defined
/// Chunking Using Bytes Pair Frequency Occurrence. Symmetry 2020, 12, 1841.
/// https://doi.org/10.3390/sym12111841
/// PDF: https://www.mdpi.com/2073-8994/12/11/1841/pdf?version=1605858554
#[derive(Debug, Clone)]
pub struct BFBCChunker {
    frequent_byte_pairs: Vec<u16>,
    min_chunk_size: usize,
    state: BFBCChunkerState,
}

impl BFBCChunker {
    /// Creates a new chunker using the Bytes Frequency-Based Chunking algorithm.
    ///
    /// The given byte pairs are checked, in order, for each sliding window after `min_chunk_size`
    /// bytes to find a chunk boundary.
    pub fn new(frequent_byte_pairs: Vec<(u8, u8)>, min_chunk_size: usize) -> BFBCChunker {
        assert!(
            min_chunk_size >= 2,
            "min_chunk_size needs to be at least 2 (the size of the window)"
        );
        BFBCChunker {
            frequent_byte_pairs: frequent_byte_pairs
                .into_iter()
                .map(|(b1, b2)| (b1 as u16) << 8 | b2 as u16)
                .collect(),
            min_chunk_size,
            state: Default::default(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct BFBCChunkerState {
    /// The current position relative to the last chunk boundary.
    pos: usize,

    /// The previously ingested byte, if `pos>0`.
    window: u16,
}

impl BFBCChunkerState {
    fn reset(&mut self) {
        self.pos = 0;
        self.window = 0;
    }

    fn ingest(&mut self, b: u8) {
        self.pos += 1;
        self.window = self.window << 8 | b as u16
    }
}

impl ChunkerImpl for BFBCChunker {
    fn find_boundary(&mut self, data: &[u8]) -> Option<usize> {
        for (i, &b) in data.iter().enumerate() {
            self.state.ingest(b);

            if self.state.pos >= self.min_chunk_size {
                for pair in self.frequent_byte_pairs.iter() {
                    if self.state.window == *pair {
                        return Some(i);
                    }
                }
            }
        }

        // No chunk boundary found in current data block.
        None
    }

    fn reset(&mut self) {
        self.state.reset()
    }
}
