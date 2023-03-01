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
    frequent_byte_pairs: Vec<(u8, u8)>,
    min_chunk_size: usize,
    state: BFBCChunkerState,
}

impl BFBCChunker {
    /// Creates a new chunker using the Bytes Frequency-Based Chunking algorithm.
    ///
    /// The given byte pairs are checked, in order, for each sliding window after `min_chunk_size`
    /// bytes to find a chunk boundary.
    pub fn new(frequent_byte_pairs: Vec<(u8, u8)>, min_chunk_size: usize) -> BFBCChunker {
        BFBCChunker {
            frequent_byte_pairs,
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
    previous_byte: Option<u8>,
}

impl BFBCChunkerState {
    fn reset(&mut self) {
        self.pos = 0;
        self.previous_byte = None;
    }

    fn ingest(&mut self, b: u8) {
        self.pos += 1;
        self.previous_byte = Some(b)
    }

    fn match_against(&self, b: u8, pair: &(u8, u8)) -> bool {
        b == pair.1
            && self
                .previous_byte
                .map(|previous_byte| previous_byte == pair.0)
                .unwrap_or_else(|| false)
    }
}

impl ChunkerImpl for BFBCChunker {
    fn find_boundary(&mut self, data: &[u8]) -> Option<usize> {
        for (i, &b) in data.iter().enumerate() {
            if self.state.pos > self.min_chunk_size {
                // TODO optimize for large number of frequent byte pairs, maybe.
                for pair in self.frequent_byte_pairs.iter() {
                    if self.state.match_against(b, pair) {
                        return Some(i);
                    }
                }
            }

            // Important: We ingest after we compare, as otherwise state.previous_byte==b.
            self.state.ingest(b);
        }

        // No chunk boundary found in current data block.
        None
    }

    fn reset(&mut self) {
        self.state.reset()
    }
}
