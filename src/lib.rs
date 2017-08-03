use std::io::{self, Read};

/// This class is the internal method of finding chunk boundaries.
///
/// It can look at the actual bytes or not, for example:
/// * Use a rolling algorithm such as ZPAQ or Adler32
/// * Find some predefined boundary in the data
/// * Make blocks of a fixed size (then it's NOT content-defined!)
///
/// This is where the internal state of the algorithm should be kept (counter,
/// hash, etc).
pub trait ChunkerImpl {
    /// Look at the new bytes to maybe find a boundary.
    fn find_boundary(&mut self, data: &[u8]) -> Some(usize);
}

/// Chunker object, wraps the rolling hash into a stream-splitting object.
pub struct Chunker<I: ChunkerImpl> {
    inner: I,
}

impl<I: ChunkerImpl> Chunker<I> {
    /// Create a Chunker from a specific way of finding chunk boundaries.
    pub fn new(inner: I) -> Chunker<I> {
        Chunker { inner: inner }
    }

    pub fn whole_chunks<R: Read>(&mut self, reader: R) -> WholeChunks {
        unimplemented!()
    }

    pub fn all_chunks<R: Read>(&mut self, reader: R)
        -> io::Result<Vec<Vec<u8>>>
    {
        let mut chunks = Vec::new();
        for chunk in self.whole_chunks(reader) {
            match chunk {
                Ok(chunk) => chunks.push(chunk),
                Err(e) => return Err(e)
            }
        }
        Ok(chunks)
    }

    pub fn stream<R: Read>(&mut self, reader: R) -> ChunkStream {
        unimplemented!()
    }

    pub fn chunks<R: Read>(&mut self, reader: R) -> ChunkInfoStream {
        let mut pos = 0;
        let mut last_chunk = 0;
        let mut chunk_iter = self.stream(reader);
        while let Some(chunk) = chunk_iter.read() {
            let chunk = chunk.unwrap();
            match chunk {
                ChunkInput::Data(d) => pos += d.len(),
                ChunkInput::End => {
                    yield ChunkInfo { start: last_chunk,
                                      length: pos - last_chunk }
                    last_chunk = pos;
                }
            }
        }
    }

    pub fn slices(&mut self, buffer: &[u8]) -> Slices {
        unimplemented!()
    }
}

struct WholeChunks;
struct ChunkStream;
struct ChunkInfoStream;
struct Slices;

#[cfg(test)]
mod tests {
    fn base() -> (Chunker<ZPAQ>, &'static [u8],
                  Cursor<&'static [u8]>, &'static [u8]) {
        let rollinghash = ZPAQ::new(3); // 8-bit chunk average
        let chunker = Chunker::new(rollinghash);
        let data = "abcdefghijklmnopqrstuvwxyz1234567890";
        (chunker, data, Cursor::new(data), expected)
    }

    #[test]
    fn test_whole_chunks() {
        let (chunker, _, reader, expected) = base();
        let result = Vec::new();

        // Read whole chunks accumulated in vectors
        for chunk: io::Result<Vec<u8>> in chunker.whole_chunks(reader) {
            let chunk = chunk.unwrap();
            result.extend(chunk);
            result.push(b'|');
        }
        assert_eq!(result, expected);
    }

    #[test]
    fn test_all_chunks() {
        let (chunker, _, reader, expected) = base();
        let result = Vec::new();

        // Read all the chunks at once
        // Like using whole_chunks(...).collect() but also handles errors
        let chunks: Vec<Vec<u8>> = chunker.all_chunks(reader).unwrap();
        for chunk: Vec<u8> in chunks {
            result.extend(chunk);
            result.push(b'|');
        }
        assert_eq!(result, expected);
    }

    #[test]
    fn test_stream() {
        let (chunker, _, reader, expected) = base();
        let result = Vec::new();

        // Zero-allocation by using a fixed-size internal buffer
        let mut chunk_iter = chunker.stream(reader);
        while let Some(chunk) = chunk_iter.read() {
            let chunk = chunk.unwrap();
            match chunk {
                ChunkInput::Data(d) => {
                    result.extend(d);
                }
                ChunkInput::End => result.push(b'|'),
            }
        }
        assert_eq!(result, expected);
    }

    #[test]
    fn test_slices() {
        let (chunker, data, _, expected) = base();
        let result = Vec::new();

        // Get slices from an in-memory buffer holding the whole input
        for slice: &[u8] in chunker.slices(data) {
            result.extend(slice);
            result.push(b'|');
        }
        assert_eq!(result, expected);
    }

    #[test]
    fn test_chunks() {
        let (chunker, data, reader, expected) = base();
        let result = Vec::new();

        // Get chunk positions
        for chunk_info in chunker.chunks(reader) {
            let chunk_info = chunk_info.unwrap();
            result.extend(&data[chunk_info.start..(chunk_info.start +
                                                   chunk_info.length)]);
            result.push(b'|');
        }
        assert_eq!(result, expected);
    }
}
