use std::io::{self, Read};
use std::mem::swap;
use std::num::Wrapping;

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
    fn find_boundary(&mut self, data: &[u8]) -> Option<usize>;

    /// Reset the internal state after a chunk has been emitted
    fn reset(&mut self) {}
}

#[cfg(not(test))]
const BUF_SIZE: usize = 4096;
#[cfg(test)]
const BUF_SIZE: usize = 8;

/// Chunker object, wraps the rolling hash into a stream-splitting object.
pub struct Chunker<I: ChunkerImpl> {
    inner: I,
}

impl<I: ChunkerImpl> Chunker<I> {
    /// Create a Chunker from a specific way of finding chunk boundaries.
    pub fn new(inner: I) -> Chunker<I> {
        Chunker { inner: inner }
    }

    pub fn whole_chunks<R: Read>(self, reader: R) -> WholeChunks<R, I> {
        WholeChunks {
            stream: self.stream(reader),
            buffer: Vec::new(),
        }
    }

    pub fn all_chunks<R: Read>(self, reader: R)
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

    pub fn stream<R: Read>(self, reader: R) -> ChunkStream<R, I> {
        ChunkStream {
            reader: reader,
            inner: self.inner,
            buffer: [0u8; BUF_SIZE],
            pos: 0,
            len: 0,
            chunk_emitted: false,
        }
    }

    pub fn chunks<R: Read>(self, reader: R) -> ChunkInfoStream<R, I> {
        ChunkInfoStream {
            stream: self.stream(reader),
            last_chunk: 0,
            pos: 0,
        }
    }

    pub fn slices(self, buffer: &[u8]) -> Slices {
        Slices {
            buffer: buffer,
            last_chunk: 0,
            pos: 0,
        }
    }
}

pub struct WholeChunks<R: Read, I: ChunkerImpl> {
    stream: ChunkStream<R, I>,
    buffer: Vec<u8>,
}

impl<R: Read, I: ChunkerImpl> Iterator for WholeChunks<R, I> {
    type Item = io::Result<Vec<u8>>;

    fn next(&mut self) -> Option<io::Result<Vec<u8>>> {
        while let Some(chunk) = self.stream.read() {
            match chunk {
                Err(e) => return Some(Err(e)),
                Ok(ChunkInput::Data(d)) => self.buffer.extend_from_slice(d),
                Ok(ChunkInput::End) => {
                    let mut res = Vec::new();
                    swap(&mut res, &mut self.buffer);
                    return Some(Ok(res));
                }
            }
        }
        None
    }
}

pub enum ChunkInput<'a> {
    Data(&'a [u8]),
    End,
}

pub struct ChunkStream<R: Read, I: ChunkerImpl> {
    reader: R,
    inner: I,
    buffer: [u8; BUF_SIZE],
    pos: usize,
    len: usize,
    chunk_emitted: bool,
}

impl<R: Read, I: ChunkerImpl> ChunkStream<R, I> {
    /// Iterate on the chunks, returning `ChunkInput` items.
    ///
    /// An item is either some data that is part of the current chunk, or `End`,
    /// indicating the boundary between chunks.
    ///
    /// `End` is always returned at the end of the last chunk.
    // Can't be Iterator because of 'a
    pub fn read<'a>(&'a mut self) -> Option<io::Result<ChunkInput<'a>>> {
        unimplemented!() // TODO: Get from dhstore's chunker
    }
}

pub struct ChunkInfo {
    start: usize,
    length: usize,
}

impl ChunkInfo {
    pub fn start(&self) -> usize {
        self.start
    }

    pub fn length(&self) -> usize {
        self.length
    }

    pub fn end(&self) -> usize {
        self.start + self.length
    }
}

pub struct ChunkInfoStream<R: Read, I: ChunkerImpl> {
    stream: ChunkStream<R, I>,
    last_chunk: usize,
    pos: usize,
}

impl<R: Read, I: ChunkerImpl> Iterator for ChunkInfoStream<R, I> {
    type Item = io::Result<ChunkInfo>;

    fn next(&mut self) -> Option<io::Result<ChunkInfo>> {
        while let Some(chunk) = self.stream.read() {
            match chunk {
                Err(e) => return Some(Err(e)),
                Ok(ChunkInput::Data(d)) => self.pos += d.len(),
                Ok(ChunkInput::End) => {
                    let start = self.last_chunk;
                    self.last_chunk = self.pos;
                    return Some(Ok(ChunkInfo { start: start,
                                               length: self.pos - start }));
                }
            }
        }
        None
    }
}

pub struct Slices<'a> {
    buffer: &'a [u8],
    last_chunk: usize,
    pos: usize,
}

impl<'a> Iterator for Slices<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        unimplemented!() // TODO: Different implementation here, don't use another buffer
    }
}

const HM: Wrapping<u32> = Wrapping(123456791);

pub struct ZPAQ {
    nbits: usize,
    c1: u8, // previous byte
    o1: [u8; 256],
    h: Wrapping<u32>,
}

impl ZPAQ {
    pub fn new(nbits: usize) -> ZPAQ {
        ZPAQ {
            nbits: nbits,
            c1: 0,
            o1: [0; 256],
            h: HM,
        }
    }
}

impl ChunkerImpl for ZPAQ {
    fn find_boundary(&mut self, data: &[u8]) -> Option<usize> {
        unimplemented!() // TODO: Get from dhstore's chunker
    }
}

#[cfg(test)]
mod tests {
    use ::{Chunker, ChunkInput, ZPAQ};
    use std::io::Cursor;

    fn base() -> (Chunker<ZPAQ>, &'static [u8],
                  Cursor<&'static [u8]>, &'static [u8]) {
        let rollinghash = ZPAQ::new(3); // 8-bit chunk average
        let chunker = Chunker::new(rollinghash);
        let data = b"abcdefghijklmnopqrstuvwxyz1234567890";
        let expected = b"abcdefghijk|lmno|pq|rstuvw|xyz123|4567890|";
        (chunker, data, Cursor::new(data), expected)
    }

    #[test]
    fn test_whole_chunks() {
        let (chunker, _, reader, expected) = base();
        let mut result = Vec::new();

        // Read whole chunks accumulated in vectors
        for chunk /* io::Result<Vec<u8>> */ in chunker.whole_chunks(reader) {
            let chunk = chunk.unwrap();
            result.extend(chunk);
            result.push(b'|');
        }
        assert_eq!(result, expected);
    }

    #[test]
    fn test_all_chunks() {
        let (chunker, _, reader, expected) = base();
        let mut result = Vec::new();

        // Read all the chunks at once
        // Like using whole_chunks(...).collect() but also handles errors
        let chunks: Vec<Vec<u8>> = chunker.all_chunks(reader).unwrap();
        for chunk /* Vec<u8> */ in chunks {
            result.extend(chunk);
            result.push(b'|');
        }
        assert_eq!(result, expected);
    }

    #[test]
    fn test_stream() {
        let (chunker, _, reader, expected) = base();
        let mut result = Vec::new();

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
        let mut result = Vec::new();

        // Get slices from an in-memory buffer holding the whole input
        for slice /* &[u8] */ in chunker.slices(data) {
            result.extend(slice);
            result.push(b'|');
        }
        assert_eq!(result, expected);
    }

    #[test]
    fn test_chunks() {
        let (chunker, data, reader, expected) = base();
        let mut result = Vec::new();

        // Get chunk positions
        for chunk_info in chunker.chunks(reader) {
            let chunk_info = chunk_info.unwrap();
            result.extend(&data[chunk_info.start()..chunk_info.end()]);
            result.push(b'|');
        }
        assert_eq!(result, expected);
    }
}
