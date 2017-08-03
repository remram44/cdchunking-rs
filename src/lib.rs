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

    pub fn chunks<R: IntoRead>(&mut self, reader: R) -> ChunkInfoStream {
        let mut reader = reader.into_reader();

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

#[test]
fn test() {
    do_test(Cursor::new("abcdef"), ZPAQ::new(4));
}

fn do_test<R: Read, I: ChunkerImpl>(reader: R, inner: I) {
    let mut stdout = io::stdout().lock();

    let rollinghash = ZPAQ::new(3); // 8-bit chunk average
    let chunker = Chunker::new(rollinghash);

    // Read whole chunks accumulated in vectors
    for chunk: io::Result<Vec<u8>> in chunker.whole_chunks(reader) {
        let chunk = chunk.unwrap();
        stdout.write(chunk).unwrap();
        stdout.write(b'\n').unwrap();
    }

    // Read all the chunks at once
    // Like using whole_chunks(...).collect() but also handles errors
    let chunks: Vec<Vec<u8>> = chunker.all_chunks(reader).unwrap();
    for chunk: Vec<u8> in chunks {
        stdout.write(chunk).unwrap();
        stdout.write(b'\n').unwrap();
    }

    // Zero-allocation by using a fixed-size internal buffer
    let mut chunk_iter = chunker.stream(reader);
    while let Some(chunk) = chunk_iter.read() {
        let chunk = chunk.unwrap();
        match chunk {
            ChunkInput::Data(d) => {
                stdout.write(d).unwrap();
            }
            ChunkInput::End => stdout.write(b'\n').unwrap();
        }
    }

    // Get slices from an in-memory buffer holding the whole input
    let input = {
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        buf
    };
    for slice: &[u8] in chunker.slices(buf) {
        stdout.write(slice).unwrap();
        stdout.write(b'\n').unwrap();
    }

    // Get chunk positions
    for chunk_info in chunker.chunks(buf) {
        stdout.write(buf[chunk_info.start..(chunk_info.start +
                                            chunk_info.length)]).unwrap();
        stdout.write(b'\n').unwrap();
    }
}
