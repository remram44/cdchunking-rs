trait ChunkerImpl {
}

struct Chunker<I: ChunkerImpl> {
    inner: I,
}

impl<I: ChunkerImpl> Chunker<I> {
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
