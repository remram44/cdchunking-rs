//! Content-Defined Chunking
//!
//! This crates provides a way to device a stream of bytes into chunks, using
//! methods that choose the splitting point from the content itself. This means
//! that adding or removing a few bytes in the stream would only change the
//! chunks directly modified. This is different from just splitting every n
//! bytes, because in that case every chunk is different unless the number of
//! bytes changed is a multiple of n.
//!
//! Content-defined chunking is useful for data de-duplication. It is used in
//! many backup software, and by the rsync data synchronization tool.
//!
//! This crate exposes both easy-to-use methods, implementing the standard
//! `Iterator` trait to iterate on chunks in an input stream, and efficient
//! zero-allocation methods that reuse an internal buffer.
//!
//! # Using this crate
//!
//! First, add a dependency on this crate by adding the following to your
//! `Cargo.toml`:
//!
//! ```toml
//! cdchunking = 0.1
//! ```
//!
//! And your `lib.rs`:
//!
//! ```
//! extern crate cdchunking;
//! ```
//!
//! Then create a `Chunker` object using a specific method, for example the ZPAQ
//! algorithm:
//!
//! ```
//! use cdchunking::{Chunker, ZPAQ};
//!
//! let chunker = Chunker::new(ZPAQ::new(13)); // 13 bits = 8 KiB block average
//! ```
//!
//! There are multiple way to get chunks out of some input data.
//!
//! ### From an in-memory buffer: iterate on slices
//!
//! If your whole input data is in memory at once, you can use the `slices()`
//! method. It will return an iterator on slices of this buffer, allowing to
//! handle those chunks with no additional allocation.
//!
//! ```
//! # use cdchunking::{Chunker, ZPAQ};
//! # let chunker = Chunker::new(ZPAQ::new(13));
//! # let data = b"abcdefghijklmnopqrstuvwxyz1234567890";
//! for slice in chunker.slices(data) {
//!     println!("{:?}", slice);
//! }
//! ```
//!
//! ### From a file object: read chunks into memory
//!
//! If you are reading from a file, or any object that implements `Read`, you
//! can use `Chunker` to read whole chunks directly. Use the `whole_chunks()`
//! method to get an iterator on chunks, read as new `Vec<u8>` objects.
//!
//! ```
//! # use cdchunking::{Chunker, ZPAQ};
//! # let chunker = Chunker::new(ZPAQ::new(13));
//! # let reader: &[u8] = b"abcdefghijklmnopqrstuvwxyz1234567890";
//! for chunk in chunker.whole_chunks(reader) {
//!     let chunk = chunk.expect("Error reading from file");
//!     println!("{:?}", chunk);
//! }
//! ```
//!
//! You can also read all the chunks from the file and collect them in a `Vec`
//! (of `Vec`s) using the `all_chunks()` method. It will take care of the IO
//! errors for you, returning an error if any of the chunks fail to read.
//!
//! ```
//! # use cdchunking::{Chunker, ZPAQ};
//! # let chunker = Chunker::new(ZPAQ::new(13));
//! # let reader: &[u8] = b"abcdefghijklmnopqrstuvwxyz1234567890";
//! let chunks: Vec<Vec<u8>> = chunker.all_chunks(reader)
//!     .expect("Error reading from file");
//! for chunk in chunks {
//!     println!("{:?}", chunk);
//! }
//! ```
//!
//! ### From a file object: streaming chunks with zero allocation
//!
//! If you are reading from a file to write to another, you might deem the
//! allocation of intermediate `Vec` objects unnecessary. If you want, you can
//! have `Chunker` provide you chunks data from the internal read buffer,
//! without allocating anything else. In that case, note that a chunk might be
//! split between multiple read operations. This method will work fine with any
//! chunk sizes.
//!
//! Use the `stream()` method to do this. Note that because an internal buffer
//! is reused, we cannot implement the `Iterator` trait, so you will have to use
//! a while loop:
//!
//! ```
//! # use cdchunking::{Chunker, ChunkInput, ZPAQ};
//! # let chunker = Chunker::new(ZPAQ::new(13));
//! # let reader: &[u8] = b"abcdefghijklmnopqrstuvwxyz1234567890";
//! let mut chunk_iterator = chunker.stream(reader);
//! while let Some(chunk) = chunk_iterator.read() {
//!     let chunk = chunk.unwrap();
//!     match chunk {
//!         ChunkInput::Data(d) => {
//!             print!("{:?}, ", d);
//!         }
//!         ChunkInput::End => println!(" end of chunk"),
//!     }
//! }
//! ```

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
            status: EmitStatus::Data,
        }
    }

    pub fn chunks<R: Read>(self, reader: R) -> ChunkInfoStream<R, I> {
        ChunkInfoStream {
            stream: self.stream(reader),
            last_chunk: 0,
            pos: 0,
        }
    }

    pub fn slices(self, buffer: &[u8]) -> Slices<I> {
        Slices {
            inner: self.inner,
            buffer: buffer,
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

/// Objects returned from the ChunkStream iterator.
///
/// This is either more data in the current chunk, or a chunk boundary.
pub enum ChunkInput<'a> {
    Data(&'a [u8]),
    End,
}

#[derive(PartialEq, Eq)]
enum EmitStatus {
    End, // We didn't emit any Data since the last End
    Data, // We have been emitting data
    AtSplit, // We found the end of a chunk, emitted the Data but not the End
}

pub struct ChunkStream<R: Read, I: ChunkerImpl> {
    reader: R,
    inner: I,
    buffer: [u8; BUF_SIZE],
    len: usize, // How much of the buffer has been read in from the reader
    pos: usize, // Where are we in handling the buffer
    status: EmitStatus,

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
        if self.pos == self.len {
            assert!(self.status != EmitStatus::AtSplit);
            self.pos = 0;
            self.len = match self.reader.read(&mut self.buffer) {
                Ok(l) => l,
                Err(e) => return Some(Err(e)),
            };
            if self.len == 0 {
                if self.status == EmitStatus::Data {
                    self.status = EmitStatus::End;
                    return Some(Ok(ChunkInput::End));
                }
                return None;
            }
        }
        if self.status == EmitStatus::AtSplit {
            self.status = EmitStatus::End;
            self.inner.reset();
            return Some(Ok(ChunkInput::End));
        }
        if let Some(split) = self.inner.find_boundary(
            &self.buffer[self.pos..self.len])
        {
            assert!(self.pos + split < self.len);
            self.status = EmitStatus::AtSplit;
            let start = self.pos;
            self.pos += split + 1;
            return Some(Ok(ChunkInput::Data(&self.buffer[start..self.pos])));
        }
        let start = self.pos;
        self.pos = self.len;
        self.status = EmitStatus::Data;
        Some(Ok(ChunkInput::Data(&self.buffer[start..self.len])))
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

pub struct Slices<'a, I: ChunkerImpl> {
    inner: I,
    buffer: &'a [u8],
    pos: usize,
}

impl<'a, I: ChunkerImpl> Iterator for Slices<'a, I> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        if self.pos == self.buffer.len() {
            None
        } else {
            if let Some(split) = self.inner.find_boundary(
                &self.buffer[self.pos..])
            {
                assert!(self.pos + split < self.buffer.len());
                let start = self.pos;
                self.pos += split + 1;
                self.inner.reset();
                Some(&self.buffer[start..self.pos])
            } else {
                let start = self.pos;
                self.pos = self.buffer.len();
                Some(&self.buffer[start..])
            }
        }
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
            nbits: 32 - nbits,
            c1: 0,
            o1: [0; 256],
            h: HM,
        }
    }

    pub fn update(&mut self, byte: u8) -> bool {
        if byte == self.o1[self.c1 as usize] {
            self.h = self.h * HM + Wrapping(byte as u32 + 1);
        } else {
            self.h = self.h * HM * Wrapping(2) + Wrapping(byte as u32 + 1);
        }
        self.o1[self.c1 as usize] = byte;
        self.c1 = byte;

        self.h.0 < (1 << self.nbits)
    }
}

impl ChunkerImpl for ZPAQ {
    fn find_boundary(&mut self, data: &[u8]) -> Option<usize> {
        let mut pos = 0;
        while pos < data.len() {
            if self.update(data[pos]) {
                return Some(pos);
            }

            pos += 1;
        }
        None
    }

    fn reset(&mut self) {
        self.c1 = 0u8;
        self.o1.clone_from_slice(&[0u8; 256]);
        self.h = HM;
    }
}

#[cfg(test)]
mod tests {
    use ::{Chunker, ChunkInput, ZPAQ};
    use std::io::Cursor;
    use std::str::from_utf8;

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
        assert_eq!(from_utf8(&result).unwrap(),
                   from_utf8(&expected).unwrap());
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
        assert_eq!(from_utf8(&result).unwrap(),
                   from_utf8(&expected).unwrap());
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
        assert_eq!(from_utf8(&result).unwrap(),
                   from_utf8(&expected).unwrap());
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
        assert_eq!(from_utf8(&result).unwrap(),
                   from_utf8(&expected).unwrap());
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
        assert_eq!(from_utf8(&result).unwrap(),
                   from_utf8(&expected).unwrap());
    }
}
