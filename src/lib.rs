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
//! errors for you, returning an error if any of the chunks failed to read.
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

#![forbid(unsafe_code)]

#[cfg(test)]
extern crate rand;

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
        Chunker { inner }
    }

    /// Iterates on whole chunks from a file, read into new vectors.
    pub fn whole_chunks<R: Read>(self, reader: R) -> WholeChunks<R, I> {
        WholeChunks {
            stream: self.stream(reader),
            buffer: Vec::new(),
        }
    }

    /// Reads all the chunks at once, in a vector of chunks (also vectors).
    ///
    /// This is similar to `.whole_chunks().collect()`, but takes care of the IO
    /// errors, returning an error if any of the chunks failed to read.
    pub fn all_chunks<R: Read>(self, reader: R) -> io::Result<Vec<Vec<u8>>> {
        let mut chunks = Vec::new();
        for chunk in self.whole_chunks(reader) {
            match chunk {
                Ok(chunk) => chunks.push(chunk),
                Err(e) => return Err(e),
            }
        }
        Ok(chunks)
    }

    /// Reads chunks with zero allocations.
    ///
    /// This streaming iterator provides you with the chunk from an internal
    /// buffer that gets reused, instead of allowing memory to hold each chunk.
    /// This is very memory efficient, even if reading large chunks from a
    /// large file (you will get chunks in multiple parts). Unfortunately
    /// because the buffer gets reused, you have to use a while loop; `Iterator`
    /// cannot be implemented.
    ///
    /// Example:
    ///
    /// ```
    /// # use cdchunking::{Chunker, ChunkInput, ZPAQ};
    /// # let chunker = Chunker::new(ZPAQ::new(13));
    /// # let reader: &[u8] = b"abcdefghijklmnopqrstuvwxyz1234567890";
    /// let mut chunk_iterator = chunker.stream(reader);
    /// while let Some(chunk) = chunk_iterator.read() {
    ///     let chunk = chunk.unwrap();
    ///     match chunk {
    ///         ChunkInput::Data(d) => {
    ///             print!("{:?}, ", d);
    ///         }
    ///         ChunkInput::End => println!(" end of chunk"),
    ///     }
    /// }
    /// ```
    pub fn stream<R: Read>(self, reader: R) -> ChunkStream<R, I> {
        ChunkStream {
            reader,
            inner: self.inner,
            buffer: [0u8; BUF_SIZE],
            pos: 0,
            len: 0,
            status: EmitStatus::Data,
        }
    }

    /// Describes the chunks (don't return the data).
    ///
    /// This iterator gives you the offset and size of the chunks, but not the
    /// data in them. If you want to iterate on the data in the chunks in an
    /// easy way, use the `whole_chunks()` method.
    pub fn chunks<R: Read>(self, reader: R) -> ChunkInfoStream<R, I> {
        ChunkInfoStream {
            stream: self.stream(reader),
            last_chunk: 0,
            pos: 0,
        }
    }

    /// Iterate on chunks in an in-memory buffer as slices.
    ///
    /// If your data is already in memory, you can use this method instead of
    /// `whole_chunks()` to get slices referencing the buffer rather than
    /// copying it to new vectors.
    pub fn slices(self, buffer: &[u8]) -> Slices<I> {
        Slices {
            inner: self.inner,
            buffer,
            pos: 0,
        }
    }

    /// Returns a new `Chunker` object that will not go over a size limit.
    ///
    /// Note that the inner chunking method IS reset when a chunk boundary is
    /// emitted because of the size limit. That means that using a size limit
    /// will not only add new boundary, inside of blocks too big, it might cause
    /// the boundary after such a one to not happen anymore.
    pub fn max_size(self, max: usize) -> Chunker<SizeLimited<I>> {
        assert!(max > 0);
        Chunker {
            inner: SizeLimited {
                inner: self.inner,
                pos: 0,
                max_size: max,
            },
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
    End,     // We didn't emit any Data since the last End
    Data,    // We have been emitting data
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
        if self.status == EmitStatus::AtSplit {
            self.status = EmitStatus::End;
            self.inner.reset();
            return Some(Ok(ChunkInput::End));
        }
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
        if let Some(split) =
            self.inner.find_boundary(&self.buffer[self.pos..self.len])
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
                    return Some(Ok(ChunkInfo {
                        start,
                        length: self.pos - start,
                    }));
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
        } else if let Some(split) =
            self.inner.find_boundary(&self.buffer[self.pos..])
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

pub struct SizeLimited<I: ChunkerImpl> {
    inner: I,
    pos: usize,
    max_size: usize,
}

impl<I: ChunkerImpl> ChunkerImpl for SizeLimited<I> {
    fn find_boundary(&mut self, data: &[u8]) -> Option<usize> {
        assert!(self.max_size > self.pos);
        if data.is_empty() {
            return None;
        }
        let left = self.max_size - self.pos;
        if left == 1 {
            Some(0)
        } else {
            let slice = if data.len() > left {
                &data[..left]
            } else {
                data
            };
            match self.inner.find_boundary(slice) {
                Some(p) => {
                    self.pos += p + 1;
                    Some(p)
                }
                None => {
                    self.pos += slice.len();
                    if data.len() >= left {
                        Some(left - 1)
                    } else {
                        None
                    }
                }
            }
        }
    }

    fn reset(&mut self) {
        self.pos = 0;
        self.inner.reset();
    }
}

const HM1: Wrapping<u32> = Wrapping(314_159_265);
const HM2: Wrapping<u32> = Wrapping(271_828_182);

/// ZPAQ-like chunking algorithm.
///
/// Note that this does NOT match the official implementation (the ZPAQ C++
/// library). This functions the same way, but does not use the same specific
/// sizes, and does not enforce chunk size limits (unless you use
/// `Chunker::max_size()` explicitly). In addition, the constants used by this
/// implementation are different; see
/// [#6](https://github.com/remram44/cdchunking-rs/issues/6).
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
            h: Wrapping(0),
        }
    }

    pub fn update(&mut self, byte: u8) -> bool {
        if byte == self.o1[self.c1 as usize] {
            self.h = HM1 * (self.h + Wrapping(byte as u32 + 1));
        } else {
            self.h = HM2 * (self.h + Wrapping(byte as u32 + 1));
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
        self.h = Wrapping(0);
    }
}

#[cfg(test)]
mod tests {
    use rand::{self, Rng};
    use std::io::{self, Read};
    use std::str::from_utf8;

    use super::{ChunkInput, Chunker, ZPAQ};

    fn base() -> (
        Chunker<ZPAQ>,
        &'static [u8],
        io::Cursor<&'static [u8]>,
        &'static [u8],
    ) {
        let rollinghash = ZPAQ::new(3); // 8-bit chunk average
        let chunker = Chunker::new(rollinghash);
        let data = b"defghijklmnopqrstuvwxyz1234567890";
        let expected = b"de|fghijklmnopqr|stuvwxyz12345|67890|";
        (chunker, data, io::Cursor::new(data), expected)
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
        assert_eq!(
            from_utf8(&result).unwrap(),
            from_utf8(&expected).unwrap()
        );
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
        assert_eq!(
            from_utf8(&result).unwrap(),
            from_utf8(&expected).unwrap()
        );
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
        assert_eq!(
            from_utf8(&result).unwrap(),
            from_utf8(&expected).unwrap()
        );
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
        assert_eq!(
            from_utf8(&result).unwrap(),
            from_utf8(&expected).unwrap()
        );
    }

    #[test]
    fn test_chunks() {
        let (chunker, _, reader, _) = base();
        let mut result = Vec::new();

        // Get chunk positions
        for chunk_info in chunker.chunks(reader) {
            let chunk_info = chunk_info.unwrap();
            result.push((chunk_info.start(), chunk_info.length()));
        }
        assert_eq!(
            result,
            vec![
                (0, 2), (2, 13), (15, 13), (28, 5),
            ]
        );
    }

    #[test]
    fn test_max_size() {
        let (chunker, _, reader, _) = base();
        let mut result = Vec::new();

        // Get chunk positions
        for chunk_info in chunker.max_size(5).chunks(reader) {
            let chunk_info = chunk_info.unwrap();
            result.push((chunk_info.start(), chunk_info.length()));
        }
        // Note that some previous block boundaries are not here (11, 23, 29)
        // It is because the ZPAQ state is reset when we hit the maximum length
        // too.
        assert_eq!(
            result,
            vec![
                (0, 2), (2, 5), (7, 5), (12, 2),
                (14, 5), (19, 5), (24, 5), (29, 4),
            ]
        );
    }

    struct RngFile<R: Rng>(R);

    impl<R: Rng> Read for RngFile<R> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.0.fill_bytes(buf);
            Ok(buf.len())
        }
    }

    #[test]
    fn test_random() {
        let mut count = 0;
        let chunker = Chunker::new(ZPAQ::new(8));

        let random = RngFile(rand::thread_rng());

        let mut total_len = 0;

        for chunk in chunker.whole_chunks(random) {
            total_len += chunk.unwrap().len();
            count += 1;
            if count >= 4096 {
                break;
            }
        }

        assert!(240 * count <= total_len && total_len <= 270 * count);
    }
}
