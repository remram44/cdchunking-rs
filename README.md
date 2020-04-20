[![Build Status](https://github.com/remram44/cdchunking-rs/workflows/Test/badge.svg)](https://github.com/remram44/cdchunking-rs/actions)
[![Crates.io](https://img.shields.io/crates/v/cdchunking.svg)](https://crates.io/crates/cdchunking)
[![Documentation](https://docs.rs/cdchunking/badge.svg)](https://docs.rs/cdchunking)
[![Say Thanks!](https://img.shields.io/badge/Say%20Thanks-!-1EAEDB.svg)](https://saythanks.io/to/remram44)

Content-Defined Chunking
========================

This crates provides a way to device a stream of bytes into chunks, using methods that choose the splitting point from the content itself. This means that adding or removing a few bytes in the stream would only change the chunks directly modified. This is different from just splitting every n bytes, because in that case every chunk is different unless the number of bytes changed is a multiple of n.

Content-defined chunking is useful for data de-duplication. It is used in many backup software, and by the rsync data synchronization tool.

This crate exposes both easy-to-use methods, implementing the standard `Iterator` trait to iterate on chunks in an input stream, and efficient zero-allocation methods that reuse an internal buffer.

Using this crate
----------------

First, add a dependency on this crate by adding the following to your `Cargo.toml`:

```
cdchunking = 1.0
```

And your `lib.rs`:

```rust
extern crate cdchunking;
```

Then create a `Chunker` object using a specific method, for example the ZPAQ algorithm:

```rust
use cdchunking::{Chunker, ZPAQ};

let chunker = Chunker::new(ZPAQ::new(13)); // 13 bits = 8 KiB block average
```

There are multiple way to get chunks out of some input data.

### From an in-memory buffer: iterate on slices

If your whole input data is in memory at once, you can use the `slices()` method. It will return an iterator on slices of this buffer, allowing to handle those chunks with no additional allocation.

```rust
for slice in chunker.slices(data) {
    println("{:?}", slice);
}
```

### From a file object: read chunks into memory

If you are reading from a file, or any object that implements `Read`, you can use `Chunker` to read whole chunks directly. Use the `whole_chunks()` method to get an iterator on chunks, read as new `Vec<u8>` objects.

```rust
for chunk in chunker.whole_chunks(reader) {
    let chunk = chunk.expect("Error reading from file");
    println!("{:?}", chunk);
}
```

You can also read all the chunks from the file and collect them in a `Vec` (of `Vec`s) using the `all_chunks()` method. It will take care of the IO errors for you, returning an error if any of the chunks failed to read.

```rust
let chunks: Vec<Vec<u8>> = chunker.all_chunks(reader)
    .expect("Error reading from file");
for chunk in chunks {
    println!("{:?}", chunk);
}
```

### From a file object: streaming chunks with zero allocation

If you are reading from a file to write to another, you might deem the allocation of intermediate `Vec` objects unnecessary. If you want, you can have `Chunker` provide you chunks data from the internal read buffer, without allocating anything else. In that case, note that a chunk might be split between multiple read operations. This method will work fine with any chunk sizes.

Use the `stream()` method to do this. Note that because an internal buffer is reused, we cannot implement the `Iterator` trait, so you will have to use a while loop:

```rust
let mut chunk_iterator = chunker.stream(reader);
while let Some(chunk) = chunk_iterator.read() {
    let chunk = chunk.unwrap();
    match chunk {
        ChunkInput::Data(d) => {
            print!("{:?}, ", d);
        }
        ChunkInput::End => println!(" end of chunk"),
    }
}
```
