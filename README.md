[![Build Status](https://travis-ci.org/remram44/cdchunking-rs.svg?branch=master)](https://travis-ci.org/remram44/cdchunking-rs/builds)
[![Crates.io](https://img.shields.io/crates/v/cdchunking.svg)](https://crates.io/crates/cdchunking)

Content-Defined Chunking
========================

This crates provides a way to device a stream of bytes into chunks, using methods that choose the splitting point from the content itself. This means that adding or removing a few bytes in the stream would only change the chunks directly modified. This is different from just splitting every n bytes, because in that case every chunk is different unless the number of bytes changed is a multiple of n.

Content-defined chunking is useful for data de-duplication. It is used in many backup software, and by the rsync data synchronization tool.

Status
------

This crate is very much work-in-progress. I am trying to figure out the best interface for content-defined chunking, exposing both easy-to-use methods and efficient zero-allocation methods.

Using this crate
----------------

It is probably that I will only put the base logic for chunking in this crate, leaving the rolling-hash algorithms for other crates to implement.
