// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

//! Rowset encoding and decoding implementation of RisingLight.
//!
//! Rowset is the minimum managing unit of RisingLight's secondary storage engine.
//! A table contains multiple Rowsets on disk. Rowset stores `DataChunk`s,
//! where each `Array` is stored in one or two column files along with an index,
//! Data are sorted by sort key.
//!
//! For example, `create table t1 (v1 primary int not null, v2 int)` will generate the
//! following Rowset directory structure:
//!
//! ```plain
//! 03_03         directory name = <TableId>_<RowsetId>
//! |- MANIFEST   manifest file, which contains column descriptions of the current Rowset
//! |- 01.col     data for v1
//! |- 01.sort    sort index for v1, which stores RowId + Key -> Block mapping
//! |- 02.col     data for v2
//! \- 02.idx     normal index for v2, which stores RowId -> Block mapping
//! ```
//!
//! Data flushed to directory will be immutable, and the directory content will remain
//! unchanged throughout the whole process. Delete vectors for all Rowsets will be
//! stored in a separate directory.
//!
//! Each index, sorted index and column data file has a pre-defined encoding scheme, and is
//! managed on the granularity of block, which might be about 4KB in size. Block builders
//! could freely encode the data as they prefer inside each block.
//!
//! There are a lot of block builders and column builders in Secondary. For each
//! encoding scheme, the the following structures should be implemented in pairs:
//!
//! * `RunLengthIntBlockBuilder` - `RunLengthIntBlock` - `RunLengthIntBlockIterator` - an entry in
//!   proto
//! * `IntColumnBuilder` - `IntColumn` - `IntColumnIterator` - an entry in proto

pub use disk_rowset::*;
pub use encoded::*;
pub use mem_rowset::*;
pub use rowset_builder::*;
pub use rowset_iterator::*;
pub use rowset_writer::*;

mod disk_rowset;
mod encoded;
mod mem_rowset;
mod rowset_builder;
mod rowset_iterator;
mod rowset_writer;
