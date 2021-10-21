//! Rowset encoding and decoding implementation of RisingLight.
//!
//! Rowset is the minimum managing unit of RisingLight's secondary storage engine.
//! A table contains multiple Rowsets on disk. Rowset stores [`DataChunk`]s,
//! where each [`Array`] is stored in one or two column files along with an index,
//! Data are sorted by sort key.
//!
//! For example, `create table t1 (v1 primary int not null, v2 int)` will generate the
//! following Rowset directory structure:
//!
//! ```plain
//! 03_03         directory name = <TableId>_<RowsetId>
//! |- MANIFEST   manifest file, which contains column descriptions of the current Rowset
//! |- 00.col     (generally should be) start timestamp
//! |- 00.idx     normal index for timestamps, which stores RowId -> Block mapping
//! |- 01.col     data for v1
//! |- 01.sort    sort index for v1, which stores RowId + Key -> Block mapping
//! |- 02.col     data for v2
//! |- 02.idx     normal index for v2, which stores RowId -> Block mapping
//! |- 02b.col    null bitmap for v2
//! \- 02b.idx    normal index for v2's null bitmap, which stores RowId -> Block mapping
//! ```
//!
//! Data flushed to directory will be immutable, and the directory content will remain
//! unchanged throughout the whole process. Delete vectors for all Rowsets will be
//! stored in a separate directory.
//!
//! Each index, sorted index and column data file has a pre-defined encoding scheme, and is
//! managed on the granularity of block, which might be about 4KB in size. [`BlockBuilder`]s
//! could freely encode the data as they prefer inside each block.
//!
//! There are a lot of [`BlockBuilder`]s and [`ColumnBuilder`]s in Secondary. For each
//! encoding scheme, the the following structures should be implemented in pairs:
//!
//! * `RunLengthIntBlockBuilder` - `RunLengthIntBlock` - `RunLengthIntBlockIterator` - an entry in proto
//! * `IntColumnBuilder` - `IntColumn` - `IntColumnIterator` - an entry in proto

use risinglight_proto::rowset::BlockIndex;

use crate::array::Array;

mod index_builder;

mod primitive_block_builder;
use primitive_block_builder::*;

mod primitive_column_builder;
use primitive_column_builder::*;

mod column_builder;
use column_builder::*;

mod rowset_builder;

mod mem_rowset;
pub use mem_rowset::*;

/// Builds a column. [`ColumnBuilder`] will automatically chunk [`Array`] into
/// blocks, calls [`BlockBuilder`] to generate a block, and builds index for a
/// column. Note that one [`Array`] might require multiple [`ColumnBuilder`] to build.
///
/// * For nullable columns, there will be a bitmap file built with [`BitmapColumnBuilder`].
/// * And for concrete data, there will be another column builder with concrete block builder.
///
/// After a single column has been built, an index file will also be generated with [`IndexBuilder`].
pub trait ColumnBuilder<A: Array> {
    /// Append an [`Array`] to the column. [`ColumnBuilder`] will automatically chunk it into
    /// small parts.
    fn append(&mut self, array: &A);

    /// Finish a column, return block index information and encoded block data
    fn finish(self) -> (Vec<BlockIndex>, Vec<u8>);
}

/// Builds a block. All builders should implement the trait, while
/// ensuring that the format follows the block encoding scheme.
///
/// In RisingLight, the block encoding scheme is as follows:
///
/// ```plain
/// | block_type | cksum_type | cksum  |    data     |
/// |    4B      |     4B     |   8B   |  variable   |
/// ```
pub trait BlockBuilder<A: Array> {
    /// Append one data into the block.
    fn append(&mut self, item: &A::Item);

    /// Get estimated size of block. Will be useful on runlength or compression encoding.
    fn estimated_size(&self) -> usize;

    /// Check if we should finish the current block. If there is no item in the current
    /// builder, this function must return `true`.
    fn should_finish(&self, next_item: &A::Item) -> bool;

    /// Finish a block and return encoded data.
    fn finish(self) -> Vec<u8>;
}

/// Iterates on a block
pub trait BlockIterator<A: Array> {}

/// Iteratos on a column
pub trait ColumnIterator<A: Array> {}
