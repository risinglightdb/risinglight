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
//! 03_03               directory name = <TableId>_<RowsetId>
//! |- 03_03_MANIFEST   manifest file, which contains column descriptions of the current Rowset
//! |- 03_03_01.col     data for v1
//! |- 03_03_01.sort    sort index for v1, which stores RowId + Key -> Block mapping
//! |- 03_03_02.col     data for v2
//! |- 03_03_02.null    null bitmap for v2
//! \- 03_03_02.idx     normal index for v2, which stores RowId -> Block mapping
//! ```
//!
//! Each index, sorted index and column data file has a pre-defined encoding scheme, and is
//! managed on the granularity of block, which might be about 4KB in size. [`BlockBuilder`]s
//! could freely encoding the data as they prefer inside each block.
//!
//! There are a lot of [`BlockBuilder`]s and [`ColumnBuilder`]s in Secondary. For each
//! encoding scheme, the the following structures should be implemented in pairs:
//!
//! * `RunLengthIntBlockBuilder` - `RunLengthIntBlockIterator`
//! * `IntColumnBuilder` - `IntColumnIterator`

use std::ops::Range;

use smallvec::SmallVec;

use crate::array::{Array, ArrayImpl, DataChunk};
use crate::storage::StorageError;

type Result<T> = std::result::Result<T, StorageError>;

pub enum ScalarImpl {
    Bool(bool),
    Int32(i32),
    Float64(f64),
    UTF8(String),
}

pub type Row = SmallVec<[ScalarImpl; 20]>;

/// Builds a Rowset from [`DataChunk`].
pub struct RowsetBuilder {}

impl RowsetBuilder {
    fn append(&mut self, chunk: DataChunk) -> Result<()> {
        todo!()
    }

    fn finish(self) -> Vec<(String, Vec<u8>)> {
        todo!()
    }
}

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
    fn append(&mut self, array: A) -> Result<()>;

    /// Finish a column, return block index information and encoded block data
    fn finish(self) -> (Vec<Index>, Vec<u8>);
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
    fn append(&mut self, item: &A::Item) -> Result<()>;
    fn estimated_size(&self) -> usize;
    fn should_finish(&self, next_item: &A::Item, target_size: usize) -> bool;
    fn finish(self) -> Vec<u8>;
}

/// Iterates
pub trait BlockIterator<A: Array> {}

pub trait ColumnIterator<A: Array> {}

pub struct RowsetIterator {}

/// Builds index file for a column.
pub struct IndexBuilder {}

pub struct I32BlockBuilder {
    data: Vec<u8>,
}
