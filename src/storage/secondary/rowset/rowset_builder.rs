#![allow(dead_code)]

use crate::array::DataChunk;

/// Builds a Rowset from [`DataChunk`].
pub struct RowsetBuilder {}

impl RowsetBuilder {
    fn append(&mut self, _chunk: DataChunk) {
        todo!()
    }

    fn finish(self) -> Vec<(String, Vec<u8>)> {
        todo!()
    }
}
