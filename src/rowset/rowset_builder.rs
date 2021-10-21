use super::Result;
use crate::array::DataChunk;

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
