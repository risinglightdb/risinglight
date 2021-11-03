use bytes::{Buf, BufMut, Bytes};
use risinglight_proto::rowset::block_checksum::ChecksumType;
use risinglight_proto::rowset::block_index::BlockType;

/// A block is simply a bytes array.
pub type Block = Bytes;

/// A key in block cache contains `rowset_id`, `column_id`
/// and `block_id`.
#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub struct BlockCacheKey {
    pub rowset_id: u32,
    pub storage_column_id: u32,
    pub block_id: u32,
}

impl BlockCacheKey {
    pub fn block(mut self, block_id: u32) -> Self {
        self.block_id = block_id;
        self
    }

    pub fn column(mut self, storage_column_id: u32) -> Self {
        self.storage_column_id = storage_column_id;
        self
    }

    pub fn rowset(mut self, rowset_id: u32) -> Self {
        self.rowset_id = rowset_id;
        self
    }
}

#[derive(Default, Debug, Clone)]
pub struct BlockHeader {
    pub block_type: BlockType,
    pub checksum_type: ChecksumType,
    pub checksum: u64,
}

pub const BLOCK_HEADER_SIZE: usize = 4 + 4 + 8;

impl BlockHeader {
    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_i32(self.block_type.into());
        buf.put_i32(self.checksum_type.into());
        buf.put_u64(self.checksum);
    }

    pub fn decode(&mut self, buf: &mut impl Buf) {
        self.block_type = BlockType::from_i32(buf.get_i32()).unwrap();
        self.checksum_type = ChecksumType::from_i32(buf.get_i32()).unwrap();
        self.checksum = buf.get_u64();
    }
}
