use std::sync::Arc;

use bytes::Buf;
use prost::Message;
use risinglight_proto::rowset::BlockIndex;

use super::SECONDARY_INDEX_MAGIC;

#[derive(Clone)]
pub struct ColumnIndex {
    indexes: Arc<[BlockIndex]>,
}

impl ColumnIndex {
    pub fn indexes(&self) -> &[BlockIndex] {
        &self.indexes
    }

    pub fn from_bytes(data: &[u8]) -> Self {
        // TODO(chi): error handling
        let mut index_data = &data[..data.len() - 16];
        let mut footer = &data[data.len() - 16..];
        assert_eq!(footer.get_u32(), SECONDARY_INDEX_MAGIC);
        let length = footer.get_u64() as usize;
        // TODO: verify checksum

        let mut indexes = vec![];
        for _ in 0..length {
            let index = BlockIndex::decode_length_delimited(&mut index_data).unwrap();
            indexes.push(index);
        }

        Self {
            indexes: indexes.into(),
        }
    }
}
