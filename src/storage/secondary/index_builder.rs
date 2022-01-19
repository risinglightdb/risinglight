// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use bytes::BufMut;
use prost::Message;
use risinglight_proto::rowset::block_checksum::ChecksumType;
use risinglight_proto::rowset::BlockIndex;

use super::build_checksum;

pub const SECONDARY_INDEX_MAGIC: u32 = 0x2333;
pub const INDEX_FOOTER_SIZE: usize = 4 + 8 + 4 + 8;

/// Builds index file for a column.
///
/// Currently, Secondary uses a very simple index format. `.idx` file is
/// simply a sequence of protubuf [`BlockIndex`] message. When a developer
/// needs to read a column, they will need to read them to memory at once.
/// The last 24 bytes of the index file is the checksum.
///
/// ```plain
/// | index | index | index | index | ... | magic number (4B) | block count (8B) | checksum type (4B) | checksum (8B) |
pub struct IndexBuilder {
    data: Vec<u8>,
    cnt: usize,
    checksum_type: ChecksumType,
}
impl IndexBuilder {
    pub fn new(checksum_type: ChecksumType, _target_entries: usize) -> Self {
        Self {
            data: vec![],
            cnt: 0,
            checksum_type,
        }
    }

    pub fn append(&mut self, index: BlockIndex) {
        self.cnt += 1;
        index.encode_length_delimited(&mut self.data).unwrap()
    }

    pub fn finish(self) -> Vec<u8> {
        let mut data = self.data;
        let checksum = build_checksum(self.checksum_type, data.as_slice());

        data.put_u32(SECONDARY_INDEX_MAGIC);

        data.put_u64(self.cnt as u64);

        data.put_i32(self.checksum_type.into());
        data.put_u64(checksum);

        data
    }
}
