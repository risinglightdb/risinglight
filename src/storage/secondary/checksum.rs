// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use risinglight_proto::rowset::block_checksum::ChecksumType;

use crate::storage::{StorageResult, TracedStorageError};

pub fn build_checksum(checksum_type: ChecksumType, block_data: &[u8]) -> u64 {
    match checksum_type {
        ChecksumType::None => 0,
        ChecksumType::Crc32 => crc32fast::hash(block_data) as u64,
    }
}

pub fn verify_checksum(
    checksum_type: ChecksumType,
    index_data: &[u8],
    checksum: u64,
) -> StorageResult<()> {
    let chksum = match checksum_type {
        ChecksumType::None => 0,
        ChecksumType::Crc32 => crc32fast::hash(index_data) as u64,
    };
    if chksum != checksum {
        return Err(TracedStorageError::checksum(chksum, checksum));
    }
    Ok(())
}
