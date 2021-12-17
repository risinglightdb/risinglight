use risinglight_proto::rowset::block_checksum::ChecksumType;

pub fn build_checksum(checksum_type: ChecksumType, block_data: &[u8]) -> u64 {
    match checksum_type {
        ChecksumType::None => 0,
        ChecksumType::Crc32 => crc32fast::hash(block_data) as u64,
    }
}

pub fn verify_checksum(checksum_type: ChecksumType, index_data: &[u8], checksum: u64) {
    match checksum_type {
        ChecksumType::None => {}
        ChecksumType::Crc32 => {
            let chksum = crc32fast::hash(index_data);
            assert_eq!(chksum as u64, checksum);
        }
    }
}
