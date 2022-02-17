use bytes::{Buf, Bytes};

use super::Block;

pub fn decode_rle_block(data: Block) -> (usize, Block, Block) {
    let mut buffer = &data[..];
    let rle_num = buffer.get_u32_le() as usize;
    let rle_length = std::mem::size_of::<u32>() + std::mem::size_of::<u16>() * rle_num;
    let rle_data = data[std::mem::size_of::<u32>()..rle_length].to_vec();
    let block_data = data[rle_length..].to_vec();
    (rle_num, Bytes::from(rle_data), Bytes::from(block_data))
}
