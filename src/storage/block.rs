use super::*;
use crate::types::DataType;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
// Block is the basic unit of storage system.
// Each block stores metadata(CRC, offsets), raw data and bitmap.
// TODO: add DeltaStorage to support update and deletion.
#[allow(dead_code)]
pub struct Block {
    inner: Mutex<BlockInner>,
}
#[allow(dead_code)]
struct BlockInner {
    #[allow(dead_code)]
    buffer: [u8; BLOCK_SIZE],
}
// Each block has a BlockHeader which managed by BlockHeaderManager.
#[allow(dead_code)]
pub struct BlockHeader {
    inner: Mutex<BlockHeaderInner>,
}
#[allow(dead_code)]
struct BlockHeaderInner {
    prev_id: Option<BlockId>,
    next_id: Option<BlockId>,
    num_tuples_: usize,
    column_type: DataType,
}

// BlockHeaderMangaer is a global BlockHeader manager.
#[allow(dead_code)]
pub struct BlockHeaderManager {
    inner: BlockHeaderManagerInner,
}
#[allow(dead_code)]
struct BlockHeaderManagerInner {
    #[allow(dead_code)]
    headers: HashMap<BlockId, Arc<BlockHeader>>,
}
