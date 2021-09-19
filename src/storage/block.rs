use super::*;
use crate::types::DataType;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
// Block is the basic unit of storage system.
// Each block stores metadata(CRC, offsets), raw data and bitmap.
// TODO: add DeltaStorage to support update and deletion.
pub struct Block {
    inner: Mutex<BlockInner>,
}

struct BlockInner {
    buffer: [u8; BLOCK_SIZE],
}
// Each block has a BlockHeader which managed by BlockHeaderManager.
pub struct BlockHeader {
    inner: Mutex<BlockHeaderInner>,
}

struct BlockHeaderInner {
    prev_id: Option<BlockId>,
    next_id: Option<BlockId>,
    num_tuples_: usize,
    column_type: DataType,
}

// BlockHeaderMangaer is a global BlockHeader manager.
pub struct BlockHeaderManager {
    inner: BlockHeaderManagerInner,
}

struct BlockHeaderManagerInner {
    headers: HashMap<BlockId, Arc<BlockHeader>>,
}
