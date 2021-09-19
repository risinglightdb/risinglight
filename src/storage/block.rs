use super::*;
use crate::types::DataType;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
// Block is the basic unit of storage system.
// Each block stores metadata(CRC, offsets), raw data and bitmap.
// TODO: add DeltaStorage to support update and deletion.
pub struct Block {
    #[allow(dead_code)]
    inner: Mutex<BlockInner>,
}

struct BlockInner {
    #[allow(dead_code)]
    buffer: [u8; BLOCK_SIZE],
}
// Each block has a BlockHeader which managed by BlockHeaderManager.
pub struct BlockHeader {
    #[allow(dead_code)]
    inner: Mutex<BlockHeaderInner>,
}

struct BlockHeaderInner {
    #[allow(dead_code)]
    prev_id: Option<BlockId>,
    next_id: Option<BlockId>,
    num_tuples_: usize,
    column_type: DataType,
}

// BlockHeaderMangaer is a global BlockHeader manager.
pub struct BlockHeaderManager {
    #[allow(dead_code)]
    inner: BlockHeaderManagerInner,
}

struct BlockHeaderManagerInner {
    #[allow(dead_code)]
    headers: HashMap<BlockId, Arc<BlockHeader>>,
}
