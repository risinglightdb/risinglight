use super::*;
use crate::types::DataType;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
// Block is the basic unit of storage system.
// Each block stores metadata(CRC, offsets), raw data and bitmap.
// TODO: add DeltaStorage to support update and deletion.
#[allow(dead_code)]
pub struct Block {
    inner: Mutex<BlockInner>,
}

impl Block {
    pub fn new() -> Block {
        Block {
            inner: Mutex::new(BlockInner::new()),
        }
    }

    pub fn get_inner_mutex(&self) -> MutexGuard<BlockInner> {
        self.inner.lock().unwrap()
    }
}

impl Default for Block {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
pub struct BlockInner {
    #[allow(dead_code)]
    buffer: Vec<u8>,
}
impl Default for BlockInner {
    fn default() -> Self {
        Self::new()
    }
}
impl BlockInner {
    pub fn new() -> Self {
        BlockInner {
            buffer: vec![0; BLOCK_SIZE],
        }
    }

    pub fn get_buffer_ref(&self) -> &[u8] {
        &self.buffer
    }

    pub fn get_buffer_mut(&mut self) -> &mut [u8] {
        &mut self.buffer
    }
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
