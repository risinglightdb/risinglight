use super::*;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
pub static DEFAULT_STROAGE_FILE_NAME: &str = "risinglight.db";

// DiskManager is responsible for managing blocks on disk.
// So we don't need use Mutex.
pub struct DiskManager {
    // We hope user don't have a huge SSD, so the id will not overflow lol. :)
    inner: Mutex<DiskManagerInner>,
}

pub struct DiskManagerInner {
    next_block_id: BlockId,
    file: File,
}

// Metablock always starts with Block 0. There will be more than one Metablock.
// TODO: Support erasing blocks.
impl DiskManagerInner {
    fn new(file: File) -> Self {
        DiskManagerInner {
            next_block_id: 0,
            file,
        }
    }
    // Read and Write block will be used by DiskManager in other functions.
    // So we add methods for DiskManagerInner, so DiskManager does not need to grab mutex for twice.
    pub fn read_meta_block(&mut self) {
        self.file.seek(SeekFrom::Start(0)).unwrap();
        let mut bytes: [u8; 4] = [0; 4];
        self.file.read_exact(&mut bytes).unwrap();
        self.next_block_id = u32::from_le_bytes(bytes);
    }

    pub fn write_meta_block(&mut self) {
        self.file.seek(SeekFrom::Start(0)).unwrap();
        self.file
            .write_all(&self.next_block_id.to_le_bytes())
            .unwrap();
    }
}

// We won't use Result in DiskManager, the system cannot run anymore and must crash when there is IO error.
impl DiskManager {
    pub fn create() -> Result<DiskManager, StorageError> {
        let file_res = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(DEFAULT_STROAGE_FILE_NAME);
        match file_res {
            Ok(file) => {
                let mut inner = DiskManagerInner::new(file);
                inner.next_block_id = 1;
                inner.write_meta_block();
                Ok(DiskManager {
                    inner: Mutex::new(inner),
                })
            }
            Err(_) => Err(StorageError::IOError("Unable to open file.")),
        }
    }

    pub fn open() -> Result<DiskManager, StorageError> {
        let file_res = OpenOptions::new()
            .read(true)
            .write(true)
            .open(DEFAULT_STROAGE_FILE_NAME);

        match file_res {
            Ok(file) => {
                let mut inner = DiskManagerInner::new(file);
                inner.read_meta_block();
                Ok(DiskManager {
                    inner: Mutex::new(inner),
                })
            }
            Err(_) => Err(StorageError::IOError("Unable to create file.")),
        }
    }

    pub fn get_next_block_id(&self) -> BlockId {
        let mut inner = self.inner.lock().unwrap();
        let id = inner.next_block_id;
        inner.next_block_id += 1;
        inner.write_meta_block();
        id
    }

    pub fn write_block(&self, block_id: BlockId, block: Arc<Block>) {
        let mut inner = self.inner.lock().unwrap();
        inner
            .file
            .seek(SeekFrom::Start(block_id as u64 * BLOCK_SIZE as u64))
            .unwrap();
        inner
            .file
            .write_all(block.get_inner_mutex().as_ref())
            .unwrap();
    }

    pub fn read_block(&self, block_id: BlockId) -> Arc<Block> {
        let block = Block::new();
        let mut inner = self.inner.lock().unwrap();
        inner
            .file
            .seek(SeekFrom::Start(block_id as u64 * BLOCK_SIZE as u64))
            .unwrap();
        inner
            .file
            .read_exact(block.get_inner_mutex().as_mut())
            .unwrap();
        Arc::new(block)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    #[test]
    fn test_disk_manager() {
        let mgr = DiskManager::create().unwrap();
        assert_eq!(1, mgr.get_next_block_id());
        assert_eq!(2, mgr.get_next_block_id());
        assert_eq!(3, mgr.get_next_block_id());
        let block = Arc::new(Block::new());
        let buf = vec![1; BLOCK_SIZE];
        let mut block_inner = block.get_inner_mutex();
        let mut_ref = block_inner.as_mut();
        mut_ref.clone_from_slice(&buf);
        drop(block_inner);
        mgr.write_block(1, block.clone());
        drop(mgr);

        let mgr2 = DiskManager::open().unwrap();
        assert_eq!(4, mgr2.get_next_block_id());
        let new_block = mgr2.read_block(1);
        let new_block_inner = new_block.get_inner_mutex();
        let new_block_ref = new_block_inner.as_ref();
        assert_eq!(buf, new_block_ref);
    }
}
