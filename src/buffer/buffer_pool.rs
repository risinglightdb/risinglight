use crate::buffer::buffer::Buffer;
use crate::storage::block::Block;
use crate::storage::file_manager::FileManager;
use crate::storage::page::Page;
use std::sync::{Arc, Mutex, RwLock};

pub trait BufferPool {
    fn new(num: u32, file_mgr: Arc<Mutex<FileManager>>) -> Self;
    fn pin(&mut self, blk: Block) -> Option<Arc<RwLock<Buffer>>>;
    fn unpin(&mut self, buf: Arc<RwLock<Buffer>>);
    fn get_available(&self) -> u32;
    fn flush_all(&self, txn_id: u64);
    fn pin_new(&mut self, file_name: &String) -> Option<Arc<RwLock<Buffer>>>;
}

//TODO: implement a LRU buffer pool
pub struct NaiveBufferPool {
    num_of_buffer: u32,
    buffers: Vec<Arc<RwLock<Buffer>>>,
    available: u32,
    file_mgr: Arc<Mutex<FileManager>>,
}

impl BufferPool for NaiveBufferPool {
    fn new(num: u32, file_mgr: Arc<Mutex<FileManager>>) -> NaiveBufferPool {
        let mut vec: Vec<Arc<RwLock<Buffer>>> = Vec::new();
        for _i in 0..num {
            vec.push(Arc::new(RwLock::new(Buffer::new())));
        }
        NaiveBufferPool {
            num_of_buffer: num,
            buffers: vec,
            available: num,
            file_mgr: file_mgr,
        }
    }

    fn pin_new(&mut self, file_name: &String) -> Option<Arc<RwLock<Buffer>>> {
        match self.find_unpinned_buffer() {
            Some(buf) => {
                let blk = self.file_mgr.lock().unwrap().append(file_name);
                let mut page = Page::new(Some(blk));
                self.file_mgr.lock().unwrap().read(&mut page);
                buf.write().unwrap().set_page(page);
                self.available -= 1;
                buf.write().unwrap().pin();
                return Some(buf.clone());
            }
            None => return None,
        }
        None
    }

    fn pin(&mut self, blk: Block) -> Option<Arc<RwLock<Buffer>>> {
        let buf_opt = self.find_existing_buffer(&blk);
        match buf_opt {
            Some(buf) => {
                if !buf.read().unwrap().is_pinned() {
                    self.available -= 1;
                }
                buf.write().unwrap().pin();
                return Some(buf);
            }
            None => match self.find_unpinned_buffer() {
                Some(buf) => {
                    let mut page = Page::new(Some(blk));
                    self.file_mgr.lock().unwrap().read(&mut page);
                    buf.write().unwrap().set_page(page);
                    if !buf.write().unwrap().is_pinned() {
                        self.available -= 1;
                    }
                    buf.write().unwrap().pin();
                    return Some(buf.clone());
                }
                None => return None,
            },
        }
    }

    fn unpin(&mut self, buf: Arc<RwLock<Buffer>>) {
        buf.write().unwrap().unpin();
        if !buf.read().unwrap().is_pinned() {
            self.available += 1;
        }
    }

    fn get_available(&self) -> u32 {
        self.available
    }

    fn flush_all(&self, txn_id: u64) {}
}

impl NaiveBufferPool {
    fn find_unpinned_buffer(&self) -> Option<Arc<RwLock<Buffer>>> {
        for (i, buf) in self.buffers.iter().enumerate() {
            if !self.buffers[i].read().unwrap().is_pinned() {
                return Some(self.buffers[i].clone());
            }
        }
        None
    }

    fn find_existing_buffer(&self, blk: &Block) -> Option<Arc<RwLock<Buffer>>> {
        for buf in self.buffers.iter() {
            if let Some(block) = buf.read().unwrap().get_block() {
                if block == *blk {
                    return Some(buf.clone());
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod buffer_pool_test {
    use super::*;

    #[test]
    fn test_space_count() {
        let file_mgr = Arc::new(Mutex::new(FileManager::new()));
        let file_name = String::from("lightdb.bin");
        for i in 0..10 {
            file_mgr.lock().unwrap().append(&file_name);
        }
        let mut buffer_pool = NaiveBufferPool::new(3, file_mgr.clone());

        let buf0 = buffer_pool
            .pin(Block {
                name: String::from("lightdb.bin"),
                id: 0,
            })
            .unwrap();

        assert_eq!(buffer_pool.get_available(), 2);

        let buf0_another = buffer_pool
            .pin(Block {
                name: String::from("lightdb.bin"),
                id: 0,
            })
            .unwrap();

        assert_eq!(buffer_pool.get_available(), 2);

        let buf1 = buffer_pool
            .pin(Block {
                name: String::from("lightdb.bin"),
                id: 1,
            })
            .unwrap();

        assert_eq!(buf1.read().unwrap().get_pin_count(), 1);

        assert_eq!(buffer_pool.get_available(), 1);

        assert_eq!(buf0.read().unwrap().get_pin_count(), 2);

        buffer_pool.unpin(buf0);

        assert_eq!(buffer_pool.get_available(), 1);

        assert_eq!(buf0_another.read().unwrap().get_pin_count(), 1);

        let buf2 = buffer_pool
            .pin(Block {
                name: String::from("lightdb.bin"),
                id: 2,
            })
            .unwrap();

        assert_eq!(buf2.read().unwrap().get_pin_count(), 1);

        assert_eq!(buffer_pool.get_available(), 0);

        assert_eq!(buf0_another.read().unwrap().get_pin_count(), 1);

        let buf3 = buffer_pool.pin(Block {
            name: String::from("lightdb.bin"),
            id: 3
        });
        if let Some(buf) = buf3 {
            panic!();
        }

        buffer_pool.unpin(buf2);
        buffer_pool.unpin(buf1);
        buffer_pool.unpin(buf0_another);
        assert_eq!(buffer_pool.get_available(), 3);
    }
}
