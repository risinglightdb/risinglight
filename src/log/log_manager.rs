use crate::log::log_record::{Log, LogType, StartTxnLog};
use crate::storage::block::Block;
use crate::storage::file_manager::FileManager;
use crate::storage::page::{Page, PAGE_SIZE};
use byteorder::{ByteOrder, LittleEndian};
use int_enum::IntEnum;

use std::fs::OpenOptions;
use std::fs::*;
use std::fs::{metadata, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::sync::{Arc, Mutex};

pub struct LogManager {
    current_lsn: u64,
    current_page: Page,
    current_offset: u64,
    file_name: String,
    file_manager: Arc<Mutex<FileManager>>,
}

impl LogManager {
    pub fn new(name: String, file_mgr: Arc<Mutex<FileManager>>) -> LogManager {
        let f = metadata(&name);
        match f {
            Ok(meta) => {
                let mut lsn: u64 = (meta.len() / PAGE_SIZE as u64) - 1;
                let mut page = Page::new(Some(Block {
                    name: name.clone(),
                    id: lsn as usize,
                }));
                file_mgr.lock().as_mut().unwrap().read(&mut page);
                let last_log_end_pos = page.get_u64(0);

                LogManager {
                    file_name: name.clone(),
                    current_lsn: lsn,
                    current_page: page,
                    current_offset: last_log_end_pos + size_of::<u64>() as u64,
                    file_manager: file_mgr.clone(),
                }
            }
            Err(_) => {
                let mut page = Page::new(Some(Block {
                    name: name.clone(),
                    id: 0,
                }));
                page.set_u64(0, 0);
                file_mgr.lock().unwrap().write(&page);

                LogManager {
                    file_name: name.clone(),
                    current_lsn: 0,
                    current_page: page,
                    current_offset: size_of::<u64>() as u64,
                    file_manager: file_mgr.clone(),
                }
            }
        }
    }

    pub fn append(&mut self, log: &dyn Log) {
        let log_data = log.serialize();
        let write_len = (log_data.len() + size_of::<u64>()) as u64;
        if write_len + self.current_offset > PAGE_SIZE as u64 {
            self.flush();
            self.current_lsn += 1;
            let mut page = Page::new(Some(Block {
                name: self.file_name.clone(),
                id: self.current_lsn as usize,
            }));
            page.set_u64(0, 0);
            self.current_offset = size_of::<u64>() as u64;
            self.current_page = page;
        }

        self.current_page
            .write_u8_vec(self.current_offset as u64, &log_data);
        let last_pos = self.current_page.get_int(0);
        self.current_page
            .set_u64(0, self.current_offset + log_data.len() as u64);
        self.current_page
            .set_u64(self.current_offset + log_data.len() as u64, last_pos as u64);
        self.current_offset += write_len;
    }

    pub fn flush(&self) {
        self.file_manager.lock().unwrap().write(&self.current_page);
    }

    pub fn get_offset(&self) -> u64 {
        self.current_offset
    }

    pub fn get_iterator(&self) -> LogIterator {
        self.flush();
        LogIterator::new(
            self.current_page.get_block().unwrap().clone(),
            self.file_manager.clone(),
        )
    }
}

struct LogIterator {
    file_name: String,
    current_lsn: u64,
    current_offset: u64,
    page: Page,
    file_manager: Arc<Mutex<FileManager>>,
}

impl LogIterator {
    pub fn new(block: Block, file_manager: Arc<Mutex<FileManager>>) -> LogIterator {
        let mut page = Page::new(Some(block));
        file_manager.lock().unwrap().read(&mut page);
        let offset = page.get_u64(0);
        LogIterator {
            file_name: page.get_block().unwrap().name.clone(),
            current_lsn: page.get_block().unwrap().id as u64,
            current_offset: offset,
            page: page,
            file_manager: file_manager,
        }
    }

    pub fn has_next(&self) -> bool {
        self.current_offset != 0 || self.current_lsn != 0
    }

    pub fn get_next(&mut self) -> Vec<u8> {
        if self.current_offset == 0 {
            self.move_to_next();
        }
        let end_pos = self.current_offset;
        self.current_offset = self.page.get_u64(self.current_offset);
        let mut vec = Vec::new();
        self.page.read_u8_vec(
            self.current_offset + size_of::<u64>() as u64,
            end_pos,
            &mut vec,
        );
        vec
    }

    fn move_to_next(&mut self) {
        self.current_lsn -= 1;
        self.page = Page::new(Some(Block {
            name: self.file_name.clone(),
            id: self.current_lsn as usize,
        }));
        self.file_manager.lock().unwrap().read(&mut self.page);
        self.current_offset = self.page.get_u64(0);
    }
}

#[cfg(test)]
mod log_manager_tests {
    use super::*;

    #[test]
    fn test_log_read_write() {
        std::fs::remove_file("log.bin");
        let mut mgr = LogManager::new(
            String::from("log.bin"),
            Arc::new(Mutex::new(FileManager::new())),
        );

        for i in 0..10 {
            let log = StartTxnLog::new(i);
            mgr.append(&log);
            assert_eq!(mgr.get_offset(), 8 + 17 * (i + 1))
        }
    }
    #[test]
    fn test_log_files_read_write() {
        std::fs::remove_file("log.bin");
        let mut mgr = LogManager::new(
            String::from("log.bin"),
            Arc::new(Mutex::new(FileManager::new())),
        );

        for i in 0..1000 {
            let log = StartTxnLog::new(i);
            mgr.append(&log);
            assert!(mgr.get_offset() % 17 == 8);
        }
        mgr.flush();
        let mut iter = mgr.get_iterator();
        let mut count = 1000;
        while iter.has_next() {
            let log_bin_data = iter.get_next();
            assert_eq!(log_bin_data.len(), 9);
            let mut dst = [0; 1];
            assert_eq!(log_bin_data[0], LogType::START.int_value());
            LittleEndian::read_u64_into(&log_bin_data[1..], &mut dst);
            assert_eq!(count - 1, dst[0]);
            count -= 1;
        }
    }
}
