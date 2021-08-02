use crate::log::log_record::Log;
use crate::storage::block::Block;
use crate::storage::file_manager::FileManager;
use crate::storage::page::{Page, PAGE_SIZE};
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
                let mut lsn: u64 = meta.len() / PAGE_SIZE as u64;
                let mut page = Page::new(Block {
                    name: name.clone(),
                    id: lsn as usize,
                });
                file_mgr.lock().as_mut().unwrap().read(&mut page);
                File::create(&name).unwrap();
                let last_log_end_pos = page.get_u64(0);

                LogManager {
                    file_name: name.clone(),
                    current_lsn: lsn,
                    current_page: page,
                    current_offset: last_log_end_pos + size_of::<i32>() as u64,
                    file_manager: file_mgr.clone(),
                }
            }
            Err(_) => {
                let mut page = Page::new(Block {
                    name: name.clone(),
                    id: 0,
                });
                page.set_u64(0, 0);
                file_mgr.lock().unwrap().write(&page);

                LogManager {
                    file_name: name.clone(),
                    current_lsn: 0,
                    current_page: page,
                    current_offset: size_of::<i32>() as u64,
                    file_manager: file_mgr.clone(),
                }
            }
        }
    }

    pub fn append(&mut self, log: &dyn Log) {
        let log_data = log.serialize();
        let write_len = (log_data.len() + size_of::<i32>()) as u64;
        if write_len + self.current_offset > PAGE_SIZE as u64 {
            self.flush();
            self.current_lsn += 1;
            let mut page = Page::new(Block {
                name: self.file_name.clone(),
                id: self.current_lsn as usize,
            });
            page.set_u64(0, 0);
            self.current_offset = size_of::<i32>() as u64;
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
}
