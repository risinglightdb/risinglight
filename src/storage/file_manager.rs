use crate::storage::block::Block;
use crate::storage::page::Page;
use crate::storage::page::PAGE_SIZE;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fs::OpenOptions;
use std::fs::*;
use std::io::{Read, Seek, SeekFrom, Write};

pub struct FileManager {
    is_new: bool,
}

impl FileManager {
    pub fn new() -> FileManager {
        FileManager { is_new: true }
    }
    pub fn read(&mut self, page: &mut Page) {
        let block = page.get_block();
        let mut file = OpenOptions::new().read(true).open(&block.name).unwrap();

        file.seek(SeekFrom::Start((block.id * PAGE_SIZE).try_into().unwrap()))
            .unwrap();
        file.read(page.get_mut_content()).unwrap();
    }

    pub fn is_new(&self) -> bool {
        self.is_new
    }

    pub fn write(&mut self, page: &Page) {
        let block = page.get_block();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&block.name)
            .unwrap();
        file.seek(SeekFrom::Start((block.id * PAGE_SIZE).try_into().unwrap()))
            .unwrap();
        file.write(page.get_content()).unwrap();
        file.flush().unwrap();
    }
}

#[cfg(test)]
mod file_manager_tests {
    use super::*;

    #[test]
    fn test_file_rw_int() {
        let mut file_mgr = FileManager::new();
        let mut page = Page::new(Block {
            name: "lightdb.bin".to_string(),
            id: 0,
        });
        page.set_int(10, 20);
        file_mgr.write(&page);
        let mut new_page = Page::new(Block {
            name: "lightdb.bin".to_string(),
            id: 0,
        });

        file_mgr.read(&mut new_page);
        let val = new_page.get_int(10);
        assert_eq!(val, 20);
    }

    #[test]
    fn test_file_rw_string() {
        let mut file_mgr = FileManager::new();
        let mut page = Page::new(Block {
            name: "lightdb.bin".to_string(),
            id: 0,
        });
        page.set_string(30, String::from("abcde"));
        file_mgr.write(&page);
        let mut new_page = Page::new(Block {
            name: "lightdb.bin".to_string(),
            id: 0,
        });

        file_mgr.read(&mut new_page);
        let string = new_page.get_string(30);
        assert_eq!(string, String::from("abcde"));
    }
}
