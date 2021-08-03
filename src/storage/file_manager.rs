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

    pub fn append(&self, filename: &String) -> Block {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(filename)
            .unwrap();
        let metadata = file.metadata().unwrap();
        let length = metadata.len();
        let id = length / PAGE_SIZE as u64;
        let buf: [u8; PAGE_SIZE] = [0; PAGE_SIZE];

        file.seek(SeekFrom::Start(
            (id as usize * PAGE_SIZE).try_into().unwrap(),
        ));
        file.write(&buf).unwrap();
        file.sync_all().unwrap();
        Block {
            name: filename.clone(),
            id: id as usize,
        }
    }
}

#[cfg(test)]
mod file_manager_tests {
    use super::*;

    #[test]
    fn test_file_rw_int() {
        std::fs::remove_file("light.bin");
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
        std::fs::remove_file("light.bin");
        let mut file_mgr = FileManager::new();
        let mut page = Page::new(Block {
            name: "lightdb.bin".to_string(),
            id: 1,
        });
        page.set_string(30, String::from("abcde"));
        file_mgr.write(&page);
        let mut new_page = Page::new(Block {
            name: "lightdb.bin".to_string(),
            id: 1,
        });

        file_mgr.read(&mut new_page);
        let string = new_page.get_string(30);
        assert_eq!(string, String::from("abcde"));
    }

    #[test]
    fn test_file_append() {
        std::fs::remove_file("light.bin");
        let mut file_mgr = FileManager::new();
        let b1 = file_mgr.append(&String::from("lightdb.bin"));
        let b2 = file_mgr.append(&String::from("lightdb.bin"));
        assert_eq!(b1.id + 1, b2.id);
    }
}
