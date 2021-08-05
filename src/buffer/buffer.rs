use crate::storage::block::Block;
use crate::storage::page::Page;
use std::sync::{Arc, RwLock};

pub struct Buffer {
    page: Page,
    num_of_pin: u32,
    modifited_by: Option<u64>
}

impl Buffer {
    pub fn new() -> Buffer {
        Buffer {
            num_of_pin: 0,
            page: Page::new(None),
            modifited_by: None
        }
    }
    
    pub fn set_int(&mut self,  offset: u64, val: i32, txn_id: u64) {
        self.page.set_int(offset, val);
        self.modifited_by = Some(txn_id);
    }

    pub fn get_int(&self,  offset: u64) -> i32 {
        self.page.get_int(offset)
    }

    pub fn set_string(&mut self, offset: u64, val: String, txn_id: u64) {
        self.page.set_string(offset, val);
        self.modifited_by = Some(txn_id);
    }

    pub fn get_string(&self, offset: u64) -> String {
        self.page.get_string(offset)
    
    }
    pub fn set_page(&mut self, page: Page) {
        self.page = page;
        self.modifited_by = None;
    }

    pub fn get_block(&self) -> Option<Block> {
        self.page.get_block()
    }

    pub fn get_modified_by(&self) -> Option<u64> {
        self.modifited_by
    }

    pub fn get_pin_count(&self) -> u32 {
        self.num_of_pin
    }
    
    pub fn is_pinned(&self) -> bool {
        self.num_of_pin > 0
    }

    pub fn pin(&mut self) {
        self.num_of_pin += 1;
    }

    pub fn unpin(&mut self) {
        self.num_of_pin -= 1;
    }

    pub(in crate::buffer) fn get_page(&self) -> &Page {
        &self.page
    }
}
