use crate::storage::block::Block;
use crate::storage::page::Page;
use std::sync::{Arc, RwLock};
pub struct Buffer {
    page: Page,
    num_of_pin: u32,
    modifited_by: Option<u64>,
}

impl Buffer {
    pub fn new() -> Buffer {
        Buffer {
            num_of_pin: 0,
            page: Page::new(None),
            modifited_by: None,
        }
    }

    pub fn set_page(&mut self, page: Page) {
        self.page = page;
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
}
