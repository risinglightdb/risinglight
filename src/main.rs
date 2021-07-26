mod storage;
use crate::storage::block::Block;
use crate::storage::page::Page;

fn main() {
    let mut page = Page::new(Block {
        name: "lightdb.bin".to_string(),
        id: 0,
    });
    page.set_int(10, 20);
}
