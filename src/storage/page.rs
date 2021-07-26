use crate::storage::block::Block;

const PAGE_SIZE: usize = 4096;
pub struct Page {
  content: [u8; PAGE_SIZE],
  block: Block
}

impl Page {
  pub fn new(blk : Block) -> Page {
    Page{
      content: [0; 4096],
      block: blk
    }
  }
  pub fn set_int(&mut self) {
     println!("Hello !");
   } 
}