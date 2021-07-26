#[derive(PartialEq, Eq, Hash)]
pub struct Block {
    pub name: String,
    pub id: usize,
}

impl Clone for Block {
    fn clone(&self) -> Block {
        Block {
            name: self.name.clone(),
            id: self.id,
        }
    }
}
