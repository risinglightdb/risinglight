/// Reference to a column in data chunk
#[derive(PartialEq, Clone)]
pub struct BoundInputRef {
    pub index: usize,
}

impl std::fmt::Debug for BoundInputRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "I{:?}", self.index)
    }
}
