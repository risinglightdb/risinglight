// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use crate::types::DataType;

/// Reference to a column in data chunk
#[derive(PartialEq, Clone)]
pub struct BoundInputRef {
    pub index: usize,
    pub return_type: DataType,
}

impl std::fmt::Debug for BoundInputRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.index)
    }
}
