// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use crate::array::{Array, ArrayImpl};
use crate::storage::RowHandler;

/// [`RowHandler`] of Secondary is a tuple of rowset id and row id.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct SecondaryRowHandler(pub u32, pub u32);

impl SecondaryRowHandler {
    pub fn rowset_id(&self) -> u32 {
        self.0
    }

    pub fn row_id(&self) -> u32 {
        self.1
    }

    pub fn as_i64(&self) -> i64 {
        (*self).into()
    }
}

impl From<i64> for SecondaryRowHandler {
    fn from(data: i64) -> Self {
        assert!(data >= 0);
        Self((data >> 32) as u32, (data & ((1 << 32) - 1)) as u32)
    }
}

impl From<SecondaryRowHandler> for i64 {
    fn from(handler: SecondaryRowHandler) -> Self {
        ((handler.0 as i64) << 32) | (handler.1 as i64)
    }
}

impl RowHandler for SecondaryRowHandler {
    fn from_column(column: &ArrayImpl, idx: usize) -> Self {
        if let ArrayImpl::Int64(array) = column {
            (*array.get(idx).unwrap()).into()
        } else {
            panic!("invalid column type")
        }
    }
}
