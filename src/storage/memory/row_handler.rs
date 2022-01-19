// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use crate::array::{Array, ArrayImpl};
use crate::storage::RowHandler;

pub struct InMemoryRowHandler(pub u64);

impl RowHandler for InMemoryRowHandler {
    fn from_column(column: &ArrayImpl, idx: usize) -> Self {
        if let ArrayImpl::Int64(array) = column {
            Self(
                *array
                    .get(idx)
                    .expect("RowHandler column should not have null elements")
                    as u64,
            )
        } else {
            panic!("invalid column type")
        }
    }
}
