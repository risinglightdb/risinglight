// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::{array::ArrayImpl, binder::Udf, types::ConvertError};

/// The executor of (recursive) sql udf
pub struct UdfExecutor {
    pub udf: Udf,
}

impl UdfExecutor {
    pub fn execute(&self, chunk: &DataChunk) -> std::result::Result<ArrayImpl, ConvertError> {
        println!("udf\n{}", chunk);
        Ok(ArrayImpl::new_null((0..1).map(|_| ()).collect()))
    }
}
