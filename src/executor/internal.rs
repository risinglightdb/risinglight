// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::array::{ArrayBuilder, ArrayImpl, Utf8ArrayBuilder};

/// The executor of internal tables.
pub struct InternalTableExecutor {
    pub table_name: String,
}

impl InternalTableExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        match self.table_name.as_ref() {
            "contributors" => {
                let mut builder: Utf8ArrayBuilder = Utf8ArrayBuilder::new();
                env!("RISINGLIGHT_CONTRIBUTORS")
                    .split(',')
                    .for_each(|s| builder.push(Some(s)));
                yield [ArrayImpl::Utf8(builder.finish())].into_iter().collect();
            }
            _ => {
                panic!(
                    "InternalTableExecutor::execute: unknown table name: {}",
                    self.table_name
                );
            }
        }
    }
}
