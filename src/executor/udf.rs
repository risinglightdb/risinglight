// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::catalog::RootCatalogRef;

/// The executor of (recursive) sql udf
pub struct UdfExecutor {
    pub catalog: RootCatalogRef,
}

impl UdfExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        todo!()
    }
}
