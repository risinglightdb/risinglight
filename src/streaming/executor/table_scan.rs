// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::*;
use crate::catalog::{ColumnRefId, TableRefId};

/// The executor of table scan operation.
pub struct TableScanExecutor {
    pub stream: BoxDiffStream,
    pub columns: Vec<ColumnRefId>,
}

impl TableScanExecutor {
    #[try_stream(boxed, ok = StreamChunk, error = Error)]
    pub async fn execute(self) {
        #[for_await]
        for batch in self.stream {
            todo!()
        }
    }
}
