// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::*;
use crate::catalog::TableRefId;
use crate::storage::Storage;

/// The executor of `drop` statement.
pub struct DropExecutor<S: Storage> {
    pub tables: Vec<TableRefId>,
    pub storage: Arc<S>,
}

impl<S: Storage> DropExecutor<S> {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        for table in self.tables {
            self.storage.drop_table(table).await?;
        }
        yield DataChunk::single(1);
    }
}
