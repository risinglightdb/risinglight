// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::*;
use crate::catalog::{RootCatalogRef, TableRefId};
use crate::storage::Storage;

/// The executor of `drop` statement.
pub struct DropExecutor<S: Storage> {
    pub tables: Vec<TableRefId>,
    pub catalog: RootCatalogRef,
    pub storage: Arc<S>,
}

impl<S: Storage> DropExecutor<S> {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        for table in self.tables {
            if self.catalog.get_table(&table).unwrap().is_view() {
                self.catalog.drop_table(table);
            } else {
                self.storage.drop_table(table).await?;
            }
        }
        yield DataChunk::single(1);
    }
}
