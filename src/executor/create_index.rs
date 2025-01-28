// Copyright 2025 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::*;
use crate::binder::CreateIndex;
use crate::storage::Storage;

/// The executor of `create index` statement.
pub struct CreateIndexExecutor<S: Storage> {
    pub index: Box<CreateIndex>,
    pub storage: Arc<S>,
}

impl<S: Storage> CreateIndexExecutor<S> {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        self.storage
            .create_index(
                self.index.schema_id,
                &self.index.index_name,
                self.index.table_id,
                &self.index.columns,
                &self.index.index_type,
            )
            .await?;

        yield DataChunk::single(1);
    }
}
