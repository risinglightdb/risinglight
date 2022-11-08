// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::*;
use crate::binder_v2::{BoundDrop, Object};
use crate::storage::Storage;

/// The executor of `drop` statement.
pub struct DropExecutor<S: Storage> {
    pub plan: BoundDrop,
    pub storage: Arc<S>,
}

impl<S: Storage> DropExecutor<S> {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        match self.plan.object {
            Object::Table(id) => self.storage.drop_table(id).await?,
        }
        yield DataChunk::single(1);
    }
}
