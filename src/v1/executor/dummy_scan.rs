// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;

/// A dummy executor that produces a single value.
pub struct DummyScanExecutor;

impl DummyScanExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {}
}
