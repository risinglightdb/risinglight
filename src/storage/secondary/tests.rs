// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::SecondaryIteratorImpl;
use crate::storage::{StorageChunk, StorageResult};

pub struct TestIterator {
    chunks: Vec<StorageChunk>,
    cnt: usize,
}

impl TestIterator {
    pub fn new(chunks: Vec<StorageChunk>) -> Self {
        Self { chunks, cnt: 0 }
    }
}

impl TestIterator {
    pub async fn next_batch(
        &mut self,
        _expected_size: Option<usize>,
    ) -> StorageResult<Option<StorageChunk>> {
        if self.cnt >= self.chunks.len() {
            return Ok(None);
        }
        let chunk = self.chunks[self.cnt].clone();
        self.cnt += 1;
        Ok(Some(chunk))
    }
}

impl SecondaryIteratorImpl for TestIterator {}
