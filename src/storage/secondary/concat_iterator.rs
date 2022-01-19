// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::{RowSetIterator, SecondaryIteratorImpl};
use crate::storage::{StorageChunk, StorageResult};

/// [`ConcatIterator`] concats data from `RowSet`s and yields data
/// from them one by one. This iterator should only be used on
/// non-overlapping `RowSet`s.
pub struct ConcatIterator {
    iters: Vec<RowSetIterator>,
    current_iter: usize,
}

impl ConcatIterator {
    pub fn new(iters: Vec<RowSetIterator>) -> Self {
        Self {
            iters,
            current_iter: 0,
        }
    }

    /// Get a batch from [`ConcatIterator`]. It is possible that less than `expected_size`
    /// rows are returned, as we fetch a batch from the RowSet boundaries.
    pub async fn next_batch(
        &mut self,
        expected_size: Option<usize>,
    ) -> StorageResult<Option<StorageChunk>> {
        loop {
            if self.current_iter >= self.iters.len() {
                return Ok(None);
            }
            if let Some(chunk) = self.iters[self.current_iter]
                .next_batch(expected_size)
                .await?
            {
                return Ok(Some(chunk));
            } else {
                self.current_iter += 1;
            }
        }
    }
}

impl SecondaryIteratorImpl for ConcatIterator {}
