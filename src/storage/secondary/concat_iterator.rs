use super::{RowSetIterator, SecondaryIteratorImpl};
use crate::storage::StorageChunk;

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
    pub async fn next_batch(&mut self, expected_size: Option<usize>) -> Option<StorageChunk> {
        loop {
            if self.current_iter >= self.iters.len() {
                return None;
            }
            if let Some(chunk) = self.iters[self.current_iter]
                .next_batch(expected_size)
                .await
            {
                return Some(chunk);
            } else {
                self.current_iter += 1;
            }
        }
    }
}

impl SecondaryIteratorImpl for ConcatIterator {}
