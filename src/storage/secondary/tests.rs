use super::SecondaryIteratorImpl;
use crate::storage::StorageChunk;

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
    pub async fn next_batch(&mut self, _expected_size: Option<usize>) -> Option<StorageChunk> {
        if self.cnt >= self.chunks.len() {
            return None;
        }
        let chunk = self.chunks[self.cnt].clone();
        self.cnt += 1;
        Some(chunk)
    }
}

impl SecondaryIteratorImpl for TestIterator {}
