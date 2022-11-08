use std::cmp::min;

use futures::Future;

use super::ColumnIterator;
use crate::array::{ArrayBuilder, I64Array, I64ArrayBuilder};
use crate::storage::secondary::SecondaryRowHandler;
use crate::storage::StorageResult;

pub struct RowHandlerColumnIterator {
    rowset_id: usize,
    row_count: usize,
    current_row_id: usize,
}

impl RowHandlerColumnIterator {
    pub fn new(rowset_id: usize, row_count: usize, first_row: usize) -> Self {
        Self {
            rowset_id,
            row_count,
            current_row_id: first_row,
        }
    }
}

impl ColumnIterator<I64Array> for RowHandlerColumnIterator {
    type NextFuture<'a> = impl Future<Output = StorageResult<Option<(u32, I64Array)>>> + 'a;

    fn next_batch(&mut self, expected_size: Option<usize>) -> Self::NextFuture<'_> {
        async move {
            if self.current_row_id >= self.row_count {
                return Ok(None);
            }

            let mut remaining_cnt = self.row_count - self.current_row_id;
            if let Some(expected_size) = expected_size {
                assert!(expected_size > 0);
                remaining_cnt = min(remaining_cnt, expected_size);
            }

            let first_row_id = self.current_row_id as u32;

            let mut builder = I64ArrayBuilder::with_capacity(remaining_cnt);
            for row_id in self.current_row_id..(self.current_row_id + remaining_cnt) {
                let item = SecondaryRowHandler(self.rowset_id as u32, row_id as u32).as_i64();
                builder.push(Some(&item));
            }
            let batch = builder.finish();

            self.current_row_id += remaining_cnt;
            Ok(Some((first_row_id, batch)))
        }
    }

    fn fetch_hint(&self) -> (usize, bool) {
        let cnt = self.row_count - self.current_row_id;
        (cnt, cnt == 0)
    }

    fn fetch_current_row_id(&self) -> u32 {
        self.current_row_id as u32
    }

    fn skip(&mut self, cnt: usize) {
        self.current_row_id += cnt
    }
}
