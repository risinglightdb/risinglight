// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::cmp::min;

use super::BlockIterator;
use crate::array::{ArrayBuilder, I64Array, I64ArrayBuilder};
use crate::storage::secondary::SecondaryRowHandler;

pub struct RowHandlerBlockIterator {
    rowset_id: usize,
    row_count: usize,
    next_row: usize,
}

impl RowHandlerBlockIterator {
    pub fn new(rowset_id: usize, row_count: usize) -> Self {
        Self {
            rowset_id,
            row_count,
            next_row: 0,
        }
    }
}

impl BlockIterator<I64Array> for RowHandlerBlockIterator {
    fn next_batch(&mut self, expected_size: Option<usize>, builder: &mut I64ArrayBuilder) -> usize {
        if self.next_row >= self.row_count {
            return 0;
        }

        let mut remaining_cnt = self.row_count - self.next_row;
        if let Some(expected_size) = expected_size {
            assert!(expected_size > 0);
            remaining_cnt = min(remaining_cnt, expected_size);
        }

        for row_id in self.next_row..(self.next_row + remaining_cnt) {
            let item = SecondaryRowHandler(self.rowset_id as u32, row_id as u32).as_i64();
            builder.push(Some(&item));
        }

        self.next_row += remaining_cnt;

        remaining_cnt
    }

    fn skip(&mut self, cnt: usize) {
        self.next_row += cnt;
    }

    fn remaining_items(&self) -> usize {
        self.row_count - self.next_row
    }
}
