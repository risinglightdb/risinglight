// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::cmp::min;
use std::marker::PhantomData;

use super::BlockIterator;
use crate::array::{Array, ArrayBuilder};

pub struct FakeBlockIterator<A: Array> {
    row_count: usize,
    next_row: usize,
    _phantom: PhantomData<A>,
}

impl<A: Array> FakeBlockIterator<A> {
    pub fn new(row_count: usize) -> Self {
        Self {
            row_count,
            next_row: 0,
            _phantom: PhantomData,
        }
    }
}

impl<A: Array> BlockIterator<A> for FakeBlockIterator<A> {
    fn next_batch(&mut self, expected_size: Option<usize>, builder: &mut A::Builder) -> usize {
        if self.next_row >= self.row_count {
            return 0;
        }
        let mut cnt = self.row_count - self.next_row;
        if let Some(expected_size) = expected_size {
            assert!(expected_size > 0);
            cnt = min(cnt, expected_size);
        }
        for _ in 0..cnt {
            builder.push(None);
        }
        self.next_row += cnt;

        cnt
    }

    fn skip(&mut self, cnt: usize) {
        self.next_row += cnt;
    }

    fn remaining_items(&self) -> usize {
        self.row_count - self.next_row
    }
}
