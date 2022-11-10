// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

use binary_heap_plus::BinaryHeap;

use super::*;
use crate::array::{DataChunk, DataChunkBuilder};
use crate::types::{DataType, Row};

/// The executor of a Top N operation.
pub struct TopNExecutor {
    pub offset: usize,
    pub limit: usize,
    /// A list of expressions to order by.
    ///
    /// e.g. `(list (asc (+ #0 #1)) (desc #0))`
    pub order_keys: RecExpr,
    pub types: Vec<DataType>,
}

impl TopNExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self, child: BoxedExecutor) {
        // initialize heap
        let heap_size = self.offset + self.limit;
        let orders = Evaluator::new(&self.order_keys).orders();
        let mut heap =
            BinaryHeap::with_capacity_by(heap_size, |row1, row2| cmp(row1, row2, &orders));

        // evaluate order keys and append the original rows
        // chunks = keys || child
        #[for_await]
        for chunk in child {
            let chunk = chunk?;
            let order_key_chunk = Evaluator::new(&self.order_keys).eval_list(&chunk)?;
            for row in order_key_chunk.row_concat(chunk).rows() {
                heap.push(row.to_owned());
                if heap.len() > heap_size {
                    heap.pop();
                }
            }
        }

        // build chunk
        let order_keys_len = self.order_keys.as_ref().last().unwrap().as_list().len();
        let mut builder = DataChunkBuilder::new(self.types.iter(), PROCESSING_WINDOW_SIZE);
        for row in heap
            .into_sorted_vec()
            .into_iter()
            .skip(self.offset)
            .take(self.limit)
        {
            if let Some(chunk) = builder.push_row(row.into_iter().skip(order_keys_len)) {
                yield chunk;
            }
        }
        if let Some(chunk) = builder.take() {
            yield chunk;
        }
    }
}

/// Compare two rows by orders.
///
/// The order is `false` for ascending and `true` for descending.
fn cmp(row1: &Row, row2: &Row, orders: &[bool]) -> Ordering {
    for ((v1, v2), desc) in row1.iter().zip_eq(row2.iter()).zip(orders) {
        match v1.cmp(v2) {
            Ordering::Equal => continue,
            o if *desc => return o.reverse(),
            o => return o,
        }
    }
    Ordering::Equal
}
