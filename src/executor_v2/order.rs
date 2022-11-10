// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

use super::*;
use crate::array::{DataChunk, DataChunkBuilder, RowRef};
use crate::types::DataType;

/// The executor of an order operation.
pub struct OrderExecutor {
    /// A list of expressions to order by.
    ///
    /// e.g. `(list (asc (+ #0 #1)) (desc #0))`
    pub order_keys: RecExpr,
    pub types: Vec<DataType>,
}

impl OrderExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self, child: BoxedExecutor) {
        // evaluate order keys and append the original rows
        // chunks = keys || child
        let mut chunks = vec![];
        #[for_await]
        for chunk in child {
            let chunk = chunk?;
            let order_key_chunk = Evaluator::new(&self.order_keys).eval_list(&chunk)?;
            chunks.push(order_key_chunk.row_concat(chunk));
        }

        // sort the rows by keys
        let mut rows = gen_row_array(&chunks);
        let orders = Evaluator::new(&self.order_keys).orders();
        rows.sort_unstable_by(|row1, row2| cmp(row1, row2, &orders));

        // build chunk by the new order
        let order_keys_len = self.order_keys.as_ref().last().unwrap().as_list().len();
        let mut builder = DataChunkBuilder::new(&self.types, PROCESSING_WINDOW_SIZE);
        for row in rows {
            if let Some(chunk) = builder.push_row(row.values().skip(order_keys_len)) {
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
fn cmp(row1: &RowRef, row2: &RowRef, orders: &[bool]) -> Ordering {
    for ((v1, v2), desc) in row1.values().zip_eq(row2.values()).zip(orders) {
        match v1.cmp(&v2) {
            Ordering::Equal => continue,
            o if *desc => return o.reverse(),
            o => return o,
        }
    }
    Ordering::Equal
}

/// Generate an array of rows for the chunks.
fn gen_row_array(chunks: &[DataChunk]) -> Vec<RowRef<'_>> {
    chunks.iter().flat_map(|chunk| chunk.rows()).collect()
}
