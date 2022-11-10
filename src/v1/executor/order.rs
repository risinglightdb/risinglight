// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;

use super::*;
use crate::array::{DataChunk, DataChunkBuilder, RowRef};
use crate::types::DataType;
use crate::v1::binder::{BoundExpr, BoundOrderBy};

/// The executor of an order operation.
pub struct OrderExecutor {
    pub child: BoxedExecutor,
    pub comparators: Vec<BoundOrderBy>,
    pub output_types: Vec<DataType>,
}

impl OrderExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        // collect all chunks
        let mut chunks = vec![];
        #[for_await]
        for batch in self.child {
            chunks.push(batch?);
        }
        // sort the indexes
        let mut indexes = gen_index_array(&chunks);
        let comparators = self.comparators;
        indexes.sort_unstable_by(|row1, row2| cmp(row1, row2, &comparators));
        // build chunk by the new order
        let mut builder = DataChunkBuilder::new(self.output_types.iter(), PROCESSING_WINDOW_SIZE);
        for row in indexes {
            if let Some(chunk) = builder.push_row(row.values()) {
                yield chunk;
            }
        }
        if let Some(chunk) = { builder }.take() {
            yield chunk;
        }
    }
}

/// Compare two rows by the comparators.
pub(super) fn cmp(row1: &RowRef, row2: &RowRef, comparators: &[BoundOrderBy]) -> Ordering {
    for cmp in comparators {
        let column_index = match &cmp.expr {
            BoundExpr::InputRef(input_ref) => input_ref.index,
            _ => todo!("only support order by columns now"),
        };
        let v1 = row1.get(column_index);
        let v2 = row2.get(column_index);
        match v1.partial_cmp(&v2).unwrap() {
            Ordering::Equal => continue,
            o if cmp.descending => return o.reverse(),
            o => return o,
        }
    }
    Ordering::Equal
}

/// Generate an array of indexes for each element of the chunks.
fn gen_index_array(chunks: &[DataChunk]) -> Vec<RowRef<'_>> {
    chunks.iter().flat_map(|chunk| chunk.rows()).collect()
}
