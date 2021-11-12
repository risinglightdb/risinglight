use super::*;
use crate::array::{ArrayBuilderImpl, DataChunk};
use crate::binder::{BoundExprKind, BoundOrderBy};
use crate::types::DataValue;
use std::cmp::Ordering;

/// The executor of an order operation.
pub struct OrderExecutor {
    pub child: BoxedExecutor,
    pub comparators: Vec<BoundOrderBy>,
}

impl OrderExecutor {
    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream! {
            // collect all chunks
            let mut chunks = vec![];
            for await batch in self.child {
                chunks.push(batch?);
            }
            // sort the indexes
            let mut indexes = gen_index_array(&chunks);
            let comparators = self.comparators;
            indexes.sort_unstable_by(|row1, row2| {
                row1.cmp_by(row2, &comparators)
            });
            // build chunk by the new order
            let mut arrays = vec![];
            for col_idx in 0..chunks[0].column_count() {
                let mut builder = ArrayBuilderImpl::from_type_of_array(chunks[0].array_at(col_idx));
                for row in indexes.iter() {
                    builder.push(&row.get(col_idx));
                }
                arrays.push(builder.finish());
            }
            let chunk: DataChunk = arrays.into_iter().collect();
            yield chunk;
        }
    }
}

/// Reference to a row in DataChunk.
struct RowRef<'a> {
    chunk: &'a DataChunk,
    row_idx: usize,
}

impl RowRef<'_> {
    /// Compare with another row by the comparators.
    fn cmp_by(&self, other: &RowRef, comparators: &[BoundOrderBy]) -> Ordering {
        for cmp in comparators {
            let column_index = match &cmp.expr.kind {
                BoundExprKind::InputRef(input_ref) => input_ref.index,
                _ => todo!("only support order by columns now"),
            };
            let v1 = self.get(column_index);
            let v2 = other.get(column_index);
            match v1.partial_cmp(&v2).unwrap() {
                Ordering::Equal => continue,
                o if cmp.descending => return o.reverse(),
                o => return o,
            }
        }
        Ordering::Equal
    }

    /// Get the value at given column index.
    fn get(&self, idx: usize) -> DataValue {
        self.chunk.array_at(idx).get(self.row_idx)
    }
}

/// Generate an array of indexes for each element of the chunks.
fn gen_index_array(chunks: &[DataChunk]) -> Vec<RowRef<'_>> {
    chunks
        .iter()
        .flat_map(|chunk| {
            (0..chunk.cardinality()).map(move |idx| RowRef {
                chunk,
                row_idx: idx,
            })
        })
        .collect()
}
