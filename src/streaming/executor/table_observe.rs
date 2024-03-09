// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::table::TableRef;
use super::*;

/// Observes the changes of a table.
pub struct TableObserve {
    pub table: TableRef,
    /// Column indices to use as a projection.
    pub projection: Vec<usize>,
}

impl TableObserve {
    #[try_stream(boxed, ok = StreamChunk, error = Error)]
    pub async fn execute(self) {
        #[for_await]
        for batch in self.table.observe(&self.projection)? {
            todo!()
        }
    }
}
