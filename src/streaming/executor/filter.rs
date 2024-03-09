// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use arrow::array::AsArray;

use super::*;

/// Filters rows from its input that do not match an expression.
pub struct Filter {
    /// The predicate expression.
    pub predicate: ExpressionRef,
}

impl Filter {
    #[try_stream(boxed, ok = DeltaBatch, error = Error)]
    pub async fn execute(self, input: DeltaBatchStream) {
        #[for_await]
        for batch in input {
            let batch = batch?;
            let predicate = self.predicate.eval(batch.data())?;
            let predicate = predicate.column(0).as_boolean();
            if predicate.true_count() != 0 {
                yield batch.filter(predicate)?;
            }
        }
    }
}
