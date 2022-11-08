// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use itertools::Itertools;

use super::*;
use crate::array::{ArrayBuilderImpl, ArrayImpl};
use crate::types::DataValue;

/// The executor of simple aggregation.
pub struct SimpleAggExecutor {
    /// A list of aggregations.
    ///
    /// e.g. `(list (sum #0) (count #1))`
    pub aggs: RecExpr,
}

impl SimpleAggExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self, child: BoxedExecutor) {
        let len = self.aggs.as_ref().last().unwrap().as_list().len();
        let mut states = vec![DataValue::Null; len];
        #[for_await]
        for chunk in child {
            let chunk = chunk?;
            ExprRef::new(&self.aggs).eval_agg_list(&mut states, &chunk)?;
        }
        yield states.iter().map(ArrayImpl::from).collect();
    }
}
