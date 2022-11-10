// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use itertools::Itertools;
use smallvec::SmallVec;

use super::*;
use crate::array::{ArrayBuilderImpl, ArrayImpl};
use crate::types::DataValue;
use crate::v1::binder::{AggKind, BoundAggCall};

/// The executor of simple aggregation.
pub struct SimpleAggExecutor {
    pub agg_calls: Vec<BoundAggCall>,
    pub child: BoxedExecutor,
}

impl SimpleAggExecutor {
    fn execute_inner(
        states: &mut [Box<dyn AggregationState>],
        chunk: DataChunk,
        agg_calls: &[BoundAggCall],
    ) -> Result<(), ExecutorError> {
        // TODO: support aggregations with multiple arguments
        let exprs: SmallVec<[ArrayImpl; 16]> = agg_calls
            .iter()
            .map(|agg| agg.args[0].eval(&chunk))
            .try_collect()?;

        for (state, expr) in states.iter_mut().zip_eq(exprs) {
            state.update(&expr)?;
        }

        Ok(())
    }

    fn finish_agg(states: SmallVec<[Box<dyn AggregationState>; 16]>) -> DataChunk {
        states
            .iter()
            .map(|s| {
                let result = &s.output();
                let mut builder = ArrayBuilderImpl::with_capacity(1, &result.data_type());
                builder.push(result);
                builder.finish()
            })
            .collect::<DataChunk>()
    }

    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        let mut states = create_agg_states(&self.agg_calls);

        #[for_await]
        for chunk in self.child {
            let chunk = chunk?;
            Self::execute_inner(&mut states, chunk, &self.agg_calls)?;
        }

        let chunk = Self::finish_agg(states);
        yield chunk;
    }
}

pub(super) fn create_agg_states(
    agg_calls: &[BoundAggCall],
) -> SmallVec<[Box<dyn AggregationState>; 16]> {
    agg_calls.iter().map(create_agg_state).collect()
}

fn create_agg_state(agg_call: &BoundAggCall) -> Box<dyn AggregationState> {
    match agg_call.kind {
        AggKind::RowCount => Box::new(RowCountAggregationState::new(DataValue::Int32(0))),
        AggKind::Count => Box::new(CountAggregationState::new(DataValue::Int32(0))),
        AggKind::Max => Box::new(MinMaxAggregationState::new(
            agg_call.return_type.kind(),
            false,
        )),
        AggKind::Min => Box::new(MinMaxAggregationState::new(
            agg_call.return_type.kind(),
            true,
        )),
        AggKind::Sum => Box::new(SumAggregationState::new(agg_call.return_type.kind())),
        AggKind::First => Box::new(FirstLastAggregationState::new(true)),
        AggKind::Last => Box::new(FirstLastAggregationState::new(false)),
        _ => panic!("Unsupported aggregate kind"),
    }
}
