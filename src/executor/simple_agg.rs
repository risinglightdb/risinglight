use super::*;
use crate::array::{ArrayBuilderImpl, ArrayImpl};
use crate::binder::{AggKind, BoundAggCall};
use crate::types::{DataType, DataTypeKind, DataValue};
use itertools::Itertools;
use smallvec::SmallVec;

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
            .map(|agg| agg.args[0].eval_array(&chunk))
            .try_collect()?;

        for (state, expr) in states.iter_mut().zip(exprs) {
            state.update(&expr)?;
        }

        Ok(())
    }

    fn finish_agg(states: SmallVec<[Box<dyn AggregationState>; 16]>) -> DataChunk {
        states
            .iter()
            .map(|s| {
                let result = &s.output();
                match &result.data_type() {
                    Some(r) => {
                        let mut builder = ArrayBuilderImpl::new(r);
                        builder.push(result);
                        builder.finish()
                    }
                    None => ArrayBuilderImpl::new(&DataType::new(DataTypeKind::Int(None), true))
                        .finish(),
                }
            })
            .collect::<DataChunk>()
    }

    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream! {
            let mut states = create_agg_states(&self.agg_calls);

            for await chunk in self.child {
                let chunk = chunk?;
                Self::execute_inner(&mut states, chunk, &self.agg_calls)?;
            }

            let chunk = Self::finish_agg(states);
            yield chunk;
        }
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
        AggKind::Max => Box::new(MinMaxAggregationState::new(
            agg_call.return_type.kind(),
            false,
        )),
        AggKind::Min => Box::new(MinMaxAggregationState::new(
            agg_call.return_type.kind(),
            true,
        )),
        AggKind::Sum => Box::new(SumAggregationState::new(agg_call.return_type.kind())),
        _ => panic!("Unsupported aggregate kind"),
    }
}
