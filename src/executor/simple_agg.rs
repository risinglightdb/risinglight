use super::*;
use crate::array::{ArrayBuilderImpl, ArrayImpl};
use crate::binder::{AggKind, BoundAggCall};
use crate::types::{DataType, DataTypeKind, DataValue};

/// The executor of simple aggregation.
pub struct SimpleAggExecutor {
    pub agg_calls: Vec<BoundAggCall>,
    pub child: BoxedExecutor,
}

impl SimpleAggExecutor {
    async fn execute_inner(
        chunks: Vec<DataChunk>,
        agg_calls: Vec<BoundAggCall>,
    ) -> Result<DataChunk, ExecutorError> {
        // TODO: support aggregations with multiple arguments
        let mut states = create_agg_states(&agg_calls);

        for chunk in chunks {
            let exprs = agg_calls
                .iter()
                .map(|agg| agg.args[0].eval_array(&chunk))
                .collect::<Result<Vec<ArrayImpl>, _>>()?;

            for (state, expr) in states.iter_mut().zip(exprs) {
                state.update(&expr, None)?;
            }
        }

        Ok(states
            .iter()
            .map(|s| {
                let result = &s.output();
                match &result.data_type() {
                    Some(r) => {
                        let mut builder = ArrayBuilderImpl::new(r);
                        builder.push(result);
                        builder.finish()
                    }
                    None => ArrayBuilderImpl::new(&DataType::new(DataTypeKind::Int, true)).finish(),
                }
            })
            .collect::<DataChunk>())
    }

    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream! {
            let mut chunks: Vec<DataChunk> = vec![];
            for await batch in self.child {
                chunks.push(batch?);
            }

            let chunk = Self::execute_inner(chunks, self.agg_calls).await?;
            yield chunk;
        }
    }
}

pub(super) fn create_agg_states(agg_calls: &[BoundAggCall]) -> Vec<Box<dyn AggregationState>> {
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
