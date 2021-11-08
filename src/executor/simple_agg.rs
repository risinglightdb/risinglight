use super::*;
use crate::array::{ArrayBuilderImpl, ArrayImpl};
use crate::binder::{AggKind, BoundAggCall};
use crate::types::{DataType, DataTypeKind, DataValue};

pub struct SimpleAggExecutor {
    pub agg_calls: Vec<BoundAggCall>,
    pub child: BoxedExecutor,
}

#[allow(dead_code)]
impl SimpleAggExecutor {
    async fn execute_inner(
        chunks: Vec<DataChunk>,
        agg_calls: Vec<BoundAggCall>,
    ) -> Result<DataChunk, ExecutorError> {
        // TODO: support aggregations with multiple arguments
        let mut states: Vec<Box<dyn AggregationState>> = agg_calls
            .iter()
            .map(|agg| match agg.kind {
                AggKind::RowCount => Box::<dyn AggregationState>::from(
                    RowCountAggregationState::new(DataValue::Int32(0)),
                ),
                AggKind::Max => Box::<dyn AggregationState>::from(MinMaxAggregationState::new(
                    agg.return_type.kind(),
                    false,
                )),
                AggKind::Min => Box::<dyn AggregationState>::from(MinMaxAggregationState::new(
                    agg.return_type.kind(),
                    true,
                )),
                AggKind::Sum => Box::<dyn AggregationState>::from(SumAggregationState::new(
                    agg.return_type.kind(),
                )),
                _ => panic!("Unsupported aggregate kind"),
            })
            .collect();

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
