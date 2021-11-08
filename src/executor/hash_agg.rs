use super::*;
use crate::array::{ArrayBuilderImpl, ArrayImpl};
use crate::binder::{AggKind, BoundAggCall, BoundExpr};
use crate::executor::aggregation::AggregationState;
use crate::types::DataValue;
use std::collections::HashMap;

pub struct HashAggExecutor {
    pub agg_calls: Vec<BoundAggCall>,
    pub group_keys: Vec<BoundExpr>,
    pub child: BoxedExecutor,
}

pub type HashKey = Vec<DataValue>;
pub type HashValue = Vec<Box<dyn AggregationState>>;

#[allow(dead_code)]
impl HashAggExecutor {
    async fn execute_inner(
        chunks: Vec<DataChunk>,
        agg_calls: Vec<BoundAggCall>,
        group_keys: Vec<BoundExpr>,
    ) -> Result<DataChunk, ExecutorError> {
        let mut state_entries = HashMap::<HashKey, HashValue>::new();

        for chunk in chunks {
            // Eval group keys
            let group_cols = group_keys
                .iter()
                .map(|e| e.eval_array(&chunk))
                .collect::<Result<Vec<ArrayImpl>, _>>()?;

            // Collect unique group keys and corresponding visibilities
            let mut unique_keys = Vec::<HashKey>::new();
            let mut key_to_vis_maps = HashMap::new();
            let num_rows = chunk.cardinality();
            for row_idx in 0..num_rows {
                let mut group_key = HashKey::new();
                for col in group_cols.iter() {
                    group_key.push(col.get(row_idx));
                }
                let vis_map = key_to_vis_maps.entry(group_key.clone()).or_insert_with(|| {
                    unique_keys.push(group_key.clone());
                    vec![false; num_rows]
                });
                vis_map[row_idx] = true;
            }

            // Update the state_entries
            for key in unique_keys.iter() {
                if !state_entries.contains_key(key) {
                    let hash_value = agg_calls
                        .iter()
                        .map(|agg| match agg.kind {
                            AggKind::RowCount => Box::<dyn AggregationState>::from(
                                RowCountAggregationState::new(DataValue::Null),
                            ),
                            AggKind::Max => Box::<dyn AggregationState>::from(
                                MinMaxAggregationState::new(agg.return_type.kind(), false),
                            ),
                            AggKind::Min => Box::<dyn AggregationState>::from(
                                MinMaxAggregationState::new(agg.return_type.kind(), true),
                            ),
                            AggKind::Sum => Box::<dyn AggregationState>::from(
                                SumAggregationState::new(agg.return_type.kind()),
                            ),
                            _ => panic!("Unsupported aggregate kind"),
                        })
                        .collect();
                    state_entries.insert(key.to_vec(), hash_value);
                }
                let states = state_entries.get_mut(key).unwrap();
                let vis_map = key_to_vis_maps.remove(key).unwrap();
                for (agg, state) in agg_calls.iter().zip(states.iter_mut()) {
                    // TODO: support aggregations with multiple arguments
                    let array = agg.args[0].eval_array(&chunk)?;
                    state.update(&array, Some(&vis_map))?;
                }
            }
        }

        let mut key_builders = group_keys
            .iter()
            .map(|e| ArrayBuilderImpl::new(e.return_type.as_ref().unwrap()))
            .collect::<Vec<ArrayBuilderImpl>>();
        let mut res_builders = agg_calls
            .iter()
            .map(|agg| ArrayBuilderImpl::new(&agg.return_type))
            .collect::<Vec<ArrayBuilderImpl>>();
        for (key, val) in state_entries.iter() {
            // Push group key
            for (k, builder) in key.iter().zip(key_builders.iter_mut()) {
                builder.push(k);
            }
            // Push aggregate result
            for (state, builder) in val.iter().zip(res_builders.iter_mut()) {
                builder.push(&state.output());
            }
        }
        key_builders.append(&mut res_builders);
        Ok(key_builders
            .into_iter()
            .map(|builder| builder.finish())
            .collect::<DataChunk>())
    }

    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream! {
            let mut chunks: Vec<DataChunk> = vec![];

            for await batch in self.child {
                chunks.push(batch?);
            }

            let chunk = Self::execute_inner(chunks, self.agg_calls, self.group_keys).await?;
            yield chunk;
        }
    }
}
