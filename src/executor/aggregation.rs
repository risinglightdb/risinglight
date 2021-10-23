use super::*;
use crate::array::{Array, ArrayBuilderImpl, ArrayImpl};
use crate::binder::{AggKind, BoundExpr};
use crate::types::{DataTypeKind, DataValue};

pub struct SimpleAggExecutor {
    pub agg_kind: Vec<AggKind>,
    pub aggregation_expressions: Vec<BoundExpr>,
    pub child: BoxedExecutor,
}

impl SimpleAggExecutor {
    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream! {
            let mut cardinality = 0;
            let mut states = self
                .aggregation_expressions
                .iter()
                .map(|e| SumAggregationState::new(e.return_type.clone().unwrap().kind()))
                .collect::<Vec<_>>();

            // Update states and cardinality in batch
            for await batch in self.child {
                let batch = batch?;
                cardinality += batch.cardinality();
                let exprs = self
                    .aggregation_expressions
                    .iter()
                    .map(|e| e.eval_array(&batch))
                    .collect::<Result<Vec<ArrayImpl>, _>>()?;

                for (idx, agg_kind) in self.agg_kind.iter().enumerate() {
                    match agg_kind {
                        AggKind::Sum => {
                            states[idx].update(&exprs[idx])?;
                        }
                    }
                }
            }

            // Output sum result
            let arrays = states
                .iter()
                .map(|s| {
                    let result = &s.output();
                    let mut builder = ArrayBuilderImpl::new(result.data_type().unwrap());
                    builder.push(result);
                    builder.finish()
                })
                .collect::<Vec<ArrayImpl>>();
            yield DataChunk::builder()
                    .cardinality(cardinality)
                    .arrays(arrays.into())
                    .build()
        }
    }
}

pub trait AggregationState {
    fn update(&mut self, array: &ArrayImpl) -> Result<(), ExecutorError>;

    fn output(&self) -> DataValue;
}

pub struct SumAggregationState {
    result: DataValue,
    input_datatype: DataTypeKind,
}

#[allow(dead_code)]
impl SumAggregationState {
    pub fn new(input_datatype: DataTypeKind) -> Box<Self> {
        Box::new(Self {
            result: DataValue::Null,
            input_datatype,
        })
    }
}

macro_rules! sum_func_gen {
    ($fn_name: ident, $input: ty, $result: ty) => {
        fn $fn_name(result: Option<$result>, input: Option<&$input>) -> Option<$result> {
            match (result, input) {
                (_, None) => result,
                (None, Some(i)) => Some(<$result>::from(*i)),
                (Some(r), Some(i)) => Some(r + <$result>::from(*i)),
            }
        }
    };
}

sum_func_gen!(sum_i32, i32, i32);

impl AggregationState for SumAggregationState {
    fn update(&mut self, array: &ArrayImpl) -> Result<(), ExecutorError> {
        match (array, &self.input_datatype) {
            (ArrayImpl::Int32(arr), DataTypeKind::Int) => {
                let mut temp: Option<i32> = None;
                temp = arr.iter().fold(temp, sum_i32);
                match temp {
                    None => self.result = DataValue::Null,
                    Some(val) => self.result = DataValue::Int32(val),
                }
            }
            _ => todo!("Support more types for aggregation."),
        }
        Ok(())
    }

    fn output(&self) -> DataValue {
        self.result.clone()
    }
}

/// TODO: remove the tests after supporting end-2-end queries.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::{BoundColumnRef, BoundExpr, BoundExprKind};
    use crate::catalog::{ColumnCatalog, ColumnRefId, TableRefId};
    use crate::executor::CreateTableExecutor;
    use crate::executor::{GlobalEnv, GlobalEnvRef};
    use crate::physical_planner::PhysicalCreateTable;
    use crate::physical_planner::{PhysicalInsert, PhysicalSeqScan};
    use crate::storage::InMemoryStorage;
    use crate::types::{DataType, DataTypeExt, DataValue};
    use futures::TryStreamExt;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_aggr() {
        let env = create_and_insert().await;
        let physical_seq_scan = PhysicalSeqScan {
            table_ref_id: TableRefId {
                database_id: 0,
                schema_id: 0,
                table_id: 0,
            },
            column_ids: vec![0, 1],
        };
        let column0 = BoundExpr {
            kind: BoundExprKind::ColumnRef(BoundColumnRef {
                table_name: "t".into(),
                column_ref_id: ColumnRefId::new(0, 0, 0, 0),
                column_index: 0,
            }),
            return_type: Some(DataType::new(DataTypeKind::Int, false)),
        };
        let column1 = BoundExpr {
            kind: BoundExprKind::ColumnRef(BoundColumnRef {
                table_name: "t".into(),
                column_ref_id: ColumnRefId::new(0, 0, 0, 1),
                column_index: 1,
            }),
            return_type: Some(DataType::new(DataTypeKind::Int, false)),
        };

        // Sum single column: select sum(a) from t
        let executor = SimpleAggExecutor {
            agg_kind: vec![AggKind::Sum],
            aggregation_expressions: vec![column0.clone()],
            child: SeqScanExecutor {
                plan: physical_seq_scan.clone(),
                storage: env.storage.as_in_memory_storage(),
            }
            .execute()
            .boxed(),
        }
        .execute()
        .boxed();
        let output: Vec<DataChunk> = executor.try_collect().await.unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].array_at(0).len(), 1);
        assert_eq!(output[0].array_at(0).get_to_string(0), "10");

        // Two column sums: select sum(a), sum(b) from t
        let executor = SimpleAggExecutor {
            agg_kind: vec![AggKind::Sum, AggKind::Sum],
            aggregation_expressions: vec![column0.clone(), column1.clone()],
            child: SeqScanExecutor {
                plan: physical_seq_scan.clone(),
                storage: env.storage.as_in_memory_storage(),
            }
            .execute()
            .boxed(),
        }
        .execute()
        .boxed();
        let output: Vec<DataChunk> = executor.try_collect().await.unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].array_at(0).len(), 1);
        assert_eq!(output[0].array_at(0).get_to_string(0), "10");
        assert_eq!(output[0].array_at(1).len(), 1);
        assert_eq!(output[0].array_at(1).get_to_string(0), "100");
    }

    async fn create_and_insert() -> GlobalEnvRef {
        let env = Arc::new(GlobalEnv {
            storage: StorageImpl::InMemoryStorage(Arc::new(InMemoryStorage::new())),
        });
        let plan = PhysicalCreateTable {
            database_id: 0,
            schema_id: 0,
            table_name: "t".into(),
            columns: vec![
                ColumnCatalog::new(0, "v1".into(), DataTypeKind::Int.not_null().to_column()),
                ColumnCatalog::new(1, "v2".into(), DataTypeKind::Int.not_null().to_column()),
            ],
        };
        let mut executor = CreateTableExecutor {
            plan,
            storage: env.storage.as_in_memory_storage(),
        }
        .execute()
        .boxed();
        executor.next().await.unwrap().unwrap();

        let values = [[1, 10], [2, 20], [3, 30], [4, 40]];
        let values = values
            .iter()
            .map(|row| {
                row.iter()
                    .map(|&v| BoundExpr::constant(DataValue::Int32(v)))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let plan = PhysicalInsert {
            table_ref_id: TableRefId {
                database_id: 0,
                schema_id: 0,
                table_id: 0,
            },
            column_ids: vec![0, 1],
            values,
        };
        let mut executor = InsertExecutor {
            plan,
            storage: env.storage.as_in_memory_storage(),
        }
        .execute()
        .boxed();
        executor.next().await.unwrap().unwrap();
        env
    }

    #[test]
    fn test_sum() {
        let mut state = SumAggregationState::new(DataTypeKind::Int);
        let array = ArrayImpl::Int32((1..5).collect());
        state.update(&array).unwrap();
        assert_eq!(state.output(), DataValue::Int32(10));
    }
}
