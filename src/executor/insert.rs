use super::*;
use crate::array::{ArrayBuilderImpl, ArrayImpl, DataChunk};
use crate::physical_planner::PhysicalInsert;
use crate::storage::{Storage, Table, Transaction};
use std::sync::Arc;

/// The executor of `insert` statement.
pub struct InsertExecutor<S: Storage> {
    pub plan: PhysicalInsert,
    pub storage: Arc<S>,
}

impl<S: Storage> InsertExecutor<S> {
    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream! {
            let cardinality = self.plan.values.len();
            assert!(cardinality > 0);

            let table = self.storage.get_table(self.plan.table_ref_id)?;
            let columns = table.column_descs(&self.plan.column_ids)?;
            let mut array_builders = columns
                .iter()
                .map(|col| ArrayBuilderImpl::new(col.datatype()))
                .collect::<Vec<ArrayBuilderImpl>>();
            for row in &self.plan.values {
                for (expr, builder) in row.iter().zip(&mut array_builders) {
                    let value = expr.eval();
                    builder.push(&value);
                }
            }
            let arrays = array_builders
                .into_iter()
                .map(|builder| builder.finish())
                .collect::<Vec<ArrayImpl>>();
            let chunk = DataChunk::builder()
                .cardinality(cardinality)
                .arrays(arrays.into())
                .build();
            let mut txn = table.write().await?;
            txn.append(chunk).await?;
            txn.commit().await?;

            yield DataChunk::builder().cardinality(1).build();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::BoundExpr;
    use crate::catalog::{ColumnCatalog, TableRefId};
    use crate::executor::CreateTableExecutor;
    use crate::executor::{GlobalEnv, GlobalEnvRef};
    use crate::physical_planner::PhysicalCreateTable;
    use crate::storage::InMemoryStorage;
    use crate::types::{DataTypeExt, DataTypeKind, DataValue};
    use std::sync::Arc;

    #[test]
    fn simple() {
        let env = create_table();
        let values = [[0, 100], [1, 101], [2, 102], [3, 103]];
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
        futures::executor::block_on(executor.next())
            .unwrap()
            .unwrap();
    }

    fn create_table() -> GlobalEnvRef {
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
        futures::executor::block_on(executor.next())
            .unwrap()
            .unwrap();
        env
    }
}
