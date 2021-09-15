use super::ExecutorError;
use crate::array::{ArrayBuilderImpl, ArrayImpl, DataChunk};
use crate::physical_plan::InsertPhysicalPlan;
use crate::storage::StorageRef;

pub struct InsertExecutor {
    pub plan: InsertPhysicalPlan,
    pub storage: StorageRef,
}

impl InsertExecutor {
    pub async fn execute(self) -> Result<(), ExecutorError> {
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
        table.append(chunk)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, TableRefId};
    use crate::executor::CreateTableExecutor;
    use crate::parser::Expression;
    use crate::physical_plan::CreateTablePhysicalPlan;
    use crate::server::{GlobalEnv, GlobalEnvRef};
    use crate::storage::InMemoryStorage;
    use crate::types::{DataTypeKind, DataValue};
    use std::sync::Arc;

    #[test]
    fn simple() {
        let env = create_table();
        let values = [[0, 100], [1, 101], [2, 102], [3, 103]];
        let values = values
            .iter()
            .map(|row| {
                row.iter()
                    .map(|&v| Expression::constant(DataValue::Int32(v)))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let plan = InsertPhysicalPlan {
            table_ref_id: TableRefId {
                database_id: 0,
                schema_id: 0,
                table_id: 0,
            },
            column_ids: vec![0, 1],
            values,
        };
        let executor = InsertExecutor {
            plan,
            storage: env.storage.clone(),
        };
        futures::executor::block_on(executor.execute()).unwrap();
    }

    fn create_table() -> GlobalEnvRef {
        let env = Arc::new(GlobalEnv {
            storage: Arc::new(InMemoryStorage::new()),
        });
        let plan = CreateTablePhysicalPlan {
            database_id: 0,
            schema_id: 0,
            table_name: "t".into(),
            column_descs: vec![
                ColumnCatalog::new(0, "v1".into(), DataTypeKind::Int32.not_null().to_column()),
                ColumnCatalog::new(1, "v2".into(), DataTypeKind::Int32.not_null().to_column()),
            ],
        };
        let executor = CreateTableExecutor {
            plan,
            env: env.clone(),
        };
        futures::executor::block_on(executor.execute()).unwrap();
        env
    }
}
