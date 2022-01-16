use std::sync::Arc;

use itertools::Itertools;

use super::*;
use crate::array::{ArrayBuilderImpl, DataChunk};
use crate::catalog::TableRefId;
use crate::storage::{Storage, Table, Transaction};
use crate::types::{ColumnId, DataType, DataValue};

/// The executor of `insert` statement.
pub struct InsertExecutor<S: Storage> {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub storage: Arc<S>,
    pub child: BoxedExecutor,
}

impl<S: Storage> InsertExecutor<S> {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        let table = self.storage.get_table(self.table_ref_id)?;
        let columns = table.columns()?;
        // Describe each column of the output chunks.
        // example:
        //    columns = [0: Int, 1: Bool, 3: Float, 4: String]
        //    column_ids = [4, 1]
        // => output_columns = [Null(Int), Pick(1), Null(Float), Pick(0)]
        let output_columns = columns
            .iter()
            .map(
                |col| match self.column_ids.iter().position(|&id| id == col.id()) {
                    Some(index) => Column::Pick { index },
                    None => Column::Null {
                        type_: col.datatype(),
                    },
                },
            )
            .collect_vec();

        let mut txn = table.write().await?;
        let mut cnt = 0;
        #[for_await]
        for chunk in self.child {
            let chunk = transform_chunk(chunk?, &output_columns);
            cnt += chunk.cardinality();
            txn.append(chunk).await?;
        }
        txn.commit().await?;

        yield DataChunk::single(cnt as i32);
    }
}

enum Column {
    /// Pick the column at `index` from child.
    Pick { index: usize },
    /// Null values with `type`.
    Null { type_: DataType },
}

fn transform_chunk(chunk: DataChunk, output_columns: &[Column]) -> DataChunk {
    output_columns
        .iter()
        .map(|col| match col {
            Column::Pick { index } => chunk.array_at(*index).clone(),
            Column::Null { type_ } => {
                let mut builder = ArrayBuilderImpl::with_capacity(chunk.cardinality(), type_);
                for _ in 0..chunk.cardinality() {
                    builder.push(&DataValue::Null);
                }
                builder.finish()
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::array::{ArrayImpl, DataChunk};
    use crate::catalog::{ColumnCatalog, TableRefId};
    use crate::executor::{CreateTableExecutor, GlobalEnv, GlobalEnvRef};
    use crate::optimizer::plan_nodes::PhysicalCreateTable;
    use crate::storage::InMemoryStorage;
    use crate::types::{DataTypeExt, DataTypeKind};

    #[tokio::test]
    async fn simple() {
        let env = create_table().await;
        let executor = InsertExecutor {
            table_ref_id: TableRefId::new(0, 0, 0),
            column_ids: vec![0, 1],
            storage: env.storage.as_in_memory_storage(),
            child: async_stream::try_stream! {
                yield [
                    ArrayImpl::Int32((0..4).collect()),
                    ArrayImpl::Int32((100..104).collect()),
                ]
                .into_iter()
                .collect::<DataChunk>();
            }
            .boxed(),
        };
        executor.execute().next().await.unwrap().unwrap();
    }

    async fn create_table() -> GlobalEnvRef {
        let env = Arc::new(GlobalEnv {
            storage: StorageImpl::InMemoryStorage(Arc::new(InMemoryStorage::new())),
        });
        let plan = PhysicalCreateTable::new(LogicalCreateTable::new(
            0,
            0,
            "t".into(),
            vec![
                ColumnCatalog::new(
                    0,
                    "v1".into(),
                    DataTypeKind::Int(None).not_null().to_column(),
                ),
                ColumnCatalog::new(
                    1,
                    "v2".into(),
                    DataTypeKind::Int(None).not_null().to_column(),
                ),
            ],
        ));
        let mut executor = CreateTableExecutor {
            plan,
            storage: env.storage.as_in_memory_storage(),
        }
        .execute()
        .boxed();
        executor.next().await.unwrap().unwrap();
        env
    }
}
