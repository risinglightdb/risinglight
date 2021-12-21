use itertools::Itertools;

use super::*;
use crate::array::{ArrayBuilderImpl, DataChunk};
use crate::catalog::{ColumnId, TableRefId};
use crate::storage::StorageRef;
use crate::types::{DataType, DataValue};

/// The executor of `INSERT` statement.
pub struct InsertExecutor {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub catalog: CatalogRef,
    pub storage: StorageRef,
    pub child: BoxedExecutor,
}

impl Executor for InsertExecutor {
    fn execute(&mut self) -> Result<DataChunk, ExecuteError> {
        let table = self.storage.get_table(self.table_ref_id)?;
        let catalog = self.catalog.get_table(self.table_ref_id).unwrap();
        // Describe each column of the output chunks.
        // example:
        //    columns = [0: Int, 1: Bool, 3: Float, 4: String]
        //    column_ids = [4, 1]
        // => output_columns = [Null(Int), Pick(1), Null(Float), Pick(0)]
        let output_columns = catalog
            .all_columns()
            .values()
            .map(
                |col| match self.column_ids.iter().position(|&id| id == col.id()) {
                    Some(index) => Column::Pick { index },
                    None => Column::Null {
                        type_: col.datatype(),
                    },
                },
            )
            .collect_vec();
        let chunk = self.child.execute()?;
        let count = chunk.cardinality();
        table.append(transform_chunk(chunk, &output_columns))?;
        Ok(DataChunk::single(count as i32))
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
            Column::Pick { index } => chunk.arrays()[*index].clone(),
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
