use std::collections::HashSet;
use std::sync::Arc;

use itertools::Itertools;

use super::*;
use crate::catalog::{ColumnCatalog, ColumnId, TableCatalog};
use crate::parser::{SetExpr, Statement};
use crate::types::{DataType, DataTypeKind};

/// A bound `INSERT` statement.
#[derive(Debug, PartialEq, Clone)]
pub struct BoundInsert {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub column_types: Vec<DataType>,
    pub values: Vec<Vec<BoundExpr>>,
}

impl Binder {
    pub fn bind_insert(&mut self, stmt: &Statement) -> Result<BoundInsert, BindError> {
        let (table_name, columns, source) = match stmt {
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => (table_name, columns, source),
            _ => panic!("mismatched statement type"),
        };
        let (table_ref_id, table, columns) = self.bind_table_columns(table_name, columns)?;
        let column_ids = columns.iter().map(|col| col.id()).collect_vec();
        let column_types = columns.iter().map(|col| col.datatype()).collect_vec();

        // Check columns after transforming.
        let col_set: HashSet<ColumnId> = column_ids.iter().cloned().collect();
        for (id, col) in table.all_columns() {
            if !col_set.contains(&id) && !col.is_nullable() {
                return Err(BindError::NotNullableColumn(col.name().into()));
            }
        }

        let values = match &source.body {
            SetExpr::Select(_) => todo!("handle 'insert into .. select .. from ..' case."),
            SetExpr::Values(values) => &values.0,
            _ => todo!("handle insert ???"),
        };

        // Handle 'insert into .. values ..' case.

        // Check inserted values, we only support inserting values now.
        let mut bound_values = Vec::with_capacity(values.len());
        for row in values.iter() {
            if row.len() > column_ids.len() {
                return Err(BindError::TupleLengthMismatch {
                    expected: columns.len(),
                    actual: row.len(),
                });
            }
            let mut bound_row = Vec::with_capacity(row.len());
            for (idx, expr) in row.iter().enumerate() {
                // Bind expression
                let expr = self.bind_expr(expr)?;

                if let Some(data_type) = &expr.return_type() {
                    // TODO: support valid type cast
                    // For example:
                    //   CREATE TABLE t (a FLOAT, b FLOAT);
                    //   INSERT INTO VALUES (1, 1);
                    // 1 should be casted to float.
                    let left_kind = data_type.kind();
                    let right_kind = column_types[idx].kind();
                    match (&left_kind, &right_kind) {
                        _ if left_kind == right_kind => {}
                        // For char types, no need to cast
                        (DataTypeKind::Char(_), DataTypeKind::Varchar(_)) => {}
                        (DataTypeKind::Varchar(_), DataTypeKind::Char(_)) => {}
                        _ => todo!("type cast: {} -> {}", left_kind, right_kind),
                    }
                } else {
                    // If the data value is null, the column must be nullable.
                    if !column_types[idx].is_nullable() {
                        return Err(BindError::NullValueInColumn(columns[idx].name().into()));
                    }
                }
                bound_row.push(expr);
            }
            bound_values.push(bound_row);
        }

        Ok(BoundInsert {
            table_ref_id,
            column_ids,
            column_types,
            values: bound_values,
        })
    }

    /// Bind `table_name [ (column_name [, ...] ) ]`
    pub(super) fn bind_table_columns(
        &mut self,
        table_name: &ObjectName,
        columns: &[Ident],
    ) -> Result<(TableRefId, Arc<TableCatalog>, Vec<ColumnCatalog>), BindError> {
        let (schema_name, table_name) = split_name(table_name)?;
        let schema = self
            .catalog
            .get_schema_by_name(schema_name)
            .ok_or_else(|| BindError::SchemaNotFound(schema_name.into()))?;
        let table = schema
            .get_table_by_name(table_name)
            .ok_or_else(|| BindError::TableNotFound(table_name.into()))?;
        let table_ref_id = TableRefId::new(schema.id(), table.id());

        let columns = if columns.is_empty() {
            // If the query does not provide column information, get all columns info.
            table.all_columns().values().cloned().collect_vec()
        } else {
            // Otherwise, we get columns info from the query.
            let mut column_catalogs = vec![];
            for col in columns.iter() {
                let col = table
                    .get_column_by_name(&col.value)
                    .ok_or_else(|| BindError::ColumnNotFound(col.value.clone()))?;
                column_catalogs.push(col);
            }
            column_catalogs
        };
        Ok((table_ref_id, table, columns))
    }
}
