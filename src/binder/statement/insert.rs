// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use itertools::Itertools;

use super::*;
use crate::catalog::{ColumnCatalog, TableCatalog};
use crate::parser::{SetExpr, Statement};
use crate::types::{ColumnId, DataType};

/// A bound `insert` statement.
#[derive(Debug, PartialEq, Clone)]
pub struct BoundInsert {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub column_types: Vec<DataType>,
    pub values: Vec<Vec<BoundExpr>>,
}

impl Binder {
    pub fn bind_insert(&mut self, stmt: &Statement) -> Result<BoundInsert, BindError> {
        match stmt {
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => {
                let (table_ref_id, table, columns) =
                    self.bind_table_columns(table_name, columns)?;
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
                let mut bound_values = vec![];
                bound_values.reserve(values.len());
                for row in values.iter() {
                    if row.len() > column_ids.len() {
                        return Err(BindError::InvalidExpression(format!(
                            "Column length mismatched. Expected: {}, Actual: {}",
                            columns.len(),
                            row.len()
                        )));
                    }
                    let mut bound_row = vec![];
                    bound_row.reserve(row.len());
                    for (idx, expr) in row.iter().enumerate() {
                        // Bind expression
                        let mut expr = self.bind_expr(expr)?;

                        if let Some(data_type) = &expr.return_type() {
                            // table t1(a float, b float)
                            // for example: insert into values (1, 1);
                            // 1 should be casted to float.
                            let left_kind = data_type.physical_kind();
                            let right_kind = column_types[idx].physical_kind();
                            if left_kind != right_kind {
                                expr = BoundExpr::TypeCast(BoundTypeCast {
                                    expr: Box::new(expr),
                                    ty: column_types[idx].kind(),
                                });
                            }
                        } else {
                            // If the data value is null, the column must be nullable.
                            if !column_types[idx].is_nullable() {
                                return Err(BindError::InvalidExpression(
                                    "Can not insert null to non null column".into(),
                                ));
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
            _ => panic!("mismatched statement type"),
        }
    }

    /// Bind `table_name [ (column_name [, ...] ) ]`
    pub(super) fn bind_table_columns(
        &mut self,
        table_name: &ObjectName,
        columns: &[Ident],
    ) -> Result<(TableRefId, Arc<TableCatalog>, Vec<ColumnCatalog>), BindError> {
        let table_name = &lower_case_name(table_name);
        let (database_name, schema_name, table_name) = split_name(table_name)?;
        let table = self
            .catalog
            .get_database_by_name(database_name)
            .ok_or_else(|| BindError::InvalidDatabase(database_name.into()))?
            .get_schema_by_name(schema_name)
            .ok_or_else(|| BindError::InvalidSchema(schema_name.into()))?
            .get_table_by_name(table_name)
            .ok_or_else(|| BindError::InvalidTable(table_name.into()))?;

        let table_ref_id = self
            .catalog
            .get_table_id_by_name(database_name, schema_name, &table.name())
            .unwrap();

        let columns = if columns.is_empty() {
            // If the query does not provide column information, get all columns info.
            table.all_columns().values().cloned().collect_vec()
        } else {
            // Otherwise, we get columns info from the query.
            let mut column_catalogs = vec![];
            for col in columns.iter() {
                let col = Ident::new(col.value.to_lowercase());
                let col = table
                    .get_column_by_name(&col.value)
                    .ok_or_else(|| BindError::InvalidColumn(col.value.clone()))?;
                column_catalogs.push(col);
            }
            column_catalogs
        };
        Ok((table_ref_id, table, columns))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::catalog::{ColumnCatalog, RootCatalog};
    use crate::parser::parse;
    use crate::types::{DataTypeExt, DataTypeKind};

    #[test]
    fn bind_insert() {
        let catalog = Arc::new(RootCatalog::new());
        let mut binder = Binder::new(catalog.clone());

        let database = catalog.get_database_by_id(0).unwrap();
        let schema = database.get_schema_by_id(0).unwrap();
        schema
            .add_table(
                "t".into(),
                vec![
                    ColumnCatalog::new(
                        0,
                        "a".into(),
                        DataTypeKind::Int(None).not_null().to_column(),
                    ),
                    ColumnCatalog::new(
                        1,
                        "b".into(),
                        DataTypeKind::Int(None).not_null().to_column(),
                    ),
                ],
                false,
            )
            .unwrap();

        let sql = "
            insert into t values (1, 1);
            insert into t (a) values (1); 
            insert into t values (1);";
        let stmts = parse(sql).unwrap();

        binder.bind_insert(&stmts[0]).unwrap();
        assert!(matches!(
            binder.bind_insert(&stmts[1]),
            Err(BindError::NotNullableColumn(_))
        ));
        binder.bind_insert(&stmts[2]).unwrap();
    }
}
