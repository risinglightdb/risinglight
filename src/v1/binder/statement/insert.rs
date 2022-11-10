// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use itertools::Itertools;
use sqlparser::ast::{Expr, Value};

use super::*;
use crate::catalog::{ColumnCatalog, ColumnId, TableCatalog, TableRefId};
use crate::parser::{Query, SetExpr, Statement};
use crate::types::DataType;

/// A bound `insert` statement.
#[derive(Debug, PartialEq, Clone)]
pub struct BoundInsert {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub column_types: Vec<DataType>,
    pub column_descs: Vec<ColumnDesc>,
    pub values: Vec<Vec<BoundExpr>>,
    pub select_stmt: Option<Box<BoundSelect>>,
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
                let column_descs = columns.iter().map(|col| col.desc().clone()).collect_vec();

                // Check columns after transforming.
                let col_set: HashSet<ColumnId> = column_ids.iter().cloned().collect();
                for (id, col) in table.all_columns() {
                    if !col_set.contains(&id) && !col.is_nullable() {
                        return Err(BindError::NotNullableColumn(col.name().into()));
                    }
                }

                match &*source.body {
                    SetExpr::Select(_) => self.bind_insert_select_from(
                        table_ref_id,
                        column_ids,
                        column_types,
                        column_descs,
                        source,
                    ),
                    SetExpr::Values(values) => self.bind_insert_values(
                        table_ref_id,
                        columns,
                        column_ids,
                        column_types,
                        column_descs,
                        &values.0,
                    ),
                    _ => todo!("handle insert ???"),
                }
            }
            _ => panic!("mismatched statement type"),
        }
    }

    pub fn bind_insert_values(
        &mut self,
        table_ref_id: TableRefId,
        columns: Vec<ColumnCatalog>,
        column_ids: Vec<ColumnId>,
        column_types: Vec<DataType>,
        column_descs: Vec<ColumnDesc>,
        values: &[Vec<Expr>],
    ) -> Result<BoundInsert, BindError> {
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
            let row = [
                row.as_slice(),
                vec![Expr::Value(Value::Null); column_ids.len() - row.len()].as_slice(),
            ]
            .concat();

            let mut bound_row = vec![];
            bound_row.reserve(row.len());
            for (idx, expr) in row.iter().enumerate() {
                // Bind expression
                let mut expr = self.bind_expr(expr)?;

                if !expr.return_type().kind().is_null() {
                    // table t1(a float, b float)
                    // for example: insert into values (1, 1);
                    // 1 should be casted to float.
                    let left_kind = expr.return_type().kind();
                    let right_kind = column_types[idx].kind();
                    if left_kind != right_kind {
                        expr = BoundExpr::TypeCast(BoundTypeCast {
                            expr: Box::new(expr),
                            ty: column_types[idx].kind(),
                        });
                    }
                } else {
                    // If the data value is null, the column must be nullable.
                    if !column_types[idx].nullable {
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
            column_descs,
            values: bound_values,
            select_stmt: None,
        })
    }

    pub fn bind_insert_select_from(
        &mut self,
        table_ref_id: TableRefId,
        column_ids: Vec<ColumnId>,
        column_types: Vec<DataType>,
        column_descs: Vec<ColumnDesc>,
        select_stmt: &Query,
    ) -> Result<BoundInsert, BindError> {
        let mut bound_select_stmt = self.bind_select(select_stmt)?;
        for (idx, expr) in bound_select_stmt.select_list.iter_mut().enumerate() {
            if !expr.return_type().kind().is_null() {
                // table t1(a float, b float)
                // for example: insert into values (1, 1);
                // 1 should be casted to float.
                let left_kind = expr.return_type().kind();
                let right_kind = column_types[idx].kind();
                if left_kind != right_kind {
                    *expr = BoundExpr::TypeCast(BoundTypeCast {
                        expr: Box::new(expr.clone()),
                        ty: column_types[idx].kind(),
                    });
                }
            } else {
                // If the data value is null, the column must be nullable.
                if !column_types[idx].nullable {
                    return Err(BindError::InvalidExpression(
                        "Can not insert null to non null column".into(),
                    ));
                }
            }
        }
        Ok(BoundInsert {
            table_ref_id,
            column_ids,
            column_types,
            column_descs,
            values: vec![],
            select_stmt: Some(bound_select_stmt),
        })
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
    use crate::types::DataTypeKind;

    #[test]
    fn bind_insert() {
        let catalog = Arc::new(RootCatalog::new());
        let mut binder = Binder::new(catalog.clone());

        let ref_id = TableRefId::new(0, 0, 0);
        catalog
            .add_table(
                ref_id,
                "t".into(),
                vec![
                    ColumnCatalog::new(0, DataTypeKind::Int32.not_null().to_column("a".into())),
                    ColumnCatalog::new(1, DataTypeKind::Int32.not_null().to_column("b".into())),
                ],
                false,
                vec![],
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
        assert!(matches!(
            binder.bind_insert(&stmts[2]),
            Err(BindError::InvalidExpression(_))
        ));
    }
}
