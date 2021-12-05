use super::*;

/// A bound column reference expression.
#[derive(PartialEq, Clone)]
pub struct BoundColumnRef {
    pub column_ref_id: ColumnRefId,
    pub return_type: DataType,
}

impl std::fmt::Debug for BoundColumnRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.column_ref_id)
    }
}

impl Binder {
    /// Expand wildcard into a list of column references.
    pub fn bind_all_column_refs(&mut self) -> Result<Vec<BoundExpr>, BindError> {
        let mut exprs = vec![];
        for &table_ref_id in self.tables.values() {
            let table = self.catalog.get_table(table_ref_id).unwrap();
            for (col_id, col) in table.all_columns() {
                let expr = BoundExpr::ColumnRef(BoundColumnRef {
                    column_ref_id: ColumnRefId::from_table(table_ref_id, col_id),
                    return_type: col.datatype(),
                });
                exprs.push(expr);
            }
        }
        Ok(exprs)
    }

    pub fn bind_column_ref(&mut self, idents: &[Ident]) -> Result<BoundExpr, BindError> {
        let (_schema_name, table_name, column_name) = match idents {
            [column] => (None, None, &column.value),
            [table, column] => (None, Some(&table.value), &column.value),
            [schema, table, column] => (Some(&schema.value), Some(&table.value), &column.value),
            _ => return Err(BindError::InvalidTableName(idents.into())),
        };
        if let Some(name) = table_name {
            let table_ref_id = *self
                .tables
                .get(name)
                .ok_or_else(|| BindError::TableNotFound(name.clone()))?;
            let table = self.catalog.get_table(table_ref_id).unwrap();
            let col = table
                .get_column_by_name(column_name)
                .ok_or_else(|| BindError::ColumnNotFound(column_name.clone()))?;
            Ok(BoundExpr::ColumnRef(BoundColumnRef {
                column_ref_id: ColumnRefId::from_table(table_ref_id, col.id()),
                return_type: col.datatype(),
            }))
        } else {
            let mut column_ref = None;
            for &table_ref_id in self.tables.values() {
                let table = self.catalog.get_table(table_ref_id).unwrap();
                if let Some(col) = table.get_column_by_name(column_name) {
                    if column_ref.is_some() {
                        return Err(BindError::AmbiguousColumnName(column_name.into()));
                    }
                    column_ref = Some(BoundColumnRef {
                        column_ref_id: ColumnRefId::from_table(table_ref_id, col.id()),
                        return_type: col.datatype(),
                    });
                }
            }
            Ok(BoundExpr::ColumnRef(column_ref.ok_or_else(|| {
                BindError::ColumnNotFound(column_name.clone())
            })?))
        }
    }
}
