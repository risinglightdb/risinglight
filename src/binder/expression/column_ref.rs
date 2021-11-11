use super::*;

/// A bound column reference expression.
#[derive(PartialEq, Clone)]
pub struct BoundColumnRef {
    pub table_name: String,
    pub column_ref_id: ColumnRefId,
    pub column_index: ColumnId,
    pub is_primary_key: bool,
}

impl std::fmt::Debug for BoundColumnRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.column_index)
    }
}

impl Binder {
    pub fn bind_all_column_refs(&mut self) -> Result<Vec<BoundExpr>, BindError> {
        let mut exprs = vec![];
        for ref_id in self.context.regular_tables.values() {
            let table = self.catalog.get_table(ref_id).unwrap();
            for (col_id, col) in table.all_columns().iter() {
                let column_ref_id = ColumnRefId::from_table(*ref_id, *col_id);
                Self::record_regular_table_column(
                    &mut self.context.column_names,
                    &mut self.context.column_ids,
                    &table.name(),
                    col.name(),
                    *col_id,
                );
                let expr = BoundExpr {
                    kind: BoundExprKind::ColumnRef(BoundColumnRef {
                        table_name: table.name().clone(),
                        column_ref_id,
                        column_index: u32::MAX,
                        is_primary_key: col.is_primary(),
                    }),
                    return_type: Some(col.datatype().clone()),
                };
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
            if !self.context.regular_tables.contains_key(name) {
                return Err(BindError::InvalidTable(name.clone()));
            }
            let table_ref_id = self.context.regular_tables[name];
            let table = self.catalog.get_table(&table_ref_id).unwrap();
            let col = table
                .get_column_by_name(column_name)
                .ok_or_else(|| BindError::InvalidColumn(column_name.clone()))?;
            let column_ref_id = ColumnRefId::from_table(table_ref_id, col.id());
            Self::record_regular_table_column(
                &mut self.context.column_names,
                &mut self.context.column_ids,
                name,
                column_name,
                col.id(),
            );
            Ok(BoundExpr {
                kind: BoundExprKind::ColumnRef(BoundColumnRef {
                    table_name: name.clone(),
                    column_ref_id,
                    column_index: u32::MAX,
                    is_primary_key: col.is_primary(),
                }),
                return_type: Some(col.datatype()),
            })
        } else {
            let mut info = None;
            for ref_id in self.context.regular_tables.values() {
                let table = self.catalog.get_table(ref_id).unwrap();
                if let Some(col) = table.get_column_by_name(column_name) {
                    if info.is_some() {
                        return Err(BindError::AmbiguousColumn);
                    }
                    let column_ref_id = ColumnRefId::from_table(*ref_id, col.id());
                    info = Some((
                        table.name().clone(),
                        column_ref_id,
                        col.datatype(),
                        col.is_primary(),
                    ));
                }
            }
            let (table_name, column_ref_id, data_type, is_primary_key) =
                info.ok_or_else(|| BindError::InvalidColumn(column_name.clone()))?;
            Self::record_regular_table_column(
                &mut self.context.column_names,
                &mut self.context.column_ids,
                &table_name,
                column_name,
                column_ref_id.column_id,
            );

            Ok(BoundExpr {
                kind: BoundExprKind::ColumnRef(BoundColumnRef {
                    table_name: table_name.clone(),
                    column_ref_id,
                    column_index: u32::MAX,
                    is_primary_key,
                }),
                return_type: Some(data_type),
            })
        }
    }

    fn record_regular_table_column(
        column_names: &mut HashMap<String, HashSet<String>>,
        column_ids: &mut HashMap<String, Vec<ColumnId>>,
        table_name: &str,
        col_name: &str,
        column_id: ColumnId,
    ) -> ColumnId {
        let names = column_names.get_mut(table_name).unwrap();
        if !names.contains(col_name) {
            let idx = names.len() as u32;
            names.insert(col_name.to_string());
            let idxs = column_ids.get_mut(table_name).unwrap();
            idxs.push(column_id);
            assert!(!idxs.is_empty());
            idx
        } else {
            let idxs = &column_ids[table_name];
            assert!(!idxs.is_empty());
            idxs.iter().position(|&r| r == column_id).unwrap() as u32
        }
    }
}
