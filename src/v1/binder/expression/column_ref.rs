// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use itertools::Itertools;
use serde::Serialize;

use super::*;
use crate::catalog::ColumnDesc;

/// A bound column reference expression.
#[derive(PartialEq, Eq, Clone, Serialize)]
pub struct BoundColumnRef {
    pub column_ref_id: ColumnRefId,
    pub is_primary_key: bool,
    pub desc: ColumnDesc,
}

impl std::fmt::Debug for BoundColumnRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.column_ref_id)
    }
}

impl Binder {
    pub fn bind_all_column_refs(&mut self) -> Result<Vec<BoundExpr>, BindError> {
        let mut exprs = vec![];
        for ref_id in self.context.regular_tables.values().cloned().collect_vec() {
            let table = self.catalog.get_table(&ref_id).unwrap();
            for (col_id, col) in &table.all_columns() {
                let column_ref_id = ColumnRefId::from_table(ref_id, *col_id);
                self.record_regular_table_column(
                    &table.name(),
                    col.name(),
                    *col_id,
                    col.desc().clone(),
                );
                let expr = BoundExpr::ColumnRef(BoundColumnRef {
                    column_ref_id,
                    is_primary_key: col.is_primary(),
                    desc: col.desc().clone(),
                });
                exprs.push(expr);
            }
        }

        Ok(exprs)
    }

    pub fn bind_column_ref(&mut self, idents: &[Ident]) -> Result<BoundExpr, BindError> {
        let idents = idents
            .iter()
            .map(|ident| Ident::new(ident.value.to_lowercase()))
            .collect_vec();
        let (_schema_name, table_name, column_name) = match idents.as_slice() {
            [column] => (None, None, &column.value),
            [table, column] => (None, Some(&table.value), &column.value),
            [schema, table, column] => (Some(&schema.value), Some(&table.value), &column.value),
            _ => return Err(BindError::InvalidTableName(idents)),
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
            self.record_regular_table_column(name, column_name, col.id(), col.desc().clone());
            Ok(BoundExpr::ColumnRef(BoundColumnRef {
                column_ref_id,
                is_primary_key: col.is_primary(),
                desc: col.desc().clone(),
            }))
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
                        col.is_primary(),
                        col.desc().clone(),
                    ));
                }
            }
            if info.is_none() {
                if let Some(index) = self
                    .context
                    .aliases
                    .iter()
                    .position(|name| column_name == name)
                {
                    Ok(BoundExpr::Alias(BoundAlias {
                        alias: column_name.clone(),
                        expr: Box::new(self.context.aliases_expressions[index].clone()),
                    }))
                } else {
                    Err(BindError::InvalidColumn(column_name.clone()))
                }
            } else {
                let (table_name, column_ref_id, is_primary_key, desc) =
                    info.ok_or_else(|| BindError::InvalidColumn(column_name.clone()))?;
                self.record_regular_table_column(
                    &table_name,
                    column_name,
                    column_ref_id.column_id,
                    desc.clone(),
                );

                Ok(BoundExpr::ColumnRef(BoundColumnRef {
                    column_ref_id,
                    is_primary_key,
                    desc,
                }))
            }
        }
    }

    pub fn record_regular_table_column(
        &mut self,
        table_name: &str,
        col_name: &str,
        column_id: ColumnId,
        desc: ColumnDesc,
    ) -> ColumnId {
        let names = self.context.column_names.get_mut(table_name).unwrap();
        let descs = self.context.column_descs.get_mut(table_name).unwrap();
        if !names.contains(col_name) {
            let idx = names.len() as u32;
            names.insert(col_name.to_string());
            let idxs = self.context.column_ids.get_mut(table_name).unwrap();
            idxs.push(column_id);
            descs.push(desc);
            assert!(!idxs.is_empty());
            idx
        } else {
            let idxs = &self.context.column_ids[table_name];
            assert!(!idxs.is_empty());
            idxs.iter().position(|&r| r == column_id).unwrap() as u32
        }
    }
}
