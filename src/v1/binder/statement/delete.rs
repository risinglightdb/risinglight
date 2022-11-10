// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use sqlparser::ast::TableFactor;

use super::*;

/// A bound `delete` statement.
#[derive(Debug, PartialEq, Clone)]
pub struct BoundDelete {
    pub from_table: BoundTableRef,
    pub where_clause: Option<BoundExpr>,
}

impl Binder {
    pub fn bind_delete(&mut self, stmt: &Statement) -> Result<Box<BoundDelete>, BindError> {
        self.push_context();
        let ret = self.bind_delete_internal(stmt);
        self.pop_context();
        ret
    }

    pub fn bind_delete_internal(
        &mut self,
        stmt: &Statement,
    ) -> Result<Box<BoundDelete>, BindError> {
        if let Statement::Delete {
            table_name,
            selection,
            ..
        } = stmt
        {
            let table_name = if let TableFactor::Table { name, .. } = table_name {
                name
            } else {
                unimplemented!()
            };
            let table_name = &lower_case_name(table_name);
            let (database_name, schema_name, table_name) = split_name(table_name)?;
            let mut from_table =
                self.bind_table_ref_with_name(database_name, schema_name, table_name)?;
            let where_clause = selection
                .as_ref()
                .map(|expr| self.bind_expr(expr))
                .transpose()?;
            self.bind_column_ids(&mut from_table);
            Ok(Box::new(BoundDelete {
                from_table,
                where_clause,
            }))
        } else {
            panic!("unmatched statement type")
        }
    }
}
