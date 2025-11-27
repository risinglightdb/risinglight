// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;

impl Binder {
    pub fn bind_insert(&mut self, insert: Insert) -> Result {
        let Some(source) = insert.source else {
            return Err(ErrorKind::InvalidSQL.with_spanned(&insert));
        };
        let table_name = match &insert.table {
            TableObject::TableName(object_name) => object_name,
            TableObject::TableFunction(_) => {
                return Err(ErrorKind::Todo("Table Function".into()).with_spanned(&insert.table));
            }
        };
        let (table, is_internal, is_view) = self.bind_table_id(table_name)?;
        if is_internal || is_view {
            return Err(ErrorKind::CanNotInsert.with_spanned(table_name));
        }
        let cols = self.bind_table_columns(table_name, &insert.columns)?;
        let source = self.bind_query(*source)?.0;
        let id = self.egraph.add(Node::Insert([table, cols, source]));
        Ok(id)
    }
}
