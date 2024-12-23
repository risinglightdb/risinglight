// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;

impl Binder {
    pub fn bind_insert(
        &mut self,
        Insert {
            table_name,
            columns,
            source,
            ..
        }: Insert,
    ) -> Result {
        let Some(source) = source else {
            return Err(BindError::InvalidSQL);
        };
        let (table, is_internal, is_view) = self.bind_table_id(&table_name)?;
        if is_internal || is_view {
            return Err(BindError::CanNotInsert);
        }
        let cols = self.bind_table_columns(&table_name, &columns)?;
        let source = self.bind_query(*source)?.0;
        let id = self.egraph.add(Node::Insert([table, cols, source]));
        Ok(id)
    }
}
