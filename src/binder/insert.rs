// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::parser::Query;

impl Binder {
    pub fn bind_insert(
        &mut self,
        table_name: ObjectName,
        columns: Vec<Ident>,
        source: Box<Query>,
    ) -> Result {
        let (table, is_internal) = self.bind_table_id(&table_name)?;
        if is_internal {
            return Err(BindError::NotSupportedOnInternalTable);
        }
        let cols = self.bind_table_columns(&table_name, &columns)?;
        let source = self.bind_query(*source)?.0;
        let id = self.egraph.add(Node::Insert([table, cols, source]));
        Ok(id)
    }
}
