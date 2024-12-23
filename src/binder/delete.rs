// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;

impl Binder {
    pub(super) fn bind_delete(&mut self, delete: Delete) -> Result {
        let from = match &delete.from {
            FromTable::WithFromKeyword(t) => t,
            FromTable::WithoutKeyword(t) => t,
        };
        if from.len() != 1 || !from[0].joins.is_empty() {
            return Err(ErrorKind::Todo(format!("delete from {from:?}")).with_spanned(&delete.from));
        }
        let TableFactor::Table { name, alias, .. } = &from[0].relation else {
            return Err(
                ErrorKind::Todo(format!("delete from {from:?}")).with_spanned(&from[0].relation)
            );
        };
        let (table_id, is_system, is_view) = self.bind_table_id(name)?;
        if is_system || is_view {
            return Err(ErrorKind::CanNotDelete.with_spanned(name));
        }
        let scan = self.bind_table_def(name, alias.clone(), true)?;
        let cond = self.bind_where(delete.selection)?;
        let filter = self.egraph.add(Node::Filter([cond, scan]));
        Ok(self.egraph.add(Node::Delete([table_id, filter])))
    }
}
