// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;

impl Binder {
    pub(super) fn bind_delete(&mut self, delete: Delete) -> Result {
        let from = match &delete.from {
            FromTable::WithFromKeyword(t) => t,
            FromTable::WithoutKeyword(t) => t,
        };
        if from.len() != 1 || !from[0].joins.is_empty() {
            return Err(ErrorKind::CanNotDelete.with_spanned(&delete.from));
        }
        let table = from[0].relation.clone();
        let TableFactor::Table { name, .. } = &table else {
            return Err(ErrorKind::CanNotDelete.with_spanned(&table));
        };
        let (table_id, is_system, is_view) = self.bind_table_id(name)?;
        if is_system || is_view {
            return Err(ErrorKind::CanNotDelete.with_spanned(name));
        }
        let mut plan = self.bind_table_factor(table, true)?;
        let cond = self.bind_where(delete.selection)?;
        let subqueries = self.take_subqueries();
        plan = self.plan_apply(subqueries, plan)?;
        plan = self.plan_filter(cond, plan)?;
        Ok(self.egraph.add(Node::Delete([table_id, plan])))
    }
}
