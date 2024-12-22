// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;

impl Binder {
    pub(super) fn bind_delete(&mut self, from: FromTable, selection: Option<Expr>) -> Result {
        let from = match from {
            FromTable::WithFromKeyword(t) => t,
            FromTable::WithoutKeyword(t) => t,
        };
        if from.len() != 1 || !from[0].joins.is_empty() {
            return Err(BindError::CanNotDelete);
        }
        let table = from.into_iter().next().unwrap().relation;
        let TableFactor::Table { name, .. } = &table else {
            return Err(BindError::Todo(format!("delete from {table:?}")));
        };
        let (table_id, is_system, is_view) = self.bind_table_id(name)?;
        if is_system || is_view {
            return Err(BindError::CanNotDelete);
        }
        let mut plan = self.bind_table_factor(table, true)?;
        let cond = self.bind_where(selection)?;
        let subqueries = self.take_subqueries();
        plan = self.plan_apply(subqueries, plan)?;
        plan = self.plan_filter(cond, plan)?;
        Ok(self.egraph.add(Node::Delete([table_id, plan])))
    }
}
