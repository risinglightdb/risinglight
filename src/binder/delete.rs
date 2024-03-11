// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;

impl Binder {
    pub(super) fn bind_delete(
        &mut self,
        from: Vec<TableWithJoins>,
        selection: Option<Expr>,
    ) -> Result {
        if from.len() != 1 || !from[0].joins.is_empty() {
            return Err(BindError::Todo(format!("delete from {from:?}")));
        }
        let TableFactor::Table { name, .. } = &from[0].relation else {
            return Err(BindError::Todo(format!("delete from {from:?}")));
        };
        let (table_id, is_system, is_view) = self.bind_table_id(name)?;
        if is_system || is_view {
            return Err(BindError::CanNotDelete);
        }
        let cols = self.bind_table_def(name, None, true)?;
        let true_ = self.egraph.add(Node::true_());
        let scan = self.egraph.add(Node::Scan([table_id, cols, true_]));
        let cond = self.bind_where(selection)?;
        let filter = self.egraph.add(Node::Filter([cond, scan]));
        Ok(self.egraph.add(Node::Delete([table_id, filter])))
    }
}
