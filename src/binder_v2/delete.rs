use super::*;

impl Binder {
    pub(super) fn bind_delete(
        &mut self,
        table_name: TableFactor,
        selection: Option<Expr>,
    ) -> Result {
        let TableFactor::Table { name, .. } = &table_name else {
            todo!("unsupported delete target: {:?}", table_name);
        };
        let (table_id, is_internal) = self.bind_table_id(name)?;
        if is_internal {
            return Err(BindError::NotSupportedOnInternalTable);
        }
        let cols = self.bind_table_name(name, None, true)?;
        let scan = self.egraph.add(Node::Scan([table_id, cols]));
        let cond = self.bind_where(selection)?;
        let filter = self.egraph.add(Node::Filter([cond, scan]));
        Ok(self.egraph.add(Node::Delete([table_id, filter])))
    }
}
