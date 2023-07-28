use super::*;

impl Binder {
    pub(super) fn bind_delete(
        &mut self,
        tables: Vec<ObjectName>,
        selection: Option<Expr>,
    ) -> Result {
        if tables.len() != 1 {
            return Err(BindError::Todo("delete from multiple tables".into()));
        }
        let name = &tables[0];
        let (table_id, is_internal) = self.bind_table_id(name)?;
        if is_internal {
            return Err(BindError::NotSupportedOnInternalTable);
        }
        let cols = self.bind_table_name(name, None, true)?;
        let true_ = self.egraph.add(Node::true_());
        let scan = self.egraph.add(Node::Scan([table_id, cols, true_]));
        let cond = self.bind_where(selection)?;
        let filter = self.egraph.add(Node::Filter([cond, scan]));
        Ok(self.egraph.add(Node::Delete([table_id, filter])))
    }
}
