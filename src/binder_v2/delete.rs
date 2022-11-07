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
        let table_id = self.bind_table_id(name)?;
        let cols = self.bind_table_name(name, true)?;
        let scan = self.egraph.add(Node::Scan([table_id, cols]));
        let cond = self.bind_where(selection)?;
        let filter = self.egraph.add(Node::Filter([cond, scan]));
        Ok(self.egraph.add(Node::Delete([table_id, filter])))
    }
}
