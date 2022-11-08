use super::*;

impl Binder {
    pub(super) fn bind_delete(
        &mut self,
        table_name: TableFactor,
        selection: Option<Expr>,
    ) -> Result {
        let table_id = self.bind_table_id(table_name.clone())?;
        let scan = self.bind_table(table_name)?;
        let cond = self.bind_condition(selection)?;
        let filter = self.egraph.add(Node::Filter([cond, scan]));
        Ok(self.egraph.add(Node::Delete([table_id, filter])))
    }
}
