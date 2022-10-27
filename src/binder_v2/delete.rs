use super::*;

impl Binder {
    pub(super) fn bind_delete(
        &mut self,
        table_name: TableFactor,
        selection: Option<Expr>,
    ) -> Result {
        let table = self.bind_table(table_name)?;
        let cond = self.bind_condition(selection)?;
        Ok(self.egraph.add(Node::Delete([table, cond])))
    }
}
