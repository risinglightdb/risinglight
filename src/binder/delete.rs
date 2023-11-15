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
        let (table_id, is_internal) = self.bind_table_id(name)?;
        if is_internal {
            return Err(BindError::NotSupportedOnInternalTable);
        }
        let cols = self.bind_table_def(name, None, true)?;
        let true_ = self.egraph.add(Node::true_());
        let scan = self.egraph.add(Node::Scan([table_id, cols, true_]));
        let cond = self.bind_where(selection)?;
        let filter = self.egraph.add(Node::Filter([cond, scan]));
        Ok(self.egraph.add(Node::Delete([table_id, filter])))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::binder::Binder;
    use crate::catalog::{ColumnCatalog, RootCatalog};
    use crate::parser::parse;

    #[test]
    fn bind_test_subquery() {
        let catalog = Arc::new(RootCatalog::new());
        let col_desc = DataTypeKind::Int32.not_null().to_column("a".into());
        let col_catalog = ColumnCatalog::new(0, col_desc);
        catalog
            .add_table(0, "t".into(), vec![col_catalog], false, vec![])
            .unwrap();

        let stmts = parse("delete from t where a").unwrap();
        let mut binder = Binder::new(catalog);
        for stmt in stmts {
            let plan = binder.bind(stmt).unwrap();
            println!("{}", plan.pretty(10));
        }
    }
}
