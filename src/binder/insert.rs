// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::parser::Query;

impl Binder {
    pub fn bind_insert(
        &mut self,
        table_name: ObjectName,
        columns: Vec<Ident>,
        source: Box<Query>,
    ) -> Result {
        let (table, is_internal) = self.bind_table_id(&table_name)?;
        if is_internal {
            return Err(BindError::NotSupportedOnInternalTable);
        }
        let cols = self.bind_table_columns(&table_name, &columns)?;
        let source = self.bind_query(*source)?.0;
        let id = self.egraph.add(Node::Insert([table, cols, source]));
        Ok(id)
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
    fn bind_insert_table() {
        let catalog = Arc::new(RootCatalog::new());
        let col_desc = DataTypeKind::Int32.not_null().to_column("a".into());
        let col_catalog = ColumnCatalog::new(0, col_desc);
        catalog
            .add_table(0, "t".into(), vec![col_catalog], false, vec![])
            .unwrap();

        let stmts = parse("insert into t (a) values (1)").unwrap();
        println!("{:?}", stmts);
        let mut binder = Binder::new(catalog);
        for stmt in stmts {
            let plan = binder.bind(stmt).unwrap();
            println!("{}", plan.pretty(10));
        }
    }
}
