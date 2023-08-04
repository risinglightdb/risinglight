use super::*;

impl Binder {
    pub(super) fn bind_drop(
        &mut self,
        object_type: ObjectType,
        if_exists: bool,
        names: Vec<ObjectName>,
        cascade: bool,
    ) -> Result {
        if !matches!(object_type, ObjectType::Table | ObjectType::View) {
            return Err(BindError::Todo(format!("drop {object_type:?}")));
        }
        if cascade {
            return Err(BindError::Todo("cascade drop".into()));
        }
        let mut table_ids = Vec::with_capacity(names.len());
        for name in names {
            let name = lower_case_name(&name);
            let (schema_name, table_name) = split_name(&name)?;
            let result = self.catalog.get_table_id_by_name(schema_name, table_name);
            if if_exists && result.is_none() {
                continue;
            }
            let table_id = result.ok_or_else(|| BindError::InvalidTable(table_name.into()))?;
            let id = self.egraph.add(Node::Table(table_id));
            table_ids.push(id);
        }
        let list = self.egraph.add(Node::List(table_ids.into()));
        let drop = self.egraph.add(Node::Drop(list));
        Ok(drop)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::catalog::RootCatalog;
    use crate::parser::parse;

    #[test]
    fn bind_drop_table() {
        let catalog = Arc::new(RootCatalog::new());
        catalog
            .add_table(0, "mytable".into(), vec![], vec![])
            .unwrap();

        let stmts = parse("drop table mytable").unwrap();
        println!("{:?}", stmts)
    }
}
