// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use serde::Serialize;

use super::*;
use crate::parser::{ObjectType, Statement};

/// A bound `drop` statement.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BoundDrop {
    pub object: Object,
    pub if_exists: bool,
    pub cascade: bool,
}

/// Identifier of an object.
#[derive(Debug, PartialEq, Eq, Clone, Serialize)]
pub enum Object {
    // TODO: Database
    // TODO: Schema
    Table(TableRefId),
}

impl Binder {
    pub fn bind_drop(&mut self, stmt: &Statement) -> Result<BoundDrop, BindError> {
        match stmt {
            Statement::Drop {
                object_type,
                names,
                if_exists,
                cascade,
                ..
            } if *object_type == ObjectType::Table => {
                let name = &lower_case_name(&names[0]);
                let (database_name, schema_name, table_name) = split_name(name)?;
                let table_ref_id = self
                    .catalog
                    .get_table_id_by_name(database_name, schema_name, table_name)
                    .ok_or_else(|| BindError::InvalidTable(table_name.into()))?;

                Ok(BoundDrop {
                    object: Object::Table(table_ref_id),
                    if_exists: *if_exists,
                    cascade: *cascade,
                })
            }
            _ => panic!("mismatched statement type"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::catalog::RootCatalog;
    use crate::parser::parse;

    #[test]
    fn bind_drop_table() {
        let catalog = Arc::new(RootCatalog::new());
        let mut binder = Binder::new(catalog.clone());

        let ref_id = TableRefId::new(0, 0, 0);
        catalog
            .add_table(ref_id, "mytable".into(), vec![], false, vec![])
            .unwrap();

        let stmts = parse("drop table mytable").unwrap();
        assert_eq!(
            binder.bind_drop(&stmts[0]).unwrap(),
            BoundDrop {
                object: Object::Table(TableRefId::new(0, 0, 0)),
                if_exists: false,
                cascade: false,
            }
        );
    }
}
