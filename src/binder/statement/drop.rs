use super::*;
use crate::parser::{ObjectType, Statement};

/// A bound `drop` statement.
#[derive(Debug, PartialEq, Clone)]
pub struct BoundDrop {
    pub object: Object,
    pub if_exists: bool,
    pub cascade: bool,
}

/// Identifier of an object.
#[derive(Debug, PartialEq, Clone)]
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
                let (database_name, schema_name, table_name) = split_name(&names[0])?;
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
    use super::*;
    use crate::{catalog::RootCatalog, parser::parse};
    use std::sync::Arc;

    #[test]
    fn bind_drop_table() {
        let catalog = Arc::new(RootCatalog::new());
        let mut binder = Binder::new(catalog.clone());

        let database = catalog.get_database_by_id(0).unwrap();
        let schema = database.get_schema_by_id(0).unwrap();
        schema.add_table("mytable".into(), vec![], false).unwrap();

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
