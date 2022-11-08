use std::result::Result as RawResult;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::*;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone, Serialize, Deserialize)]
pub struct BoundDrop {
    pub object: Object,
    pub if_exists: bool,
    pub cascade: bool,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone, Serialize, Deserialize)]
pub enum Object {
    Table(TableRefId),
}

impl std::fmt::Display for BoundDrop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "object: {}, exists: {}, cascade: {}",
            self.object, self.if_exists, self.cascade,
        )
    }
}

impl std::fmt::Display for Object {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Object::Table(table_id_ref) => write!(f, "table {}", table_id_ref),
        }
    }
}

impl FromStr for BoundDrop {
    type Err = ();

    fn from_str(_s: &str) -> RawResult<Self, Self::Err> {
        Err(())
    }
}

impl Binder {
    pub(super) fn bind_drop(
        &mut self,
        object_type: ObjectType,
        if_exists: bool,
        names: Vec<ObjectName>,
        cascade: bool,
    ) -> Result {
        match object_type {
            ObjectType::Table => {
                let name = lower_case_name(&names[0]);
                let (database_name, schema_name, table_name) = split_name(&name)?;
                let table_ref_id = self
                    .catalog
                    .get_table_id_by_name(database_name, schema_name, table_name)
                    .ok_or_else(|| BindError::InvalidTable(table_name.into()))?;

                Ok(self.egraph.add(Node::Drop(BoundDrop {
                    object: Object::Table(table_ref_id),
                    if_exists,
                    cascade,
                })))
            }
            _ => todo!(),
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

        let ref_id = TableRefId::new(0, 0, 0);
        catalog
            .add_table(ref_id, "mytable".into(), vec![], false, vec![])
            .unwrap();

        let stmts = parse("drop table mytable").unwrap();
        println!("{:?}", stmts)
    }
}
