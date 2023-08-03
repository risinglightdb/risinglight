use std::result::Result as RawResult;
use std::str::FromStr;

use pretty_xmlish::helper::delegate_fmt;
use pretty_xmlish::Pretty;
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
        let explainer = Pretty::childless_record("Drop", self.pretty_table());
        delegate_fmt(&explainer, f, String::with_capacity(1000))
    }
}

impl BoundDrop {
    pub fn pretty_table<'a>(&self) -> Vec<(&'a str, Pretty<'a>)> {
        vec![
            ("object", Pretty::display(&self.object)),
            ("if_exists", Pretty::display(&self.if_exists)),
            ("cascade", Pretty::display(&self.cascade)),
        ]
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
                let (schema_name, table_name) = split_name(&name)?;
                let table_ref_id = self
                    .catalog
                    .get_table_id_by_name(schema_name, table_name)
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
