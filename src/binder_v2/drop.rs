use std::result::Result as RawResult;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::*;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone, Serialize, Deserialize)]
pub struct BindDrop {
    pub object: Object,
    pub if_exists: bool,
    pub cascade: bool,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone, Serialize, Deserialize)]
pub enum Object {
    Table(TableRefId),
}

impl std::fmt::Display for BindDrop {
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
            Object::Table(table_id_ref) => write!(f, "{}", table_id_ref),
        }
    }
}

#[derive(thiserror::Error, Debug, Clone)]
#[error("parse drop id error: {}")]
pub enum ParseDropError {
    #[error("no leading '$'")]
    NoLeadingDollar,
    #[error("invalid table")]
    InvalidTable,
    #[error("invalid number: {0}")]
    InvalidNum(#[from] std::num::ParseIntError),
    #[error("invalid bool: {0}")]
    InvalidBool(#[from] std::str::ParseBoolError),
}

impl FromStr for BindDrop {
    type Err = ParseDropError;

    fn from_str(s: &str) -> RawResult<Self, Self::Err> {
        let body = s.strip_prefix('$').ok_or(Self::Err::NoLeadingDollar)?;
        let mut parts = body.rsplit('.');
        let table_id = parts.next().ok_or(Self::Err::InvalidTable)?.parse()?;
        let schema_id = parts.next().map_or(Ok(0), |s| s.parse())?;
        let database_id = parts.next().map_or(Ok(0), |s| s.parse())?;
        let if_exists = parts.next().map_or(Ok(false), |s| s.parse())?;
        let cascade = parts.next().map_or(Ok(false), |s| s.parse())?;
        Ok(BindDrop {
            object: Object::Table(TableRefId {
                database_id,
                schema_id,
                table_id,
            }),
            if_exists,
            cascade,
        })
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
        self.push_context();
        let ret = self.bind_drop_internal(object_type, if_exists, names, cascade);
        self.pop_context();
        ret
    }

    fn bind_drop_internal(
        &mut self,
        object_type: ObjectType,
        if_exists: bool,
        names: Vec<ObjectName>,
        cascade: bool,
    ) -> Result {
        match object_type {
            ObjectType::Table => {
                let name = lower_case_name(names[0].clone());
                let (database_name, schema_name, table_name) = split_name(&name)?;
                let table_ref_id = self
                    .catalog
                    .get_table_id_by_name(database_name, schema_name, table_name)
                    .ok_or_else(|| BindError::InvalidTable(table_name.into()))?;

                let drop = self.egraph.add(Node::BindDrop(BindDrop {
                    object: Object::Table(table_ref_id),
                    if_exists,
                    cascade,
                }));

                Ok(self.egraph.add(Node::Drop(drop)))
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
