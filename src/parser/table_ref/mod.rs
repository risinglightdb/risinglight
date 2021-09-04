use super::*;
use postgres_parser as pg;
use std::convert::{TryFrom, TryInto};

mod base;
mod join;

pub use self::base::BaseTableRef;
pub use self::join::JoinRef;

#[derive(Debug, PartialEq)]
pub enum TableRef {
    Base(BaseTableRef),
    Join(JoinRef),
}

impl TableRef {
    pub const fn base(table_name: String) -> Self {
        TableRef::Base(BaseTableRef {
            table_name,
            database_name: None,
            schema_name: None,
            alias: None,
        })
    }
}

impl TryFrom<&pg::Node> for TableRef {
    type Error = ParseError;

    fn try_from(root: &pg::Node) -> Result<Self, Self::Error> {
        match root {
            pg::Node::RangeVar(root) => Ok(Self::Base(root.into())),
            pg::Node::JoinExpr(root) => Ok(Self::Join(root.try_into()?)),
            _ => todo!("unsupported FROM type"),
        }
    }
}
