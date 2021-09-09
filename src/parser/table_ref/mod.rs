use super::*;
use postgres_parser as pg;
use std::convert::{TryFrom, TryInto};

mod base;
mod join;
mod subquery;

pub use self::base::BaseTableRef;
pub use self::join::JoinRef;
pub use self::subquery::SubqueryRef;

#[derive(Debug, PartialEq)]
pub enum TableRef {
    Base(BaseTableRef),
    Join(JoinRef),
    Subquery(SubqueryRef),
}

impl TryFrom<&pg::Node> for TableRef {
    type Error = ParseError;

    fn try_from(root: &pg::Node) -> Result<Self, Self::Error> {
        match root {
            pg::Node::RangeVar(root) => Ok(Self::Base(root.into())),
            pg::Node::JoinExpr(root) => Ok(Self::Join(root.try_into()?)),
            pg::Node::RangeSubselect(root) => Ok(Self::Subquery(root.try_into()?)),
            _ => todo!("unsupported FROM type"),
        }
    }
}
