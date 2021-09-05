use super::*;
use crate::{parser::expression::Expression, types::JoinType};
use postgres_parser as pg;
use std::convert::TryFrom;

#[derive(Debug, PartialEq)]
pub struct JoinRef {
    pub left: Box<TableRef>,
    pub right: Box<TableRef>,
    pub condition: Expression,
    pub type_: JoinType,
}

impl TryFrom<&pg::nodes::JoinExpr> for JoinRef {
    type Error = ParseError;

    fn try_from(root: &pg::nodes::JoinExpr) -> Result<Self, Self::Error> {
        let type_ = JoinType::from(&root.jointype);
        let left = TableRef::try_from(root.larg.as_ref().unwrap().as_ref())?;
        let right = TableRef::try_from(root.rarg.as_ref().unwrap().as_ref())?;
        if let Some(quals) = &root.quals {
            let condition = Expression::try_from(quals.as_ref())?;
            Ok(JoinRef {
                left: Box::new(left),
                right: Box::new(right),
                condition,
                type_,
            })
        } else {
            todo!("cross product");
        }
    }
}

impl From<&pg::sys::JoinType> for JoinType {
    fn from(ty: &pg::sys::JoinType) -> Self {
        match ty {
            pg::sys::JoinType::JOIN_INNER => Self::Inner,
            pg::sys::JoinType::JOIN_LEFT => Self::Left,
            pg::sys::JoinType::JOIN_FULL => Self::Outer,
            pg::sys::JoinType::JOIN_RIGHT => Self::Right,
            pg::sys::JoinType::JOIN_SEMI => Self::Semi,
            _ => todo!("join type"),
        }
    }
}
