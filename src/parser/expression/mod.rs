use super::*;
use crate::catalog::ColumnRefId;
use crate::types::{ColumnId, DataType, DataValue};
use postgres_parser as pg;
use std::convert::{TryFrom, TryInto};

mod aggregate;
mod column_ref;
mod comparison;
mod constant;
mod typecast;

pub use self::aggregate::*;
pub use self::column_ref::*;
pub use self::comparison::*;
pub use self::typecast::*;

#[derive(Debug, PartialEq, Clone)]
pub struct Expression {
    pub(crate) kind: ExprKind,
    pub(crate) alias: Option<String>,
    pub(crate) return_type: Option<DataType>,
}

impl Expression {
    pub fn get_name(&self) -> String {
        match &self.alias {
            Some(string) => string.clone(),
            None => match &self.kind {
                ExprKind::Constant(_) => "CONSTANT".to_string(),
                ExprKind::ColumnRef(col_ref) => col_ref.column_name.clone(),
                ExprKind::Star => "STAR".to_string(),
                ExprKind::Comparison(_) => "COMPARISION".to_string(),
                ExprKind::TypeCast(_) => "TYPECAST".to_string(),
                ExprKind::Aggregate(_) => "AGGREGATE".to_string(),
            },
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum ExprKind {
    Constant(DataValue),
    ColumnRef(ColumnRef),
    /// A (*) in the SELECT clause.
    Star,
    Comparison(Comparison),
    TypeCast(TypeCast),
    Aggregate(Aggregate),
}

impl TryFrom<&pg::Node> for Expression {
    type Error = ParseError;

    fn try_from(node: &pg::Node) -> Result<Self, Self::Error> {
        match node {
            pg::Node::ColumnRef(node) => node.try_into(),
            pg::Node::A_Const(node) => node.try_into(),
            pg::Node::A_Expr(node) => node.try_into(),
            pg::Node::TypeCast(node) => node.try_into(),
            pg::Node::FuncCall(node) => node.try_into(),
            _ => todo!("expression type"),
        }
    }
}

fn node_to_string(node: &pg::Node) -> Result<&String, ParseError> {
    let v = try_match!(node, pg::Node::Value(v) => v, "value");
    let s = try_match!(v.string, Some(s) => s, "string value");
    Ok(s)
}
