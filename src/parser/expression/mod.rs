use super::*;
use crate::types::DataValue;
use postgres_parser as pg;
use std::convert::TryFrom;

#[derive(Debug, PartialEq)]
pub struct Expression {
    pub(crate) data: ExprData,
}

#[derive(Debug, PartialEq)]
pub enum ExprData {
    Constant(DataValue),
}

impl Expression {
    pub fn constant(value: DataValue) -> Self {
        Expression {
            data: ExprData::Constant(value),
        }
    }
}

impl TryFrom<&pg::Node> for Expression {
    type Error = ParseError;

    fn try_from(node: &pg::Node) -> Result<Self, Self::Error> {
        match node {
            pg::Node::A_Const(c) => Ok(Expression {
                data: ExprData::Constant(DataValue::try_from(&c.val)?),
            }),
            _ => todo!("expression type"),
        }
    }
}

impl TryFrom<&pg::nodes::Value> for DataValue {
    type Error = ParseError;

    fn try_from(value: &pg::nodes::Value) -> Result<Self, Self::Error> {
        if value.null.is_some() {
            return Ok(DataValue::Null);
        }
        if let Some(v) = value.int {
            return Ok(DataValue::Int32(v));
        }
        if let Some(v) = &value.float {
            return Ok(DataValue::Float64(v.parse().unwrap()));
        }
        if let Some(v) = &value.string {
            return Ok(DataValue::String(v.clone()));
        }
        if let Some(v) = &value.bit_string {
            return Ok(DataValue::String(v.clone()));
        }
        Err(ParseError::InvalidInput("value"))
    }
}
