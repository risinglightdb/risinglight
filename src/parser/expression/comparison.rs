use std::str::FromStr;

use crate::types::DataTypeKind;

use super::*;

#[derive(Debug, PartialEq, Clone)]
pub struct Comparison {
    pub kind: ComparisonKind,
    pub left: Box<Expression>,
    pub right: Box<Expression>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum ComparisonKind {
    Equal,
    NotEqual,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,
    Like,
    NotLike,
    IsDistinctFrom,
}

impl FromStr for ComparisonKind {
    type Err = ParseError;

    fn from_str(op: &str) -> Result<Self, Self::Err> {
        match op {
            "=" => Ok(Self::Equal),
            "!=" | "<>" => Ok(Self::NotEqual),
            "<" => Ok(Self::LessThan),
            ">" => Ok(Self::GreaterThan),
            "<=" => Ok(Self::LessThanOrEqual),
            ">=" => Ok(Self::GreaterThanOrEqual),
            "~~" => Ok(Self::Like),
            "!~~" => Ok(Self::NotLike),
            _ => Err(ParseError::InvalidInput("operator")),
        }
    }
}

impl Expression {
    pub fn comparison(kind: ComparisonKind, left: Expression, right: Expression) -> Self {
        Expression {
            kind: ExprKind::Comparison(Comparison {
                kind,
                left: Box::new(left),
                right: Box::new(right),
            }),
            alias: None,
            return_type: Some(DataTypeKind::Bool.not_null()),
        }
    }
}

impl TryFrom<&pg::nodes::A_Expr> for Expression {
    type Error = ParseError;

    fn try_from(node: &pg::nodes::A_Expr) -> Result<Self, Self::Error> {
        let name = node_to_string(&node.name.as_ref().unwrap()[0])?;
        use pg::sys::A_Expr_Kind as Kind;
        match &node.kind {
            Kind::AEXPR_DISTINCT => Ok(Expression::comparison(
                ComparisonKind::IsDistinctFrom,
                Expression::try_from(node.lexpr.as_ref().unwrap().as_ref())?,
                Expression::try_from(node.rexpr.as_ref().unwrap().as_ref())?,
            )),
            Kind::AEXPR_IN => todo!("in"),
            Kind::AEXPR_BETWEEN | Kind::AEXPR_NOT_BETWEEN => todo!("between"),
            _ => {
                let left = Expression::try_from(node.lexpr.as_ref().unwrap().as_ref())?;
                let right = Expression::try_from(node.rexpr.as_ref().unwrap().as_ref())?;
                if let Ok(kind) = name.parse::<ComparisonKind>() {
                    return Ok(Expression::comparison(kind, left, right));
                }
                todo!("operator");
            }
        }
    }
}
