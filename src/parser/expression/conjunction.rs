use super::*;
use crate::types::DataTypeKind;

#[derive(Debug, PartialEq, Clone)]
pub enum Conjunction {
    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
}

impl Expression {
    pub fn and(left: Expression, right: Expression) -> Self {
        Expression {
            kind: ExprKind::Conjunction(Conjunction::And(Box::new(left), Box::new(right))),
            alias: None,
            return_type: Some(DataTypeKind::Bool.not_null()),
        }
    }

    pub fn or(left: Expression, right: Expression) -> Self {
        Expression {
            kind: ExprKind::Conjunction(Conjunction::Or(Box::new(left), Box::new(right))),
            alias: None,
            return_type: Some(DataTypeKind::Bool.not_null()),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn not(expr: Expression) -> Self {
        Expression {
            kind: ExprKind::Conjunction(Conjunction::Not(Box::new(expr))),
            alias: None,
            return_type: Some(DataTypeKind::Bool.not_null()),
        }
    }
}

impl TryFrom<&pg::nodes::BoolExpr> for Expression {
    type Error = ParseError;

    fn try_from(node: &pg::nodes::BoolExpr) -> Result<Self, Self::Error> {
        let args = node.args.as_ref().unwrap();
        match node.boolop {
            pg::sys::BoolExprType::AND_EXPR => Ok(Expression::and(
                (&args[0]).try_into()?,
                (&args[1]).try_into()?,
            )),
            pg::sys::BoolExprType::OR_EXPR => Ok(Expression::or(
                (&args[0]).try_into()?,
                (&args[1]).try_into()?,
            )),
            pg::sys::BoolExprType::NOT_EXPR => Ok(Expression::not((&args[0]).try_into()?)),
        }
    }
}
