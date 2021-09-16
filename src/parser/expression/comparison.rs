use std::str::FromStr;

use crate::types::DataTypeKind;

use super::*;

#[derive(Debug, PartialEq, Clone)]
pub struct Comparison {
    pub kind: CmpKind,
    pub left: Box<Expression>,
    pub right: Box<Expression>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum CmpKind {
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

impl FromStr for CmpKind {
    type Err = ();

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
            _ => Err(()),
        }
    }
}

impl Expression {
    pub fn comparison(kind: CmpKind, left: Expression, right: Expression) -> Self {
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

    pub fn between(value: Expression, left: Expression, right: Expression) -> Self {
        let l_comp = Expression::comparison(CmpKind::GreaterThanOrEqual, value.clone(), left);
        let r_comp = Expression::comparison(CmpKind::LessThanOrEqual, value, right);
        Expression::and(l_comp, r_comp)
    }
}

impl TryFrom<&pg::nodes::A_Expr> for Expression {
    type Error = ParseError;

    fn try_from(node: &pg::nodes::A_Expr) -> Result<Self, Self::Error> {
        let name = node_to_string(&node.name.as_ref().unwrap()[0])?;
        use pg::sys::A_Expr_Kind as Kind;
        match &node.kind {
            Kind::AEXPR_DISTINCT => Ok(Expression::comparison(
                CmpKind::IsDistinctFrom,
                Expression::try_from(node.lexpr.as_ref().unwrap().as_ref())?,
                Expression::try_from(node.rexpr.as_ref().unwrap().as_ref())?,
            )),
            Kind::AEXPR_IN => todo!("in"),
            Kind::AEXPR_BETWEEN | Kind::AEXPR_NOT_BETWEEN => {
                let args = try_match!(node.rexpr.as_ref().unwrap().as_ref(), pg::Node::List(l) => l, "between list");
                let (left, right) = match args.as_slice() {
                    [l, r] => (l, r),
                    _ => return Err(ParseError::InvalidInput("between need 2 args")),
                };
                let value = Expression::try_from(node.lexpr.as_ref().unwrap().as_ref())?;
                let left = Expression::try_from(left)?;
                let right = Expression::try_from(right)?;
                let comp = Expression::between(value, left, right);
                if node.kind == Kind::AEXPR_BETWEEN {
                    return Ok(comp);
                }
                Ok(Expression::not(comp))
            }
            _ => {
                let left = Expression::try_from(node.lexpr.as_ref().unwrap().as_ref())?;
                let right = Expression::try_from(node.rexpr.as_ref().unwrap().as_ref())?;
                if let Ok(kind) = name.parse::<CmpKind>() {
                    return Ok(Expression::comparison(kind, left, right));
                }
                if let Ok(kind) = name.parse::<OpKind>() {
                    return Ok(Expression::operator(kind, left, right));
                }
                todo!("operator");
            }
        }
    }
}
