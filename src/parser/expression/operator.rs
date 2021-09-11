use super::*;
use std::str::FromStr;

#[derive(Debug, PartialEq, Clone)]
pub struct Operator {
    pub kind: OpKind,
    pub left: Box<Expression>,
    pub right: Box<Expression>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum OpKind {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Concat,
    // IsNull,
    // IsNotNull,
    // InList,
    // NotInList,
}

impl FromStr for OpKind {
    type Err = ();

    fn from_str(op: &str) -> Result<Self, Self::Err> {
        match op {
            "+" => Ok(Self::Add),
            "-" => Ok(Self::Sub),
            "*" => Ok(Self::Mul),
            "/" => Ok(Self::Div),
            "%" => Ok(Self::Mod),
            "||" => Ok(Self::Concat),
            _ => Err(()),
        }
    }
}

impl Expression {
    pub fn operator(kind: OpKind, left: Expression, right: Expression) -> Self {
        Expression {
            kind: ExprKind::Operator(Operator {
                kind,
                left: Box::new(left),
                right: Box::new(right),
            }),
            alias: None,
            return_type: None,
        }
    }
}
