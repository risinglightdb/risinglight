use super::*;
use crate::types::DataTypeKind;

#[derive(Debug, PartialEq, Clone)]
pub struct TypeCast {
    pub type_: DataTypeKind,
    pub child: Box<Expression>,
}

impl Expression {
    pub fn typecast(type_: DataTypeKind, child: Expression) -> Self {
        Expression {
            kind: ExprKind::TypeCast(TypeCast {
                type_,
                child: Box::new(child),
            }),
            alias: None,
            return_type: Some(type_.not_null()),
        }
    }
}

impl TryFrom<&pg::nodes::TypeCast> for Expression {
    type Error = ParseError;

    fn try_from(node: &pg::nodes::TypeCast) -> Result<Self, Self::Error> {
        let names = node.typeName.as_ref().unwrap().names.as_ref().unwrap();
        let name = node_to_string(names.last().unwrap())?;
        let type_ = name
            .parse::<DataTypeKind>()
            .map_err(|_| ParseError::InvalidInput("type"))?;
        let child = Expression::try_from(node.arg.as_ref().unwrap().as_ref())?;
        Ok(Expression::typecast(type_, child))
    }
}
