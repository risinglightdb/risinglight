use super::*;

impl Expression {
    pub const fn constant(value: DataValue) -> Self {
        let return_type = value.data_type();
        Expression {
            alias: None,
            kind: ExprKind::Constant(value),
            return_type,
        }
    }
}

impl TryFrom<&pg::nodes::A_Const> for Expression {
    type Error = ParseError;

    fn try_from(node: &pg::nodes::A_Const) -> Result<Self, Self::Error> {
        Ok(Expression::constant(DataValue::try_from(&node.val)?))
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
