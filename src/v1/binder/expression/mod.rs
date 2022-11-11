// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use bitvec::prelude::BitVec;
use serde::Serialize;

use super::*;
use crate::catalog::ColumnRefId;
use crate::parser::{BinaryOperator, DateTimeField, Expr, Function, UnaryOperator, Value};
use crate::types::{DataType, DataTypeKind, DataValue, Interval};

mod agg_call;
mod binary_op;
mod column_ref;
mod expr_with_alias;
mod input_ref;
mod isnull;
mod type_cast;
mod unary_op;

pub use self::agg_call::*;
pub use self::binary_op::*;
pub use self::column_ref::*;
pub use self::expr_with_alias::*;
pub use self::input_ref::*;
pub use self::isnull::*;
pub use self::type_cast::*;
pub use self::unary_op::*;

/// A bound expression.
#[derive(PartialEq, Clone, Serialize)]
pub enum BoundExpr {
    Constant(DataValue),
    ColumnRef(BoundColumnRef),
    /// Only used after column ref is resolved into input ref
    InputRef(BoundInputRef),
    BinaryOp(BoundBinaryOp),
    UnaryOp(BoundUnaryOp),
    TypeCast(BoundTypeCast),
    AggCall(BoundAggCall),
    IsNull(BoundIsNull),
    ExprWithAlias(BoundExprWithAlias),
    Alias(BoundAlias),
}

impl BoundExpr {
    pub fn return_type(&self) -> DataType {
        match self {
            Self::Constant(v) => v.data_type(),
            Self::ColumnRef(expr) => expr.desc.datatype().clone(),
            Self::BinaryOp(expr) => expr.return_type.clone(),
            Self::UnaryOp(expr) => expr.return_type.clone(),
            Self::TypeCast(expr) => expr.ty.clone().nullable(),
            Self::AggCall(expr) => expr.return_type.clone(),
            Self::InputRef(expr) => expr.return_type.clone(),
            Self::IsNull(_) => DataTypeKind::Bool.not_null(),
            Self::ExprWithAlias(expr) => expr.expr.return_type(),
            Self::Alias(expr) => expr.expr.return_type(),
        }
    }

    fn get_filter_column_inner(&self, filter_column: &mut BitVec) {
        struct Visitor<'a>(&'a mut BitVec);
        impl<'a> ExprVisitor for Visitor<'a> {
            fn visit_input_ref(&mut self, expr: &BoundInputRef) {
                self.0.set(expr.index, true)
            }
        }
        Visitor(filter_column).visit_expr(self);
    }

    pub fn get_filter_column(&self, len: usize) -> BitVec {
        let mut filter_column = BitVec::repeat(false, len);
        self.get_filter_column_inner(&mut filter_column);
        filter_column
    }

    pub fn contains_column_ref(&self) -> bool {
        struct Visitor(bool);
        impl ExprVisitor for Visitor {
            fn visit_column_ref(&mut self, _: &BoundColumnRef) {
                self.0 = true;
            }
            fn visit_alias(&mut self, _: &BoundAlias) {
                self.0 = true;
            }
        }
        let mut visitor = Visitor(false);
        visitor.visit_expr(self);
        visitor.0
    }

    pub fn contains_row_count(&self) -> bool {
        struct Visitor(bool);
        impl ExprVisitor for Visitor {
            fn visit_agg_call(&mut self, expr: &BoundAggCall) {
                self.0 = expr.kind == AggKind::RowCount;
            }
        }
        let mut visitor = Visitor(false);
        visitor.visit_expr(self);
        visitor.0
    }

    pub fn resolve_column_ref_id(&self, column_ref_ids: &mut Vec<ColumnRefId>) {
        struct Visitor<'a>(&'a mut Vec<ColumnRefId>);
        impl<'a> ExprVisitor for Visitor<'a> {
            fn visit_column_ref(&mut self, expr: &BoundColumnRef) {
                self.0.push(expr.column_ref_id);
            }
        }
        Visitor(column_ref_ids).visit_expr(self);
    }

    pub fn resolve_input_ref(&self, input_refs: &mut Vec<BoundInputRef>) {
        struct Visitor<'a>(&'a mut Vec<BoundInputRef>);
        impl<'a> ExprVisitor for Visitor<'a> {
            fn visit_input_ref(&mut self, expr: &BoundInputRef) {
                self.0.push(expr.clone());
            }
        }
        Visitor(input_refs).visit_expr(self);
    }

    pub fn contains_agg_call(&self) -> bool {
        struct Visitor(bool);
        impl ExprVisitor for Visitor {
            fn visit_agg_call(&mut self, _: &BoundAggCall) {
                self.0 = true;
            }
        }
        let mut visitor = Visitor(false);
        visitor.visit_expr(self);
        visitor.0
    }

    pub fn format_name(&self, child_schema: &Vec<ColumnDesc>) -> String {
        match self {
            Self::Constant(DataValue::Int64(num)) => format!("{}", num),
            Self::Constant(DataValue::Int32(num)) => format!("{}", num),
            Self::Constant(DataValue::Float64(num)) => format!("{:.3}", num),
            Self::BinaryOp(expr) => {
                let left_expr_name = expr.left_expr.format_name(child_schema);
                let right_expr_name = expr.right_expr.format_name(child_schema);
                format!("{}{}{}", left_expr_name, expr.op, right_expr_name)
            }
            Self::UnaryOp(expr) => {
                let expr_name = expr.expr.format_name(child_schema);
                format!("{}{}", expr.op, expr_name)
            }
            Self::InputRef(expr) => child_schema[expr.index].name().to_string(),
            _ => "".to_string(),
        }
    }
}

impl std::fmt::Debug for BoundExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Constant(expr) => write!(f, "{:?} (const)", expr)?,
            Self::ColumnRef(expr) => write!(f, "Column #{:?}", expr)?,
            Self::BinaryOp(expr) => write!(f, "{:?}", expr)?,
            Self::UnaryOp(expr) => write!(f, "{:?}", expr)?,
            Self::TypeCast(expr) => write!(f, "{:?}", expr)?,
            Self::AggCall(expr) => write!(f, "{:?} (agg)", expr)?,
            Self::InputRef(expr) => write!(f, "InputRef #{:?}", expr)?,
            Self::IsNull(expr) => write!(f, "{:?} (isnull)", expr)?,
            Self::ExprWithAlias(expr) => write!(f, "{:?}", expr)?,
            Self::Alias(expr) => write!(f, "{:?}", expr)?,
        }
        Ok(())
    }
}

impl std::fmt::Display for BoundExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Constant(expr) => write!(f, "{}", expr)?,
            Self::ColumnRef(expr) => write!(f, "Column #{:?}", expr)?,
            Self::BinaryOp(expr) => write!(f, "{}", expr)?,
            Self::UnaryOp(expr) => write!(f, "{:?}", expr)?,
            Self::TypeCast(expr) => write!(f, "{}", expr)?,
            Self::AggCall(expr) => write!(f, "{:?} (agg)", expr)?,
            Self::InputRef(expr) => write!(f, "InputRef #{:?}", expr)?,
            Self::IsNull(expr) => write!(f, "{:?} (isnull)", expr)?,
            Self::ExprWithAlias(expr) => write!(f, "{}", expr)?,
            Self::Alias(expr) => write!(f, "{:?}", expr)?,
        }
        Ok(())
    }
}

impl Binder {
    /// Bind an expression.
    pub fn bind_expr(&mut self, expr: &Expr) -> Result<BoundExpr, BindError> {
        match expr {
            Expr::Value(v) => Ok(BoundExpr::Constant(v.clone().into())),
            Expr::Identifier(ident) => self.bind_column_ref(std::slice::from_ref(ident)),
            Expr::CompoundIdentifier(idents) => self.bind_column_ref(idents),
            Expr::BinaryOp { left, op, right } => self.bind_binary_op(left, op, right),
            Expr::UnaryOp { op, expr } => self.bind_unary_op(op, expr),
            Expr::Nested(expr) => self.bind_expr(expr),
            Expr::Cast { expr, data_type } => self.bind_type_cast(expr, data_type.clone()),
            Expr::Function(func) => self.bind_function(func),
            Expr::IsNull(expr) => self.bind_isnull(expr),
            Expr::IsNotNull(expr) => {
                let expr = self.bind_isnull(expr)?;
                Ok(BoundExpr::UnaryOp(BoundUnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(expr),
                    return_type: DataTypeKind::Bool.not_null(),
                }))
            }
            Expr::TypedString { data_type, value } => self.bind_typed_string(data_type, value),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => self.bind_between(expr, negated, low, high),
            Expr::Interval {
                value,
                leading_field,
                ..
            } => self.bind_interval(value, leading_field),
            _ => todo!("bind expression: {:?}", expr),
        }
    }

    fn bind_typed_string(
        &mut self,
        data_type: &crate::parser::DataType,
        value: &str,
    ) -> Result<BoundExpr, BindError> {
        match data_type {
            crate::parser::DataType::Date => {
                let date = value.parse().map_err(|_| {
                    BindError::CastError(DataValue::String(value.into()), DataTypeKind::Date)
                })?;
                Ok(BoundExpr::Constant(DataValue::Date(date)))
            }
            t => todo!("support typed string: {:?}", t),
        }
    }

    fn bind_between(
        &mut self,
        expr: &Expr,
        negated: &bool,
        low: &Expr,
        high: &Expr,
    ) -> Result<BoundExpr, BindError> {
        use BinaryOperator::{And, Gt, GtEq, Lt, LtEq, Or};

        let (left_op, right_op, final_op) = match negated {
            false => (GtEq, LtEq, And),
            true => (Lt, Gt, Or),
        };

        let left_expr = self.bind_binary_op(expr, &left_op, low)?;
        let right_expr = self.bind_binary_op(expr, &right_op, high)?;
        Ok(BoundExpr::BinaryOp(BoundBinaryOp {
            op: final_op,
            left_expr: Box::new(left_expr),
            right_expr: Box::new(right_expr),
            return_type: DataTypeKind::Bool.not_null(),
        }))
    }

    fn bind_interval(
        &mut self,
        value: &Expr,
        leading_field: &Option<DateTimeField>,
    ) -> Result<BoundExpr, BindError> {
        let Expr::Value(Value::Number(v, _) | Value::SingleQuotedString(v)) = value else {
            panic!("interval value must be number or string");
        };
        let num = v.parse().expect("interval value is not a number");
        let value = DataValue::Interval(match leading_field {
            Some(DateTimeField::Day) => Interval::from_days(num),
            Some(DateTimeField::Month) => Interval::from_months(num),
            Some(DateTimeField::Year) => Interval::from_years(num),
            _ => todo!("Support interval with leading field: {:?}", leading_field),
        });
        Ok(BoundExpr::Constant(value))
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{BinaryOperator, UnaryOperator};

    use crate::catalog::ColumnDesc;
    use crate::types::{DataType, DataTypeKind, DataValue};
    use crate::v1::binder::{BoundBinaryOp, BoundExpr, BoundInputRef, BoundUnaryOp};

    // test when BoundExpr is Constant
    #[test]
    fn test_format_name_constant() {
        let expr = BoundExpr::Constant(DataValue::Int32(1));
        assert_eq!("1", expr.format_name(&vec![]));
        let expr = BoundExpr::Constant(DataValue::Int64(1));
        assert_eq!("1", expr.format_name(&vec![]));
        let expr = BoundExpr::Constant(DataValue::Float64(32.0.into()));
        assert_eq!("32.000", expr.format_name(&vec![]));
    }

    // test when BoundExpr is UnaryOp(form like -a)
    #[test]
    fn test_format_name_unary_op() {
        let data_type = DataType::new(DataTypeKind::Int32, true);
        let expr = BoundExpr::InputRef(BoundInputRef {
            index: 0,
            return_type: data_type.clone(),
        });
        let child_schema = vec![ColumnDesc::new(data_type.clone(), "a".to_string(), false)];
        let expr = BoundExpr::UnaryOp(BoundUnaryOp {
            op: UnaryOperator::Minus,
            expr: Box::new(expr),
            return_type: data_type,
        });
        assert_eq!("-a", expr.format_name(&child_schema));
    }

    // test when BoundExpr is BinaryOp
    #[test]
    fn test_format_name_binary_op() {
        // forms like a + 1
        {
            let left_data_type = DataType::new(DataTypeKind::Int32, true);
            let left_expr = BoundExpr::InputRef(BoundInputRef {
                index: 0,
                return_type: left_data_type.clone(),
            });
            let right_expr = BoundExpr::Constant(DataValue::Int64(1));
            let child_schema = vec![ColumnDesc::new(left_data_type, "a".to_string(), false)];
            let expr = BoundExpr::BinaryOp(BoundBinaryOp {
                op: BinaryOperator::Plus,
                left_expr: Box::new(left_expr),
                right_expr: Box::new(right_expr),
                return_type: DataTypeKind::Int32.nullable(),
            });
            assert_eq!("a+1", expr.format_name(&child_schema));
        }
        // forms like a + b
        {
            let data_type = DataType::new(DataTypeKind::Int32, true);
            let left_expr = BoundExpr::InputRef(BoundInputRef {
                index: 0,
                return_type: data_type.clone(),
            });
            let right_expr = BoundExpr::InputRef(BoundInputRef {
                index: 1,
                return_type: data_type.clone(),
            });
            let child_schema = vec![
                ColumnDesc::new(data_type.clone(), "a".to_string(), false),
                ColumnDesc::new(data_type.clone(), "b".to_string(), false),
            ];

            let expr = BoundExpr::BinaryOp(BoundBinaryOp {
                op: BinaryOperator::Plus,
                left_expr: Box::new(left_expr),
                right_expr: Box::new(right_expr),
                return_type: data_type,
            });
            assert_eq!("a+b", expr.format_name(&child_schema));
        }
    }
}
