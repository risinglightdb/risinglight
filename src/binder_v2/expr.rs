// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use rust_decimal::Decimal;

use super::*;
use crate::catalog::ColumnRefId;
use crate::parser::{
    BinaryOperator, DataType, DateTimeField, Expr, Function, FunctionArg, FunctionArgExpr,
    UnaryOperator, Value,
};
use crate::types::{DataTypeKind, DataValue, Interval};

impl Binder {
    /// Bind an expression.
    pub fn bind_expr(&mut self, expr: Expr) -> Result {
        let id = match expr {
            Expr::Value(v) => Ok(self.egraph.add(Node::Constant(v.into()))),
            Expr::Identifier(ident) => self.bind_ident([ident]),
            Expr::CompoundIdentifier(idents) => self.bind_ident(idents),
            Expr::BinaryOp { left, op, right } => self.bind_binary_op(*left, op, *right),
            Expr::UnaryOp { op, expr } => self.bind_unary_op(op, *expr),
            Expr::Nested(expr) => self.bind_expr(*expr),
            Expr::Cast { expr, data_type } => self.bind_cast(*expr, data_type),
            Expr::Function(func) => self.bind_function(func),
            Expr::IsNull(expr) => self.bind_is_null(*expr),
            Expr::IsNotNull(expr) => {
                let isnull = self.bind_is_null(*expr)?;
                Ok(self.egraph.add(Node::Not(isnull)))
            }
            Expr::TypedString { data_type, value } => self.bind_typed_string(data_type, value),
            Expr::Like {
                negated,
                expr,
                pattern,
                ..
            } => self.bind_like(*expr, *pattern, negated),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => self.bind_between(*expr, negated, *low, *high),
            Expr::Interval {
                value,
                leading_field,
                ..
            } => self.bind_interval(*value, leading_field),
            _ => todo!("bind expression: {:?}", expr),
        }?;
        self.check_type(id)?;
        Ok(id)
    }

    fn bind_ident(&mut self, idents: impl IntoIterator<Item = Ident>) -> Result {
        let idents = idents
            .into_iter()
            .map(|ident| Ident::new(ident.value.to_lowercase()))
            .collect_vec();
        let (_schema_name, table_name, column_name) = match idents.as_slice() {
            [column] => (None, None, &column.value),
            [table, column] => (None, Some(&table.value), &column.value),
            [schema, table, column] => (Some(&schema.value), Some(&table.value), &column.value),
            _ => return Err(BindError::InvalidTableName(idents)),
        };
        if let Some(name) = table_name {
            let table_ref_id = *self
                .current_ctx()
                .tables
                .get(name)
                .ok_or_else(|| BindError::InvalidTable(name.clone()))?;
            let table = self.catalog.get_table(&table_ref_id).unwrap();
            let col = table
                .get_column_by_name(column_name)
                .ok_or_else(|| BindError::InvalidColumn(column_name.into()))?;
            let column_ref_id = ColumnRefId::from_table(table_ref_id, col.id());
            return Ok(self.egraph.add(Node::Column(column_ref_id)));
        }
        // find column in all tables
        let mut column_ids = self.current_ctx().tables.values().filter_map(|table_id| {
            self.catalog
                .get_table(table_id)
                .unwrap()
                .get_column_by_name(column_name)
                .map(|col| ColumnRefId::from_table(*table_id, col.id()))
        });

        if let Some(column_ref_id) = column_ids.next() {
            if column_ids.next().is_some() {
                return Err(BindError::AmbiguousColumn(column_name.into()));
            }
            let id = self.egraph.add(Node::Column(column_ref_id));
            return Ok(id);
        }
        if let Some(id) = self.current_ctx().aliases.get(column_name) {
            return Ok(*id);
        }
        Err(BindError::InvalidColumn(column_name.into()))
    }

    fn bind_binary_op(&mut self, left: Expr, op: BinaryOperator, right: Expr) -> Result {
        use BinaryOperator::*;

        let l = self.bind_expr(left)?;
        let r = self.bind_expr(right)?;
        let node = match op {
            Plus => Node::Add([l, r]),
            Minus => Node::Sub([l, r]),
            Multiply => Node::Mul([l, r]),
            Divide => Node::Div([l, r]),
            Modulo => Node::Mod([l, r]),
            StringConcat => Node::StringConcat([l, r]),
            Gt => Node::Gt([l, r]),
            Lt => Node::Lt([l, r]),
            GtEq => Node::GtEq([l, r]),
            LtEq => Node::LtEq([l, r]),
            Eq => Node::Eq([l, r]),
            NotEq => Node::NotEq([l, r]),
            And => Node::And([l, r]),
            Or => Node::Or([l, r]),
            Xor => Node::Xor([l, r]),
            _ => todo!("bind binary op: {:?}", op),
        };
        Ok(self.egraph.add(node))
    }

    fn bind_unary_op(&mut self, op: UnaryOperator, expr: Expr) -> Result {
        use UnaryOperator::*;
        let expr = self.bind_expr(expr)?;
        Ok(match op {
            Plus => expr,
            Minus => self.egraph.add(Node::Neg(expr)),
            Not => self.egraph.add(Node::Not(expr)),
            _ => todo!("bind unary operator: {:?}", op),
        })
    }

    fn bind_cast(&mut self, expr: Expr, mut ty: DataType) -> Result {
        let expr = self.bind_expr(expr)?;
        // workaround for 'BLOB'
        if let DataType::Custom(name, _modifiers) = &ty {
            if name.0.len() == 1 && name.0[0].value.to_lowercase() == "blob" {
                ty = DataType::Blob(None);
            }
        }
        let ty = self.egraph.add(Node::Type((&ty).into()));
        Ok(self.egraph.add(Node::Cast([ty, expr])))
    }

    fn bind_is_null(&mut self, expr: Expr) -> Result {
        let expr = self.bind_expr(expr)?;
        Ok(self.egraph.add(Node::IsNull(expr)))
    }

    fn bind_typed_string(&mut self, data_type: DataType, value: String) -> Result {
        match data_type {
            DataType::Date => {
                let date = value.parse().map_err(|_| {
                    BindError::CastError(DataValue::String(value), DataTypeKind::Date)
                })?;
                Ok(self.egraph.add(Node::Constant(DataValue::Date(date))))
            }
            t => todo!("support typed string: {:?}", t),
        }
    }

    fn bind_like(&mut self, expr: Expr, pattern: Expr, negated: bool) -> Result {
        let expr = self.bind_expr(expr)?;
        let pattern = self.bind_expr(pattern)?;
        let like = self.egraph.add(Node::Like([expr, pattern]));
        if negated {
            Ok(self.egraph.add(Node::Not(like)))
        } else {
            Ok(like)
        }
    }

    fn bind_between(&mut self, expr: Expr, negated: bool, low: Expr, high: Expr) -> Result {
        let expr = self.bind_expr(expr)?;
        let low = self.bind_expr(low)?;
        let high = self.bind_expr(high)?;
        let left = self.egraph.add(Node::GtEq([expr, low]));
        let right = self.egraph.add(Node::LtEq([expr, high]));
        let between = self.egraph.add(Node::And([left, right]));
        if negated {
            Ok(self.egraph.add(Node::Not(between)))
        } else {
            Ok(between)
        }
    }

    fn bind_interval(&mut self, value: Expr, leading_field: Option<DateTimeField>) -> Result {
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
        Ok(self.egraph.add(Node::Constant(value)))
    }

    fn bind_function(&mut self, func: Function) -> Result {
        // TODO: Support scalar function
        let mut args = vec![];
        for arg in func.args {
            // ignore argument name
            let arg = match arg {
                FunctionArg::Named { arg, .. } => arg,
                FunctionArg::Unnamed(arg) => arg,
            };
            match arg {
                FunctionArgExpr::Expr(expr) => args.push(self.bind_expr(expr)?),
                FunctionArgExpr::Wildcard => {
                    // No argument in row count
                    args.clear();
                    break;
                }
                FunctionArgExpr::QualifiedWildcard(_) => todo!("support qualified wildcard"),
            }
        }
        let node = match func.name.to_string().to_lowercase().as_str() {
            "count" if args.is_empty() => Node::RowCount,
            "count" => Node::Count(args[0]),
            "max" => Node::Max(args[0]),
            "min" => Node::Min(args[0]),
            "sum" => Node::Sum(args[0]),
            "avg" => {
                let sum = self.egraph.add(Node::Sum(args[0]));
                let count = self.egraph.add(Node::Count(args[0]));
                Node::Div([sum, count])
            }
            "first" => Node::First(args[0]),
            "last" => Node::Last(args[0]),
            name => todo!("Unsupported function: {}", name),
        };
        Ok(self.egraph.add(node))
    }
}

impl From<Value> for DataValue {
    fn from(v: Value) -> Self {
        match v {
            Value::Number(n, _) => {
                if let Ok(int) = n.parse::<i32>() {
                    Self::Int32(int)
                } else if let Ok(bigint) = n.parse::<i64>() {
                    Self::Int64(bigint)
                } else if let Ok(decimal) = n.parse::<Decimal>() {
                    Self::Decimal(decimal)
                } else {
                    panic!("invalid digit: {}", n);
                }
            }
            Value::SingleQuotedString(s) => Self::String(s),
            Value::DoubleQuotedString(s) => Self::String(s),
            Value::Boolean(b) => Self::Bool(b),
            Value::Null => Self::Null,
            _ => todo!("parse value: {:?}", v),
        }
    }
}
