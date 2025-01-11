// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use rust_decimal::Decimal;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Span;

use super::*;
use crate::parser::{
    self, BinaryOperator, DataType, DateTimeField, Expr, Function, FunctionArg, FunctionArgExpr,
    UnaryOperator, Value,
};
use crate::types::{DataValue, Interval};

impl Binder {
    /// Bind an expression.
    pub fn bind_expr(&mut self, expr: Expr) -> Result {
        let id = match expr {
            Expr::Value(v) => {
                // This is okay since only sql udf relies on
                // parameter-like (i.e., `$1`) values at present
                // TODO: consider formally `bind_parameter` in the future
                // e.g., lambda function support, etc.
                if let Value::Placeholder(key) = &v {
                    self.udf_context
                        .get_expr(key)
                        .cloned()
                        .ok_or_else(|| ErrorKind::InvalidSQL.with_spanned(&v))
                } else {
                    Ok(self.egraph.add(Node::Constant(v.into())))
                }
            }
            Expr::Identifier(ident) => self.bind_ident([ident]),
            Expr::CompoundIdentifier(idents) => self.bind_ident(idents),
            Expr::BinaryOp { left, op, right } => self.bind_binary_op(*left, op, *right),
            Expr::UnaryOp { op, expr } => self.bind_unary_op(op, *expr),
            Expr::Nested(expr) => self.bind_expr(*expr),
            Expr::Cast {
                expr, data_type, ..
            } => self.bind_cast(*expr, data_type),
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
            Expr::Interval(interval) => self.bind_interval(interval),
            Expr::Extract { field, expr, .. } => self.bind_extract(field, *expr),
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => self.bind_substring(*expr, substring_from, substring_for),
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => self.bind_case(operand, conditions, results, else_result),
            Expr::InList {
                expr,
                list,
                negated,
            } => self.bind_in_list(*expr, list, negated),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => self.bind_in_subquery(*expr, *subquery, negated),
            Expr::Exists { subquery, negated } => self.bind_exists(*subquery, negated),
            Expr::Subquery(query) => self.bind_subquery(*query),
            _ => todo!("bind expression: {:?}", expr),
        }?;
        self.type_(id)?;
        Ok(id)
    }

    /// Bind a list of expressions.
    pub fn bind_exprs(&mut self, exprs: Vec<Expr>) -> Result {
        let list = exprs
            .into_iter()
            .map(|expr| self.bind_expr(expr))
            .try_collect()?;
        Ok(self.egraph.add(Node::List(list)))
    }

    fn bind_ident(&self, idents: impl IntoIterator<Item = Ident>) -> Result {
        let idents = idents
            .into_iter()
            .map(|ident| Ident::with_span(ident.span, ident.value.to_lowercase()))
            .collect_vec();
        let (_schema_ident, table_ident, column_ident) = match idents.as_slice() {
            [column] => (None, None, column),
            [table, column] => (None, Some(table), column),
            [schema, table, column] => (Some(schema), Some(table), column),
            _ => {
                let span = Span::union_iter(idents.iter().map(|ident| ident.span));
                return Err(ErrorKind::InvalidTableName(idents).with_span(span));
            }
        };

        // Special check for sql udf
        if let Some(id) = self.udf_context.get_expr(&column_ident.value) {
            return Ok(*id);
        }

        self.find_alias(column_ident, table_ident)
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
            Spaceship => Node::VectorCosineDistance([l, r]),
            Custom(name) => match name.as_str() {
                "<->" => Node::VectorL2Distance([l, r]),
                "<#>" => Node::VectorNegtiveInnerProduct([l, r]),
                op => todo!("bind custom binary op: {:?}", op),
            },
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
                    ErrorKind::CastError(
                        DataValue::String(value.into()),
                        crate::types::DataType::Date,
                    )
                })?;
                Ok(self.egraph.add(Node::Constant(DataValue::Date(date))))
            }
            DataType::Timestamp(_, _) => {
                let timestamp = value.parse().map_err(|_| {
                    ErrorKind::CastError(
                        DataValue::String(value.into()),
                        crate::types::DataType::Timestamp,
                    )
                })?;
                Ok(self
                    .egraph
                    .add(Node::Constant(DataValue::Timestamp(timestamp))))
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

    fn bind_interval(&mut self, interval: parser::Interval) -> Result {
        let Expr::Value(Value::Number(v, _) | Value::SingleQuotedString(v)) = *interval.value
        else {
            panic!("interval value must be number or string");
        };
        let num = v.parse().expect("interval value is not a number");
        let value = DataValue::Interval(match interval.leading_field {
            Some(DateTimeField::Day) => Interval::from_days(num),
            Some(DateTimeField::Month) => Interval::from_months(num),
            Some(DateTimeField::Year) => Interval::from_years(num),
            f => todo!("Support interval with leading field: {f:?}"),
        });
        Ok(self.egraph.add(Node::Constant(value)))
    }

    fn bind_extract(&mut self, field: DateTimeField, expr: Expr) -> Result {
        let expr = self.bind_expr(expr)?;
        let field = self.egraph.add(Node::Field(field.into()));
        Ok(self.egraph.add(Node::Extract([field, expr])))
    }

    fn bind_case(
        &mut self,
        operand: Option<Box<Expr>>,
        conditions: Vec<Expr>,
        results: Vec<Expr>,
        else_result: Option<Box<Expr>>,
    ) -> Result {
        let operand = operand.map(|expr| self.bind_expr(*expr)).transpose()?;
        let mut case = match else_result {
            Some(expr) => self.bind_expr(*expr)?,
            None => self.egraph.add(Node::null()),
        };
        for (cond, result) in conditions.into_iter().rev().zip(results.into_iter().rev()) {
            let mut cond = self.bind_expr(cond)?;
            if let Some(operand) = operand {
                cond = self.egraph.add(Node::Eq([operand, cond]));
            }
            let mut result = self.bind_expr(result)?;
            (result, case) = self.implicit_type_cast(result, case)?;
            case = self.egraph.add(Node::If([cond, result, case]));
        }
        Ok(case)
    }

    fn bind_in_list(&mut self, expr: Expr, list: Vec<Expr>, negated: bool) -> Result {
        let expr = self.bind_expr(expr)?;
        let list = self.bind_exprs(list)?;
        let in_list = self.egraph.add(Node::In([expr, list]));
        if negated {
            Ok(self.egraph.add(Node::Not(in_list)))
        } else {
            Ok(in_list)
        }
    }

    fn bind_in_subquery(&mut self, expr: Expr, subquery: Query, negated: bool) -> Result {
        let expr = self.bind_expr(expr)?;
        let (subquery, _) = self.bind_query(subquery)?;
        let in_subquery = self.egraph.add(Node::In([expr, subquery]));
        if negated {
            Ok(self.egraph.add(Node::Not(in_subquery)))
        } else {
            Ok(in_subquery)
        }
    }

    fn bind_exists(&mut self, subquery: Query, negated: bool) -> Result {
        let (subquery, _) = self.bind_query(subquery)?;
        let exists = self.egraph.add(Node::Exists(subquery));
        if negated {
            Ok(self.egraph.add(Node::Not(exists)))
        } else {
            Ok(exists)
        }
    }

    fn bind_subquery(&mut self, subquery: Query) -> Result {
        let (id, _) = self.bind_query(subquery)?;
        Ok(self.egraph.add(Node::Max1Row(id)))
    }

    fn bind_substring(
        &mut self,
        expr: Expr,
        from: Option<Box<Expr>>,
        for_: Option<Box<Expr>>,
    ) -> Result {
        let expr = self.bind_expr(expr)?;
        let from = match from {
            Some(expr) => self.bind_expr(*expr)?,
            None => self.egraph.add(Node::Constant(DataValue::Int32(1))),
        };
        let for_ = match for_ {
            Some(expr) => self.bind_expr(*expr)?,
            None => self.egraph.add(Node::Constant(DataValue::Int32(i32::MAX))),
        };
        Ok(self.egraph.add(Node::Substring([expr, from, for_])))
    }

    fn bind_function(&mut self, func: Function) -> Result {
        let mut distinct = false;
        let function_args = match &func.args {
            FunctionArguments::None => &[],
            FunctionArguments::Subquery(subquery) => {
                return Err(ErrorKind::Todo("subquery argument".into()).with_spanned(&**subquery));
            }
            FunctionArguments::List(arg_list) => {
                distinct = arg_list.duplicate_treatment == Some(DuplicateTreatment::Distinct);
                arg_list.args.as_slice()
            }
        };
        let mut args = vec![];
        for arg in function_args {
            // ignore argument name
            let arg = match arg {
                FunctionArg::Named { arg, .. } => arg,
                FunctionArg::ExprNamed { arg, .. } => arg,
                FunctionArg::Unnamed(arg) => arg,
            };
            match arg {
                FunctionArgExpr::Expr(expr) => args.push(self.bind_expr(expr.clone())?),
                FunctionArgExpr::Wildcard => {
                    // No argument in row count
                    args.clear();
                    break;
                }
                FunctionArgExpr::QualifiedWildcard(_) => {
                    todo!("support qualified wildcard")
                }
            }
        }

        let catalog = self.catalog();
        let Ok((schema_name, function_name)) = split_name(&func.name) else {
            return Err(ErrorKind::BindFunctionError(format!(
                "failed to parse the function name {}",
                func.name
            ))
            .with_spanned(&func.name));
        };

        // See if the input function is sql udf
        if let Some(ref function_catalog) = catalog.get_function_by_name(schema_name, function_name)
        {
            // Create the brand new `udf_context`
            let Ok(context) = UdfContext::create_udf_context(function_args, function_catalog)
            else {
                return Err(
                    ErrorKind::InvalidExpression("failed to create udf context".into())
                        .with_spanned(&func.name),
                );
            };

            let mut udf_context = HashMap::new();
            // Bind each expression in the newly created `udf_context`
            for (c, e) in context {
                let Ok(e) = self.bind_expr(e) else {
                    return Err(ErrorKind::BindFunctionError(
                        "failed to bind arguments within the given sql udf".into(),
                    )
                    .with_spanned(&func.name));
                };
                udf_context.insert(c, e);
            }

            // Parse the sql body using `function_catalog`
            let dialect = GenericDialect {};
            let Ok(ast) = Parser::parse_sql(&dialect, &function_catalog.body) else {
                return Err(ErrorKind::InvalidSQL.with_spanned(&func.name));
            };

            // Extract the corresponding udf expression out from `ast`
            let Ok(expr) = UdfContext::extract_udf_expression(ast) else {
                return Err(ErrorKind::InvalidExpression(
                    "failed to bind the sql udf expression".into(),
                )
                .with_spanned(&func.name));
            };

            let stashed_udf_context = self.udf_context.get_context();

            // Update the `udf_context` in `Binder` before binding
            self.udf_context.update_context(udf_context);

            // Bind the expression in sql udf body
            let Ok(bind_result) = self.bind_expr(expr) else {
                return Err(
                    ErrorKind::InvalidExpression("failed to bind the expression".into())
                        .with_spanned(&func.name),
                );
            };

            // Restore the context after binding
            // to avoid affecting the potential subsequent binding(s)
            self.udf_context.update_context(stashed_udf_context);

            return Ok(bind_result);
        }

        let node = match func.name.to_string().to_lowercase().as_str() {
            "count" if args.is_empty() => Node::RowCount,
            "count" if distinct => Node::CountDistinct(args[0]),
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
            "replace" => Node::Replace([args[0], args[1], args[2]]),
            "row_number" => Node::RowNumber,
            name => todo!("Unsupported function: {}", name),
        };
        let mut id = self.egraph.add(node);
        if let Some(window) = func.over {
            id = self.bind_window_function(id, window, &func.name)?;
        }
        Ok(id)
    }

    fn bind_window_function(&mut self, func: Id, window: WindowType, name: &ObjectName) -> Result {
        let window = match window {
            WindowType::WindowSpec(window) => window,
            WindowType::NamedWindow(name) => {
                return Err(ErrorKind::Todo("named window".into()).with_span(name.span));
            }
        };
        if !self.node(func).is_window_function() {
            return Err(ErrorKind::NotAgg(self.node(func).to_string()).with_spanned(name));
        }
        if !self.overs(func).is_empty() {
            return Err(ErrorKind::NestedWindow.with_spanned(name));
        }
        let partitionby = self.bind_exprs(window.partition_by)?;
        let orderby = self.bind_orderby(window.order_by)?;
        if window.window_frame.is_some() {
            todo!("support window frame");
        }
        Ok(self.egraph.add(Node::Over([func, partitionby, orderby])))
    }

    /// Add optional type cast to the expressions to make them return the same type.
    fn implicit_type_cast(&mut self, mut id1: Id, mut id2: Id) -> Result<(Id, Id)> {
        let ty1 = self.type_(id1)?;
        let ty2 = self.type_(id2)?;
        if let Some(compatible_type) = ty1.union(&ty2) {
            if compatible_type != ty1 {
                let id = self.egraph.add(Node::Type(compatible_type.clone()));
                id1 = self.egraph.add(Node::Cast([id, id1]));
            }
            if compatible_type != ty2 {
                let id = self.egraph.add(Node::Type(compatible_type));
                id2 = self.egraph.add(Node::Cast([id, id2]));
            }
        }
        Ok((id1, id2))
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
            Value::SingleQuotedString(s) => Self::String(s.into()),
            Value::DoubleQuotedString(s) => Self::String(s.into()),
            Value::Boolean(b) => Self::Bool(b),
            Value::Null => Self::Null,
            _ => todo!("parse value: {:?}", v),
        }
    }
}
