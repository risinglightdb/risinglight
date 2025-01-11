// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::types::DataType;

/// The data type of type analysis.
pub type Type = Result<DataType, TypeError>;

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TypeError {
    #[error("type is not available for node {0:?}")]
    Unavailable(String),
    #[error("no function for {op}{operands:?}")]
    NoFunction { op: String, operands: Vec<DataType> },
    #[error("no cast {from} -> {to}")]
    NoCast { from: DataType, to: DataType },
}

/// Returns data type of the expression.
pub fn analyze_type(
    enode: &Expr,
    x: impl Fn(&Id) -> Type,
    node0: impl Fn(&Id) -> Expr,
    catalog: &RootCatalogRef,
) -> Type {
    use Expr::*;
    let concat_struct = |t1: DataType, t2: DataType| match (t1, t2) {
        (DataType::Struct(l), DataType::Struct(r)) => {
            Ok(DataType::Struct(l.into_iter().chain(r).collect()))
        }
        _ => panic!("not struct type"),
    };
    match enode {
        // values
        Constant(v) => Ok(v.data_type()),
        Type(t) => Ok(t.clone()),
        Column(col) => Ok(catalog
            .get_column(col)
            .ok_or_else(|| TypeError::Unavailable(enode.to_string()))?
            .data_type()),
        Ref(a) => x(a),
        List(list) => Ok(DataType::Struct(list.iter().map(x).try_collect()?)),

        // cast
        Cast([ty, a]) => merge(enode, [x(ty)?, x(a)?], |[ty, _]| Some(ty)),

        // number ops
        Neg(a) => check(enode, x(a)?, |a| a.is_number()),
        Add([a, b]) | Sub([a, b]) | Mul([a, b]) | Div([a, b]) | Mod([a, b]) => {
            merge(enode, [x(a)?, x(b)?], |[a, b]| {
                match if a > b { (b, a) } else { (a, b) } {
                    (DataType::Null, _) => Some(DataType::Null),
                    (
                        DataType::Decimal(Some(p1), Some(s1)),
                        DataType::Decimal(Some(p2), Some(s2)),
                    ) => match enode {
                        Add(_) | Sub(_) => Some(DataType::Decimal(
                            Some((p1 - s1).max(p2 - s2) + s1.max(s2) + 1),
                            Some(s1.max(s2)),
                        )),
                        Mul(_) => Some(DataType::Decimal(Some(p1 + p2), Some(s1 + s2))),
                        Div(_) | Mod(_) => Some(DataType::Decimal(None, None)),
                        _ => unreachable!(),
                    },
                    (a, b) if a.is_number() && b.is_number() => Some(b),
                    (DataType::Date, DataType::Interval) => Some(DataType::Date),
                    _ => None,
                }
            })
        }

        // string ops
        StringConcat([a, b]) => merge(enode, [x(a)?, x(b)?], |[a, b]| {
            (a == DataType::String && b == DataType::String).then_some(DataType::String)
        }),
        Like([a, b]) => merge(enode, [x(a)?, x(b)?], |[a, b]| {
            (a == DataType::String && b == DataType::String).then_some(DataType::Bool)
        }),

        // vector ops
        VectorL2Distance([a, b])
        | VectorCosineDistance([a, b])
        | VectorNegtiveInnerProduct([a, b]) => merge(enode, [x(a)?, x(b)?], |[a, b]| {
            (matches!(a, DataType::Vector(_)) && matches!(b, DataType::Vector(_)))
                .then_some(DataType::Float64)
        }),

        // bool ops
        Not(a) => check(enode, x(a)?, |a| a == &DataType::Bool),
        Gt([a, b]) | Lt([a, b]) | GtEq([a, b]) | LtEq([a, b]) | Eq([a, b]) | NotEq([a, b]) => {
            merge(enode, [x(a)?, x(b)?], |[a, b]| {
                (a.is_number() && b.is_number()
                    || a == b
                    || (a == DataType::String || b == DataType::String)
                    || (a == DataType::Null || b == DataType::Null))
                    .then_some(DataType::Bool)
            })
        }
        And([a, b]) | Or([a, b]) | Xor([a, b]) => merge(enode, [x(a)?, x(b)?], |[a, b]| {
            (matches!(a, DataType::Bool | DataType::Null)
                && matches!(b, DataType::Bool | DataType::Null))
            .then_some(DataType::Bool)
        }),
        If([cond, then, else_]) => merge(
            enode,
            [x(cond)?, x(then)?, x(else_)?],
            |[cond, then, else_]| (cond == DataType::Bool && then == else_).then_some(then),
        ),
        In([expr, list]) => {
            let expr = x(expr)?;
            let list = x(list)?;
            if list.as_struct().iter().any(|t| t != &expr) {
                return Err(TypeError::NoFunction {
                    op: "in".into(),
                    operands: vec![expr, list],
                });
            }
            Ok(DataType::Bool)
        }
        Exists(_) => Ok(DataType::Bool),

        // null ops
        IsNull(_) => Ok(DataType::Bool),

        // functions
        Extract([_, a]) => merge(enode, [x(a)?], |[a]| {
            matches!(a, DataType::Date | DataType::Interval).then_some(DataType::Int32)
        }),
        Substring([str, start, len]) => {
            merge(enode, [x(str)?, x(start)?, x(len)?], |[str, start, len]| {
                (str == DataType::String && start == DataType::Int32 && len == DataType::Int32)
                    .then_some(DataType::String)
            })
        }

        // number agg
        Max(a) | Min(a) => x(a),
        Sum(a) => check(enode, x(a)?, |a| a.is_number()),
        Avg(a) => check(enode, x(a)?, |a| a.is_number()),

        // agg
        RowCount | RowNumber | Count(_) | CountDistinct(_) => Ok(DataType::Int32),
        First(a) | Last(a) => x(a),
        Over([f, _, _]) => x(f),

        // scalar functions
        Replace([a, from, to]) => merge(enode, [x(a)?, x(from)?, x(to)?], |[a, from, to]| {
            (a == DataType::String && from == DataType::String && to == DataType::String)
                .then_some(DataType::String)
        }),

        // equal to child
        Filter([_, c]) | Order([_, c]) | Limit([_, _, c]) | TopN([_, _, _, c]) | Empty(c) => x(c),

        // concat 2 children
        Join([t, _, l, r]) | HashJoin([t, _, _, _, l, r]) | MergeJoin([t, _, _, _, l, r]) => {
            match node0(t) {
                Semi | Anti => x(l),
                _ => concat_struct(x(l)?, x(r)?),
            }
        }

        // plans that change schema
        Scan([_, columns, _]) => x(columns),
        Values(rows) => {
            if rows.is_empty() {
                return Ok(DataType::Null);
            }
            let mut type_ = x(&rows[0])?;
            for row in rows.iter().skip(1) {
                let ty = x(row)?;
                type_ = type_.union(&ty).ok_or(TypeError::NoCast {
                    from: ty,
                    to: type_,
                })?;
            }
            Ok(type_)
        }
        Proj([exprs, _]) | Agg([exprs, _]) => x(exprs),
        Window([exprs, c]) => concat_struct(x(c)?, x(exprs)?),
        HashAgg([keys, aggs, _]) | SortAgg([keys, aggs, _]) => concat_struct(x(keys)?, x(aggs)?),
        Max1Row(c) => Ok(x(c)?.as_struct()[0].clone()),

        // other plan nodes
        _ => Err(TypeError::Unavailable(enode.to_string())),
    }
}

fn check(enode: &Expr, a: DataType, check: impl FnOnce(&DataType) -> bool) -> Type {
    if check(&a) {
        Ok(a)
    } else {
        Err(TypeError::NoFunction {
            op: enode.to_string(),
            operands: vec![a],
        })
    }
}

fn merge<const N: usize>(
    enode: &Expr,
    types: [DataType; N],
    merge: impl FnOnce([DataType; N]) -> Option<DataType>,
) -> Type {
    if let Some(ty) = merge(types.clone()) {
        Ok(ty)
    } else {
        Err(TypeError::NoFunction {
            op: enode.to_string(),
            operands: types.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value() {
        assert_type_eq("null", Ok(DataType::Null));
        assert_type_eq("false", Ok(DataType::Bool));
        assert_type_eq("true", Ok(DataType::Bool));
        assert_type_eq("1", Ok(DataType::Int32));
        assert_type_eq("1.0", Ok(DataType::Decimal(None, None)));
        assert_type_eq("'hello'", Ok(DataType::String));
        assert_type_eq("b'\\xAA'", Ok(DataType::Blob));
        assert_type_eq("date'2022-10-14'", Ok(DataType::Date));
        assert_type_eq("interval'1_day'", Ok(DataType::Interval));
    }

    #[test]
    fn cast() {
        assert_type_eq("(cast INT 1)", Ok(DataType::Int32));
        assert_type_eq("(cast INT 1.0)", Ok(DataType::Int32));
        assert_type_eq("(cast INT null)", Ok(DataType::Int32));
    }

    #[test]
    fn add() {
        assert_type_eq("(+ 1 2)", Ok(DataType::Int32));
        assert_type_eq("(+ 1 1.0)", Ok(DataType::Decimal(None, None)));
        assert_type_eq("(+ 1.0 2.0)", Ok(DataType::Decimal(None, None)));
        assert_type_eq("(+ null null)", Ok(DataType::Null));
        assert_type_eq("(+ date'2022-10-14' interval'1_day')", Ok(DataType::Date));
        assert_type_eq("(+ interval'1_day' date'2022-10-14')", Ok(DataType::Date));
        assert_type_eq("(+ 1 null)", Ok(DataType::Null));

        assert_type_eq(
            "(+ true false)",
            Err(TypeError::NoFunction {
                op: "+".into(),
                operands: vec![DataType::Bool, DataType::Bool],
            }),
        );

        assert_type_eq(
            "(+ 1 false)",
            Err(TypeError::NoFunction {
                op: "+".into(),
                operands: vec![DataType::Int32, DataType::Bool],
            }),
        );
    }

    #[test]
    fn cmp() {
        assert_type_eq("(= 1 1)", Ok(DataType::Bool));
        assert_type_eq("(= 1 1.0)", Ok(DataType::Bool));
        assert_type_eq("(= 1 '1')", Ok(DataType::Bool));
        assert_type_eq("(= 1 null)", Ok(DataType::Bool));
        assert_type_eq("(= '2022-10-14' date'2022-10-14')", Ok(DataType::Bool));
        assert_type_eq(
            "(= date'2022-10-14' 1)",
            Err(TypeError::NoFunction {
                op: "=".into(),
                operands: vec![DataType::Date, DataType::Int32],
            }),
        );
    }

    #[track_caller]
    fn assert_type_eq(expr: &str, expected: Type) {
        assert_eq!(type_of(expr), expected);
    }

    fn type_of(expr: &str) -> Type {
        let mut egraph = egg::EGraph::<Expr, TypeSchemaAnalysis>::default();
        let id = egraph.add_expr(&expr.parse().unwrap());
        egraph[id].data.type_.clone()
    }
}
