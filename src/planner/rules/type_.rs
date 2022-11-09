use super::*;
use crate::types::{DataType, DataTypeKind as Kind};

/// The data type of type analysis.
pub type Type = Result<DataType, TypeError>;

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TypeError {
    #[error("type is not available for node")]
    Unavailable,
    #[error("no function for {op}{operands:?}")]
    NoFunction { op: String, operands: Vec<Kind> },
    #[error("no cast {from} -> {to}")]
    NoCast { from: Kind, to: Kind },
}

/// Returns data type of the expression.
pub fn analyze_type(enode: &Expr, x: impl Fn(&Id) -> Type, catalog: &RootCatalogRef) -> Type {
    use Expr::*;
    let concat_struct = |t1: DataType, t2: DataType| match (t1.kind, t2.kind) {
        (Kind::Struct(l), Kind::Struct(r)) => {
            Ok(Kind::Struct(l.into_iter().chain(r).collect()).not_null())
        }
        _ => panic!("not struct type"),
    };
    match enode {
        // values
        Constant(v) => Ok(v.data_type()),
        Type(t) => Ok(t.clone().not_null()),
        Column(col) => Ok(catalog
            .get_column(col)
            .ok_or(TypeError::Unavailable)?
            .datatype()),

        Nested(a) => x(a),
        List(list) => Ok(Kind::Struct(list.iter().map(x).try_collect()?).not_null()),

        // cast
        Cast([ty, a]) => merge(enode, [x(ty)?, x(a)?], |[ty, _]| Some(ty)),

        // number ops
        Neg(a) => check(enode, x(a)?, |a| a.is_number()),
        Add([a, b]) | Sub([a, b]) | Mul([a, b]) | Div([a, b]) | Mod([a, b]) => {
            merge(enode, [x(a)?, x(b)?], |[a, b]| {
                match if a > b { (b, a) } else { (a, b) } {
                    (Kind::Null, _) => Some(Kind::Null),
                    (a, b) if a.is_number() && b.is_number() => Some(b),
                    (Kind::Date, Kind::Interval) => Some(Kind::Date),
                    _ => None,
                }
            })
        }

        // string ops
        StringConcat([a, b]) | Like([a, b]) => merge(enode, [x(a)?, x(b)?], |[a, b]| {
            (a == Kind::String && b == Kind::String).then_some(Kind::String)
        }),

        // bool ops
        Not(a) => check(enode, x(a)?, |a| a == Kind::Bool),
        Gt([a, b]) | Lt([a, b]) | GtEq([a, b]) | LtEq([a, b]) | Eq([a, b]) | NotEq([a, b]) => {
            merge(enode, [x(a)?, x(b)?], |[a, b]| {
                (a.is_number() && b.is_number()
                    || a == b
                    || (a == Kind::String || b == Kind::String)
                    || (a == Kind::Null || b == Kind::Null))
                    .then_some(Kind::Bool)
            })
        }
        And([a, b]) | Or([a, b]) | Xor([a, b]) => merge(enode, [x(a)?, x(b)?], |[a, b]| {
            (a == Kind::Bool && b == Kind::Bool).then_some(Kind::Bool)
        }),
        If([cond, then, else_]) => merge(
            enode,
            [x(cond)?, x(then)?, x(else_)?],
            |[cond, then, else_]| (cond == Kind::Bool && then == else_).then_some(then),
        ),

        // null ops
        IsNull(_) => Ok(Kind::Bool.not_null()),

        // number agg
        Max(a) | Min(a) => x(a),
        Sum(a) => check(enode, x(a)?, |a| a.is_number()),
        Avg(a) => check(enode, x(a)?, |a| a.is_number()),

        // agg
        RowCount | Count(_) => Ok(Kind::Int32.not_null()),
        First(a) | Last(a) => x(a),

        // equal to child
        Filter([_, c]) | Order([_, c]) | Limit([_, _, c]) | TopN([_, _, _, c]) => x(c),

        // concat 2 children
        Join([_, _, l, r]) | HashJoin([_, _, _, l, r]) => concat_struct(x(l)?, x(r)?),

        // plans that change schema
        Scan([_, columns]) => x(columns),
        Values(rows) => {
            if rows.is_empty() {
                return Ok(Kind::Null.not_null());
            }
            let mut type_ = x(&rows[0])?;
            for row in rows.iter().skip(1) {
                let ty = x(row)?;
                type_ = type_.union(&ty).ok_or(TypeError::NoCast {
                    from: ty.kind,
                    to: type_.kind,
                })?;
            }
            Ok(type_)
        }
        Proj([exprs, _]) => x(exprs),
        Agg([exprs, group_keys, _]) => concat_struct(x(exprs)?, x(group_keys)?),
        Empty(ids) => {
            let mut types = vec![];
            for id in ids.iter() {
                let Kind::Struct(list) = x(id)?.kind else { panic!("not struct type") };
                types.extend(list);
            }
            Ok(Kind::Struct(types).not_null())
        }

        // other plan nodes
        _ => Err(TypeError::Unavailable),
    }
}

fn check(enode: &Expr, a: DataType, check: impl FnOnce(Kind) -> bool) -> Type {
    if check(a.kind()) {
        Ok(a)
    } else {
        Err(TypeError::NoFunction {
            op: enode.to_string(),
            operands: vec![a.kind()],
        })
    }
}

fn merge<const N: usize>(
    enode: &Expr,
    types: [DataType; N],
    merge: impl FnOnce([Kind; N]) -> Option<Kind>,
) -> Type {
    let kinds = types.each_ref().map(|t| t.kind());
    if let Some(kind) = merge(kinds.clone()) {
        Ok(DataType {
            kind,
            nullable: types.map(|t| t.nullable).iter().any(|b| *b),
        })
    } else {
        Err(TypeError::NoFunction {
            op: enode.to_string(),
            operands: kinds.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value() {
        assert_type_eq("null", Ok(Kind::Null.nullable()));
        assert_type_eq("false", Ok(Kind::Bool.not_null()));
        assert_type_eq("true", Ok(Kind::Bool.not_null()));
        assert_type_eq("1", Ok(Kind::Int32.not_null()));
        assert_type_eq("1.0", Ok(Kind::Decimal(None, None).not_null()));
        assert_type_eq("'hello'", Ok(Kind::String.not_null()));
        assert_type_eq("b'\\xAA'", Ok(Kind::Blob.not_null()));
        assert_type_eq("date'2022-10-14'", Ok(Kind::Date.not_null()));
        assert_type_eq("interval'1_day'", Ok(Kind::Interval.not_null()));
    }

    #[test]
    fn cast() {
        assert_type_eq("(cast INT 1)", Ok(Kind::Int32.not_null()));
        assert_type_eq("(cast INT 1.0)", Ok(Kind::Int32.not_null()));
        assert_type_eq("(cast INT null)", Ok(Kind::Int32.nullable()));
    }

    #[test]
    fn add() {
        assert_type_eq("(+ 1 2)", Ok(Kind::Int32.not_null()));
        assert_type_eq("(+ 1 1.0)", Ok(Kind::Decimal(None, None).not_null()));
        assert_type_eq("(+ 1.0 2.0)", Ok(Kind::Decimal(None, None).not_null()));
        assert_type_eq("(+ null null)", Ok(Kind::Null.nullable()));
        assert_type_eq(
            "(+ date'2022-10-14' interval'1_day')",
            Ok(Kind::Date.not_null()),
        );
        assert_type_eq(
            "(+ interval'1_day' date'2022-10-14')",
            Ok(Kind::Date.not_null()),
        );
        assert_type_eq("(+ 1 null)", Ok(Kind::Null.nullable()));

        assert_type_eq(
            "(+ true false)",
            Err(TypeError::NoFunction {
                op: "+".into(),
                operands: vec![Kind::Bool, Kind::Bool],
            }),
        );

        assert_type_eq(
            "(+ 1 false)",
            Err(TypeError::NoFunction {
                op: "+".into(),
                operands: vec![Kind::Int32, Kind::Bool],
            }),
        );
    }

    #[test]
    fn cmp() {
        assert_type_eq("(= 1 1)", Ok(Kind::Bool.not_null()));
        assert_type_eq("(= 1 1.0)", Ok(Kind::Bool.not_null()));
        assert_type_eq("(= 1 '1')", Ok(Kind::Bool.not_null()));
        assert_type_eq("(= 1 null)", Ok(Kind::Bool.nullable()));
        assert_type_eq(
            "(= '2022-10-14' date'2022-10-14')",
            Ok(Kind::Bool.not_null()),
        );
        assert_type_eq(
            "(= date'2022-10-14' 1)",
            Err(TypeError::NoFunction {
                op: "=".into(),
                operands: vec![Kind::Date, Kind::Int32],
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
