use super::*;
use crate::types::{DataType, DataTypeKind as Kind};

/// The data type of type analysis.
pub type Type = Result<DataType, TypeError>;

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq, Hash)]
pub enum TypeError {
    #[error("unknown type from null")]
    Null,
    #[error("the type should be set manually")]
    Uninit,
    #[error("type is not available for node")]
    Unavailable,
    #[error("no function for {op}{operands:?}")]
    NoFunction { op: String, operands: Vec<Kind> },
}

/// Returns data type of the expression.
pub fn analyze_type(egraph: &EGraph, enode: &Expr) -> Type {
    use Expr::*;
    let x = |i: &Id| egraph[*i].data.type_.clone();
    match enode {
        // values
        Constant(v) => Ok(v.data_type().ok_or(TypeError::Null)?),
        Type(t) => Ok((*t).not_null()),
        Column(_) | ColumnIndex(_) => Err(TypeError::Uninit), // binder should set the type

        // cast
        Cast([ty, a]) => merge(enode, [x(ty)?, x(a)?], |_| true, |[ty, _]| ty),

        // number ops
        Neg(a) => x(a),
        Add([a, b]) | Sub([a, b]) | Mul([a, b]) | Div([a, b]) | Mod([a, b]) => {
            merge(enode, [x(a)?, x(b)?], |[a, b]| a == b, |[a, _]| a)
        }

        // string ops
        StringConcat([a, b]) | Like([a, b]) => merge(
            enode,
            [x(a)?, x(b)?],
            |[a, b]| a == Kind::String && b == Kind::String,
            |_| Kind::String,
        ),

        // bool ops
        Not(a) => check(enode, x(a)?, |a| a == Kind::Bool),
        Gt([a, b]) | Lt([a, b]) | GtEq([a, b]) | LtEq([a, b]) | Eq([a, b]) | NotEq([a, b]) => {
            merge(enode, [x(a)?, x(b)?], |[a, b]| a == b, |_| Kind::Bool)
        }
        And([a, b]) | Or([a, b]) | Xor([a, b]) => merge(
            enode,
            [x(a)?, x(b)?],
            |[a, b]| a == Kind::Bool && b == Kind::Bool,
            |_| Kind::Bool,
        ),
        If([cond, then, else_]) => merge(
            enode,
            [x(cond)?, x(then)?, x(else_)?],
            |[cond, then, else_]| cond == Kind::Bool && then == else_,
            |[_, then, _]| then,
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

        // other plan nodes
        _ => Err(TypeError::Unavailable),
    }
}

/// Merge two type analysis results.
pub fn merge_types(to: &mut Type, from: Type) -> DidMerge {
    match (to, from) {
        (Err(_), Err(_)) => DidMerge(false, true),
        (to @ Err(_), from @ Ok(_)) => {
            *to = from;
            DidMerge(true, false)
        }
        (Ok(_), Err(_)) => DidMerge(false, true),
        (Ok(a), Ok(b)) => DidMerge(false, true),
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
    can_merge: impl FnOnce([Kind; N]) -> bool,
    merge: impl FnOnce([Kind; N]) -> Kind,
) -> Type {
    let kinds = types.map(|t| t.kind());
    if can_merge(kinds) {
        Ok(DataType {
            kind: merge(kinds),
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
        assert_type_eq("null", Err(TypeError::Null));
        assert_type_eq("false", Ok(Kind::Bool.not_null()));
        assert_type_eq("true", Ok(Kind::Bool.not_null()));
        assert_type_eq("1", Ok(Kind::Int32.not_null()));
        assert_type_eq("1.0", Ok(Kind::Float64.not_null()));
        assert_type_eq("'hello'", Ok(Kind::String.not_null()));
        assert_type_eq("b'\\xAA'", Ok(Kind::Blob.not_null()));
        assert_type_eq("date'2022-10-14'", Ok(Kind::Date.not_null()));
        assert_type_eq("interval'1_day'", Ok(Kind::Interval.not_null()));
    }

    #[test]
    fn cast() {
        assert_type_eq("(cast INT 1)", Ok(Kind::Int32.not_null()));
        assert_type_eq("(cast INT 1.0)", Ok(Kind::Int32.not_null()));
        // FIXME: cast null
        // assert_type_eq("(cast INT null)", Ok(Kind::Int32.nullable()));
    }

    #[test]
    fn add() {
        assert_type_eq("(+ 1 2)", Ok(Kind::Int32.not_null()));
        assert_type_eq("(+ 1.0 2.0)", Ok(Kind::Float64.not_null()));
        assert_type_eq("(+ null null)", Err(TypeError::Null));
        assert_type_eq(
            "(+ date'2022-10-14' interval'1_day')",
            Ok(Kind::Date.not_null()),
        );
        // FIXME: int + null => int
        // assert_type_eq("(+ 1 null)", Ok(Kind::Int32.nullable()));

        // FIXME: interval + date => date
        // assert_type_eq(
        //     "(+ interval'1_day' date'2022-10-14')",
        //     Ok(Kind::Date.not_null()),
        // );

        // FIXME: bool + bool => error
        // assert_type_eq(
        //     "(+ true false)",
        //     Err(TypeError::NoFunction {
        //         op: "+".into(),
        //         operands: vec![Kind::Bool, Kind::Bool],
        //     }),
        // );

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
        // FIXME: compare compatible types
        // assert_type_eq("(= 1 1.0)", Ok(Kind::Bool.not_null()));
        // assert_type_eq("(= 1 '1')", Ok(Kind::Bool.not_null()));
        // assert_type_eq("(= 1 null)", Ok(Kind::Bool.nullable()));
        // assert_type_eq(
        //     "(= '2022-10-14' date'2022-10-14')",
        //     Ok(Kind::Bool.not_null()),
        // );
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
        let mut egraph = EGraph::default();
        let id = egraph.add_expr(&expr.parse().unwrap());
        egraph[id].data.type_.clone()
    }
}
