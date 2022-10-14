use super::*;
use crate::types::{DataType, DataTypeKind as Kind};

/// The data type of type analysis.
pub type Type = Result<DataType, TypeError>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TypeError {
    /// NULL expression can be any type.
    Null,
    /// The type should be set manually.
    Uninit,
    /// Type is not available for node.
    Unavailable,
    /// Invalid type for function.
    NoFunction { op: String, operands: Vec<Kind> },
}

/// Returns data type of the expression.
pub fn analyze_type(egraph: &EGraph, enode: &Expr) -> Type {
    use Expr::*;
    let x = |i: &Id| egraph[*i].data.type_.clone();
    match enode {
        // values
        Constant(v) => Ok(v.data_type().ok_or(TypeError::Null)?),
        Type(t) => Ok(t.clone().nullable()),
        Column(_) | ColumnIndex(_) => Err(TypeError::Uninit), // binder should set the type

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
        Gt([a, b]) | Lt([a, b]) | GtEq([a, b]) | LtEq([a, b]) | Eq([a, b]) | NotEq([a, b])
        | And([a, b]) | Or([a, b]) | Xor([a, b]) => merge(
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
        (Ok(a), Ok(b)) => {
            assert_eq!(*a, b);
            DidMerge(false, true)
        }
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
