#![allow(unused)]

use std::time::Duration;

use egg::{define_language, Id, Symbol};

use crate::binder_v2::BoundDrop;
use crate::catalog::{ColumnRefId, TableRefId};
use crate::parser::{BinaryOperator, UnaryOperator};
use crate::types::{ColumnIndex, DataTypeKind, DataValue};

mod cost;
mod explain;
mod rules;

pub use explain::explain;
pub use rules::ExprAnalysis;

// Alias types for our language.
type EGraph = egg::EGraph<Expr, ExprAnalysis>;
type Rewrite = egg::Rewrite<Expr, ExprAnalysis>;
type Pattern = egg::Pattern<Expr>;
pub type RecExpr = egg::RecExpr<Expr>;

define_language! {
    pub enum Expr {
        // values
        Constant(DataValue),            // null, true, 1, 1.0, "hello", ...
        Type(DataTypeKind),             // BOOLEAN, INT, DECIMAL(5), ...
        // Table(TableRefId),              // $1, $2, ...
        Column(ColumnRefId),            // $1.2, $2.1, ...
        ColumnIndex(ColumnIndex),       // #0, #1, ...
        BoundDrop(BoundDrop),

        // binary operations
        "+" = Add([Id; 2]),
        "-" = Sub([Id; 2]),
        "*" = Mul([Id; 2]),
        "/" = Div([Id; 2]),
        "%" = Mod([Id; 2]),
        "||" = StringConcat([Id; 2]),
        ">" = Gt([Id; 2]),
        "<" = Lt([Id; 2]),
        ">=" = GtEq([Id; 2]),
        "<=" = LtEq([Id; 2]),
        "=" = Eq([Id; 2]),
        "<>" = NotEq([Id; 2]),
        "and" = And([Id; 2]),
        "or" = Or([Id; 2]),
        "xor" = Xor([Id; 2]),
        "like" = Like([Id; 2]),

        // unary operations
        "-" = Neg(Id),
        "not" = Not(Id),
        "isnull" = IsNull(Id),

        "if" = If([Id; 3]),                     // (if cond then else)

        // aggregates
        "max" = Max(Id),
        "min" = Min(Id),
        "sum" = Sum(Id),
        "avg" = Avg(Id),
        "count" = Count(Id),
        "rowcount" = RowCount,
        "first" = First(Id),
        "last" = Last(Id),

        // subquery related
        "exists" = Exists(Id),
        "in" = In([Id; 2]),

        "cast" = Cast([Id; 2]),                 // (cast type expr)
        "as" = Alias([Id; 2]),                  // (as name expr)
        "fn" = Function(Box<[Id]>),             // (fn name args..)

        "select" = Select([Id; 6]),             // (select
                                                //      distinct=[expr..]
                                                //      select_list=[expr..]
                                                //      from=join
                                                //      where=expr
                                                //      groupby=[expr..]
                                                //      having=expr
                                                // )
        "distinct" = Distinct([Id; 2]),         // (distinct [expr..] child)

        // plans
        "scan" = Scan(Id),                      // (scan [column..])
        "values" = Values(Box<[Id]>),           // (values [expr..]..)
        "proj" = Proj([Id; 2]),                 // (proj [expr..] child)
        "filter" = Filter([Id; 2]),             // (filter expr child)
        "order" = Order([Id; 2]),               // (order [order_key..] child)
            "asc" = Asc(Id),                        // (asc key)
            "desc" = Desc(Id),                      // (desc key)
        "limit" = Limit([Id; 3]),               // (limit limit offset child)
        "topn" = TopN([Id; 4]),                 // (topn limit offset [order_key..] child)
        "join" = Join([Id; 4]),                 // (join join_type expr left right)
        "hashjoin" = HashJoin([Id; 5]),         // (hashjoin join_type [left_expr..] [right_expr..] left right)
            "inner" = Inner,
            "left_outer" = LeftOuter,
            "right_outer" = RightOuter,
            "full_outer" = FullOuter,
            "cross" = Cross,
        "agg" = Agg([Id; 3]),                   // (agg aggs=[expr..] group_keys=[expr..] child)
                                                    // expressions must be agg
                                                    // output = aggs || group_keys
        "create" = Create([Id; 2]),             // (create table [column_desc..])
        // "drop" = Drop(Id),                      // (drop table)
        "insert" = Insert([Id; 3]),             // (insert table [column..] child)
        "delete" = Delete([Id; 2]),             // (delete table condition=expr)
        "copy_from" = CopyFrom(Id),             // (copy_from dest)
        "copy_to" = CopyTo([Id; 2]),            // (copy_to dest child)
        "explain" = Explain(Id),                // (explain child)

        // utilities
        "list" = List(Box<[Id]>),               // (list ...)

        // internal functions
        "prune" = Prune([Id; 2]),               // (prune node child)
                                                    // do column prune on `child`
                                                    // with the used columns in `node`

        Symbol(Symbol),
    }
}

impl Expr {
    pub const fn true_() -> Self {
        Self::Constant(DataValue::Bool(true))
    }

    pub const fn null() -> Self {
        Self::Constant(DataValue::Null)
    }

    const fn binary_op(&self) -> Option<(BinaryOperator, Id, Id)> {
        use BinaryOperator as Op;
        #[allow(clippy::match_ref_pats)]
        Some(match self {
            &Self::Add([a, b]) => (Op::Plus, a, b),
            &Self::Sub([a, b]) => (Op::Minus, a, b),
            &Self::Mul([a, b]) => (Op::Multiply, a, b),
            &Self::Div([a, b]) => (Op::Divide, a, b),
            &Self::Mod([a, b]) => (Op::Modulo, a, b),
            &Self::StringConcat([a, b]) => (Op::StringConcat, a, b),
            &Self::Gt([a, b]) => (Op::Gt, a, b),
            &Self::Lt([a, b]) => (Op::Lt, a, b),
            &Self::GtEq([a, b]) => (Op::GtEq, a, b),
            &Self::LtEq([a, b]) => (Op::LtEq, a, b),
            &Self::Eq([a, b]) => (Op::Eq, a, b),
            &Self::NotEq([a, b]) => (Op::NotEq, a, b),
            &Self::And([a, b]) => (Op::And, a, b),
            &Self::Or([a, b]) => (Op::Or, a, b),
            &Self::Xor([a, b]) => (Op::Xor, a, b),
            &Self::Like([a, b]) => (Op::Like, a, b),
            _ => return None,
        })
    }

    const fn unary_op(&self) -> Option<(UnaryOperator, Id)> {
        use UnaryOperator as Op;
        #[allow(clippy::match_ref_pats)]
        Some(match self {
            &Self::Neg(a) => (Op::Minus, a),
            &Self::Not(a) => (Op::Not, a),
            _ => return None,
        })
    }
}

/// Optimize the given expression.
pub fn optimize(expr: &RecExpr) -> RecExpr {
    let mut runner = egg::Runner::default()
        // .with_explanations_enabled()
        .with_expr(expr)
        .with_time_limit(Duration::from_secs(1))
        .run(&rules::all_rules());
    // extract the best expression
    let cost_fn = cost::CostFn {
        egraph: &runner.egraph,
    };
    let extractor = egg::Extractor::new(&runner.egraph, cost_fn);
    let root = runner.roots[0];
    let (_, best) = extractor.find_best(root);
    // explain the optimization
    // println!(
    //     "{}",
    //     runner
    //         .explain_equivalence(&expr, &best)
    //         .get_string_with_let()
    // );
    best
}
