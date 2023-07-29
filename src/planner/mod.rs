// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use egg::{define_language, Id, Symbol};

use crate::binder::copy::ExtSource;
use crate::binder::{BoundDrop, CreateTable};
use crate::catalog::{ColumnRefId, TableRefId};
use crate::parser::{BinaryOperator, UnaryOperator};
use crate::types::{ColumnIndex, DataTypeKind, DataValue, DateTimeField};

mod cost;
mod explain;
mod optimizer;
mod rules;

pub use explain::Explain;
pub use optimizer::{Config, Optimizer};
pub use rules::{ExprAnalysis, TypeError, TypeSchemaAnalysis};

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
        Column(ColumnRefId),            // $1.2, $2.1, ...
        Table(TableRefId),              // $1, $2, ...
        ColumnIndex(ColumnIndex),       // #0, #1, ...
        ExtSource(ExtSource),

        // utilities
        "ref" = Ref(Id),                // (ref expr)
                                            // refer the expr as a column
                                            // it can also prevent optimization
        "list" = List(Box<[Id]>),       // (list ...)

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

        // functions
        "extract" = Extract([Id; 2]),           // (extract field expr)
            Field(DateTimeField),
        "replace" = Replace([Id; 3]),           // (replace expr pattern replacement)

        // aggregations
        "max" = Max(Id),
        "min" = Min(Id),
        "sum" = Sum(Id),
        "avg" = Avg(Id),
        "count" = Count(Id),
        "rowcount" = RowCount,
        "first" = First(Id),
        "last" = Last(Id),
        // window functions
        "over" = Over([Id; 3]),                 // (over window_function [partition_key..] [order_key..])
        // TODO: support frame clause
            // "range" = Range([Id; 2]),               // (range start end)
        "row_number" = RowNumber,

        // subquery related
        "exists" = Exists(Id),
        "in" = In([Id; 2]),                     // (in expr list)

        "cast" = Cast([Id; 2]),                 // (cast type expr)

        // plans
        "scan" = Scan([Id; 3]),                 // (scan table [column..] filter)
        "internal" = Internal([Id; 2]),         // (internal table [column..])
        "values" = Values(Box<[Id]>),           // (values [expr..]..)
        "proj" = Proj([Id; 2]),                 // (proj [expr..] child)
        "filter" = Filter([Id; 2]),             // (filter expr child)
        "order" = Order([Id; 2]),               // (order [order_key..] child)
            "desc" = Desc(Id),                      // (desc key)
        "limit" = Limit([Id; 3]),               // (limit limit offset child)
        "topn" = TopN([Id; 4]),                 // (topn limit offset [order_key..] child)
        "join" = Join([Id; 4]),                 // (join join_type expr left right)
        "hashjoin" = HashJoin([Id; 5]),         // (hashjoin join_type [left_expr..] [right_expr..] left right)
        "mergejoin" = MergeJoin([Id; 5]),       // (mergejoin join_type [left_expr..] [right_expr..] left right)
            "inner" = Inner,
            "left_outer" = LeftOuter,
            "right_outer" = RightOuter,
            "full_outer" = FullOuter,
        "agg" = Agg([Id; 2]),                   // (agg aggs=[expr..] child)
                                                    // expressions must be aggregate functions
        "hashagg" = HashAgg([Id; 3]),           // (hashagg aggs=[expr..] group_keys=[expr..] child)
                                                    // output = aggs || group_keys
        "sortagg" = SortAgg([Id; 3]),           // (sortagg aggs=[expr..] group_keys=[expr..] child)
                                                    // child must be ordered by group_keys
        "window" = Window([Id; 2]),             // (window [over..] child)
                                                    // output = child || exprs
        CreateTable(CreateTable),
        Drop(BoundDrop),
        "insert" = Insert([Id; 3]),             // (insert table [column..] child)
        "delete" = Delete([Id; 2]),             // (delete table child)
        "copy_from" = CopyFrom([Id; 2]),        // (copy_from dest types)
        "copy_to" = CopyTo([Id; 2]),            // (copy_to dest child)
        "explain" = Explain(Id),                // (explain child)

        // internal functions
        "empty" = Empty(Box<[Id]>),             // (empty child..)
                                                    // returns empty chunk
                                                    // with the same schema as `child`

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

    pub const fn zero() -> Self {
        Self::Constant(DataValue::Int32(0))
    }

    pub fn as_const(&self) -> DataValue {
        let Self::Constant(v) = self else { panic!("not a constant: {self}") };
        v.clone()
    }

    pub fn as_list(&self) -> &[Id] {
        let Self::List(l) = self else { panic!("not a list: {self}") };
        l
    }

    pub fn as_column(&self) -> ColumnRefId {
        let Self::Column(c) = self else { panic!("not a columnn: {self}") };
        *c
    }

    pub fn as_table(&self) -> TableRefId {
        let Self::Table(t) = self else { panic!("not a table: {self}") };
        *t
    }

    pub fn as_type(&self) -> &DataTypeKind {
        let Self::Type(t) = self else { panic!("not a type: {self}") };
        t
    }

    pub fn as_ext_source(&self) -> ExtSource {
        let Self::ExtSource(v) = self else { panic!("not an external source: {self}") };
        v.clone()
    }

    pub const fn binary_op(&self) -> Option<(BinaryOperator, Id, Id)> {
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
            _ => return None,
        })
    }

    pub const fn unary_op(&self) -> Option<(UnaryOperator, Id)> {
        use UnaryOperator as Op;
        #[allow(clippy::match_ref_pats)]
        Some(match self {
            &Self::Neg(a) => (Op::Minus, a),
            &Self::Not(a) => (Op::Not, a),
            _ => return None,
        })
    }

    pub const fn is_aggregate_function(&self) -> bool {
        use Expr::*;
        matches!(
            self,
            RowCount | Max(_) | Min(_) | Sum(_) | Avg(_) | Count(_) | First(_) | Last(_)
        )
    }

    pub const fn is_window_function(&self) -> bool {
        use Expr::*;
        matches!(self, RowNumber) || self.is_aggregate_function()
    }
}

trait ExprExt {
    fn as_list(&self) -> &[Id];
    fn as_column(&self) -> ColumnRefId;
}

impl<D> ExprExt for egg::EClass<Expr, D> {
    fn as_list(&self) -> &[Id] {
        self.iter()
            .find_map(|e| match e {
                Expr::List(list) => Some(list),
                _ => None,
            })
            .expect("not a list")
    }

    fn as_column(&self) -> ColumnRefId {
        self.iter()
            .find_map(|e| match e {
                Expr::Column(cid) => Some(*cid),
                _ => None,
            })
            .expect("not a column")
    }
}
