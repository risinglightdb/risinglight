use std::collections::HashSet;
use std::hash::Hash;

use egg::*;

use crate::array::ArrayImpl;
use crate::catalog::ColumnRefId;
use crate::parser::{BinaryOperator, UnaryOperator};
use crate::types::{DataValue, PhysicalDataTypeKind};

// mod plan_nodes;
mod rules;

type EGraph = egg::EGraph<Plan, PlanAnalysis>;
type Rewrite = egg::Rewrite<Plan, PlanAnalysis>;

define_language! {
    pub enum Plan {
        Constant(DataValue),
        Type(PhysicalDataTypeKind),
        Column(ColumnRefId),
        // "*" = Wildcard,

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

        // aggregate
        "max" = Max(Id),
        "min" = Min(Id),
        "sum" = Sum(Id),
        "avg" = Avg(Id),
        "count" = Count(Id),
        "rowcount" = RowCount,
        "first" = First(Id),
        "last" = Last(Id),

        "cast" = Cast([Id; 2]),                 // (cast type expr)
        "as" = Alias([Id; 2]),                  // (as name expr)
        "fn" = Function(Box<[Id]>),             // (fn name args..)

        "select" = Select([Id; 8]),             // (select
                                                //      select_list=[expr..]
                                                //      from=join
                                                //      where=expr
                                                //      groupby=[expr..]
                                                //      having=expr
                                                //      orderby=[expr..]
                                                //      limit=expr
                                                //      offset=expr
                                                // )

        "scan" = Scan(Id),                      // (scan [column..])
        "values" = Values(Box<[Id]>),           // (values tuple..)
        "proj" = Proj([Id; 2]),                 // (proj [expr..] child)
        "filter" = Filter([Id; 2]),             // (filter expr child)
        "order" = Order([Id; 2]),               // (order [order_key..] child)
            "order_key" = OrderKey([Id; 2]),        // (order_key expr asc/desc)
                "asc" = Asc,
                "desc" = Desc,
        "limit" = Limit([Id; 3]),               // (limit offset limit child)
        "topn" = TopN([Id; 4]),                 // (topn offset limit [order_key..] child)
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
        "projagg" = ProjAgg([Id; 3]),           // (projagg [expr..] group_keys=[expr..] child)
                                                    // expressions may contain agg inside
                                                    // output = exprs
        "create" = Create([Id; 2]),             // (create table [column_desc..])
        "drop" = Drop(Id),                      // (drop table)
        "insert" = Insert([Id; 3]),             // (insert table [column..] child)
        "delete" = Delete([Id; 2]),             // (delete table child/true)
        "copy_from" = CopyFrom(Id),             // (copy_from dest)
        "copy_to" = CopyTo([Id; 2]),            // (copy_to dest child)
        "explain" = Explain(Id),                // (explain child)

        "tuple" = Tuple(Box<[Id]>),             // (tuple expr..)
        "list" = List(Box<[Id]>),               // (list ...)

        Symbol(Symbol),
    }
}

impl Plan {
    const fn true_() -> Self {
        Plan::Constant(DataValue::Bool(true))
    }

    const fn binary_op(&self) -> Option<(BinaryOperator, Id, Id)> {
        use BinaryOperator as Op;
        Some(match self {
            &Plan::Add([a, b]) => (Op::Plus, a, b),
            &Plan::Sub([a, b]) => (Op::Minus, a, b),
            &Plan::Mul([a, b]) => (Op::Multiply, a, b),
            &Plan::Div([a, b]) => (Op::Divide, a, b),
            &Plan::Mod([a, b]) => (Op::Modulo, a, b),
            &Plan::StringConcat([a, b]) => (Op::StringConcat, a, b),
            &Plan::Gt([a, b]) => (Op::Gt, a, b),
            &Plan::Lt([a, b]) => (Op::Lt, a, b),
            &Plan::GtEq([a, b]) => (Op::GtEq, a, b),
            &Plan::LtEq([a, b]) => (Op::LtEq, a, b),
            &Plan::Eq([a, b]) => (Op::Eq, a, b),
            &Plan::NotEq([a, b]) => (Op::NotEq, a, b),
            &Plan::And([a, b]) => (Op::And, a, b),
            &Plan::Or([a, b]) => (Op::Or, a, b),
            &Plan::Xor([a, b]) => (Op::Xor, a, b),
            &Plan::Like([a, b]) => (Op::Like, a, b),
            _ => return None,
        })
    }

    const fn unary_op(&self) -> Option<(UnaryOperator, Id)> {
        use UnaryOperator as Op;
        Some(match self {
            &Plan::Neg(a) => (Op::Minus, a),
            &Plan::Not(a) => (Op::Not, a),
            _ => return None,
        })
    }
}

#[derive(Default)]
struct PlanAnalysis;

#[derive(Debug)]
struct Data {
    /// Some if the expression is a constant.
    val: Option<DataValue>,
    /// All columns involved in the node.
    columns: ColumnSet,
    /// The schema for plan node: a list of expressions.
    schema: Option<Vec<Id>>,
    /// All aggragations in the tree.
    aggs: NodeSet,
}

type ColumnSet = HashSet<ColumnRefId>;
type NodeSet = HashSet<Plan>;

impl Analysis<Plan> for PlanAnalysis {
    type Data = Data;

    fn merge(&mut self, to: &mut Self::Data, from: Self::Data) -> DidMerge {
        let merge_val = egg::merge_max(&mut to.val, from.val);
        let merge_col = merge_small_set(&mut to.columns, from.columns);
        let merge_schema = egg::merge_max(&mut to.schema, from.schema);
        let merge_agg = merge_small_set(&mut to.aggs, from.aggs);
        merge_val | merge_col | merge_schema | merge_agg
    }

    fn make(egraph: &EGraph, enode: &Plan) -> Self::Data {
        Data {
            val: eval(egraph, enode),
            columns: analyze_columns(egraph, enode),
            schema: analyze_schema(egraph, enode),
            aggs: analyze_aggs(egraph, enode),
        }
    }

    fn modify(egraph: &mut EGraph, id: Id) {
        // add a new constant node
        if let Some(val) = &egraph[id].data.val {
            let added = egraph.add(Plan::Constant(val.clone()));
            egraph.union(id, added);
        }
    }
}

/// Evaluate constant.
fn eval(egraph: &EGraph, enode: &Plan) -> Option<DataValue> {
    use Plan::*;
    let x = |i: Id| egraph[i].data.val.as_ref();
    if let Constant(v) = enode {
        Some(v.clone())
    } else if let Some((op, a, b)) = enode.binary_op() {
        let array_a = ArrayImpl::from(x(a)?);
        let array_b = ArrayImpl::from(x(b)?);
        Some(array_a.binary_op(&op, &array_b).get(0))
    } else if let Some((op, a)) = enode.unary_op() {
        let array_a = ArrayImpl::from(x(a)?);
        Some(array_a.unary_op(&op).get(0))
    } else if let &IsNull(a) = enode {
        Some(DataValue::Bool(x(a)?.is_null()))
    } else if let &Cast(_) = enode {
        // TODO: evaluate type cast
        None
    } else if let &Max(a) | &Min(a) | &Avg(a) | &First(a) | &Last(a) = enode {
        x(a).cloned()
    } else {
        None
    }
}

/// Returns all columns involved in the node.
fn analyze_columns(egraph: &EGraph, enode: &Plan) -> ColumnSet {
    let columns = |i: &Id| &egraph[*i].data.columns;
    if let Plan::Column(col) = enode {
        return [*col].into_iter().collect();
    }
    if let Plan::Proj([exprs, _]) | Plan::ProjAgg([exprs, _, _]) = enode {
        // only from projection lists
        return columns(exprs).clone();
    }
    if let Plan::Agg([exprs, group_keys, _]) = enode {
        // only from projection lists
        return columns(exprs).union(columns(group_keys)).cloned().collect();
    }
    // merge the set from all children
    (enode.children().iter())
        .flat_map(|id| columns(id).iter().cloned())
        .collect()
}

/// Merge 2 set.
fn merge_small_set<T: Eq + Hash>(to: &mut HashSet<T>, from: HashSet<T>) -> DidMerge {
    if from.len() < to.len() {
        *to = from;
        DidMerge(true, false)
    } else {
        DidMerge(false, true)
    }
}

/// Returns the output expressions for plan node.
fn analyze_schema(egraph: &EGraph, enode: &Plan) -> Option<Vec<Id>> {
    use Plan::*;
    let x = |i: Id| egraph[i].data.schema.clone().unwrap();
    let concat = |v1: Vec<Id>, v2: Vec<Id>| v1.into_iter().chain(v2.into_iter()).collect();
    Some(match enode {
        // equal to child
        Filter([_, c]) | Order([_, c]) | Limit([_, _, c]) | TopN([_, _, _, c]) => x(*c),

        // concat 2 children
        Join([_, _, l, r]) | HashJoin([_, _, _, l, r]) => concat(x(*l), x(*r)),

        // list is the source for the following nodes
        List(ids) => ids.to_vec(),

        // plans that change schema
        Scan(columns) => x(*columns),
        Values(_) => todo!("add schema for values plan"),
        Proj([exprs, _]) | ProjAgg([exprs, _, _]) => x(*exprs),
        Agg([exprs, group_keys, _]) => concat(x(*exprs), x(*group_keys)),

        // not plan node
        _ => return None,
    })
}

/// Returns all aggragations in the tree.
fn analyze_aggs(egraph: &EGraph, enode: &Plan) -> NodeSet {
    use Plan::*;
    let x = |i: &Id| &egraph[*i].data.aggs;
    if let RowCount = enode {
        return [enode.clone()].into_iter().collect();
    }
    if let Max(c) | Min(c) | Sum(c) | Avg(c) | Count(c) | First(c) | Last(c) = enode {
        assert!(x(c).is_empty(), "agg in agg");
        return [enode.clone()].into_iter().collect();
    }
    // TODO: ignore plan nodes
    // merge the set from all children
    (enode.children().iter())
        .flat_map(|id| x(id).iter().cloned())
        .collect()
}
