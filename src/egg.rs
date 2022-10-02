use std::collections::HashSet;

use egg::{rewrite as rw, *};

use crate::array::ArrayImpl;
use crate::catalog::ColumnRefId;
use crate::parser::{BinaryOperator, UnaryOperator};
use crate::types::{DataValue, PhysicalDataTypeKind};

define_language! {
    pub enum Plan {
        Constant(DataValue),
        Type(PhysicalDataTypeKind),
        ColumnRef(ColumnRefId),

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
        "rowcount" = RowCount(Id),
        "first" = First(Id),
        "last" = Last(Id),

        "cast" = TypeCast([Id; 2]),
        "as" = Alias([Id; 2]),
        "fn" = Function(Box<[Id]>),

        "scan" = Scan([Id; 2]),                 // (scan table [column..])
        "values" = Values(Box<[Id]>),           // (values tuple..)
        "projection" = Projection([Id; 2]),     // (projection [expr..] child)
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
        "agg" = Agg([Id; 3]),                   // (agg aggs=[expr..] group_keys=[expr..] child)

        "tuple" = Tuple(Box<[Id]>),             // (tuple expr..)
        "list" = List(Box<[Id]>),               // (list ...)

        Symbol(Symbol),
    }
}

impl Plan {
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
pub struct PlanAnalysis;

#[derive(Debug)]
pub struct Data {
    /// Some if the expression is a constant.
    val: Option<DataValue>,
    /// All columns involved in the node.
    columns: ColumnSet,
}

type ColumnSet = HashSet<ColumnRefId>;

impl Analysis<Plan> for PlanAnalysis {
    type Data = Data;

    fn merge(&mut self, to: &mut Self::Data, from: Self::Data) -> DidMerge {
        let merge_val = egg::merge_max(&mut to.val, from.val);
        let merge_col = merge_columns(&mut to.columns, from.columns);
        merge_val | merge_col
    }

    fn make(egraph: &EGraph, enode: &Plan) -> Self::Data {
        Data {
            val: eval(egraph, enode),
            columns: analyze_columns(egraph, enode),
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
    } else if let &TypeCast(_) = enode {
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
    if let Plan::ColumnRef(col) = enode {
        return [*col].into_iter().collect();
    }
    // merge the set from all children
    (enode.children().iter())
        .flat_map(|id| egraph[*id].data.columns.iter().cloned())
        .collect()
}

/// Merge 2 column set.
fn merge_columns(to: &mut ColumnSet, from: ColumnSet) -> DidMerge {
    let len = to.len();
    if from.is_subset(to) {
        *to = from;
        DidMerge(to.len() != len, false)
    } else {
        DidMerge(false, true)
    }
}

pub type EGraph = egg::EGraph<Plan, PlanAnalysis>;
pub type Rewrite = egg::Rewrite<Plan, PlanAnalysis>;

#[rustfmt::skip]
pub fn rules() -> Vec<Rewrite> { vec![
    rw!("add-zero";  "(+ ?a 0)" => "?a"),
    rw!("add-comm";  "(+ ?a ?b)" => "(+ ?b ?a)"),
    rw!("add-assoc"; "(+ ?a (+ ?b ?c))" => "(+ (+ ?a ?b) ?c)"),
    rw!("add-same";  "(+ ?a ?a)" => "(* ?a 2)"),
    rw!("add-neg";   "(+ ?a (- ?b))" => "(- ?a ?b)"),

    rw!("mul-zero";  "(* ?a 0)" => "0"),
    rw!("mul-one";   "(* ?a 1)" => "?a"),
    rw!("mul-minus"; "(* ?a -1)" => "(- ?a)"),
    rw!("mul-comm";  "(* ?a ?b)"        => "(* ?b ?a)"),
    rw!("mul-assoc"; "(* ?a (* ?b ?c))" => "(* (* ?a ?b) ?c)"),

    // rw!("sub-canon"; "(- ?a ?b)" => "(+ ?a (* -1 ?b))"),
    // rw!("canon-sub"; "(+ ?a (* -1 ?b))" => "(- ?a ?b)"),

    rw!("neg-neg";    "(- (- ?a))" => "?a"),
    rw!("neg-sub";    "(- (- ?a ?b))" => "(- ?b ?a)"),

    rw!("sub-zero";   "(- ?a 0)" => "?a"),
    rw!("zero-sub";   "(- 0 ?a)" => "(- ?a)"),
    rw!("sub-cancel"; "(- ?a ?a)" => "0"),

    rw!("div-cancel"; "(/ ?a ?a)" => "1" if is_not_zero("?a")),

    rw!("mul-add-distri";   "(* ?a (+ ?b ?c))" => "(+ (* ?a ?b) (* ?a ?c))"),
    rw!("mul-add-factor";   "(+ (* ?a ?b) (* ?a ?c))" => "(* ?a (+ ?b ?c))"),

    rw!("recip-mul-div"; "(* ?x (/ 1 ?x))" => "1" if is_not_zero("?x")),

    rw!("eq-eq";     "(=  ?a ?a)" => "true"),
    rw!("ne-eq";     "(<> ?a ?a)" => "false"),
    rw!("gt-eq";     "(>  ?a ?a)" => "false"),
    rw!("lt-eq";     "(<  ?a ?a)" => "false"),
    rw!("ge-eq";     "(>= ?a ?a)" => "true"),
    rw!("le-eq";     "(<= ?a ?a)" => "true"),
    rw!("eq-comm";   "(=  ?a ?b)" => "(=  ?b ?a)"),
    rw!("ne-comm";   "(<> ?a ?b)" => "(<> ?b ?a)"),
    rw!("gt-comm";   "(>  ?a ?b)" => "(<  ?b ?a)"),
    rw!("lt-comm";   "(<  ?a ?b)" => "(>  ?b ?a)"),
    rw!("ge-comm";   "(>= ?a ?b)" => "(<= ?b ?a)"),
    rw!("le-comm";   "(<= ?a ?b)" => "(>= ?b ?a)"),
    rw!("eq-add";    "(=  (+ ?a ?b) ?c)" => "(=  ?a (- ?c ?b))"),
    rw!("ne-add";    "(<> (+ ?a ?b) ?c)" => "(<> ?a (- ?c ?b))"),
    rw!("gt-add";    "(>  (+ ?a ?b) ?c)" => "(>  ?a (- ?c ?b))"),
    rw!("lt-add";    "(<  (+ ?a ?b) ?c)" => "(<  ?a (- ?c ?b))"),
    rw!("ge-add";    "(>= (+ ?a ?b) ?c)" => "(>= ?a (- ?c ?b))"),
    rw!("le-add";    "(<= (+ ?a ?b) ?c)" => "(<= ?a (- ?c ?b))"),
    rw!("eq-trans";  "(and (= ?a ?b) (= ?b ?c))" => "(and (= ?a ?b) (= ?a ?c))"),

    rw!("not-eq";    "(not (=  ?a ?b))" => "(<> ?a ?b)"),
    rw!("not-ne";    "(not (<> ?a ?b))" => "(=  ?a ?b)"),
    rw!("not-gt";    "(not (>  ?a ?b))" => "(<= ?a ?b)"),
    rw!("not-ge";    "(not (>= ?a ?b))" => "(<  ?a ?b)"),
    rw!("not-lt";    "(not (<  ?a ?b))" => "(>= ?a ?b)"),
    rw!("not-le";    "(not (<= ?a ?b))" => "(>  ?a ?b)"),
    rw!("not-and";   "(not (and ?a ?b))" => "(or  (not ?a) (not ?b))"),
    rw!("not-or";    "(not (or  ?a ?b))" => "(and (not ?a) (not ?b))"),
    rw!("not-not";   "(not (not ?a))"    => "?a"),

    rw!("and-false"; "(and false ?a)"   => "false"),
    rw!("and-true";  "(and true ?a)"    => "?a"),
    rw!("and-null";  "(and null ?a)"    => "?a"),
    rw!("and-comm";  "(and ?a ?b)"      => "(and ?b ?a)"),
    rw!("and-assoc"; "(and ?a (and ?b ?c))" => "(and (and ?a ?b) ?c)"),

    rw!("or-false";  "(or false ?a)" => "?a"),
    rw!("or-true";   "(or true ?a)"  => "true"),
    rw!("or-null";   "(or null ?a)"  => "?a"),
    rw!("or-comm";   "(or ?a ?b)"    => "(or ?b ?a)"),
    rw!("or-assoc";  "(or ?a (or ?b ?c))" => "(or (or ?a ?b) ?c)"),

    rw!("avg";       "(avg ?a)" => "(/ (sum ?a) (count ?a))"),

    rw!("limit-order=topn";
        "(limit ?offset ?limit (order ?keys ?child))" =>
        "(topn ?offset ?limit ?keys ?child)"
    ),
    rw!("filter-merge";
        "(filter (filter ?cond1 ?child) ?cond2)" =>
        "(filter (and ?cond1 ?cond2) ?child)"
    ),
    rw!("predicate-pushdown-filter-join";
        "(filter ?condition (join ?type ?on ?left ?right))" =>
        "(join ?type (and ?on ?condition) ?left ?right)"
    ),
    rw!("predicate-pushdown-join-left";
        "(join ?type (and ?cond1 ?cond2) ?left ?right)" =>
        "(join ?type ?cond2 (filter ?cond1 ?left) ?right)"
        if columns_is_subset("?cond1", "?left")
    ),
    rw!("predicate-pushdown-join-right";
        "(join ?type (and ?cond1 ?cond2) ?left ?right)" =>
        "(join ?type ?cond2 ?left (filter ?cond1 ?right))"
        if columns_is_subset("?cond1", "?right")
    ),
    rw!("join-reorder";
        "(join inner ?cond2 (join inner ?cond1 ?left ?mid) ?right)" =>
        "(join inner ?cond1 ?left (join inner ?cond2 ?mid ?right))"
        if columns_is_disjoint("?cond2", "?left")
    ),
    rw!("hash-join-on-one-eq";
        "(join ?type (= ?el ?er) ?left ?right)" =>
        "(hashjoin ?type (list ?el) (list ?er) ?left ?right)"
        if columns_is_subset("?el", "?left")
        if columns_is_subset("?er", "?right")
    ),

    rw!("limit-0"; "(limit ?offset 0 ?child)" => "(values)"),
    rw!("filter-true"; "(filter ?child true)" => "?child"),
    rw!("filter-false"; "(filter ?child false)" => "(values)"),
    rw!("join-on-false"; "(join ?type false ?left ?right)" => "(values)"),
]}

fn var(s: &str) -> Var {
    s.parse().expect("invalid variable")
}

fn value_is(v: &str, f: impl Fn(&DataValue) -> bool) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    let v = var(v);
    move |egraph, _, subst| {
        if let Some(n) = &egraph[subst[v]].data.val {
            f(n)
        } else {
            false
        }
    }
}

fn is_not_zero(var: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    value_is(var, |v| !v.is_zero())
}

fn is_const(var: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    value_is(var, |_| true)
}

/// Returns true if the columns in `var1` are a subset of the columns in `var2`.
fn columns_is_subset(var1: &str, var2: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    columns_is(var1, var2, HashSet::is_subset)
}

/// Returns true if the columns in `var1` has no elements in common with the columns in `var2`.
fn columns_is_disjoint(var1: &str, var2: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    columns_is(var1, var2, HashSet::is_disjoint)
}

fn columns_is(
    var1: &str,
    var2: &str,
    f: impl Fn(&ColumnSet, &ColumnSet) -> bool,
) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    let var1 = var(var1);
    let var2 = var(var2);
    move |egraph, _, subst| {
        let var1_set = &egraph[subst[var1]].data.columns;
        let var2_set = &egraph[subst[var2]].data.columns;
        f(var1_set, var2_set)
    }
}

egg::test_fn! {
    and_eq_const,
    rules(),
    "(and (= a 1) (= a b))" => "(and (= a 1) (= b 1))",
}

egg::test_fn! {
    constant_folding,
    rules(),
    "(* (- (+ 1 2) 4) (/ 6 2))" => "-3",
}

egg::test_fn! {
    constant_moving,
    rules(),
    "(> (+ 100 a) 300)" => "(> a 200)",
}

egg::test_fn! {
    predicate_pushdown,
    rules(),
    // SELECT s.name, e.cid
    // FROM student AS s, enrolled AS e
    // WHERE s.sid = e.sid AND e.grade = 'A'
    "
    (projection
        (list $1.2 $2.2)
        (filter
            (and (= $1.1 $2.1) (= $2.3 'A'))
            (join
                inner
                true
                (scan student (list $1.1 $1.2))
                (scan enrolled (list $2.1 $2.2 $2.3))
            )
        )
    )" => "
    (projection
        (list $1.2 $2.2)
        (join
            inner
            (= $1.1 $2.1)
            (scan student (list $1.1 $1.2))
            (filter
                (= $2.3 'A')
                (scan enrolled (list $2.1 $2.2 $2.3))
            )
        )
    )"
}

egg::test_fn! {
    join_reorder,
    rules(),
    // SELECT * FROM t1, t2, t3
    // WHERE t1.id = t2.id AND t3.id = t2.id
    "
    (filter
        (and (= $1.1 $2.1) (= $3.1 $2.1))
        (join
            inner
            true
            (join
                inner
                true
                (scan t1 (list $1.1 $1.2))
                (scan t2 (list $2.1 $2.2))
            )
            (scan t3 (list $3.1 $3.2))
        )
    )" => "
    (join
        inner
        (= $1.1 $2.1)
        (scan t1 (list $1.1 $1.2))
        (join
            inner
            (= $2.1 $3.1)
            (scan t2 (list $2.1 $2.2))
            (scan t3 (list $3.1 $3.2))
        )
    )
    "
}

egg::test_fn! {
    hash_join,
    rules(),
    // SELECT * FROM t1, t2
    // WHERE t1.id = t2.id AND t1.age > 2
    "
    (filter
        (and (= $1.1 $2.1) (> $1.2 2))
        (join
            inner
            true
            (scan t1 (list $1.1 $1.2))
            (scan t2 (list $2.1 $2.2))
        )
    )" => "
    (hashjoin
        inner
        (list $1.1)
        (list $2.1)
        (filter
            (> $1.2 2)
            (scan t1 (list $1.1 $1.2))
        )
        (scan t2 (list $2.1 $2.2))
    )"
}
