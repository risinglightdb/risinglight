//! Plan optimization rules.

use super::*;
use crate::catalog::ColumnRefId;

/// Returns the rules that always improve the plan.
pub fn always_better_rules() -> Vec<Rewrite> {
    let mut rules = vec![];
    rules.extend(cancel_rules());
    rules.extend(merge_rules());
    rules.extend(pushdown_rules());
    rules
}

#[rustfmt::skip]
fn cancel_rules() -> Vec<Rewrite> { vec![
    rw!("limit-null";       "(limit null 0 ?child)"     => "?child"),
    rw!("limit-0";          "(limit 0 ?offset ?child)"  => "(empty ?child)"),
    rw!("order-null";       "(order (list) ?child)"     => "?child"),
    rw!("filter-true";      "(filter true ?child)"      => "?child"),
    rw!("filter-false";     "(filter false ?child)"     => "(empty ?child)"),
    rw!("inner-join-false"; "(join inner false ?l ?r)"  => "(empty ?l ?r)"),

    rw!("proj-on-empty";    "(proj ?exprs (empty ?c))"                  => "(empty ?c)"),
    // TODO: only valid when aggs don't contain `count`
    // rw!("agg-on-empty";     "(agg ?aggs ?groupby (empty ?c))"           => "(empty ?c)"),
    rw!("filter-on-empty";  "(filter ?cond (empty ?c))"                 => "(empty ?c)"),
    rw!("order-on-empty";   "(order ?keys (empty ?c))"                  => "(empty ?c)"),
    rw!("limit-on-empty";   "(limit ?limit ?offset (empty ?c))"         => "(empty ?c)"),
    rw!("topn-on-empty";    "(topn ?limit ?offset ?keys (empty ?c))"    => "(empty ?c)"),
    rw!("inner-join-on-left-empty";  "(join inner ?on (empty ?l) ?r)"   => "(empty ?l ?r)"),
    rw!("inner-join-on-right-empty"; "(join inner ?on ?l (empty ?r))"   => "(empty ?l ?r)"),
]}

#[rustfmt::skip]
fn merge_rules() -> Vec<Rewrite> { vec![
    rw!("limit-order-topn";
        "(limit ?limit ?offset (order ?keys ?child))" =>
        "(topn ?limit ?offset ?keys ?child)"
    ),
    rw!("filter-merge";
        "(filter ?cond1 (filter ?cond2 ?child))" =>
        "(filter (and ?cond1 ?cond2) ?child)"
    ),
    rw!("proj-merge";
        "(proj ?exprs1 (proj ?exprs2 ?child))" =>
        "(proj ?exprs1 ?child)"
    ),
]}

#[rustfmt::skip]
fn pushdown_rules() -> Vec<Rewrite> { vec![
    pushdown("proj", "?exprs", "limit", "?limit ?offset"),
    pushdown("limit", "?limit ?offset", "proj", "?exprs"),
    pushdown("filter", "?cond", "order", "?keys"),
    pushdown("filter", "?cond", "limit", "?limit ?offset"),
    pushdown("filter", "?cond", "topn", "?limit ?offset ?keys"),
    rw!("pushdown-filter-join";
        "(filter ?cond (join inner ?on ?left ?right))" =>
        "(join inner (and ?on ?cond) ?left ?right)"
    ),
    rw!("pushdown-join-left";
        "(join inner (and ?cond1 ?cond2) ?left ?right)" =>
        "(join inner ?cond2 (filter ?cond1 ?left) ?right)"
        if columns_is_subset("?cond1", "?left")
    ),
    rw!("pushdown-join-left-1";
        "(join inner ?cond1 ?left ?right)" =>
        "(join inner true (filter ?cond1 ?left) ?right)"
        if columns_is_subset("?cond1", "?left")
    ),
    rw!("pushdown-join-right";
        "(join inner (and ?cond1 ?cond2) ?left ?right)" =>
        "(join inner ?cond2 ?left (filter ?cond1 ?right))"
        if columns_is_subset("?cond1", "?right")
    ),
    rw!("pushdown-join-right-1";
        "(join inner ?cond1 ?left ?right)" =>
        "(join inner true ?left (filter ?cond1 ?right))"
        if columns_is_subset("?cond1", "?right")
    ),
]}

/// Returns a rule to pushdown plan `a` through `b`.
fn pushdown(a: &str, a_args: &str, b: &str, b_args: &str) -> Rewrite {
    let name = format!("pushdown-{a}-{b}");
    let searcher = format!("({a} {a_args} ({b} {b_args} ?child))");
    let applier = format!("({b} {b_args} ({a} {a_args} ?child))");
    Rewrite::new(name, pattern(&searcher), pattern(&applier)).unwrap()
}

#[rustfmt::skip]
pub fn join_rules() -> Vec<Rewrite> { vec![
    // we only have right rotation rule,
    // because the initial state is always a left-deep tree
    // thus left rotation is not needed.
    rw!("join-reorder";
        "(join ?type ?cond2 (join ?type ?cond1 ?left ?mid) ?right)" =>
        "(join ?type ?cond1 ?left (join ?type ?cond2 ?mid ?right))"
        if columns_is_disjoint("?cond2", "?left")
    ),
    rw!("hash-join-on-one-eq";
        "(join ?type (= ?el ?er) ?left ?right)" =>
        "(hashjoin ?type (list ?el) (list ?er) ?left ?right)"
        if columns_is_subset("?el", "?left")
        if columns_is_subset("?er", "?right")
    ),
    rw!("hash-join-on-two-eq";
        "(join ?type (and (= ?l1 ?r1) (= ?l2 ?r2)) ?left ?right)" =>
        "(hashjoin ?type (list ?l1 ?l2) (list ?r1 ?r2) ?left ?right)"
        if columns_is_subset("?l1", "?left")
        if columns_is_subset("?l2", "?left")
        if columns_is_subset("?r1", "?right")
        if columns_is_subset("?r2", "?right")
    ),
    // TODO: support more than two equals
]}

/// Column pruning rules remove unused columns from a plan.
/// 
/// We introduce an internal node [`Expr::Prune`] 
/// to top-down traverse the plan tree and collect all used columns.
#[rustfmt::skip]
pub fn column_prune_rules() -> Vec<Rewrite> { vec![
    // projection is the source of prune node
    //   note that this rule may be applied for a lot of times,
    //   so it's not recommand to apply column pruning with other rules together.
    rw!("prune-gen";
        "(proj ?exprs ?child)" =>
        "(proj ?exprs (prune ?exprs ?child))"
    ),
    // then it is pushed down through the plan node tree,
    // merging all used columns along the way
    rw!("prune-limit";
        "(prune ?set (limit ?limit ?offset ?child))" =>
        "(limit ?limit ?offset (prune ?set ?child))"
    ),
    // note that we use `list` to represent the union of multiple column sets.
    // because the column set of `list` is calculated by union all its children.
    // see `analyze_columns()`.
    rw!("prune-order";
        "(prune ?set (order ?keys ?child))" =>
        "(order ?keys (prune (list ?set ?keys) ?child))"
    ),
    rw!("prune-filter";
        "(prune ?set (filter ?cond ?child))" =>
        "(filter ?cond (prune (list ?set ?cond) ?child))"
    ),
    rw!("prune-agg";
        "(prune ?set (agg ?aggs ?groupby ?child))" =>
        "(agg ?aggs ?groupby (prune (list ?set ?aggs ?groupby) ?child))"
    ),
    rw!("prune-join";
        "(prune ?set (join ?type ?on ?left ?right))" =>
        "(join ?type ?on
            (prune (list ?set ?on) ?left)
            (prune (list ?set ?on) ?right)
        )"
    ),
    // projection and scan is the sink of prune node
    rw!("prune-proj";
        "(prune ?set (proj ?exprs ?child))" =>
        "(proj (prune ?set ?exprs) ?child))"
    ),
    rw!("prune-scan";
        "(prune ?set (scan ?table ?columns))" =>
        "(scan ?table (prune ?set ?columns))"
    ),
    // finally the prune is applied to a list of expressions
    rw!("prune-list";
        "(prune ?set ?list)" =>
        { PruneList {
            set: var("?set"),
            list: var("?list"),
        }}
        if is_list("?list")
    ),
]}

/// Returns true if the columns in `var1` are a subset of the columns in `var2`.
fn columns_is_subset(var1: &str, var2: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    columns_is(var1, var2, ColumnSet::is_subset)
}

/// Returns true if the columns in `var1` has no elements in common with the columns in `var2`.
fn columns_is_disjoint(var1: &str, var2: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    columns_is(var1, var2, ColumnSet::is_disjoint)
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

/// Returns true if the variable is a list.
fn is_list(v: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    let v = var(v);
    // we have no rule to rewrite a list,
    // so it should only contains one `Expr::List` in `nodes`.
    move |egraph, _, subst| matches!(egraph[subst[v]].nodes.first(), Some(Expr::List(_)))
}

/// The data type of column analysis.
pub type ColumnSet = HashSet<ColumnRefId>;

/// Returns all columns involved in the node.
pub fn analyze_columns(egraph: &EGraph, enode: &Expr) -> ColumnSet {
    use Expr::*;
    let x = |i: &Id| &egraph[*i].data.columns;
    match enode {
        Column(col) => [*col].into_iter().collect(),
        Proj([exprs, _]) => x(exprs).clone(),
        Agg([exprs, group_keys, _]) => x(exprs).union(x(group_keys)).cloned().collect(),
        Prune([cols, child]) => x(cols).intersection(x(child)).cloned().collect(),
        _ => {
            // merge the columns from all children
            (enode.children().iter())
                .flat_map(|id| x(id).iter().cloned())
                .collect()
        }
    }
}

/// Remove unused columns in `set` from `list`.
struct PruneList {
    set: Var,
    list: Var,
}

impl Applier<Expr, ExprAnalysis> for PruneList {
    fn apply_one(
        &self,
        egraph: &mut EGraph,
        eclass: Id,
        subst: &Subst,
        _searcher_ast: Option<&PatternAst<Expr>>,
        _rule_name: Symbol,
    ) -> Vec<Id> {
        let used_columns = &egraph[subst[self.set]].data.columns;
        let list = egraph[subst[self.list]].nodes[0].as_list();
        let pruned = (list.iter().cloned())
            .filter(|id| !egraph[*id].data.columns.is_disjoint(used_columns))
            .collect();
        let id = egraph.add(Expr::List(pruned));

        // copied from `Pattern::apply_one`
        if egraph.union(eclass, id) {
            vec![eclass]
        } else {
            vec![]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn rules() -> Vec<Rewrite> {
        let mut rules = vec![];
        rules.append(&mut expr::rules());
        rules.append(&mut plan::always_better_rules());
        rules.append(&mut plan::join_rules());
        rules
    }

    egg::test_fn! {
        predicate_pushdown,
        rules(),
        // SELECT s.name, e.cid
        // FROM student AS s, enrolled AS e
        // WHERE s.sid = e.sid AND e.grade = 'A'
        "
        (proj (list $1.2 $2.2)
        (filter (and (= $1.1 $2.1) (= $2.3 'A'))
        (join inner true
            (scan $1 (list $1.1 $1.2))
            (scan $2 (list $2.1 $2.2 $2.3))
        )))" => "
        (proj (list $1.2 $2.2)
        (join inner (= $1.1 $2.1)
            (scan $1 (list $1.1 $1.2))
            (filter (= $2.3 'A')
                (scan $2 (list $2.1 $2.2 $2.3))
            )
        ))"
    }

    egg::test_fn! {
        join_reorder,
        rules(),
        // SELECT * FROM t1, t2, t3
        // WHERE t1.id = t2.id AND t3.id = t2.id
        "
        (filter (and (= $1.1 $2.1) (= $3.1 $2.1))
        (join inner true
            (join inner true
                (scan $1 (list $1.1 $1.2))
                (scan $2 (list $2.1 $2.2))
            )
            (scan $3 (list $3.1 $3.2))
        ))" => "
        (join inner (= $1.1 $2.1)
            (scan $1 (list $1.1 $1.2))
            (join inner (= $2.1 $3.1)
                (scan $2 (list $2.1 $2.2))
                (scan $3 (list $3.1 $3.2))
            )
        )"
    }

    egg::test_fn! {
        hash_join,
        rules(),
        // SELECT * FROM t1, t2
        // WHERE t1.id = t2.id AND t1.age > 2
        "
        (filter (and (= $1.1 $2.1) (> $1.2 2))
        (join inner true
            (scan $1 (list $1.1 $1.2))
            (scan $2 (list $2.1 $2.2))
        ))" => "
        (hashjoin inner (list $1.1) (list $2.1)
            (filter (> $1.2 2)
                (scan $1 (list $1.1 $1.2))
            )
            (scan $2 (list $2.1 $2.2))
        )"
    }

    egg::test_fn! {
        column_prune,
        column_prune_rules(),
        // SELECT a FROM t1(id, a) JOIN t2(id, b, c) ON t1.id = t2.id WHERE a + b > 1;
        "
        (proj (list $1.2)
        (filter (> (+ $1.2 $2.2) 1)
        (join inner (= $1.1 $2.1)
            (scan $1 (list $1.1 $1.2))
            (scan $2 (list $2.1 $2.2 $2.3))
        )))" => "
        (proj (list $1.2)
        (filter (> (+ $1.2 $2.2) 1)
        (join inner (= $1.1 $2.1)
            (scan $1 (list $1.1 $1.2))
            (scan $2 (list $2.1 $2.2))
        )))"
    }
}
