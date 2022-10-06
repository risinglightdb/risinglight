//! Plan optimization rules.

use super::*;
use crate::catalog::ColumnRefId;

/// Returns all rules of plan optimization.
pub fn rules() -> Vec<Rewrite> {
    let mut rules = vec![];
    rules.extend(cancel_rules());
    rules.extend(merge_rules());
    rules.extend(pushdown_rules());
    rules.extend(join_rules());
    rules.extend(column_prune_rules());
    rules
}

#[rustfmt::skip]
fn cancel_rules() -> Vec<Rewrite> { vec![
    rw!("limit-null";   "(limit null null ?child)" => "?child"),
    rw!("limit-0";      "(limit ?offset 0 ?child)" => "(values)"),
    rw!("filter-true";  "(filter true ?child)" => "?child"),
    rw!("filter-false"; "(filter false ?child)" => "(values)"),
    rw!("join-false";   "(join ?type false ?left ?right)" => "(values)"),
    rw!("order-null";   "(order (list) ?child)" => "?child"),
]}

#[rustfmt::skip]
fn merge_rules() -> Vec<Rewrite> { vec![
    rw!("limit-order=topn";
        "(limit ?offset ?limit (order ?keys ?child))" =>
        "(topn ?offset ?limit ?keys ?child)"
    ),
    rw!("filter-merge";
        "(filter (filter ?cond1 ?child) ?cond2)" =>
        "(filter (and ?cond1 ?cond2) ?child)"
    ),
    rw!("proj-merge";
        "(proj ?exprs1 (proj ?exprs2 ?child))" =>
        "(proj ?exprs1 ?child)"
    ),
]}

#[rustfmt::skip]
fn pushdown_rules() -> Vec<Rewrite> { vec![
    pushdown("proj", "?exprs", "order", "?keys"),
    pushdown("proj", "?exprs", "limit", "?offset ?limit"),
    pushdown("proj", "?exprs", "topn", "?offset ?limit ?keys"),
    pushdown("filter", "?cond", "order", "?keys"),
    pushdown("filter", "?cond", "limit", "?offset ?limit"),
    pushdown("filter", "?cond", "topn", "?offset ?limit ?keys"),
    rw!("pushdown-filter-join";
        "(filter ?cond (join ?type ?on ?left ?right))" =>
        "(join ?type (and ?on ?cond) ?left ?right)"
    ),
    rw!("pushdown-join-left";
        "(join ?type (and ?cond1 ?cond2) ?left ?right)" =>
        "(join ?type ?cond2 (filter ?cond1 ?left) ?right)"
        if columns_is_subset("?cond1", "?left")
    ),
    rw!("pushdown-join-right";
        "(join ?type (and ?cond1 ?cond2) ?left ?right)" =>
        "(join ?type ?cond2 ?left (filter ?cond1 ?right))"
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
fn join_rules() -> Vec<Rewrite> { vec![
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
]}

/// Column pruning rules remove unused columns from a plan.
/// 
/// We introduce an internal node [`Expr::Prune`] 
/// to top-down traverse the plan tree and collect all used columns.
#[rustfmt::skip]
fn column_prune_rules() -> Vec<Rewrite> { vec![
    // projection is the source of prune node
    rw!("prune-gen";
        "(proj ?exprs ?child)" =>
        "(proj ?exprs (prune ?exprs ?child))"
    ),
    // then it is pushed down through the plan node tree,
    // merging all used columns along the way
    rw!("prune-limit";
        "(prune ?set (limit ?offset ?limit ?child))" =>
        "(limit ?offset ?limit (prune ?set ?child))"
    ),
    // note that we use `+` to represent the union of two column sets.
    // in fact, it doesn't matter what operator we use,
    // because the set of a node is calculated by union all its children.
    // see `analyze_columns()`.
    rw!("prune-order";
        "(prune ?set (order ?keys ?child))" =>
        "(order ?keys (prune (+ ?set ?keys) ?child))"
    ),
    rw!("prune-filter";
        "(prune ?set (filter ?cond ?child))" =>
        "(filter ?cond (prune (+ ?set ?cond) ?child))"
    ),
    rw!("prune-agg";
        "(prune ?set (agg ?aggs ?groupby ?child))" =>
        "(agg ?aggs ?groupby (prune (+ (+ ?set ?aggs) ?groupby) ?child))"
    ),
    rw!("prune-join";
        "(prune ?set (join ?type ?on ?left ?right))" =>
        "(join ?type ?on (prune (+ ?set ?on) ?left) (prune (+ ?set ?on) ?right))"
    ),
    // projection and scan is the sink of prune node
    rw!("prune-proj";
        "(prune ?set (proj ?exprs ?child))" =>
        "(proj (prune ?set ?exprs) ?child))"
    ),
    rw!("prune-scan";
        "(prune ?set (scan ?columns))" =>
        "(scan (prune ?set ?columns))"
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
    let columns = |i: &Id| &egraph[*i].data.columns;
    if let Expr::Column(col) = enode {
        return [*col].into_iter().collect();
    }
    if let Expr::Proj([exprs, _]) | Expr::Select([exprs, ..]) = enode {
        // only from projection lists
        return columns(exprs).clone();
    }
    if let Expr::Agg([exprs, group_keys, _]) = enode {
        return columns(exprs).union(columns(group_keys)).cloned().collect();
    }
    // merge the set from all children
    (enode.children().iter())
        .flat_map(|id| columns(id).iter().cloned())
        .collect()
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
        let list = match &egraph[subst[self.list]].nodes[0] {
            Expr::List(list) => list.as_slice(),
            _ => unreachable!("should be a list"),
        };
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
        rules.append(&mut plan::rules());
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
            (scan (list $1.1 $1.2))
            (scan (list $2.1 $2.2 $2.3))
        )))" => "
        (proj (list $1.2 $2.2)
        (join inner (= $1.1 $2.1)
            (scan (list $1.1 $1.2))
            (filter (= $2.3 'A')
                (scan (list $2.1 $2.2 $2.3))
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
                (scan (list $1.1 $1.2))
                (scan (list $2.1 $2.2))
            )
            (scan (list $3.1 $3.2))
        ))" => "
        (join inner (= $1.1 $2.1)
            (scan (list $1.1 $1.2))
            (join inner (= $2.1 $3.1)
                (scan (list $2.1 $2.2))
                (scan (list $3.1 $3.2))
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
            (scan (list $1.1 $1.2))
            (scan (list $2.1 $2.2))
        ))" => "
        (hashjoin inner (list $1.1) (list $2.1)
            (filter (> $1.2 2)
                (scan (list $1.1 $1.2))
            )
            (scan (list $2.1 $2.2))
        )"
    }

    egg::test_fn! {
        column_prune,
        rules(),
        // SELECT a FROM t1(id, a) JOIN t2(id, b, c) ON t1.id = t2.id WHERE a + b > 1;
        "
        (proj (list $1.2)
        (filter (> (+ $1.2 $2.2) 1)
        (join inner (= $1.1 $2.1)
            (scan (list $1.1 $1.2))
            (scan (list $2.1 $2.2 $2.3))
        )))" => "
        (proj (list $1.2)
        (filter (> (+ $1.2 $2.2) 1)
        (join inner (= $1.1 $2.1)
            (scan (list $1.1 $1.2))
            (scan (list $2.1 $2.2))
        )))"
    }
}
