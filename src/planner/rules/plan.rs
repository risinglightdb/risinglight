// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

//! Plan optimization rules.

use super::schema::schema_is_eq;
use super::*;
use crate::planner::ExprExt;

/// Returns the rules that always improve the plan.
pub fn always_better_rules() -> Vec<Rewrite> {
    let mut rules = vec![];
    rules.extend(cancel_rules());
    rules.extend(merge_rules());
    rules.extend(predicate_pushdown_rules());
    rules.extend(projection_pushdown_rules());
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
]}

#[rustfmt::skip]
fn predicate_pushdown_rules() -> Vec<Rewrite> { vec![
    pushdown("filter", "?cond", "order", "?keys"),
    pushdown("filter", "?cond", "limit", "?limit ?offset"),
    pushdown("filter", "?cond", "topn", "?limit ?offset ?keys"),
    rw!("pushdown-filter-join";
        "(filter ?cond (join inner ?on ?left ?right))" =>
        "(join inner (and ?on ?cond) ?left ?right)"
    ),
    rw!("pushdown-filter-join-left";
        "(join inner (and ?cond1 ?cond2) ?left ?right)" =>
        "(join inner ?cond2 (filter ?cond1 ?left) ?right)"
        if columns_is_subset("?cond1", "?left")
    ),
    rw!("pushdown-filter-join-left-1";
        "(join inner ?cond1 ?left ?right)" =>
        "(join inner true (filter ?cond1 ?left) ?right)"
        if columns_is_subset("?cond1", "?left")
    ),
    rw!("pushdown-filter-join-right";
        "(join inner (and ?cond1 ?cond2) ?left ?right)" =>
        "(join inner ?cond2 ?left (filter ?cond1 ?right))"
        if columns_is_subset("?cond1", "?right")
    ),
    rw!("pushdown-filter-join-right-1";
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

/// Pushdown projections and prune unused columns.
#[rustfmt::skip]
pub fn projection_pushdown_rules() -> Vec<Rewrite> { vec![
    rw!("identical-proj";
        "(proj ?expr ?child)" => "?child" 
        if schema_is_eq("?expr", "?child")
    ),
    pushdown("proj", "?exprs", "limit", "?limit ?offset"),
    pushdown("limit", "?limit ?offset", "proj", "?exprs"),
    rw!("pushdown-proj-order";
        "(proj ?exprs (order ?keys ?child))" =>
        { ProjectionPushdown {
            pattern: pattern("(proj ?exprs (order ?keys ?child))"),
            used: [var("?exprs"), var("?keys")],
            children: vec![var("?child")],
        }}
    ),
    rw!("pushdown-proj-topn";
        "(proj ?exprs (topn ?limit ?offset ?keys ?child))" =>
        { ProjectionPushdown {
            pattern: pattern("(proj ?exprs (topn ?limit ?offset ?keys ?child))"),
            used: [var("?exprs"), var("?keys")],
            children: vec![var("?child")],
        }}
    ),
    rw!("pushdown-proj-filter";
        "(proj ?exprs (filter ?cond ?child))" =>
        { ProjectionPushdown {
            pattern: pattern("(proj ?exprs (filter ?cond ?child))"),
            used: [var("?exprs"), var("?cond")],
            children: vec![var("?child")],
        }}
    ),
    rw!("pushdown-proj-agg";
        "(agg ?aggs ?groupby ?child)" =>
        { ProjectionPushdown {
            pattern: pattern("(agg ?aggs ?groupby ?child)"),
            used: [var("?aggs"), var("?groupby")],
            children: vec![var("?child")],
        }}
    ),
    rw!("pushdown-proj-join";
        "(proj ?exprs (join ?type ?on ?left ?right))" =>
        { ProjectionPushdown {
            pattern: pattern("(proj ?exprs (join ?type ?on ?left ?right))"),
            used: [var("?exprs"), var("?on")],
            children: vec![var("?left"), var("?right")],
        }}
    ),
    // column pruning
    rw!("pushdown-proj-scan";
        "(proj ?exprs (scan ?table ?columns ?filter))" =>
        { ColumnPrune {
            pattern: pattern("(proj ?exprs (scan ?table ?columns ?filter))"),
            used: [var("?exprs"), var("?filter")],
            columns: var("?columns"),
        }}
    ),
]}

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
    f: impl Fn(&HashSet<Id>, &HashSet<Id>) -> bool,
) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    let var1 = var(var1);
    let var2 = var(var2);
    move |egraph, _, subst| {
        let get_set = |var| {
            egraph[subst[var]]
                .data
                .columns
                .iter()
                .map(|e| egraph.lookup(e.clone()).unwrap())
                .collect()
        };
        f(&get_set(var1), &get_set(var2))
    }
}

/// The data type of column analysis.
///
/// The elements of the set are either `Column` or child of `Ref`.
pub type ColumnSet = HashSet<Expr>;

/// Returns all columns involved in the node.
pub fn analyze_columns(egraph: &EGraph, enode: &Expr) -> ColumnSet {
    use Expr::*;
    let x = |i: &Id| &egraph[*i].data.columns;
    let output = |i: &Id| {
        egraph[*i]
            .as_list()
            .iter()
            .map(|id| egraph[*id].nodes[0].clone())
            .collect::<ColumnSet>()
    };
    match enode {
        // source
        Column(_) => [enode.clone()].into_iter().collect(),
        Ref(c) => [egraph[*c].nodes[0].clone()].into_iter().collect(),

        Proj([exprs, _]) => output(exprs),
        Agg([exprs, group_keys, _]) => output(exprs).union(&output(group_keys)).cloned().collect(),

        // expressions: merge from all children
        _ => (enode.children().iter())
            .flat_map(|id| x(id).iter().cloned())
            .collect(),
    }
}

/// Generate a projection node over each children.
struct ProjectionPushdown {
    pattern: Pattern,
    used: [Var; 2],
    children: Vec<Var>,
}

impl Applier<Expr, ExprAnalysis> for ProjectionPushdown {
    fn apply_one(
        &self,
        egraph: &mut EGraph,
        eclass: Id,
        subst: &Subst,
        searcher_ast: Option<&PatternAst<Expr>>,
        rule_name: Symbol,
    ) -> Vec<Id> {
        let used1 = &egraph[subst[self.used[0]]].data.columns;
        let used2 = &egraph[subst[self.used[1]]].data.columns;
        let mut used: Vec<&Expr> = used1.union(used2).collect();
        used.sort_unstable();
        let used = used
            .into_iter()
            .map(|col| egraph.lookup(col.clone()).unwrap())
            .collect::<Vec<Id>>();

        let mut subst = subst.clone();
        for &child in &self.children {
            let child_id = subst[child];
            let filtered = if self.children.len() == 1 {
                // no need to filter
                used.clone().into()
            } else {
                let child_set = (egraph[child_id].data.columns.iter())
                    .map(|e| egraph.lookup(e.clone()).unwrap())
                    .collect::<HashSet<Id>>();
                (used.iter().cloned())
                    .filter(|id| child_set.contains(id))
                    .collect()
            };
            let id = egraph.add(Expr::List(filtered));
            let id = egraph.add(Expr::Proj([id, child_id]));
            subst.insert(child, id);
        }

        self.pattern
            .apply_one(egraph, eclass, &subst, searcher_ast, rule_name)
    }
}

/// Remove element from `columns` whose column set is not a subset of `used`
struct ColumnPrune {
    pattern: Pattern,
    used: [Var; 2],
    columns: Var,
}

impl Applier<Expr, ExprAnalysis> for ColumnPrune {
    fn apply_one(
        &self,
        egraph: &mut EGraph,
        eclass: Id,
        subst: &Subst,
        searcher_ast: Option<&PatternAst<Expr>>,
        rule_name: Symbol,
    ) -> Vec<Id> {
        let used1 = &egraph[subst[self.used[0]]].data.columns;
        let used2 = &egraph[subst[self.used[1]]].data.columns;
        let used = used1.union(used2).cloned().collect();
        let columns = egraph[subst[self.columns]].as_list();
        let filtered = (columns.iter().cloned())
            .filter(|id| egraph[*id].data.columns.is_subset(&used))
            .collect();
        let id = egraph.add(Expr::List(filtered));

        let mut subst = subst.clone();
        subst.insert(self.columns, id);
        self.pattern
            .apply_one(egraph, eclass, &subst, searcher_ast, rule_name)
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
            (scan $1 (list $1.1 $1.2) true)
            (scan $2 (list $2.1 $2.2 $2.3) true)
        )))" => "
        (proj (list $1.2 $2.2)
        (join inner (= $1.1 $2.1)
            (scan $1 (list $1.1 $1.2) true)
            (filter (= $2.3 'A')
                (scan $2 (list $2.1 $2.2 $2.3) true)
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
                (scan $1 (list $1.1 $1.2) true)
                (scan $2 (list $2.1 $2.2) true)
            )
            (scan $3 (list $3.1 $3.2) true)
        ))" => "
        (join inner (= $1.1 $2.1)
            (scan $1 (list $1.1 $1.2) true)
            (join inner (= $2.1 $3.1)
                (scan $2 (list $2.1 $2.2) true)
                (scan $3 (list $3.1 $3.2) true)
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
            (scan $1 (list $1.1 $1.2) true)
            (scan $2 (list $2.1 $2.2) true)
        ))" => "
        (hashjoin inner (list $1.1) (list $2.1)
            (filter (> $1.2 2)
                (scan $1 (list $1.1 $1.2) true)
            )
            (scan $2 (list $2.1 $2.2) true)
        )"
    }

    egg::test_fn! {
        projection_pushdown,
        projection_pushdown_rules(),
        // SELECT a FROM t1(id, a, b) JOIN t2(id, c, d) ON t1.id = t2.id WHERE a + c > 1;
        "
        (proj (list $1.2)
        (filter (> (+ $1.2 $2.2) 1)
        (join inner (= $1.1 $2.1)
            (scan $1 (list $1.1 $1.2 $1.3) true)
            (scan $2 (list $2.1 $2.2 $2.3) true)
        )))" => "
        (proj (list $1.2)
        (filter (> (+ $1.2 $2.2) 1)
        (proj (list $1.2 $2.2)
        (join inner (= $1.1 $2.1)
            (scan $1 (list $1.1 $1.2) true)
            (scan $2 (list $2.1 $2.2) true)
        ))))"
    }
}
