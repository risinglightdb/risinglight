// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

//! Plan optimization rules.

use itertools::Itertools;

use super::*;
use crate::planner::ExprExt;

/// Returns the rules that always improve the plan.
pub fn always_better_rules() -> Vec<Rewrite> {
    let mut rules = vec![];
    rules.extend(cancel_rules());
    rules.extend(merge_rules());
    rules
}

#[rustfmt::skip]
fn cancel_rules() -> Vec<Rewrite> { vec![
    rw!("limit-null";       "(limit null 0 ?child)"     => "?child"),
    rw!("order-null";       "(order (list) ?child)"     => "?child"),
    rw!("filter-true";      "(filter true ?child)"      => "?child"),
    rw!("filter-false";     "(filter false ?child)"     => "(empty ?child)"),
    rw!("window-null";      "(window (list) ?child)"    => "?child"),
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
    rw!("filter-split";
        "(filter (and ?cond1 ?cond2) ?child)" =>
        "(filter ?cond1 (filter ?cond2 ?child))"
    ),
]}

#[rustfmt::skip]
pub fn predicate_pushdown_rules() -> Vec<Rewrite> { vec![
    pushdown("filter", "?cond", "order", "?keys"),
    pushdown("filter", "?cond", "limit", "?limit ?offset"),
    pushdown("filter", "?cond", "topn", "?limit ?offset ?keys"),
    rw!("pushdown-filter-proj";
        "(filter ?cond (proj ?proj ?child))" =>
        "(proj ?proj (filter ?cond ?child))"
        if all_depend_on("?cond", "?child")
    ),
    rw!("pushdown-filter-hashagg";
        "(filter ?cond (hashagg ?keys ?aggs ?child))" =>
        "(hashagg ?keys ?aggs (filter ?cond ?child))"
        if not_depend_on("?cond", "?aggs")
    ),
    rw!("pushdown-filter-inner-join";
        "(filter ?cond (join inner ?on ?left ?right))" =>
        "(join inner (and ?on ?cond) ?left ?right)"
    ),
    rw!("pushdown-filter-semi-join";
        "(filter ?cond (join semi ?on ?left ?right))" =>
        "(join semi (and ?on ?cond) ?left ?right)"
    ),
    rw!("pushdown-filter-anti-join";
        "(filter ?cond (join anti ?on ?left ?right))" =>
        "(join anti ?on (filter ?cond ?left) ?right)"
        if not_depend_on("?cond", "?right")
    ),
    rw!("pushdown-filter-left-outer-join";
        "(filter ?cond (join left_outer ?on ?left ?right))" =>
        "(join left_outer ?on (filter ?cond ?left) ?right)"
        if not_depend_on("?cond", "?right")
    ),
    rw!("pushdown-join-condition-left";
        "(join ?type (and ?cond1 ?cond2) ?left ?right)" =>
        "(join ?type ?cond2 (filter ?cond1 ?left) ?right)"
        if not_depend_on("?cond1", "?right")
    ),
    rw!("pushdown-join-condition-left-1";
        "(join ?type ?cond1 ?left ?right)" =>
        "(join ?type true (filter ?cond1 ?left) ?right)"
        if not_depend_on("?cond1", "?right")
    ),
    rw!("pushdown-join-condition-right";
        "(join ?type (and ?cond1 ?cond2) ?left ?right)" =>
        "(join ?type ?cond2 ?left (filter ?cond1 ?right))"
        if not_depend_on("?cond1", "?left")
    ),
    rw!("pushdown-join-condition-right-1";
        "(join ?type ?cond1 ?left ?right)" =>
        "(join ?type true ?left (filter ?cond1 ?right))"
        if not_depend_on("?cond1", "?left")
    ),
    rw!("pushdown-filter-apply-left";
        "(filter ?cond (apply ?type ?left ?right))" =>
        "(apply ?type (filter ?cond ?left) ?right)"
        if not_depend_on("?cond", "?right")
    ),
    rw!("pushdown-filter-mark-join-to-semi";
        "(proj ?proj (filter (ref mark) (join mark ?on ?child ?subquery)))" =>
        "(proj ?proj (join semi ?on ?child ?subquery))"
        if not_depend_on_column("?proj", "(ref mark)")
    ),
    rw!("pushdown-filter-mark-join-to-semi-1";
        "(proj ?proj (filter (and (ref mark) ?cond) (join mark ?on ?child ?subquery)))" =>
        "(proj ?proj (filter ?cond (join semi ?on ?child ?subquery)))"
        if not_depend_on_column("?proj", "(ref mark)")
        if not_depend_on_column("?cond", "(ref mark)")
    ),
    rw!("pushdown-filter-mark-join-to-anti";
        "(proj ?proj (filter (not (ref mark)) (join mark ?on ?child ?subquery)))" =>
        "(proj ?proj (join anti ?on ?child ?subquery))"
        if not_depend_on_column("?proj", "(ref mark)")
    ),
    rw!("pushdown-filter-mark-join-to-anti-1";
        "(proj ?proj (filter (and (not (ref mark)) ?cond) (join mark ?on ?child ?subquery)))" =>
        "(proj ?proj (filter ?cond (join anti ?on ?child ?subquery)))"
        if not_depend_on_column("?proj", "(ref mark)")
        if not_depend_on_column("?cond", "(ref mark)")
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
pub fn join_reorder_rules() -> Vec<Rewrite> { vec![
    // we only have right rotation rule,
    // because the initial state is always a left-deep tree
    // thus left rotation is not needed.
    rw!("inner-join-right-rotate";
        "(join inner ?cond1 (join inner ?cond2 ?left ?mid) ?right)" =>
        "(join inner (and ?cond1 ?cond2) ?left (join inner true ?mid ?right))"
    ),
    rw!("inner-join-right-rotate-1";
        "(proj ?proj (join inner ?cond
            (proj ?projl (join inner ?condl ?left ?mid))
            ?right
        ))" =>
        "(proj ?proj (join inner (and ?cond ?condl)
            ?left
            (join inner true ?mid ?right)
        ))"
    ),
    rw!("inner-join-swap";
        // needs a top projection to keep the schema
        "(proj ?proj (join inner ?cond ?left ?right))" =>
        "(proj ?proj (join inner ?cond ?right ?left))"
    ),
    rw!("inner-hash-join-swap";
        "(proj ?proj (hashjoin inner ?cond ?lkeys ?rkeys ?left ?right))" =>
        "(proj ?proj (hashjoin inner ?cond ?rkeys ?lkeys ?right ?left))"
    ),
]}

#[rustfmt::skip]
pub fn hash_join_rules() -> Vec<Rewrite> { vec![
    rw!("hash-join-on-one-eq";
        "(join ?type (= ?l1 ?r1) ?left ?right)" =>
        "(hashjoin ?type true (list ?l1) (list ?r1) ?left ?right)"
        if not_depend_on("?l1", "?right")
        if not_depend_on("?r1", "?left")
    ),
    rw!("hash-join-on-two-eq";
        "(join ?type (and (= ?l1 ?r1) (= ?l2 ?r2)) ?left ?right)" =>
        "(hashjoin ?type true (list ?l1 ?l2) (list ?r1 ?r2) ?left ?right)"
        if not_depend_on("?l1", "?right")
        if not_depend_on("?l2", "?right")
        if not_depend_on("?r1", "?left")
        if not_depend_on("?r2", "?left")
    ),
    rw!("hash-join-on-three-eq";
        "(join ?type (and (= ?l1 ?r1) (and (= ?l2 ?r2) (= ?l3 ?r3))) ?left ?right)" =>
        "(hashjoin ?type true (list ?l1 ?l2 ?l3) (list ?r1 ?r2 ?r3) ?left ?right)"
        if not_depend_on("?l1", "?right")
        if not_depend_on("?l2", "?right")
        if not_depend_on("?l3", "?right")
        if not_depend_on("?r1", "?left")
        if not_depend_on("?r2", "?left")
        if not_depend_on("?r3", "?left")
    ),
    rw!("hash-join-on-one-eq-1";
        // only valid for inner join
        "(join inner (and (= ?l1 ?r1) ?cond) ?left ?right)" =>
        "(filter ?cond (hashjoin inner true (list ?l1) (list ?r1) ?left ?right))"
        if not_depend_on("?l1", "?right")
        if not_depend_on("?r1", "?left")
    ),
    rw!("hash-join-on-one-eq-2";
        "(join semi (and (= ?l1 ?r1) ?cond) ?left ?right)" =>
        "(hashjoin semi ?cond (list ?l1) (list ?r1) ?left ?right)"
        if not_depend_on("?l1", "?right")
        if not_depend_on("?r1", "?left")
    ),
    rw!("hash-join-on-one-eq-3";
        "(join anti (and (= ?l1 ?r1) ?cond) ?left ?right)" =>
        "(hashjoin anti ?cond (list ?l1) (list ?r1) ?left ?right)"
        if not_depend_on("?l1", "?right")
        if not_depend_on("?r1", "?left")
    ),
    // allow reverting hashjoin to join so that projections and filters can be pushed down
    rw!("hash-join-on-one-eq-rev";
        "(hashjoin ?type ?cond (list ?l1) (list ?r1) ?left ?right)" =>
        "(join ?type (and ?cond (= ?l1 ?r1)) ?left ?right)"
    ),
    rw!("hash-join-on-two-eq-rev";
        "(hashjoin ?type ?cond (list ?l1 ?l2) (list ?r1 ?r2) ?left ?right)" =>
        "(join ?type (and ?cond (and (= ?l1 ?r1) (= ?l2 ?r2))) ?left ?right)"
    ),
    rw!("hash-join-on-three-eq-rev";
        "(hashjoin ?type ?cond (list ?l1 ?l2 ?l3) (list ?r1 ?r2 ?r3) ?left ?right)" =>
        "(join ?type (and ?cond (and (= ?l1 ?r1) (and (= ?l2 ?r2) (= ?l3 ?r3)))) ?left ?right)"
    ),
]}

#[rustfmt::skip]
pub fn subquery_rules() -> Vec<Rewrite> { vec![
    rw!("mark-apply-to-semi-apply";
        "(proj ?proj (filter (ref mark) (apply mark ?child ?subquery)))" =>
        "(proj ?proj (apply semi ?child ?subquery))"
        if not_depend_on_column("?proj", "(ref mark)")
    ),
    rw!("mark-apply-to-anti-apply";
        "(proj ?proj (filter (not (ref mark)) (apply mark ?child ?subquery)))" =>
        "(proj ?proj (apply anti ?child ?subquery))"
        if not_depend_on_column("?proj", "(ref mark)")
    ),
    rw!("outer-apply-to-cross-apply-filter";
        "(filter ?cond (apply left_outer ?left ?right))" =>
        "(filter ?cond (apply inner ?left ?right))"
        // FIXME: should be
        // if null_reject("?right", "?cond")
        if depend_on("?cond", "?right")
    ),
    rw!("outer-apply-to-cross-apply-agg";
        // agg always returns a single row
        "(apply left_outer ?left (proj ?proj (agg ?aggs ?right)))" =>
        "(apply inner ?left (proj ?proj (agg ?aggs ?right)))"
    ),
    // Orthogonal Optimization of Subqueries and Aggregation
    // https://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.563.8492&rep=rep1&type=pdf
    // Figure 4 Rule (1)
    rw!("apply-to-join";
        "(apply ?type ?left ?right)" =>
        "(join ?type true ?left ?right)"
        if not_depend_on("?right", "?left")
    ),
    // Figure 4 Rule (2)
    rw!("apply-filter-to-join";
        "(apply ?type ?left (filter ?cond ?right))" =>
        "(join ?type ?cond ?left ?right)"
        if not_depend_on("?right", "?left")
    ),
    // Figure 4 Rule (3)
    rw!("pushdown-apply-filter";
        "(apply inner ?left (filter ?cond ?right))" =>
        "(filter ?cond (apply inner ?left ?right))"
    ),
    // Figure 4 Rule (4)
    rw!("pushdown-apply-proj";
        "(apply inner ?left (proj ?keys ?right))" =>
        { extract_key("(proj ?new_keys (apply inner ?left ?right))") }
    ),
    rw!("pushdown-semi-apply-proj";
        "(apply semi ?left (proj ?proj ?right))" =>
        "(apply semi ?left ?right)"
    ),
    rw!("pushdown-anti-apply-proj";
        "(apply anti ?left (proj ?proj ?right))" =>
        "(apply anti ?left ?right)"
    ),
    rw!("pushdown-mark-apply-proj";
        "(apply mark ?left (proj ?proj ?right))" =>
        "(apply mark ?left ?right)"
    ),
    // Figure 4 Rule (8)
    rw!("pushdown-apply-group-agg";
        "(apply inner ?left (hashagg ?keys ?aggs ?right))" =>
        // ?new_keys = ?left || ?keys
        { extract_key("(hashagg ?new_keys ?aggs (apply inner ?left ?right))") }
        // FIXME: this rule is correct only if
        // 1. all aggregate functions satisfy: agg({}) = agg({null})
        // 2. the left table has a key
    ),
    // Figure 4 Rule (9)
    rw!("pushdown-apply-scalar-agg";
        "(apply inner ?left (agg ?aggs ?right))" =>
        // ?new_keys = ?left
        { extract_key("(hashagg ?new_keys ?aggs (apply left_outer ?left ?right))") }
        // FIXME: this rule is correct only if
        // 1. all aggregate functions satisfy: agg({}) = agg({null})
        // 2. the left table has a key
    ),
]}

/// Returns an applier that replaces `?new_keys` with the schema of `?left` (|| `?keys`).
fn extract_key(pattern_str: &str) -> impl Applier<Expr, ExprAnalysis> {
    struct ExtractKey {
        pattern: Pattern,
        left: Var,
        keys: Var,
        new_keys: Var,
    }
    impl Applier<Expr, ExprAnalysis> for ExtractKey {
        fn apply_one(
            &self,
            egraph: &mut EGraph,
            eclass: Id,
            subst: &Subst,
            searcher_ast: Option<&PatternAst<Expr>>,
            rule_name: Symbol,
        ) -> Vec<Id> {
            let mut new_keys = egraph[subst[self.left]].data.schema.clone();
            if let Some(keys_id) = subst.get(self.keys) {
                new_keys.extend_from_slice(&egraph[*keys_id].data.schema);
            }
            let id = egraph.add(Expr::List(new_keys.into()));
            let mut subst = subst.clone();
            subst.insert(self.new_keys, id);
            self.pattern
                .apply_one(egraph, eclass, &subst, searcher_ast, rule_name)
        }
    }
    ExtractKey {
        pattern: pattern(pattern_str),
        left: var("?left"),
        keys: var("?keys"),
        new_keys: var("?new_keys"),
    }
}

/// Pushdown projections and prune unused columns.
#[rustfmt::skip]
pub fn projection_pushdown_rules() -> Vec<Rewrite> { vec![
    rw!("identical-proj";
        "(proj ?expr ?child)" => "?child" 
        if produced_eq("?expr", "?child")
    ),
    pushdown("proj", "?exprs", "limit", "?limit ?offset"),
    pushdown("limit", "?limit ?offset", "proj", "?exprs"),
    rw!("pushdown-proj-order";
        "(proj ?exprs (order ?keys ?child))" =>
        { apply_proj("(proj [?exprs] (order [?keys] ?child))") }
    ),
    rw!("pushdown-proj-topn";
        "(proj ?exprs (topn ?limit ?offset ?keys ?child))" =>
        { apply_proj("(proj [?exprs] (topn ?limit ?offset [?keys] ?child))") }
    ),
    rw!("pushdown-proj-filter";
        "(proj ?exprs (filter ?cond ?child))" =>
        { apply_proj("(proj [?exprs] (filter [?cond] ?child))") }
    ),
    rw!("pushdown-proj-agg";
        "(agg ?aggs ?child)" =>
        { apply_proj("(agg [?aggs] ?child)") }
    ),
    rw!("pushdown-proj-hashagg";
        "(hashagg ?keys ?aggs ?child)" =>
        { apply_proj("(hashagg [?keys] [?aggs] ?child)") }
    ),
    rw!("pushdown-proj-join";
        "(proj ?exprs (join ?type ?on ?left ?right))" =>
        { apply_proj("(proj [?exprs] (join ?type [?on] ?left ?right))") }
    ),
    rw!("pushdown-proj-apply";
        "(proj ?exprs (apply ?type ?left ?right))" =>
        { apply_proj("(proj [?exprs] (apply ?type ?left [?right]))") }
    ),
    rw!("pushdown-proj-prune-scan";
        "(proj ?exprs (scan ?table ?columns ?filter))" =>
        { column_prune("(proj [?exprs] (scan ?table ?columns [?filter]))") }
    ),
    rw!("pushdown-proj-prune-agg";
        "(proj ?exprs (agg ?columns ?child))" =>
        { column_prune("(proj [?exprs] (agg ?columns ?child))") }
    ),
    rw!("pushdown-proj-prune-proj";
        "(proj ?exprs (proj ?columns ?child))" =>
        { column_prune("(proj [?exprs] (proj ?columns ?child))") }
    ),
]}

/// Returns true if the column `column` is not used in `expr`.
fn not_depend_on_column(expr: &str, column: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    let expr = var(expr);
    let column_expr = column.parse().unwrap();
    move |egraph, _, subst| {
        let column = egraph.add_expr(&column_expr);
        let columns = &egraph[column].data.columns;
        let used = &egraph[subst[expr]].data.columns;
        used.is_disjoint(columns)
    }
}

/// Returns true if the columns used in `expr` is disjoint from columns produced by `plan`.
fn not_depend_on(expr: &str, plan: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    let expr = var(expr);
    let plan = var(plan);
    move |egraph, _, subst| {
        let used = &egraph[subst[expr]].data.columns;
        let produced = produced(egraph, subst[plan]).collect();
        used.is_disjoint(&produced)
    }
}

/// Returns true if the columns used in `expr` is disjoint from columns produced by `plan`.
fn depend_on(expr: &str, plan: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    let expr = var(expr);
    let plan = var(plan);
    move |egraph, _, subst| {
        let used = &egraph[subst[expr]].data.columns;
        let produced = produced(egraph, subst[plan]).collect();
        !used.is_disjoint(&produced)
    }
}

/// Returns true if the columns used in `expr` is subset of columns produced by `plan`.
fn all_depend_on(expr: &str, plan: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    let expr = var(expr);
    let plan = var(plan);
    move |egraph, _, subst| {
        let used = &egraph[subst[expr]].data.columns;
        let produced = produced(egraph, subst[plan]).collect();
        used.is_subset(&produced)
    }
}

/// Returns the columns produced by the plan.
fn produced(egraph: &EGraph, plan: Id) -> impl Iterator<Item = Expr> + '_ {
    (egraph[plan].data.schema.iter()).map(|id| wrap_ref(egraph, *id))
}

/// Wraps the node with `Ref` if it is not already a `Ref` or `Column`.
fn wrap_ref(egraph: &EGraph, expr: Id) -> Expr {
    egraph[expr]
        .iter()
        .find(|e| matches!(e, Expr::Column(_) | Expr::Ref(_)))
        .cloned()
        .unwrap_or(Expr::Ref(expr))
}

/// Returns true if the columns produced by the two expressions are equal.
///
/// # Example
///
/// The following two expressions produce the same columns:
/// ```text
/// (proj (list $1.1 (ref (+ $1.1 $1.2))) ?child)
/// (list $1.1 (+ $1.1 $1.2))
/// ```
fn produced_eq(expr1: &str, expr2: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    let expr1 = var(expr1);
    let expr2 = var(expr2);
    move |egraph, _, subst| {
        let produced1 = produced(egraph, subst[expr1]).collect_vec();
        let produced2 = produced(egraph, subst[expr2]).collect_vec();
        produced1 == produced2
    }
}

/// The data type of column analysis.
pub type ColumnSet = HashSet<Expr>;

/// Returns all columns referenced in the node.
///
/// For expressions, it is the set of all used columns.
/// For plans, it is the set of all external columns,
/// i.e., columns that are used but not produced by the plan.
///
/// The elements of the set are either `Column` or `Ref`.
///
/// # Example
///
/// ```text
/// (list $1.1 (+ $1.2 (ref (- $1.1))))
/// => { $1.1, $1.2, (ref (- $1.1)) }
///
/// (proj (list $1.1 $2.1)
///     (scan $1 (list $1.1) true))
/// => { $2.1 }
/// ```
pub fn analyze_columns(egraph: &EGraph, enode: &Expr) -> ColumnSet {
    use Expr::*;
    let columns = |i: &Id| &egraph[*i].data.columns;
    let external = |exprs: &[Id], children: &[Id]| {
        let mut set = HashSet::new();
        for id in exprs {
            set.extend(columns(id).clone());
        }
        for id in children {
            for col in produced(egraph, *id) {
                set.remove(&col);
            }
        }
        for id in children {
            set.extend(columns(id).clone());
        }
        set
    };
    match enode {
        // columns
        Column(_) | Ref(_) => [enode.clone()].into(),

        // plans
        Scan(_) => [].into(),
        Values(_) => [].into(),
        Proj([exprs, c]) => external(&[*exprs], &[*c]),
        Filter([cond, c]) => external(&[*cond], &[*c]),
        Order([keys, c]) => external(&[*keys], &[*c]),
        Limit([limit, offset, c]) => external(&[*limit, *offset], &[*c]),
        TopN([limit, offset, keys, c]) => external(&[*limit, *offset, *keys], &[*c]),
        Join([_, on, l, r]) => external(&[*on], &[*l, *r]),
        HashJoin([_, on, lkeys, rkeys, l, r]) | MergeJoin([_, on, lkeys, rkeys, l, r]) => {
            external(&[*on, *lkeys, *rkeys], &[*l, *r])
        }
        Apply([_, l, r]) => external(&[], &[*l, *r]),
        Agg([exprs, c]) => external(&[*exprs], &[*c]),
        HashAgg([keys, aggs, c]) | SortAgg([keys, aggs, c]) => external(&[*keys, *aggs], &[*c]),
        Window([exprs, c]) => external(&[*exprs], &[*c]),

        // other expressions: union columns from all children
        _ => (enode.children().iter())
            .flat_map(|id| columns(id).iter().cloned())
            .collect(),
    }
}

/// Returns an applier that:
/// 1. collect all used columns from `[?vars]`.
/// 2. generate a `proj` node over `?child`, `?left` or `?right`. the projection list is the
///    intersection of used and produced columns.
/// 3. apply the rest `pattern`.
fn apply_proj(pattern_str: &str) -> impl Applier<Expr, ExprAnalysis> {
    struct ProjectionPushdown {
        pattern: Pattern,
        used: Vec<Var>,
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
            let used = (self.used.iter())
                .flat_map(|v| &egraph[subst[*v]].data.columns)
                .cloned()
                .collect::<HashSet<Expr>>();

            let mut subst = subst.clone();
            for &child in &self.children {
                // filter out unused columns from child's schema
                let child_id = subst[child];
                let filtered = produced(egraph, child_id)
                    .filter(|col| used.contains(col))
                    .collect_vec();
                let filtered_ids = filtered.into_iter().map(|col| egraph.add(col)).collect();
                let id = egraph.add(Expr::List(filtered_ids));
                let id = egraph.add(Expr::Proj([id, child_id]));
                subst.insert(child, id);
            }

            self.pattern
                .apply_one(egraph, eclass, &subst, searcher_ast, rule_name)
        }
    }
    ProjectionPushdown {
        pattern: pattern(&pattern_str.replace(['[', ']'], "")),
        used: pattern_str
            .split_whitespace()
            .filter(|s| s.starts_with('[') && s.ends_with(']'))
            .map(|s| var(&s[1..s.len() - 1]))
            .collect(),
        children: ["?child", "?left", "?right"]
            .into_iter()
            .filter(|s| pattern_str.contains(s))
            .map(var)
            .collect(),
    }
}

/// Returns an applier that:
/// 1. collect all used columns from `[?vars]`.
/// 2. filter out unused columns from `?columns`.
/// 3. apply the rest `pattern`.
fn column_prune(pattern_str: &str) -> impl Applier<Expr, ExprAnalysis> {
    struct ColumnPrune {
        pattern: Pattern,
        used: Vec<Var>,
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
            let used = (self.used.iter())
                .flat_map(|v| &egraph[subst[*v]].data.columns)
                .cloned()
                .collect::<HashSet<Expr>>();
            let columns = egraph[subst[self.columns]].as_list();
            let filtered = (columns.iter().cloned())
                .filter(|id| used.contains(&wrap_ref(egraph, *id)))
                .collect();
            let id = egraph.add(Expr::List(filtered));

            let mut subst = subst.clone();
            subst.insert(self.columns, id);
            self.pattern
                .apply_one(egraph, eclass, &subst, searcher_ast, rule_name)
        }
    }
    ColumnPrune {
        pattern: pattern(&pattern_str.replace(['[', ']'], "")),
        used: pattern_str
            .split_whitespace()
            .filter(|s| s.starts_with('[') && s.ends_with(']'))
            .map(|s| var(&s[1..s.len() - 1]))
            .collect(),
        columns: var("?columns"),
    }
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::*;

    fn rules() -> Vec<Rewrite> {
        let mut rules = vec![];
        rules.append(&mut expr::rules());
        rules.append(&mut plan::always_better_rules());
        rules.append(&mut plan::predicate_pushdown_rules());
        rules.append(&mut plan::join_reorder_rules());
        rules.append(&mut plan::hash_join_rules());
        rules
    }

    egg::test_fn! {
        cancel_limit,
        rules(),
        // SELECT name
        // FROM student
        // WHERE true
        // LIMIT 0
        "
        (proj (list $1.2)
        (limit null 0
            (filter true
                (scan $1 (list $1.1 $1.2) null)
            )
        ))" => "
        (proj
            (list $1.2)
            (scan $1 (list $1.1 $1.2) null))
        "
    }

    egg::test_fn! {
        merge_filter,
        rules(),
        // SELECT name
        // FROM student
        // LIMIT 10
        // Order by name
        "
        (proj (list $1.2)
            (limit 10 0
                (order (list $1.2) 
                    (scan $1 (list $1.1 $1.2) null))))
        " => "
        (proj (list $1.2)
            (topn 10 0 (list $1.2)
                (scan $1 (list $1.1 $1.2) null)))
        "
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
            (scan $1 (list $1.1 $1.2) null)
            (scan $2 (list $2.1 $2.2 $2.3) null)
        )))" => "
        (proj (list $1.2 $2.2)
        (join inner (= $1.1 $2.1)
            (scan $1 (list $1.1 $1.2) null)
            (filter (= $2.3 'A')
                (scan $2 (list $2.1 $2.2 $2.3) null)
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
                (scan $1 (list $1.1 $1.2) null)
                (scan $2 (list $2.1 $2.2) null)
            )
            (scan $3 (list $3.1 $3.2) null)
        ))" => "
        (join inner (= $1.1 $2.1)
            (scan $1 (list $1.1 $1.2) null)
            (join inner (= $2.1 $3.1)
                (scan $2 (list $2.1 $2.2) null)
                (scan $3 (list $3.1 $3.2) null)
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
            (scan $1 (list $1.1 $1.2) null)
            (scan $2 (list $2.1 $2.2) null)
        ))" => "
        (hashjoin inner true (list $1.1) (list $2.1)
            (filter (> $1.2 2)
                (scan $1 (list $1.1 $1.2) null)
            )
            (scan $2 (list $2.1 $2.2) null)
        )"
    }

    egg::test_fn! {
        projection_pushdown,
        projection_pushdown_rules(),
        // SELECT sum(a) FROM t1(id, a, b) JOIN t2(id, c, d) ON t1.id = t2.id WHERE a + c > 1;
        "
        (proj (list (ref (sum $1.2)))
            (agg (list (sum $1.2) (sum $2.3))
                (filter (> (+ $1.2 $2.2) 1)
                    (join inner (= $1.1 $2.1)
                        (scan $1 (list $1.1 $1.2 $1.3) null)
                        (scan $2 (list $2.1 $2.2 $2.3) null)
        ))))" => "
        (proj (list (ref (sum $1.2)))
            (agg (list (sum $1.2))
                (proj (list $1.2)
                    (filter (> (+ $1.2 $2.2) 1)
                        (proj (list $1.2 $2.2)
                            (join inner (= $1.1 $2.1)
                                (scan $1 (list $1.1 $1.2) null)
                                (scan $2 (list $2.1 $2.2) null)
        ))))))"
    }

    #[test_case(
        "(list $1.1 (+ $1.2 (ref (- $1.1))))",
        "(list $1.1 $1.2 (ref (- $1.1)))"
    )]
    #[test_case(
        "(agg (list (sum $2.4))
            (window (list (over (sum $2.3) list list))
                (filter (= $1.1 $2.2)
                    (proj (list $1.1 $2.1)
                        (scan $1 (list $1.1) true)))))",
        "(list $2.1 $2.2 $2.3 $2.4)"
    )]
    #[test_case(
        "(join inner (= $1.1 $3.3)
            (filter (= $1.1 $3.1)
                (scan $1 (list $1.1) true))
            (filter (= $2.1 $3.2)
                (scan $2 (list $2.1) true)))",
        "(list $3.1 $3.2 $3.3)"
    )]
    fn column_analysis(expr1: &str, expr2: &str) {
        let mut egraph = EGraph::new(Default::default());
        let id1 = egraph.add_expr(&expr1.parse().unwrap());
        let id2 = egraph.add_expr(&expr2.parse().unwrap());
        let columns1 = &egraph[id1].data.columns;
        let columns2 = &egraph[id2].data.columns;
        assert_eq!(columns1, columns2);
    }
}
