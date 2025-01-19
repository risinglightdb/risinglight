// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

//! Plan optimization rules.

use itertools::Itertools;

use super::schema::schema_is_eq;
use super::*;
use crate::binder::{IndexType, VectorDistance};
use crate::planner::ExprExt;
use crate::types::DataValue;

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
    // in -> exists
    rw!("in-to-exists";
        "(in ?expr ?subquery)" =>
        { apply_column0("(exists (filter (= ?expr ?column0) ?subquery))") }
        if is_not_list("?subquery")
    ),
    // exists -> semi apply
    rw!("exists-to-semi-apply";
        "(filter (exists ?subquery) ?child)" =>
        "(apply semi ?child ?subquery)"
        if is_not_list("?subquery")
    ),
    rw!("not-exists-to-anti-apply";
        "(filter (not (exists ?subquery)) ?child)" =>
        "(apply anti ?child ?subquery)"
        if is_not_list("?subquery")
    ),
    rw!("left-outer-apply-to-inner-apply";
        "(filter ?cond (apply left_outer ?left ?right))" =>
        "(filter ?cond (apply inner ?left ?right))"
        // FIXME: should be
        // if null_reject("?right", "?cond")
        if depend_on("?cond", "?right")
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

/// Returns an applier that replaces `?column0` with the first column of `?subquery`.
fn apply_column0(pattern_str: &str) -> impl Applier<Expr, ExprAnalysis> {
    struct ApplyColumn0 {
        pattern: Pattern,
        subquery: Var,
        column0: Var,
    }
    impl Applier<Expr, ExprAnalysis> for ApplyColumn0 {
        fn apply_one(
            &self,
            egraph: &mut EGraph,
            eclass: Id,
            subst: &Subst,
            searcher_ast: Option<&PatternAst<Expr>>,
            rule_name: Symbol,
        ) -> Vec<Id> {
            let mut id = egraph[subst[self.subquery]].data.schema[0];
            if !egraph[id]
                .nodes
                .iter()
                .any(|e| matches!(e, Expr::Column(_) | Expr::Ref(_)))
            {
                id = egraph.add(Expr::Ref(id));
            }
            let mut subst = subst.clone();
            subst.insert(self.column0, id);
            self.pattern
                .apply_one(egraph, eclass, &subst, searcher_ast, rule_name)
        }
    }
    ApplyColumn0 {
        pattern: pattern(pattern_str),
        subquery: var("?subquery"),
        column0: var("?column0"),
    }
}

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
        if schema_is_eq("?expr", "?child")
    ),
    rw!("pushdown-proj-proj";
        "(proj ?proj1 (proj ?proj2 ?child))" =>
        "(proj ?proj1 ?child)"
        if all_depend_on("?proj1", "?child")
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
    rw!("pushdown-proj-scan";
        "(proj ?exprs (scan ?table ?columns ?filter))" =>
        { column_prune("(proj ?exprs (scan ?table ?columns ?filter))") }
    ),
]}

/// Pushdown projections and prune unused columns.
#[rustfmt::skip]
pub fn index_scan_rules() -> Vec<Rewrite> { vec![
    rw!("vector-index-scan-1";
        "(order (list (<-> ?column ?vector)) (scan ?table ?columns ?filter))" => "(vector_index_scan ?table ?columns ?filter <-> ?column ?vector)"
        if has_vector_index("?column", "<->", "?vector", "?filter")
    ),
    rw!("vector-index-scan-2";
        "(order (list (<#> ?column ?vector)) (scan ?table ?columns ?filter))" => "(vector_index_scan ?table ?columns ?filter <#> ?column ?vector)"
        if has_vector_index("?column", "<#>", "?vector", "?filter")
    ),
    rw!("vector-index-scan-3";
        "(order (list (<=> ?column ?vector)) (scan ?table ?columns ?filter))" => "(vector_index_scan ?table ?columns ?filter <=> ?column ?vector)"
        if has_vector_index("?column", "<=>", "?vector", "?filter")
    ),
]}

fn has_vector_index(
    column: &str,
    op: &str,
    vector: &str,
    filter: &str,
) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    let column = var(column);
    let vector = var(vector);
    let filter = var(filter);
    let op = op.to_string();
    move |egraph, _, subst| {
        let filter = &egraph[subst[filter]].data;
        let vector = &egraph[subst[vector]].data;
        let column = &egraph[subst[column]].data;
        let Ok(vector_op) = op.parse::<VectorDistance>() else {
            return false;
        };
        // Only support null filter or always true filter for now
        if !matches!(filter.constant, Some(DataValue::Bool(true)) | None) {
            return false;
        }
        if !matches!(vector.constant, Some(DataValue::Vector(_))) {
            return false;
        }
        if column.columns.len() != 1 {
            return false;
        }
        let column = column.columns.iter().next().unwrap();
        let Expr::Column(col) = column else {
            return false;
        };
        let catalog = &egraph.analysis.catalog;
        let indexes = catalog.get_index_on_table(col.schema_id, col.table_id);
        for index_id in indexes {
            let index = catalog.get_index_by_id(col.schema_id, index_id).unwrap();
            if index.column_idxs() != [col.column_id] {
                continue;
            }
            if let IndexType::IvfFlat { distance, .. } = index.index_type() {
                if distance == vector_op {
                    return true;
                }
            }
        }
        false
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
    (egraph[plan].data.schema.iter()).map(|id| {
        egraph[*id]
            .iter()
            .find(|e| matches!(e, Expr::Column(_) | Expr::Ref(_)))
            .cloned()
            .unwrap_or(Expr::Ref(*id))
    })
}

/// Returns true if the node `var1` is not a list.
fn is_not_list(var1: &str) -> impl Fn(&mut EGraph, Id, &Subst) -> bool {
    let var1 = var(var1);
    move |egraph, _, subst| {
        !egraph[subst[var1]]
            .nodes
            .iter()
            .any(|e| matches!(e, Expr::List(_)))
    }
}

/// The data type of column analysis.
///
/// It is the set of columns used in the expression or plan.
/// The elements of the set are either `Column` or `Ref`.
pub type ColumnSet = HashSet<Expr>;

/// Returns all columns involved in the node.
pub fn analyze_columns(egraph: &EGraph, enode: &Expr) -> ColumnSet {
    use Expr::*;
    let columns = |i: &Id| &egraph[*i].data.columns;
    match enode {
        Column(_) | Ref(_) => [enode.clone()].into_iter().collect(),
        // others: merge from all children
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
/// 1. collect all used columns from `?exprs` and `?filter`.
/// 2. filter out unused columns from `?columns`.
/// 3. apply the rest `pattern`.
fn column_prune(pattern_str: &str) -> impl Applier<Expr, ExprAnalysis> {
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
    ColumnPrune {
        pattern: pattern(pattern_str),
        used: [var("?exprs"), var("?filter")],
        columns: var("?columns"),
    }
}

#[cfg(test)]
mod tests {
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
        proj (list $1.2)
        (limit 10 0
            (order (list $1.2) 
                (scan $1 (list $1.1 $1.2) null)
            )
        )" => "
        proj (list $1.2)
        （topn 10 0 (list $1.2) 
            (scan $1 (list $1.1 $1.2) null)
        )
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
        // SELECT a FROM t1(id, a, b) JOIN t2(id, c, d) ON t1.id = t2.id WHERE a + c > 1;
        "
        (proj (list $1.2)
        (filter (> (+ $1.2 $2.2) 1)
        (join inner (= $1.1 $2.1)
            (scan $1 (list $1.1 $1.2 $1.3) null)
            (scan $2 (list $2.1 $2.2 $2.3) null)
        )))" => "
        (proj (list $1.2)
        (filter (> (+ $1.2 $2.2) 1)
        (proj (list $1.2 $2.2)
        (join inner (= $1.1 $2.1)
            (scan $1 (list $1.1 $1.2) null)
            (scan $2 (list $2.1 $2.2) null)
        ))))"
    }
}
