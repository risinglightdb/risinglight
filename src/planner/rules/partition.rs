// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

//! This module converts physical plans into parallel plans.
//!
//! In a parallel query plan, each node represents one or more physical operators, with each
//! operator processing data from one partition. Each node has a [`Partition`] property that
//! describes how the data is partitioned.
//!
//! After the conversion, [`Exchange`](Expr::Exchange) nodes will be inserted when necessary to
//! redistribute data between partitions.

use std::sync::LazyLock;

use egg::{rewrite as rw, Analysis, EGraph, Language};

use super::*;
use crate::planner::RecExpr;

/// Converts a physical plan into a parallel plan.
pub fn to_parallel_plan(mut plan: RecExpr) -> RecExpr {
    // add to_parallel to the root node
    let root_id = Id::from(plan.as_ref().len() - 1);
    plan.add(Expr::ToParallel(root_id));

    let runner = egg::Runner::<_, _, ()>::new(PartitionAnalysis)
        .with_expr(&plan)
        .run(TO_PARALLEL_RULES.iter());
    let extractor = egg::Extractor::new(&runner.egraph, NoToParallel);
    let (_, expr) = extractor.find_best(runner.roots[0]);

    assert!(
        expr.as_ref()
            .iter()
            .all(|node| !matches!(node, Expr::ToParallel(_))),
        "unexpected ToParallel in the parallel plan:\n{}",
        expr.pretty(60)
    );
    expr
}

struct NoToParallel;

impl egg::CostFunction<Expr> for NoToParallel {
    type Cost = usize;
    fn cost<C>(&mut self, enode: &Expr, mut costs: C) -> Self::Cost
    where
        C: FnMut(Id) -> Self::Cost,
    {
        let cost = enode.fold(1usize, |sum, id| sum.saturating_add(costs(id)));
        // if all candidates contain ToParallel, the one with the deepest ToParallel will be chosen.
        if let Expr::ToParallel(_) = enode {
            return cost * 1024;
        }
        cost
    }
}

type Rewrite = egg::Rewrite<Expr, PartitionAnalysis>;

static TO_PARALLEL_RULES: LazyLock<Vec<Rewrite>> = LazyLock::new(|| {
    vec![
        // scan is not partitioned
        rw!("scan-to-parallel";
            "(to_parallel (scan ?table ?columns ?filter))" =>
            "(exchange random (scan ?table ?columns ?filter))"
        ),
        // values and empty are not partitioned
        rw!("values-to-parallel";
            "(to_parallel ?child)" =>
            "(exchange random ?child)"
            if node_is("?child", &["values", "empty"])
        ),
        // projection does not change distribution
        rw!("proj-to-parallel";
            "(to_parallel (proj ?projs ?child))" =>
            "(proj ?projs (to_parallel ?child))"
        ),
        // filter does not change distribution
        rw!("filter-to-parallel";
            "(to_parallel (filter ?cond ?child))" =>
            "(filter ?cond (to_parallel ?child))"
        ),
        // order can not be partitioned
        rw!("order-to-parallel";
            "(to_parallel (order ?key ?child))" =>
            "(order ?key (exchange single (to_parallel ?child)))"
            // TODO: 2-phase ordering
            // "(order ?key (exchange single (order ?key (to_parallel ?child))))"
            // TODO: merge sort in the second phase?
        ),
        // limit can not be partitioned
        rw!("limit-to-parallel";
            "(to_parallel (limit ?limit ?offset ?child))" =>
            "(limit ?limit ?offset (exchange single (to_parallel ?child)))"
        ),
        // topn can not be partitioned
        rw!("topn-to-parallel";
            "(to_parallel (topn ?limit ?offset ?key ?child))" =>
            "(topn ?limit ?offset ?key (exchange single (to_parallel ?child)))"
        ),
        // join is partitioned by left
        rw!("join-to-parallel";
            "(to_parallel (join ?type ?cond ?left ?right))" =>
            "(join ?type ?cond
                (exchange random (to_parallel ?left))
                (exchange broadcast (to_parallel ?right)))"
            if node_is("?type", &["inner", "left_outer", "semi", "anti"])
        ),
        // hash join can be partitioned by join key
        rw!("hashjoin-to-parallel";
            "(to_parallel (hashjoin ?type ?cond ?lkey ?rkey ?left ?right))" =>
            "(hashjoin ?type ?cond ?lkey ?rkey
                (exchange (hash ?lkey) (to_parallel ?left))
                (exchange (hash ?rkey) (to_parallel ?right)))"
        ),
        // merge join can be partitioned by join key
        rw!("mergejoin-to-parallel";
            "(to_parallel (mergejoin ?type ?cond ?lkey ?rkey ?left ?right))" =>
            "(mergejoin ?type ?cond ?lkey ?rkey
                (exchange (hash ?lkey) (to_parallel ?left))
                (exchange (hash ?rkey) (to_parallel ?right)))"
        ),
        // 2-phase aggregation
        rw!("agg-to-parallel";
            "(to_parallel (agg ?aggs ?child))" =>
            { apply_global_aggs("
                (schema ?aggs (agg ?global_aggs (exchange single 
                    (agg ?aggs (exchange random (to_parallel ?child))))))
            ") }
            // to keep the schema unchanged, we add a `schema` node
            // FIXME: check if all aggs are supported in 2-phase aggregation
        ),
        // hash aggregation can be partitioned by group key
        rw!("hashagg-to-parallel";
            "(to_parallel (hashagg ?keys ?aggs ?child))" =>
            "(hashagg ?keys ?aggs (exchange (hash ?keys) (to_parallel ?child)))"
        ),
        // sort aggregation can be partitioned by group key
        rw!("sortagg-to-parallel";
            "(to_parallel (sortagg ?keys ?aggs ?child))" =>
            "(sortagg ?keys ?aggs (exchange (hash ?keys) (to_parallel ?child)))"
        ),
        // window function can not be partitioned for now
        rw!("window-to-parallel";
            "(to_parallel (window ?exprs ?child))" =>
            "(window ?exprs (exchange single (to_parallel ?child)))"
        ),
        // insert
        rw!("insert-to-parallel";
            "(to_parallel (insert ?table ?columns ?child))" =>
            "(insert ?table ?columns (to_parallel ?child))"
        ),
        // delete
        rw!("delete-to-parallel";
            "(to_parallel (delete ?table ?child))" =>
            "(delete ?table (to_parallel ?child))"
        ),
        // copy_from
        rw!("copy_from-to-parallel";
            "(to_parallel (copy_from ?dest ?types))" =>
            "(copy_from ?dest ?types)"
        ),
        // copy_to
        rw!("copy_to-to-parallel";
            "(to_parallel (copy_to ?dest ?child))" =>
            "(copy_to ?dest (to_parallel ?child))"
        ),
        // explain
        rw!("explain-to-parallel";
            "(to_parallel (explain ?child))" =>
            "(explain (to_parallel ?child))"
        ),
        // analyze
        rw!("analyze-to-parallel";
            "(to_parallel (analyze ?child))" =>
            "(analyze (to_parallel ?child))"
        ),
        // no parallel for DDL
        rw!("ddl-to-parallel";
            "(to_parallel ?child)" => "?child"
            if node_is("?child", &["create_table", "create_view", "create_function", "drop"])
        ),
        // unnecessary exchange can be removed
        rw!("remove-exchange";
            "(exchange ?dist ?child)" => "?child"
            if partition_is_same("?child", "?dist")
        ),
        rw!("dedup-exchange";
            "(exchange ?dist (exchange ?dist2 ?child))" =>
            "(exchange ?dist ?child)"
        ),
    ]
});

/// Returns true if the distribution of the used columns is the same as the produced columns.
fn partition_is_same(
    a: &str,
    b: &str,
) -> impl Fn(&mut EGraph<Expr, PartitionAnalysis>, Id, &Subst) -> bool {
    let a = var(a);
    let b = var(b);
    move |egraph, _, subst| egraph[subst[a]].data == egraph[subst[b]].data
}

/// Returns true if the given node is one of the candidates.
fn node_is(
    a: &str,
    candidates: &'static [&'static str],
) -> impl Fn(&mut EGraph<Expr, PartitionAnalysis>, Id, &Subst) -> bool {
    let a = var(a);
    move |egraph, _, subst| candidates.contains(&egraph[subst[a]].nodes[0].to_string().as_str())
}

/// Returns an applier that replaces `?global_aggs` with the nested `?aggs`.
///
/// ```text
/// ?aggs        = (list (sum a) (count b))
/// ?global_aggs = (list (sum (ref (sum a))) (count (ref (count b))))
/// ```
fn apply_global_aggs(pattern_str: &str) -> impl Applier<Expr, PartitionAnalysis> {
    struct ApplyGlobalAggs {
        pattern: Pattern,
        aggs: Var,
        global_aggs: Var,
    }
    impl Applier<Expr, PartitionAnalysis> for ApplyGlobalAggs {
        fn apply_one(
            &self,
            egraph: &mut EGraph<Expr, PartitionAnalysis>,
            eclass: Id,
            subst: &Subst,
            searcher_ast: Option<&PatternAst<Expr>>,
            rule_name: Symbol,
        ) -> Vec<Id> {
            let aggs = egraph[subst[self.aggs]].as_list().to_vec();
            let mut global_aggs = vec![];
            for agg in aggs {
                use Expr::*;
                let ref_id = egraph.add(Expr::Ref(agg));
                let global_agg = match &egraph[agg].nodes[0] {
                    Max(_) => Max(ref_id),
                    Min(_) => Min(ref_id),
                    Sum(_) => Sum(ref_id),
                    Avg(_) => panic!("avg is not supported in 2-phase aggregation"),
                    RowCount => Sum(ref_id),
                    Count(_) => Sum(ref_id),
                    CountDistinct(_) => {
                        panic!("count distinct is not supported in 2-phase aggregation")
                    }
                    First(_) => First(ref_id),
                    Last(_) => Last(ref_id),
                    node => panic!("invalid agg: {}", node),
                };
                global_aggs.push(egraph.add(global_agg));
            }
            let id = egraph.add(Expr::List(global_aggs.into()));
            let mut subst = subst.clone();
            subst.insert(self.global_aggs, id);
            self.pattern
                .apply_one(egraph, eclass, &subst, searcher_ast, rule_name)
        }
    }
    ApplyGlobalAggs {
        pattern: pattern(pattern_str),
        aggs: var("?aggs"),
        global_aggs: var("?global_aggs"),
    }
}

/// Describes how data is partitioned.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum Partition {
    /// Distribution is unknown.
    #[default]
    Unknown,
    /// Data is not partitioned.
    Single,
    /// Data is randomly partitioned.
    Random,
    /// Data is broadcasted to all partitions.
    Broadcast,
    /// Data is partitioned by hash of keys.
    Hash(Box<[Id]>),
}

struct PartitionAnalysis;

impl Analysis<Expr> for PartitionAnalysis {
    type Data = Partition;

    fn make(egraph: &EGraph<Expr, Self>, enode: &Expr) -> Self::Data {
        let x = |id: &Id| egraph[*id].data.clone();
        analyze_partition(enode, x)
    }

    fn merge(&mut self, a: &mut Self::Data, b: Self::Data) -> egg::DidMerge {
        merge_partition(a, b)
    }
}

/// Returns partition of the given plan node.
pub fn analyze_partition(enode: &Expr, x: impl Fn(&Id) -> Partition) -> Partition {
    use Expr::*;
    match enode {
        // partition nodes
        Single => Partition::Single,
        Random => Partition::Random,
        Broadcast => Partition::Broadcast,
        Hash(list) => x(list),
        List(ids) => Partition::Hash(ids.clone()),

        // exchange node changes distribution
        Exchange([dist, _]) => x(dist),

        // leaf nodes
        Scan(_) | Values(_) => Partition::Single,

        // equal to child or left child
        Proj([_, c])
        | Filter([_, c])
        | Order([_, c])
        | Limit([_, _, c])
        | TopN([_, _, _, c])
        | Empty(c)
        | Window([_, c])
        | Agg([_, c])
        | HashAgg([_, _, c])
        | SortAgg([_, _, c])
        | Join([_, _, c, _])
        | Apply([_, c, _])
        | HashJoin([_, _, _, _, c, _])
        | MergeJoin([_, _, _, _, c, _]) => x(c),

        // not a plan node
        _ => Partition::Unknown,
    }
}

fn merge_partition(a: &mut Partition, b: Partition) -> egg::DidMerge {
    if *a == Partition::Unknown && b != Partition::Unknown {
        *a = b;
        egg::DidMerge(true, false)
    } else {
        egg::DidMerge(false, true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_parallel() {
        let input = "
            (hashjoin inner true (list a) (list b)
                (scan t1 (list a) true)
                (scan t2 (list b) true)
            )
        ";
        let distributed = "
            (hashjoin inner true (list a) (list b)
                (exchange (hash (list a)) 
                    (scan t1 (list a) true))
                (exchange (hash (list b)) 
                    (scan t2 (list b) true))
            )
        ";
        let output = to_parallel_plan(input.parse().unwrap());
        let expected: RecExpr = distributed.parse().unwrap();
        assert_eq!(output.to_string(), expected.to_string());
    }

    #[test]
    fn test_two_phase_agg() {
        let input = "
            (agg (list (sum a)) 
                (scan t1 (list a) true))
        ";
        let distributed = "
            (schema (list (sum a))
                (agg (list (sum (ref (sum a))))
                    (exchange single
                        (agg (list (sum a))
                            (exchange random
                                (scan t1 (list a) true))))))
        ";
        let output = to_parallel_plan(input.parse().unwrap());
        let expected: RecExpr = distributed.parse().unwrap();
        assert_eq!(output.to_string(), expected.to_string());
    }
}
