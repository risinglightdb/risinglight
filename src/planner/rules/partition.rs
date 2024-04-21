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
    expr
}

struct NoToParallel;

impl egg::CostFunction<Expr> for NoToParallel {
    type Cost = usize;
    fn cost<C>(&mut self, enode: &Expr, mut costs: C) -> Self::Cost
    where
        C: FnMut(Id) -> Self::Cost,
    {
        if let Expr::ToParallel(_) = enode {
            return usize::MAX;
        }
        enode.fold(1, |sum, id| sum.saturating_add(costs(id)))
    }
}

type Rewrite = egg::Rewrite<Expr, PartitionAnalysis>;

static TO_PARALLEL_RULES: LazyLock<Vec<Rewrite>> = LazyLock::new(|| {
    vec![
        // scan is not partitioned
        rw!("scan-to-dist";
            "(to_parallel (scan ?table ?columns ?filter))" =>
            "(exchange random (scan ?table ?columns ?filter))"
        ),
        // values is not partitioned
        rw!("values-to-dist";
            "(to_parallel (values ?values))" =>
            "(exchange random (values ?values))"
        ),
        // projection does not change distribution
        rw!("proj-to-dist";
            "(to_parallel (proj ?projs ?child))" =>
            "(proj ?projs (to_parallel ?child))"
        ),
        // filter does not change distribution
        rw!("filter-to-dist";
            "(to_parallel (filter ?cond ?child))" =>
            "(filter ?cond (to_parallel ?child))"
        ),
        // order can not be partitioned
        rw!("order-to-dist";
            "(to_parallel (order ?key ?child))" =>
            "(order ?key (exchange single (to_parallel ?child)))"
            // TODO: 2-phase ordering
            // "(order ?key (exchange single (order ?key (to_parallel ?child))))"
            // TODO: merge sort in the second phase?
        ),
        // limit can not be partitioned
        rw!("limit-to-dist";
            "(to_parallel (limit ?limit ?offset ?child))" =>
            "(limit ?limit ?offset (exchange single (to_parallel ?child)))"
        ),
        // topn can not be partitioned
        rw!("topn-to-dist";
            "(to_parallel (topn ?limit ?offset ?key ?child))" =>
            "(topn ?limit ?offset ?key (exchange single (to_parallel ?child)))"
        ),
        // inner join is partitioned by left
        // as the left side is materialized in memory
        rw!("inner-join-to-dist";
            "(to_parallel (join inner ?cond ?left ?right))" =>
            "(join inner ?cond
                (exchange random (to_parallel ?left))
                (exchange broadcast (to_parallel ?right)))"
        ),
        // outer join can not be partitioned
        rw!("join-to-dist";
            "(to_parallel (join full_outer ?cond ?left ?right))" =>
            "(join full_outer ?cond
                (exchange single (to_parallel ?left))
                (exchange single (to_parallel ?right)))"
        ),
        // hash join can be partitioned by join key
        rw!("hashjoin-to-dist";
            "(to_parallel (hashjoin ?type ?cond ?lkey ?rkey ?left ?right))" =>
            "(hashjoin ?type ?cond ?lkey ?rkey
                (exchange (hash ?lkey) (to_parallel ?left))
                (exchange (hash ?rkey) (to_parallel ?right)))"
        ),
        // merge join can be partitioned by join key
        rw!("mergejoin-to-dist";
            "(to_parallel (mergejoin ?type ?cond ?lkey ?rkey ?left ?right))" =>
            "(mergejoin ?type ?cond ?lkey ?rkey
                (exchange (hash ?lkey) (to_parallel ?left))
                (exchange (hash ?rkey) (to_parallel ?right)))"
        ),
        // 2-phase aggregation
        rw!("agg-to-dist";
            "(to_parallel (agg ?exprs ?child))" =>
            "(agg ?exprs (exchange single (agg ?exprs (exchange random (to_parallel ?child)))))"
        ),
        // hash aggregation can be partitioned by group key
        rw!("hashagg-to-dist";
            "(to_parallel (hashagg ?keys ?aggs ?child))" =>
            "(hashagg ?keys ?aggs (exchange (hash ?keys) (to_parallel ?child)))"
        ),
        // sort aggregation can be partitioned by group key
        rw!("sortagg-to-dist";
            "(to_parallel (sortagg ?keys ?aggs ?child))" =>
            "(sortagg ?keys ?aggs (exchange (hash ?keys) (to_parallel ?child)))"
        ),
        // window function can not be partitioned for now
        rw!("window-to-dist";
            "(to_parallel (window ?exprs ?child))" =>
            "(window ?exprs (exchange single (to_parallel ?child)))"
        ),
        // explain
        rw!("explain-to-dist";
            "(to_parallel (explain ?child))" =>
            "(explain (to_parallel ?child))"
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
}
