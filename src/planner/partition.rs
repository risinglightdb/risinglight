// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

//! This module converts physical plans into distributed plans.
//!
//! `Exchange` nodes are inserted to shuffle data between partitions.

use egg::{rewrite as rw, Analysis, EGraph, Language};

use super::*;
use crate::planner::RecExpr;

/// Converts a physical plan into a distributed plan.
pub fn to_distributed(plan: RecExpr) -> RecExpr {
    Builder::new(plan).build()
}

/// This builder converts a physical plan into a distributed plan recursively.
struct Builder {
    egraph: egg::EGraph<Expr, PartitionAnalysis>,
    root: Id,
}

impl Builder {
    /// Creates a new builder for the given physical plan.
    fn new(plan: RecExpr) -> Self {
        let mut egraph = egg::EGraph::new(PartitionAnalysis);
        let root = egraph.add_expr(&plan);
        Self { egraph, root }
    }

    fn build(mut self) -> RecExpr {
        let root = self.build_id(self.root, &[]);
        self.recexpr(root)
    }

    /// Get the node from id.
    fn node(&self, id: Id) -> &Expr {
        // each e-class has exactly one node since there is no rewrite or union.
        &self.egraph[id].nodes[0]
    }

    /// Extract a `RecExpr` from id.
    fn recexpr(&self, id: Id) -> RecExpr {
        self.node(id).build_recexpr(|id| self.node(id).clone())
    }

    /// Builds a distributed plan for the given physical `plan`,
    /// requiring that it must be partitioned by `partition_key`.
    /// Returns the node id in the egraph.
    fn build_id(&mut self, plan: Id, partition_key: &[Id]) -> Id {
        use Expr::*;
        let new_plan = match self.node(plan).clone() {
            Proj([proj, child]) => {
                let child = self.build_id(child, partition_key);
                self.egraph.add(Proj([proj, child]))
            }
            Filter([cond, child]) => {
                let child = self.build_id(child, partition_key);
                self.egraph.add(Filter([cond, child]))
            }
            Order([key, child]) => {
                let child = self.build_id(child, partition_key);
                self.egraph.add(Order([key, child]))
            }
            Limit([limit, offset, child]) => {
                let child = self.build_id(child, partition_key);
                self.egraph.add(Limit([limit, offset, child]))
            }
            HashJoin([t, cond, lkey, rkey, left, right]) => {
                let lpartition = self.node(lkey).as_list().to_vec();
                let rpartition = self.node(rkey).as_list().to_vec();
                let left = self.build_id(left, &lpartition);
                let right = self.build_id(right, &rpartition);
                self.egraph
                    .add(HashJoin([t, cond, lkey, rkey, left, right]))
            }
            _ => todo!(),
        };
        if !partition_key.is_empty() && &self.egraph[new_plan].data[..] != partition_key {
            let list = self.egraph.add(List(partition_key.into()));
            let dist = self.egraph.add(Hash(list));
            self.egraph.add(Exchange([dist, new_plan]))
        } else {
            new_plan
        }
    }
}

fn exchange_rules() -> Vec<Rewrite> {
    vec![rw!("exchange-single";
        "(exchange ?dist (exchange ?any ?child))" =>
        "(exchange ?dist ?child)"
        // FIXME: what if ?any = broadcast
    )]
}

fn to_dist_rules() -> Vec<Rewrite> {
    vec![
        rw!("scan-to-dist";
            "(to_dist (scan ?table ?columns ?filter))" =>
            "(exchange random (scan ?table ?columns ?filter))"
        ),
        rw!("proj-to-dist";
            "(to_dist (proj ?projs ?child))" =>
            "(proj ?projs (to_dist ?child))"
        ),
        rw!("filter-to-dist";
            "(to_dist (filter ?cond ?child))" =>
            "(filter ?cond (to_dist ?child))"
        ),
        rw!("order-to-dist";
            "(to_dist (order ?key ?child))" =>
            "(order ?key (exchange single (order ?key (to_dist ?child))))"
            // TODO: merge sort in the second phase?
        ),
        rw!("limit-to-dist";
            "(to_dist (limit ?limit ?offset ?child))" =>
            "(limit ?limit ?offset (exchange single (to_dist ?child)))"
        ),
        rw!("topn-to-dist";
            "(to_dist (topn ?limit ?offset ?key ?child))" =>
            "(topn ?limit ?offset ?key (exchange single (to_dist ?child)))"
        ),
        // inner join can be partitioned by left key
        rw!("inner-join-to-dist-left";
            "(to_dist (join inner ?cond ?left ?right))" =>
            "(join inner ?cond
                (exchange random (to_dist ?left))
                (exchange broadcast (to_dist ?right)))"
        ),
        // ... or by right key
        rw!("inner-join-to-dist-right";
            "(to_dist (join inner ?cond ?left ?right))" =>
            "(join inner ?cond
                (exchange broadcast (to_dist ?left))
                (exchange random (to_dist ?right)))"
        ),
        // outer join can not be partitioned
        rw!("join-to-dist-left";
            "(to_dist (join full_outer ?cond ?left ?right))" =>
            "(join full_outer ?cond
                (exchange single (to_dist ?left))
                (exchange single (to_dist ?right)))"
        ),
        // hash join can be partitioned by join key
        rw!("hashjoin-to-dist";
            "(to_dist (hashjoin ?type ?cond ?lkey ?rkey ?left ?right))" =>
            "(hashjoin ?type ?cond ?lkey ?rkey
                (exchange (hash ?lkey) (to_dist ?left))
                (exchange (hash ?rkey) (to_dist ?right)))"
        ),
        // merge join can be partitioned by join key
        rw!("mergejoin-to-dist";
            "(to_dist (mergejoin ?type ?cond ?lkey ?rkey ?left ?right))" =>
            "(mergejoin ?type ?cond ?lkey ?rkey
                (exchange (hash ?lkey) (to_dist ?left))
                (exchange (hash ?rkey) (to_dist ?right)))"
        ),
        // 2-phase aggregation
        rw!("agg-to-dist";
            "(to_dist (agg ?exprs ?child))" =>
            "(agg ?exprs (exchange single (agg ?exprs (exchange random (to_dist ?child)))))"
        ),
        // hash aggregation can be partitioned by group key
        rw!("hashagg-to-dist";
            "(to_dist (hashagg ?keys ?aggs ?child))" =>
            "(hashagg ?keys ?aggs (exchange (hash ?keys) (to_dist ?child)))"
        ),
        // sort aggregation can be partitioned by group key
        rw!("sortagg-to-dist";
            "(to_dist (sortagg ?keys ?aggs ?child))" =>
            "(sortagg ?keys ?aggs (exchange (hash ?keys) (to_dist ?child)))"
        ),
        // window function can not be partitioned for now
        rw!("window-to-dist";
            "(to_dist (window ?exprs ?child))" =>
            "(window ?exprs (exchange single (to_dist ?child)))"
        ),
    ]
}

struct PartitionAnalysis;

impl Analysis<Expr> for PartitionAnalysis {
    type Data = Box<[Id]>;

    /// Analyze a node and give the result.
    fn make(egraph: &EGraph<Expr, Self>, enode: &Expr) -> Self::Data {
        use Expr::*;
        let x = |c: &Id| egraph[*c].data.clone();
        match enode {
            // list is the source for the following nodes
            List(ids) => ids.clone(),

            // equal to child
            Proj([_, c]) | Agg([_, c]) | Filter([_, c]) | Order([_, c]) | Limit([_, _, c])
            | TopN([_, _, _, c]) | Empty(c) | Window([_, c]) => x(c),

            Join(_) | Apply(_) => Box::new([]),
            Scan(_) | Values(_) => Box::new([]),

            HashJoin([_, _, lkey, _, _, _]) | MergeJoin([_, _, lkey, _, _, _]) => x(lkey),
            HashAgg([key, _, _]) | SortAgg([key, _, _]) => x(key),

            // not plan node
            _ => Box::new([]),
        }
    }

    fn merge(&mut self, a: &mut Self::Data, b: Self::Data) -> egg::DidMerge {
        unimplemented!("merge should not be called")
    }
}

// /// The data type of partition analysis.
// pub type PartitionKey = Box<[Id]>;

// /// Returns all columns involved in the node.
// pub fn analyze_partition_key(enode: &Expr, x: impl Fn(&Id) -> PartitionKey) -> PartitionKey {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_distributed() {
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
        let output = to_distributed(input.parse().unwrap());
        println!("{}", output.pretty(60));
    }
}
