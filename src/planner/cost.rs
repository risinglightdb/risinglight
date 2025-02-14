// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

//! Cost functions to extract the best plan.

use egg::Language;
use tracing::debug;

use super::*;

/// The main cost function.
pub struct CostFn<'a> {
    pub egraph: &'a EGraph,
}

impl egg::CostFunction<Expr> for CostFn<'_> {
    type Cost = f32;
    fn cost<C>(&mut self, enode: &Expr, mut costs: C) -> Self::Cost
    where
        C: FnMut(Id) -> Self::Cost,
    {
        use Expr::*;
        let id = &self.egraph.lookup(enode.clone()).unwrap();
        let mut costs = |i: &Id| costs(*i);
        let rows = |i: &Id| self.egraph[*i].data.rows;
        let cols = |i: &Id| self.egraph[*i].data.schema.len() as f32;
        let nlogn = |x: f32| x * (x + 1.0).log2();
        // The cost of build output chunks of a plan.
        let build = || rows(id) * cols(id);
        // The cost of an operation in hash table.
        let hash = |size: f32| (size + 1.0).log2() * 0.01;

        let c = match enode {
            // plan nodes
            Scan(_) | Values(_) | IndexScan(_) => build(),
            Order([_, c]) => nlogn(rows(c)) + build() + costs(c),
            Filter([exprs, c]) => costs(exprs) * rows(c) + build() + costs(c),
            Proj([exprs, c]) | Window([exprs, c]) => costs(exprs) * rows(c) + costs(c),
            Agg([exprs, c]) => costs(exprs) * rows(c) + build() + costs(c),
            HashAgg([keys, aggs, c]) => {
                (hash(rows(id)) + costs(keys) + costs(aggs)) * rows(c) + build() + costs(c)
            }
            SortAgg([keys, aggs, c]) => (costs(keys) + costs(aggs)) * rows(c) + build() + costs(c),
            Limit([_, _, c]) => build() + costs(c),
            TopN([_, _, _, c]) => (rows(id) + 1.0).log2() * rows(c) + build() + costs(c),
            Join([_, cond, l, r]) => {
                costs(cond) * rows(l) * rows(r) + build() + costs(l) + costs(r)
            }
            HashJoin([t, cond, lkey, rkey, l, r]) => {
                let hash = match self.egraph[*t].nodes[0] {
                    Semi | Anti => hash(rows(r)) * (rows(l) + rows(r)),
                    _ => hash(rows(l)) * (rows(l) + rows(r)),
                };
                hash + costs(lkey) * rows(l)
                    + costs(rkey) * rows(r)
                    + costs(cond) * (rows(l) + rows(r)) // may not right
                    + build()
                    + costs(l)
                    + costs(r)
            }
            MergeJoin([_, cond, lkey, rkey, l, r]) => {
                build()
                    + costs(lkey) * rows(l)
                    + costs(rkey)  * rows(r)
                    + costs(cond) * (rows(l) + rows(r)) // may not right
                    + costs(l)
                    + costs(r)
            }
            Apply([_, l, r]) => build() + costs(l) + rows(l) * costs(r),
            Insert([_, _, c]) | CopyTo([_, c]) => rows(c) * cols(c) + costs(c),
            Empty(_) => 0.0,
            Max1Row(c) => costs(c),
            // expressions
            Column(_) | Ref(_) => 0.01, // column reference is almost free
            List(_) => enode.fold(0.01, |sum, id| sum + costs(&id)), // list is almost free
            // each operator has a cost of 0.1
            _ => enode.fold(0.1, |sum, id| sum + costs(&id)),
        };
        debug!(
            "{id}\t{enode:?}\tcost={c}, rows={}, cols={}",
            rows(id),
            cols(id)
        );
        c
    }
}
