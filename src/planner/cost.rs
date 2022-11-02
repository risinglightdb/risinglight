use egg::Language;
use tracing::debug;

use super::*;
use crate::planner::rules::analyze_rows;

pub struct NoPrune;

impl egg::CostFunction<Expr> for NoPrune {
    type Cost = u32;
    fn cost<C>(&mut self, enode: &Expr, mut costs: C) -> Self::Cost
    where
        C: FnMut(Id) -> Self::Cost,
    {
        match enode {
            Expr::Prune(_) | Expr::Select(_) | Expr::Distinct(_) => u32::MAX,
            _ => enode.fold(1, |sum, id| sum.checked_add(costs(id)).unwrap_or(u32::MAX)),
        }
    }
}

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
        let rows = |i: &Id| self.egraph[*i].data.rows as f32;
        let cols = |i: &Id| match &self.egraph[*i].data.schema {
            Some(s) => s.len() as f32,
            None => f32::INFINITY,
        };
        let nlogn = |x: f32| x * (x + 1.0).log2();

        let c = match enode {
            Select(_) | Prune(_) => f32::INFINITY, // should no longer exists
            Scan(_) => cols(id) * rows(id) * 10.0,
            Order([_, c]) => nlogn(rows(c)) + costs(c),
            Proj([exprs, c]) | Filter([exprs, c]) => costs(exprs) * rows(c) + costs(c),
            Agg([exprs, groupby, c]) => (costs(exprs) + costs(groupby)) * rows(c) + costs(c),
            Limit([_, _, c]) => rows(id) + costs(c),
            TopN([_, limit, _, c]) => (rows(id) + 1.0).log2() * rows(c) + costs(c),
            Join([_, _, l, r]) => rows(l) * rows(r) * (cols(l) + cols(r)) + costs(l) + costs(r),
            HashJoin([_, _, _, l, r]) => {
                (rows(l) + rows(r)) * (cols(l) + cols(r)) + costs(l) + costs(r)
            }
            Values(_) | _ => enode.fold(1.0, |sum, id| sum + costs(&id)),
        };
        println!(
            "{id}\t{enode:?}\tcost={c}, rows={}, cols={}",
            rows(id),
            cols(id)
        );
        c
    }
}
