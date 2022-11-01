use egg::Language;

use super::*;
use crate::planner::rules::analyze_rows;

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
        let id = self.egraph.lookup(enode.clone()).unwrap();
        let rows = |i: &Id| self.egraph[*i].data.rows;
        let nlogn = |x: f32| x * (x + 1.0).log2();

        let c = match enode {
            Select(_) | Prune(_) => f32::INFINITY, // should no longer exists
            Scan(list) => {
                let columns = self.egraph[*list].nodes[0].len();
                columns as f32 * rows(&id) as f32
            }
            Order([_, c]) => nlogn(rows(c) as f32) + costs(*c),
            Proj([exprs, c]) | Filter([exprs, c]) => costs(*exprs) * rows(c) as f32 + costs(*c),
            Agg([exprs, groupby, c]) => {
                (costs(*exprs) + costs(*groupby)) * rows(c) as f32 + costs(*c)
            }
            Limit([_, _, c]) => rows(&id) as f32 + costs(*c),
            TopN([_, limit, _, c]) => (rows(&id) as f32 + 1.0).log2() * rows(c) as f32 + costs(*c),
            Join([_, _, l, r]) => rows(l) as f32 * rows(r) as f32 + costs(*l) + costs(*r),
            HashJoin([_, _, _, l, r]) => rows(l) as f32 + rows(r) as f32 + costs(*l) + costs(*r),
            Values(_) | _ => enode.fold(1.0, |sum, id| sum + costs(id)),
        };
        println!("cost: {id} {enode:?} = {c}");
        c
    }
}
