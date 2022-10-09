use egg::Language;

use super::*;

pub struct CostFn<'a> {
    pub egraph: &'a EGraph,
}

impl egg::CostFunction<Expr> for CostFn<'_> {
    type Cost = u32;
    fn cost<C>(&mut self, enode: &Expr, mut costs: C) -> Self::Cost
    where
        C: FnMut(Id) -> Self::Cost,
    {
        let op_cost = match enode {
            // should no longer exists
            Expr::Select(_) | Expr::Prune(_) => u32::MAX,
            _ => 1,
        };
        enode.fold(op_cost, |sum, id| {
            sum.checked_add(costs(id)).unwrap_or(u32::MAX)
        })
    }
}
