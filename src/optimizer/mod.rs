use crate::logical_planner::{
    LogicalCreateTable, LogicalDelete, LogicalDrop, LogicalExplain, LogicalFilter, LogicalHashAgg,
    LogicalInsert, LogicalJoin, LogicalJoinTable, LogicalLimit, LogicalOrder, LogicalPlan,
    LogicalProjection, LogicalSeqScan, LogicalSimpleAgg,
};

mod arith_expr_simplification;
mod constant_folding;

pub use arith_expr_simplification::*;
pub use constant_folding::*;

// The optimizer will do query optimization.
// It will do both rule-based optimization (predicate pushdown, constant folding and common
// expression extraction) , and cost-based optimization (Join reordering and join algorithm
// selection). It takes LogicalPlan as input and returns a new LogicalPlan which could be used to
// generate phyiscal plan.
#[derive(Default)]
pub struct Optimizer {}

impl Optimizer {
    pub fn optimize(&mut self, plan: LogicalPlan) -> LogicalPlan {
        // TODO: add optimization rules
        let mut constant_folding = ConstantFoldingRewriter {};
        let plan_0 = constant_folding.rewrite_plan(plan);
        let mut arith_expr_simplification = ArithExprSimplification {};
        arith_expr_simplification.rewrite_plan(plan_0)
    }
}

// PlanRewriter is a plan visitor.
// User could implement the own optimization rules by implement PlanRewriter trait easily.
pub trait PlanRewriter {
    fn rewrite_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Dummy => LogicalPlan::Dummy,
            LogicalPlan::CreateTable(plan) => self.rewrite_create_table(plan),
            LogicalPlan::Drop(plan) => self.rewrite_drop(plan),
            LogicalPlan::Insert(plan) => self.rewrite_insert(plan),
            LogicalPlan::Join(plan) => self.rewrite_join(plan),
            LogicalPlan::SeqScan(plan) => self.rewrite_seqscan(plan),
            LogicalPlan::Projection(plan) => self.rewrite_projection(plan),
            LogicalPlan::Filter(plan) => self.rewrite_filter(plan),
            LogicalPlan::Order(plan) => self.rewrite_order(plan),
            LogicalPlan::Limit(plan) => self.rewrite_limit(plan),
            LogicalPlan::Explain(plan) => self.rewrite_explain(plan),
            LogicalPlan::SimpleAgg(plan) => self.rewrite_simple_agg(plan),
            LogicalPlan::HashAgg(plan) => self.rewrite_hash_agg(plan),
            LogicalPlan::Delete(plan) => self.rewrite_delete(plan),
        }
    }

    fn rewrite_create_table(&mut self, plan: LogicalCreateTable) -> LogicalPlan {
        LogicalPlan::CreateTable(plan)
    }

    fn rewrite_drop(&mut self, plan: LogicalDrop) -> LogicalPlan {
        LogicalPlan::Drop(plan)
    }

    fn rewrite_insert(&mut self, plan: LogicalInsert) -> LogicalPlan {
        LogicalPlan::Insert(plan)
    }

    fn rewrite_join(&mut self, plan: LogicalJoin) -> LogicalPlan {
        let mut join_table_plans = vec![];
        for plan in plan.join_table_plans.into_iter() {
            join_table_plans.push(LogicalJoinTable {
                table_plan: Box::new(self.rewrite_plan(*plan.table_plan)),
                join_op: plan.join_op,
            });
        }
        LogicalPlan::Join(LogicalJoin {
            relation_plan: Box::new(self.rewrite_plan(*plan.relation_plan)),
            join_table_plans,
        })
    }

    fn rewrite_seqscan(&mut self, plan: LogicalSeqScan) -> LogicalPlan {
        LogicalPlan::SeqScan(plan)
    }

    fn rewrite_projection(&mut self, plan: LogicalProjection) -> LogicalPlan {
        LogicalPlan::Projection(LogicalProjection {
            project_expressions: plan.project_expressions,
            child: Box::new(self.rewrite_plan(*plan.child)),
        })
    }

    fn rewrite_filter(&mut self, plan: LogicalFilter) -> LogicalPlan {
        LogicalPlan::Filter(LogicalFilter {
            expr: plan.expr,
            child: Box::new(self.rewrite_plan(*plan.child)),
        })
    }

    fn rewrite_order(&mut self, plan: LogicalOrder) -> LogicalPlan {
        LogicalPlan::Order(LogicalOrder {
            comparators: plan.comparators,
            child: Box::new(self.rewrite_plan(*plan.child)),
        })
    }

    fn rewrite_limit(&mut self, plan: LogicalLimit) -> LogicalPlan {
        LogicalPlan::Limit(LogicalLimit {
            offset: plan.offset,
            limit: plan.limit,
            child: Box::new(self.rewrite_plan(*plan.child)),
        })
    }

    fn rewrite_explain(&mut self, plan: LogicalExplain) -> LogicalPlan {
        LogicalPlan::Explain(LogicalExplain {
            plan: Box::new(self.rewrite_plan(*plan.plan)),
        })
    }

    fn rewrite_simple_agg(&mut self, plan: LogicalSimpleAgg) -> LogicalPlan {
        LogicalPlan::SimpleAgg(LogicalSimpleAgg {
            agg_calls: plan.agg_calls,
            child: Box::new(self.rewrite_plan(*plan.child)),
        })
    }

    fn rewrite_hash_agg(&mut self, plan: LogicalHashAgg) -> LogicalPlan {
        LogicalPlan::HashAgg(LogicalHashAgg {
            agg_calls: plan.agg_calls,
            group_keys: plan.group_keys,
            child: Box::new(self.rewrite_plan(*plan.child)),
        })
    }

    fn rewrite_delete(&mut self, plan: LogicalDelete) -> LogicalPlan {
        LogicalPlan::Delete(plan)
    }
}
