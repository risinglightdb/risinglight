use crate::logical_planner::LogicalPlan;
// The optimizer will do query optimization.
// It will do both rule-based optimization (predicate pushdown, constant folding and common expression extraction)
// , and cost-based optimization (Join reordering and join algorithm selection).
// It takes LogicalPlan as input and returns a new LogicalPlan which could be used to generate phyiscal plan.
pub struct Optimizer {}

impl Optimizer {
    pub fn optimize(&mut self, plan: LogicalPlan) -> LogicalPlan {
        // TODO: add optimization rules
        plan
    }
}

// PlanRewriter is a plan visitor.
// User could implement the own optimization rules by implement PlanRewriter trait easily.
pub trait PlanRewriter {
    fn rewrite_plan(&mut self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Dummy => LogicalPlan::Dummy,
            LogicalPlan::CreateTable(plan) => LogicalPlan::CreateTable(plan),
            LogicalPlan::Drop(plan) => LogicalPlan::Drop(plan),
            LogicalPlan::Insert(plan) => LogicalPlan::Insert(plan),
            LogicalPlan::Join(plan) => LogicalPlan::Join(plan),
            LogicalPlan::SeqScan(plan) => LogicalPlan::SeqScan(plan),
            LogicalPlan::Projection(plan) => LogicalPlan::Projection(plan),
            LogicalPlan::Filter(plan) => LogicalPlan::Filter(plan),
            LogicalPlan::Order(plan) => LogicalPlan::Order(plan),
            LogicalPlan::Limit(plan) => LogicalPlan::Limit(plan),
            LogicalPlan::Explain(plan) => LogicalPlan::Explain(plan),
            LogicalPlan::Delete(plan) => LogicalPlan::Delete(plan),
        }
    }

    fn rewrite_create_table(&mut self, plan: LogicalCreateTable) -> LogicalPlan {
        
    }
}
