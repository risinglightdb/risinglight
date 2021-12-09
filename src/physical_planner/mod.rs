mod aggregate;
mod copy;
mod create;
mod delete;
mod drop;
mod explain;
mod filter;
mod insert;
mod join;
mod limit;
mod order;
mod projection;
mod seq_scan;

pub use crate::logical_optimizer::plan_rewriter::input_ref_resolver::*;
pub use aggregate::*;
pub use copy::*;
pub use create::*;
pub use delete::*;
pub use drop::*;
pub use explain::*;
pub use filter::*;
pub use insert::*;
pub use join::*;
pub use limit::*;
pub use order::*;
pub use projection::*;
pub use seq_scan::*;

use crate::logical_optimizer::plan_nodes::LogicalPlan;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum PhysicalPlanError {
    #[error("invalid SQL")]
    InvalidLogicalPlan,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Dummy;

#[derive(Debug, PartialEq, Clone)]
pub enum PhysicalPlan {
    Dummy(Dummy),
    SeqScan(PhysicalSeqScan),
    Insert(PhysicalInsert),
    Values(PhysicalValues),
    CreateTable(PhysicalCreateTable),
    Drop(PhysicalDrop),
    Projection(PhysicalProjection),
    Filter(PhysicalFilter),
    Explain(PhysicalExplain),
    Join(PhysicalJoin),
    SimpleAgg(PhysicalSimpleAgg),
    HashAgg(PhysicalHashAgg),
    Order(PhysicalOrder),
    Limit(PhysicalLimit),
    Delete(PhysicalDelete),
    CopyFromFile(PhysicalCopyFromFile),
    CopyToFile(PhysicalCopyToFile),
}

#[derive(Default)]
pub struct PhysicalPlaner;

impl PhysicalPlaner {
    fn plan_inner(&self, plan: LogicalPlan) -> Result<PhysicalPlan, PhysicalPlanError> {
        match plan {
            LogicalPlan::Dummy(_) => Ok(PhysicalPlan::Dummy(Dummy {})),
            LogicalPlan::LogicalCreateTable(plan) => self.plan_create_table(plan),
            LogicalPlan::LogicalDrop(plan) => self.plan_drop(plan),
            LogicalPlan::LogicalInsert(plan) => self.plan_insert(plan),
            LogicalPlan::LogicalValues(plan) => self.plan_values(plan),
            LogicalPlan::LogicalJoin(plan) => self.plan_join(plan),
            LogicalPlan::LogicalSeqScan(plan) => self.plan_seq_scan(plan),
            LogicalPlan::LogicalProjection(plan) => self.plan_projection(plan),
            LogicalPlan::LogicalFilter(plan) => self.plan_filter(plan),
            LogicalPlan::LogicalOrder(plan) => self.plan_order(plan),
            LogicalPlan::LogicalLimit(plan) => self.plan_limit(plan),
            LogicalPlan::LogicalExplain(plan) => self.plan_explain(plan),
            LogicalPlan::LogicalAggregate(plan) => self.plan_aggregate(plan),
            LogicalPlan::LogicalDelete(plan) => self.plan_delete(plan),
            LogicalPlan::LogicalCopyFromFile(plan) => self.plan_copy_from_file(plan),
            LogicalPlan::LogicalCopyToFile(plan) => self.plan_copy_to_file(plan),
        }
    }

    pub fn plan(&self, plan: LogicalPlan) -> Result<PhysicalPlan, PhysicalPlanError> {
        // Create physical plan
        self.plan_inner(plan)
    }
}

pub trait PlanExplainable {
    fn explain_inner(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result;

    fn explain(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", " ".repeat(level * 2))?;
        self.explain_inner(level, f)
    }
}

impl PlanExplainable for Dummy {
    fn explain_inner(&self, _level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Dummy")
    }
}

impl PhysicalPlan {
    fn explain(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dummy(p) => p.explain(level, f),
            Self::SeqScan(p) => p.explain(level, f),
            Self::Insert(p) => p.explain(level, f),
            Self::Values(p) => p.explain(level, f),
            Self::CreateTable(p) => p.explain(level, f),
            Self::Drop(p) => p.explain(level, f),
            Self::Projection(p) => p.explain(level, f),
            Self::Filter(p) => p.explain(level, f),
            Self::Explain(p) => p.explain(level, f),
            Self::Join(p) => p.explain(level, f),
            Self::Order(p) => p.explain(level, f),
            Self::Limit(p) => p.explain(level, f),
            Self::SimpleAgg(p) => p.explain(level, f),
            Self::HashAgg(p) => p.explain(level, f),
            Self::Delete(p) => p.explain(level, f),
            Self::CopyFromFile(p) => p.explain(level, f),
            Self::CopyToFile(p) => p.explain(level, f),
        }
    }
}

impl std::fmt::Display for PhysicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.explain(0, f)
    }
}
