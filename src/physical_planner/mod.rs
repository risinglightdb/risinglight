mod aggregation;
mod create;
mod drop;
mod filter;
mod insert;
mod projection;
mod seq_scan;

pub use aggregation::*;
pub use create::*;
pub use drop::*;
pub use filter::*;
pub use insert::*;
pub use projection::*;
pub use seq_scan::*;

use crate::logical_planner::LogicalPlan;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum PhysicalPlanError {
    #[error("invalid SQL")]
    InvalidLogicalPlan,
}

#[derive(Debug, PartialEq, Clone)]
pub enum PhysicalPlan {
    Dummy,
    SeqScan(PhysicalSeqScan),
    Insert(PhysicalInsert),
    CreateTable(PhysicalCreateTable),
    Drop(PhysicalDrop),
    Projection(PhysicalProjection),
    Filter(PhysicalFilter),
    Aggregation(PhysicalAggregation),
}

#[derive(Default)]
pub struct PhysicalPlaner;

impl PhysicalPlaner {
    pub fn plan(&self, plan: LogicalPlan) -> Result<PhysicalPlan, PhysicalPlanError> {
        match plan {
            LogicalPlan::Dummy => Ok(PhysicalPlan::Dummy),
            LogicalPlan::CreateTable(plan) => self.plan_create_table(plan),
            LogicalPlan::Drop(plan) => self.plan_drop(plan),
            LogicalPlan::Insert(plan) => self.plan_insert(plan),
            LogicalPlan::SeqScan(plan) => self.plan_seq_scan(plan),
            LogicalPlan::Projection(plan) => self.plan_projection(plan),
            LogicalPlan::Filter(plan) => self.plan_filter(plan),
            LogicalPlan::Aggregation(plan) => self.plan_aggregation(plan),
        }
    }
}
