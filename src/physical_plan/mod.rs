mod create;
mod insert;
mod projection;
mod seq_scan;

pub use create::*;
pub use insert::*;
pub use projection::*;
pub use seq_scan::*;

use crate::logical_plan::LogicalPlan;

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
    Projection(PhysicalProjection),
}

#[derive(Default)]
pub struct PhysicalPlaner;

impl PhysicalPlaner {
    pub fn plan(&self, plan: LogicalPlan) -> Result<PhysicalPlan, PhysicalPlanError> {
        match plan {
            LogicalPlan::Dummy => Ok(PhysicalPlan::Dummy),
            LogicalPlan::CreateTable(plan) => self.plan_create_table(plan),
            LogicalPlan::Insert(plan) => self.plan_insert(plan),
            LogicalPlan::SeqScan(plan) => self.plan_seq_scan(plan),
            LogicalPlan::Projection(plan) => self.plan_projection(plan),
        }
    }
}
