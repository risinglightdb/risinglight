use crate::logical_planner::{Explain, LogicalPlan};
use enum_dispatch::enum_dispatch;

mod create;
mod insert;

pub use self::create::*;
pub use self::insert::*;

/// The physical plan.
#[enum_dispatch(Explain)]
#[derive(Debug, PartialEq, Clone)]
pub enum PhysicalPlan {
    PhysicalCreateTable,
    PhysicalInsert,
    PhysicalValues,
}

/// Physical planner transforms the logical plan tree into a physical plan tree.
#[derive(Default)]
pub struct PhysicalPlaner;

/// The error type of physical planner.
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum PhysicalPlanError {}

impl PhysicalPlaner {
    /// Generate [`PhysicalPlan`] from a [`LogicalPlan`].
    pub fn plan(&self, plan: &LogicalPlan) -> Result<PhysicalPlan, PhysicalPlanError> {
        match plan {
            LogicalPlan::LogicalCreateTable(plan) => self.plan_create_table(plan),
            LogicalPlan::LogicalInsert(plan) => self.plan_insert(plan),
            LogicalPlan::LogicalValues(plan) => self.plan_values(plan),
        }
    }
}
