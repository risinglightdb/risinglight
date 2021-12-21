use enum_dispatch::enum_dispatch;

use crate::logical_planner::{Explain, LogicalPlan};

mod create;
mod dummy;
mod explain;
mod insert;
mod projection;
mod seq_scan;

pub use self::create::*;
pub use self::dummy::*;
pub use self::explain::*;
pub use self::insert::*;
pub use self::projection::*;
pub use self::seq_scan::*;

/// The physical plan.
#[enum_dispatch(Explain)]
#[derive(Debug, PartialEq, Clone)]
pub enum PhysicalPlan {
    PhysicalCreateTable,
    PhysicalInsert,
    PhysicalValues,
    PhysicalExplain,
    PhysicalDummy,
    PhysicalSeqScan,
    PhysicalProjection,
}

impl std::fmt::Display for PhysicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.explain(0, f)
    }
}

/// Physical planner transforms the logical plan tree into a physical plan tree.
#[derive(Default)]
pub struct PhysicalPlanner;

/// The error type of physical planner.
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum PhysicalPlanError {}

impl PhysicalPlanner {
    /// Generate [`PhysicalPlan`] from a [`LogicalPlan`].
    pub fn plan(&self, plan: &LogicalPlan) -> Result<PhysicalPlan, PhysicalPlanError> {
        use LogicalPlan::*;
        match plan {
            LogicalCreateTable(plan) => self.plan_create_table(plan),
            LogicalInsert(plan) => self.plan_insert(plan),
            LogicalValues(plan) => self.plan_values(plan),
            LogicalExplain(plan) => self.plan_explain(plan),
            LogicalDummy(plan) => self.plan_dummy(plan),
            LogicalGet(plan) => self.plan_get(plan),
            LogicalProjection(plan) => self.plan_projection(plan),
        }
    }
}
