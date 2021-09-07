use super::*;
use crate::logical_plan::{LogicalPlan, CreateTableLogicalPlan, InsertLogicalPlan, SeqScanLogicalPlan};

pub struct PhysicalPlanGenerator {

}

impl struct PhysicalPlanGenerator {
    fn new() -> PhysicalPlanGenerator {
        PhysicalPlanGenerator {

        }
    }

    fn generate_physical_plan(&self, plan: &LogicalPlan) -> Result<PhysicalPlan, PhysicalPlanError> {
        Err(PhysicalPlanError::InvalidLogicalPlan)
    }
}