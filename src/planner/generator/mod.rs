use super::*;
use crate::parser::SQLStatement;

pub struct PlanGenerator {
   
}

impl PlanGenerator {
    pub fn new() -> PlanGenerator {
        PlanGenerator {

        }
    }
    pub fn generate_plan(&self, sql:& SQLStatement) -> Result<Plan, PlanError> {
        Err(PlanError::InvalidSQL)
    }
}