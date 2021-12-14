use std::fmt;

use super::PlanRef;

/// The physical plan of `explain`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalExplain {
    pub plan: PlanRef,
}

impl fmt::Display for PhysicalExplain {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Huh, explain myself?")
    }
}
