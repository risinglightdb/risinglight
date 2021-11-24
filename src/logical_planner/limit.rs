use super::*;

/// The logical plan of limit operation.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalLimit {
    pub offset: usize,
    pub limit: usize,
    pub child: LogicalPlanRef,
}
