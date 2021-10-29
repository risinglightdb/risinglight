use super::*;
use crate::binder::BoundOrderBy;

/// The logical plan of order.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalOrder {
    pub comparators: Vec<BoundOrderBy>,
    pub child: Box<LogicalPlan>,
}
