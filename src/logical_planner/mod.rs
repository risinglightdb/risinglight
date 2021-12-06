use std::rc::Rc;

use crate::{binder::BoundStatement, types::ConvertError};

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
mod select;
mod seq_scan;

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

/// The error type of logical planner.
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum LogicalPlanError {
    #[error("invalid SQL")]
    InvalidSQL,
    #[error("conversion error: {0}")]
    Convert(#[from] ConvertError),
}

/// An enumeration which record all necessary information of execution plan,
/// which will be used by optimizer and executor.

pub(crate) type LogicalPlanRef = Rc<LogicalPlan>;
#[derive(Debug, PartialEq, Clone)]
pub enum LogicalPlan {
    Dummy,
    LogicalSeqScan(LogicalSeqScan),
    LogicalInsert(LogicalInsert),
    LogicalValues(LogicalValues),
    LogicalCreateTable(LogicalCreateTable),
    LogicalDrop(LogicalDrop),
    LogicalProjection(LogicalProjection),
    LogicalFilter(LogicalFilter),
    LogicalExplain(LogicalExplain),
    LogicalJoin(LogicalJoin),
    LogicalAggregate(LogicalAggregate),
    LogicalOrder(LogicalOrder),
    LogicalLimit(LogicalLimit),
    LogicalDelete(LogicalDelete),
    LogicalCopyFromFile(LogicalCopyFromFile),
    LogicalCopyToFile(LogicalCopyToFile),
}

#[derive(Default)]
pub struct LogicalPlaner;

impl LogicalPlaner {
    /// Generate the logical plan from a bound statement.
    pub fn plan(&self, stmt: BoundStatement) -> Result<LogicalPlan, LogicalPlanError> {
        match stmt {
            BoundStatement::CreateTable(stmt) => self.plan_create_table(stmt),
            BoundStatement::Drop(stmt) => self.plan_drop(stmt),
            BoundStatement::Insert(stmt) => self.plan_insert(stmt),
            BoundStatement::Copy(stmt) => self.plan_copy(stmt),
            BoundStatement::Select(stmt) => self.plan_select(stmt),
            BoundStatement::Explain(stmt) => self.plan_explain(*stmt),
            BoundStatement::Delete(stmt) => self.plan_delete(*stmt),
        }
    }
}
