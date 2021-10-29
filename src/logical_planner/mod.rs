use crate::binder::BoundStatement;

mod create;
mod drop;
mod explain;
mod filter;
mod insert;
mod join;
mod order;
mod projection;
mod select;
mod seq_scan;

pub use create::*;
pub use drop::*;
pub use explain::*;
pub use filter::*;
pub use insert::*;
pub use join::*;
pub use order::*;
pub use projection::*;
pub use seq_scan::*;

/// The error type of logical planner.
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum LogicalPlanError {
    #[error("invalid SQL")]
    InvalidSQL,
}

/// An enumeration which record all necessary information of execution plan,
/// which will be used by optimizer and executor.
#[derive(Debug, PartialEq, Clone)]
pub enum LogicalPlan {
    Dummy,
    SeqScan(LogicalSeqScan),
    Insert(LogicalInsert),
    CreateTable(LogicalCreateTable),
    Drop(LogicalDrop),
    Projection(LogicalProjection),
    Filter(LogicalFilter),
    Explain(LogicalExplain),
    Join(LogicalJoin),
    Order(LogicalOrder),
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
            BoundStatement::Select(stmt) => self.plan_select(stmt),
            BoundStatement::Explain(stmt) => self.plan_explain(*stmt),
        }
    }
}
