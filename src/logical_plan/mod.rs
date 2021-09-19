use crate::binder::BoundStatement;

mod create;
mod insert;
mod projection;
mod select;
mod seq_scan;

pub use create::*;
pub use insert::*;
pub use projection::*;
pub use seq_scan::*;

// LogicalPlan is an enumeration which record all necessary information of execution plan, which will be used by optimizer and executor.
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum LogicalPlanError {
    #[error("invalid SQL")]
    InvalidSQL,
}

#[derive(Debug, PartialEq, Clone)]
pub enum LogicalPlan {
    Dummy,
    SeqScan(LogicalSeqScan),
    Insert(LogicalInsert),
    CreateTable(LogicalCreateTable),
    Projection(LogicalProjection),
}

#[derive(Default)]
pub struct LogicalPlaner;

impl LogicalPlaner {
    pub fn plan(&self, stmt: BoundStatement) -> Result<LogicalPlan, LogicalPlanError> {
        match stmt {
            BoundStatement::CreateTable(stmt) => self.plan_create_table(stmt),
            BoundStatement::Insert(stmt) => self.plan_insert(stmt),
            BoundStatement::Select(stmt) => self.plan_select(stmt),
        }
    }
}
