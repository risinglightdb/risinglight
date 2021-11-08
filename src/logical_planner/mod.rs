use crate::{binder::BoundStatement, types::ConvertError};

mod create;
mod delete;
mod drop;
mod explain;
mod filter;
mod hash_agg;
mod insert;
mod join;
mod limit;
mod order;
mod projection;
mod select;
mod seq_scan;
mod simple_agg;

pub use create::*;
pub use delete::*;
pub use drop::*;
pub use explain::*;
pub use filter::*;
pub use hash_agg::*;
pub use insert::*;
pub use join::*;
pub use limit::*;
pub use order::*;
pub use projection::*;
pub use seq_scan::*;
pub use simple_agg::*;

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
    SimpleAgg(LogicalSimpleAgg),
    HashAgg(LogicalHashAgg),
    Order(LogicalOrder),
    Limit(LogicalLimit),
    Delete(LogicalDelete),
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
            BoundStatement::Delete(stmt) => self.plan_delete(*stmt),
        }
    }
}
