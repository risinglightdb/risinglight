use std::rc::Rc;

use crate::binder::BoundStatement;
use crate::optimizer::plan_nodes::PlanRef;
use crate::types::ConvertError;

mod copy;
mod create;
mod delete;
mod drop;
mod explain;
mod insert;
mod select;

pub use copy::*;
pub use create::*;
pub use delete::*;
pub use drop::*;
pub use explain::*;
pub use insert::*;

/// The error type of logical planner.
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum LogicalPlanError {
    #[error("invalid SQL")]
    InvalidSQL,
    #[error("conversion error: {0}")]
    Convert(#[from] ConvertError),
}

#[derive(Default)]
pub struct LogicalPlaner;

impl LogicalPlaner {
    /// Generate the logical plan from a bound statement.
    pub fn plan(&self, stmt: BoundStatement) -> Result<PlanRef, LogicalPlanError> {
        use BoundStatement::*;
        match stmt {
            CreateTable(stmt) => self.plan_create_table(stmt),
            Drop(stmt) => self.plan_drop(stmt),
            Insert(stmt) => self.plan_insert(stmt),
            Copy(stmt) => self.plan_copy(stmt),
            Select(stmt) => self.plan_select(stmt),
            Explain(stmt) => self.plan_explain(*stmt),
            Delete(stmt) => self.plan_delete(*stmt),
        }
    }
}
