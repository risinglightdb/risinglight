// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crate::types::ConvertError;
use crate::v1::binder::BoundStatement;
use crate::v1::optimizer::plan_nodes::PlanRef;

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
    #[error("{0} must appear in the GROUP BY clause or be used in an aggregate function")]
    IllegalGroupBySQL(String),
    #[error("ORDER BY items must appear in the select list if SELECT DISTINCT is specified")]
    IllegalDistinctSQL,
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
