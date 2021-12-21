//! Logical planner of `select` statement.
//!
//! A `select` statement will be planned to a compose of:
//!
//! - [`LogicalGet`] (from *) or [`LogicalDummy`] (no from)
//! - [`LogicalProjection`] (select *)

use super::*;
use crate::binder::{BoundExpr, BoundSelect};
use crate::catalog::{ColumnId, TableRefId};

/// The logical plan of dummy get.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalDummy;

/// The logical plan of get.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalGet {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
}

/// The logical plan of projection.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalProjection {
    pub exprs: Vec<BoundExpr>,
    pub child: LogicalPlanRef,
}

impl LogicalPlanner {
    pub fn plan_select(&self, stmt: BoundSelect) -> Result<LogicalPlan, LogicalPlanError> {
        let mut plan: LogicalPlan = LogicalDummy.into();

        if let Some(table_ref) = stmt.from_list.get(0) {
            plan = LogicalGet {
                table_ref_id: table_ref.table_ref_id,
                column_ids: table_ref.column_ids.clone(),
            }
            .into();
        }
        if !stmt.select_list.is_empty() {
            plan = LogicalProjection {
                exprs: stmt.select_list,
                child: plan.into(),
            }
            .into();
        }
        Ok(plan)
    }
}

impl Explain for LogicalDummy {
    fn explain_inner(&self, _level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Dummy:")
    }
}

impl Explain for LogicalGet {
    fn explain_inner(&self, _level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Get: table: {:?}, columns: {:?}",
            self.table_ref_id, self.column_ids
        )
    }
}

impl Explain for LogicalProjection {
    fn explain_inner(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Projection: exprs: {:?}", self.exprs)?;
        self.child.explain(level + 1, f)
    }
}
