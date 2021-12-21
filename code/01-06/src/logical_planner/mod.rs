use std::rc::Rc;

use enum_dispatch::enum_dispatch;

use crate::binder::BoundStatement;

mod create;
mod explain;
mod insert;

pub use self::create::*;
pub use self::explain::*;
pub use self::insert::*;

/// The logical plan.
#[enum_dispatch(Explain)]
#[derive(Debug, PartialEq, Clone)]
pub enum LogicalPlan {
    LogicalCreateTable,
    LogicalInsert,
    LogicalValues,
    LogicalExplain,
}

/// The reference type of logical plan.
pub type LogicalPlanRef = Rc<LogicalPlan>;

impl std::fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.explain(0, f)
    }
}

/// Logical planner transforms the AST into a logical operations tree.
#[derive(Default)]
pub struct LogicalPlanner;

/// The error type of logical planner.
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum LogicalPlanError {}

impl LogicalPlanner {
    /// Generate [`LogicalPlan`] from a [`BoundStatement`].
    pub fn plan(&self, stmt: BoundStatement) -> Result<LogicalPlan, LogicalPlanError> {
        match stmt {
            BoundStatement::CreateTable(stmt) => self.plan_create_table(stmt),
            BoundStatement::Insert(stmt) => self.plan_insert(stmt),
            BoundStatement::Explain(stmt) => self.plan_explain(*stmt),
        }
    }
}

/// Format a plan in `EXPLAIN` statement.
#[enum_dispatch]
pub trait Explain {
    fn explain_inner(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result;

    fn explain(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", "  ".repeat(level))?;
        self.explain_inner(level, f)
    }
}
