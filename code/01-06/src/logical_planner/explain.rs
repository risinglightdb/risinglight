use super::*;

/// The logical plan of `EXPLAIN`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalExplain {
    pub child: LogicalPlanRef,
}

impl LogicalPlanner {
    pub fn plan_explain(&self, stmt: BoundStatement) -> Result<LogicalPlan, LogicalPlanError> {
        Ok(LogicalExplain {
            child: self.plan(stmt)?.into(),
        }
        .into())
    }
}

impl Explain for LogicalExplain {
    fn explain_inner(&self, _level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Huh, explain myself?")
    }
}
