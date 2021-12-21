use itertools::Itertools;

use super::*;
use crate::binder::{BoundExpr, BoundInsert};
use crate::catalog::{ColumnId, TableRefId};
use crate::types::DataType;

/// The logical plan of `INSERT`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalInsert {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub child: LogicalPlanRef,
}

/// The logical plan of `VALUES`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalValues {
    pub column_types: Vec<DataType>,
    pub values: Vec<Vec<BoundExpr>>,
}

impl LogicalPlanner {
    pub fn plan_insert(&self, stmt: BoundInsert) -> Result<LogicalPlan, LogicalPlanError> {
        Ok(LogicalInsert {
            table_ref_id: stmt.table_ref_id,
            column_ids: stmt.column_ids,
            child: Rc::new(
                LogicalValues {
                    column_types: stmt.column_types,
                    values: stmt.values,
                }
                .into(),
            ),
        }
        .into())
    }
}

impl Explain for LogicalInsert {
    fn explain_inner(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Insert: table {}, columns [{}]",
            self.table_ref_id.table_id,
            self.column_ids.iter().map(ToString::to_string).join(", ")
        )?;
        self.child.explain(level + 1, f)
    }
}

impl Explain for LogicalValues {
    fn explain_inner(&self, _level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Values: {} rows", self.values.len())
    }
}
