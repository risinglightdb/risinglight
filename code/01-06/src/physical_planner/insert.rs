use itertools::Itertools;

use super::*;
use crate::binder::BoundExpr;
use crate::catalog::{ColumnId, TableRefId};
use crate::logical_planner::{LogicalInsert, LogicalValues};
use crate::types::DataType;

/// The physical plan of `INSERT`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalInsert {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub child: Box<PhysicalPlan>,
}

/// The physical plan of `VALUES`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalValues {
    pub column_types: Vec<DataType>,
    pub values: Vec<Vec<BoundExpr>>,
}

impl PhysicalPlanner {
    pub fn plan_insert(&self, plan: &LogicalInsert) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalInsert {
            table_ref_id: plan.table_ref_id,
            column_ids: plan.column_ids.clone(),
            child: self.plan(&plan.child)?.into(),
        }
        .into())
    }

    pub fn plan_values(&self, plan: &LogicalValues) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalValues {
            column_types: plan.column_types.clone(),
            values: plan.values.clone(),
        }
        .into())
    }
}

impl Explain for PhysicalInsert {
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

impl Explain for PhysicalValues {
    fn explain_inner(&self, _level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Values: {} rows", self.values.len())
    }
}
