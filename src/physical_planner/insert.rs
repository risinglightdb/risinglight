use itertools::Itertools;

use super::*;
use crate::binder::BoundExpr;
use crate::catalog::TableRefId;
use crate::logical_optimizer::plan_nodes::logical_values::LogicalValues;
use crate::logical_optimizer::plan_nodes::logical_insert::LogicalInsert;
use crate::types::{ColumnId, DataType};

/// The physical plan of `insert`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalInsert {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub child: Box<PhysicalPlan>,
}

/// The physical plan of `values`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalValues {
    pub column_types: Vec<DataType>,
    pub values: Vec<Vec<BoundExpr>>,
}

impl PhysicalPlaner {
    pub fn plan_insert(&self, plan: LogicalInsert) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::Insert(PhysicalInsert {
            table_ref_id: plan.table_ref_id,
            column_ids: plan.column_ids,
            child: self.plan_inner(plan.child.as_ref().clone())?.into(),
        }))
    }

    pub fn plan_values(&self, plan: LogicalValues) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::Values(PhysicalValues {
            column_types: plan.column_types,
            values: plan.values,
        }))
    }
}

impl PlanExplainable for PhysicalInsert {
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

impl PlanExplainable for PhysicalValues {
    fn explain_inner(&self, _level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Values: {} rows", self.values.len())
    }
}
