use std::fmt;

use itertools::Itertools;

use crate::binder::BoundExpr;
use crate::catalog::TableRefId;
use crate::logical_optimizer::plan_nodes::logical_insert::LogicalInsert;
use crate::logical_optimizer::plan_nodes::logical_values::LogicalValues;

use crate::types::{ColumnId, DataType};

/// The physical plan of `insert`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalInsert {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
    pub child: PlanRef,
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

impl fmt::Display for PhysicalValues {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalValues: {} rows", self.values.len())
    }
}

impl fmt::Display for PhysicalInsert {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalInsert: table {}, columns [{}]",
            self.table_ref_id.table_id,
            self.column_ids.iter().map(ToString::to_string).join(", ")
        )?;
    }
}
