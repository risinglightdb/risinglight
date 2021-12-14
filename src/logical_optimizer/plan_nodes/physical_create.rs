use std::fmt;

use itertools::Itertools;

use super::{impl_plan_tree_node_for_leaf, Plan, PlanRef, PlanTreeNode};
use crate::catalog::ColumnCatalog;
use crate::types::{DatabaseId, SchemaId};

/// The physical plan of `create table`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalCreateTable {
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
    pub table_name: String,
    pub columns: Vec<ColumnCatalog>,
}
impl_plan_tree_node_for_leaf! {PhysicalCreateTable}

impl fmt::Display for PhysicalCreateTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalCreateTable: table {}, columns [{}]",
            self.table_name,
            self.columns
                .iter()
                .map(|x| format!("{}:{:?}", x.name(), x.datatype()))
                .join(", ")
        )
    }
}
