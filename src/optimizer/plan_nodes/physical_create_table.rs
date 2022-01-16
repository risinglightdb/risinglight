use std::fmt;

use itertools::Itertools;

use super::*;
use crate::catalog::ColumnCatalog;
use crate::types::{DatabaseId, SchemaId};

/// The physical plan of `CREATE TABLE`.
#[derive(Debug, Clone)]
pub struct PhysicalCreateTable {
    logical: LogicalCreateTable,
}

impl PhysicalCreateTable {
    pub fn new(logical: LogicalCreateTable) -> Self {
        Self { logical }
    }

    /// Get a reference to the physical create table's logical.
    pub fn logical(&self) -> &LogicalCreateTable {
        &self.logical
    }
}

impl PlanTreeNodeLeaf for LogicalCreateTable {}
impl_plan_tree_node_for_leaf!(LogicalCreateTable);

impl PlanNode for PhysicalCreateTable {}

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
