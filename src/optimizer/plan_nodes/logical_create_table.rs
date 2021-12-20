use std::fmt;

use itertools::Itertools;

use super::*;
use crate::catalog::ColumnCatalog;
use crate::types::{DatabaseId, SchemaId};

/// The logical plan of `CREATE TABLE`.
#[derive(Debug, Clone)]
pub struct LogicalCreateTable {
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
    pub table_name: String,
    pub columns: Vec<ColumnCatalog>,
}

impl_plan_tree_node!(LogicalCreateTable);
impl PlanNode for LogicalCreateTable {}

impl fmt::Display for LogicalCreateTable {
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
