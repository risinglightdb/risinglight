use std::fmt;

use itertools::Itertools;

use super::impl_plan_tree_node_for_leaf;
use crate::catalog::ColumnCatalog;
use crate::types::{DatabaseId, SchemaId};
/// The logical plan of `create table`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalCreateTable {
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
    pub table_name: String,
    pub columns: Vec<ColumnCatalog>,
}
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
impl_plan_tree_node_for_leaf! {LogicalCreateTable}
