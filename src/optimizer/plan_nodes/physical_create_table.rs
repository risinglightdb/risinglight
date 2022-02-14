// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use itertools::Itertools;
use serde::Serialize;

use super::*;

/// The physical plan of `CREATE TABLE`.
#[derive(Debug, Clone, Serialize)]
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

impl PlanTreeNodeLeaf for PhysicalCreateTable {}
impl_plan_tree_node_for_leaf!(PhysicalCreateTable);

impl PlanNode for PhysicalCreateTable {
    fn schema(&self) -> Vec<ColumnDesc> {
        self.logical.schema()
    }
}

impl fmt::Display for PhysicalCreateTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalCreateTable: table {}, columns [{}]",
            self.logical().table_name(),
            self.logical()
                .columns()
                .iter()
                .map(|x| format!("{}:{:?}", x.name(), x.datatype()))
                .join(", ")
        )
    }
}
