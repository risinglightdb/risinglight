// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use itertools::Itertools;
use serde::Serialize;

use super::*;
use crate::catalog::ColumnCatalog;
use crate::types::{DatabaseId, SchemaId};

/// The logical plan of `CREATE TABLE`.
#[derive(Debug, Clone, Serialize)]
pub struct LogicalCreateTable {
    database_id: DatabaseId,
    schema_id: SchemaId,
    table_name: String,
    columns: Vec<ColumnCatalog>,
}

impl LogicalCreateTable {
    pub fn new(
        database_id: DatabaseId,
        schema_id: SchemaId,
        table_name: String,
        columns: Vec<ColumnCatalog>,
    ) -> Self {
        Self {
            database_id,
            schema_id,
            table_name,
            columns,
        }
    }

    /// Get a reference to the logical create table's database id.
    pub fn database_id(&self) -> u32 {
        self.database_id
    }

    /// Get a reference to the logical create table's schema id.
    pub fn schema_id(&self) -> u32 {
        self.schema_id
    }

    /// Get a reference to the logical create table's table name.
    pub fn table_name(&self) -> &str {
        self.table_name.as_ref()
    }

    /// Get a reference to the logical create table's columns.
    pub fn columns(&self) -> &[ColumnCatalog] {
        self.columns.as_ref()
    }
}
impl PlanTreeNodeLeaf for LogicalCreateTable {}
impl_plan_tree_node_for_leaf!(LogicalCreateTable);
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
