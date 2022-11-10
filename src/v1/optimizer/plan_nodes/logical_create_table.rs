// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use itertools::Itertools;
use serde::Serialize;

use super::*;
use crate::catalog::{ColumnCatalog, ColumnId, DatabaseId, SchemaId};
use crate::types::DataTypeKind;

/// The logical plan of `CREATE TABLE`.
#[derive(Debug, Clone, Serialize)]
pub struct LogicalCreateTable {
    database_id: DatabaseId,
    schema_id: SchemaId,
    table_name: String,
    columns: Vec<ColumnCatalog>,
    ordered_pk_ids: Vec<ColumnId>,
}

impl LogicalCreateTable {
    pub fn new(
        database_id: DatabaseId,
        schema_id: SchemaId,
        table_name: String,
        columns: Vec<ColumnCatalog>,
        ordered_pk_ids: Vec<ColumnId>,
    ) -> Self {
        Self {
            database_id,
            schema_id,
            table_name,
            columns,
            ordered_pk_ids,
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

    /// Get the logical create table's `ordered_pk_ids`.
    pub fn ordered_pk_ids(&self) -> &[ColumnId] {
        self.ordered_pk_ids.as_ref()
    }
}

impl PlanTreeNodeLeaf for LogicalCreateTable {}

impl_plan_tree_node_for_leaf!(LogicalCreateTable);

impl PlanNode for LogicalCreateTable {
    fn schema(&self) -> Vec<ColumnDesc> {
        vec![ColumnDesc::new(
            DataType::new(DataTypeKind::Int32, false),
            "$create".to_string(),
            false,
        )]
    }
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
