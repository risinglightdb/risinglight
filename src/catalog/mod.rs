// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

pub use self::column::*;
pub use self::database::*;
pub use self::root::*;
pub use self::schema::*;
pub use self::table::*;
use crate::types::*;

pub static DEFAULT_DATABASE_NAME: &str = "postgres";
pub static DEFAULT_SCHEMA_NAME: &str = "postgres";
pub static INTERNAL_SCHEMA_NAME: &str = "pg_catalog";

mod column;
mod database;
mod root;
mod schema;
mod table;

pub type RootCatalogRef = Arc<RootCatalog>;

/// The reference ID of a table.
#[derive(PartialEq, Eq, Hash, Copy, Clone, Serialize, Deserialize)]
pub struct TableRefId {
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
    pub table_id: TableId,
}

impl std::fmt::Debug for TableRefId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{}.{}",
            self.database_id, self.schema_id, self.table_id
        )
    }
}

impl TableRefId {
    pub const fn new(database_id: DatabaseId, schema_id: SchemaId, table_id: TableId) -> Self {
        TableRefId {
            database_id,
            schema_id,
            table_id,
        }
    }
}

/// The reference ID of a column.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone, Serialize)]
pub struct ColumnRefId {
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
    pub table_id: TableId,
    pub column_id: ColumnId,
}

impl ColumnRefId {
    pub const fn from_table(table: TableRefId, column_id: ColumnId) -> Self {
        ColumnRefId {
            database_id: table.database_id,
            schema_id: table.schema_id,
            table_id: table.table_id,
            column_id,
        }
    }

    pub const fn new(
        database_id: DatabaseId,
        schema_id: SchemaId,
        table_id: TableId,
        column_id: ColumnId,
    ) -> Self {
        ColumnRefId {
            database_id,
            schema_id,
            table_id,
            column_id,
        }
    }
}

/// The error type of catalog operations.
#[derive(thiserror::Error, Debug)]
pub enum CatalogError {
    #[error("{0} not found: {1}")]
    NotFound(&'static str, String),
    #[error("duplicated {0}: {1}")]
    Duplicated(&'static str, String),
}
