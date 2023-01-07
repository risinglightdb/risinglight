// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::str::FromStr;
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

pub type DatabaseId = u32;
pub type SchemaId = u32;
pub type TableId = u32;
pub type ColumnId = u32;

pub type RootCatalogRef = Arc<RootCatalog>;

/// The reference ID of a table.
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone, Serialize, Deserialize)]
pub struct TableRefId {
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
    pub table_id: TableId,
}

impl std::fmt::Debug for TableRefId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO: now ignore database and schema
        write!(f, "${}", self.table_id)
    }
}

impl std::fmt::Display for TableRefId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(thiserror::Error, Debug, Clone)]
#[error("parse table id error: {}")]
pub enum ParseTableIdError {
    #[error("no leading '$'")]
    NoLeadingDollar,
    #[error("invalid table")]
    InvalidTable,
    #[error("invalid number: {0}")]
    InvalidNum(#[from] std::num::ParseIntError),
}

impl FromStr for TableRefId {
    type Err = ParseColumnIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let body = s.strip_prefix('$').ok_or(Self::Err::NoLeadingDollar)?;
        let mut parts = body.rsplit('.');
        let table_id = parts.next().ok_or(Self::Err::InvalidTable)?.parse()?;
        let schema_id = parts.next().map_or(Ok(0), |s| s.parse())?;
        let database_id = parts.next().map_or(Ok(0), |s| s.parse())?;
        Ok(TableRefId {
            database_id,
            schema_id,
            table_id,
        })
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
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone, Serialize)]
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

    pub const fn table(&self) -> TableRefId {
        TableRefId {
            database_id: self.database_id,
            schema_id: self.schema_id,
            table_id: self.table_id,
        }
    }
}

impl std::fmt::Debug for ColumnRefId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO: now ignore database and schema
        write!(f, "${}.{}", self.table_id, self.column_id)
    }
}

impl std::fmt::Display for ColumnRefId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(thiserror::Error, Debug, Clone)]
#[error("parse column id error: {}")]
pub enum ParseColumnIdError {
    #[error("no leading '$'")]
    NoLeadingDollar,
    #[error("invalid column")]
    InvalidColumn,
    #[error("invalid table")]
    InvalidTable,
    #[error("invalid number: {0}")]
    InvalidNum(#[from] std::num::ParseIntError),
}

impl FromStr for ColumnRefId {
    type Err = ParseColumnIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let body = s.strip_prefix('$').ok_or(Self::Err::NoLeadingDollar)?;
        let mut parts = body.rsplit('.');
        let column_id = parts.next().ok_or(Self::Err::InvalidColumn)?.parse()?;
        let table_id = parts.next().ok_or(Self::Err::InvalidTable)?.parse()?;
        let schema_id = parts.next().map_or(Ok(0), |s| s.parse())?;
        let database_id = parts.next().map_or(Ok(0), |s| s.parse())?;
        Ok(ColumnRefId {
            database_id,
            schema_id,
            table_id,
            column_id,
        })
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
