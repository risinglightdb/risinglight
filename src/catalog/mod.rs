// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::str::FromStr;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

pub use self::column::*;
pub use self::index::*;
pub use self::root::*;
pub use self::schema::*;
pub use self::table::*;
use crate::types::*;

mod column;
pub mod function;
mod index;
mod root;
mod schema;
mod table;

pub type SchemaId = u32;
pub type TableId = u32;
pub type IndexId = u32;
pub type ColumnId = u32;

pub type RootCatalogRef = Arc<RootCatalog>;

/// An unique ID to a table in the query.
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone, Serialize, Deserialize)]
pub struct TableRefId {
    pub schema_id: SchemaId,
    pub table_id: TableId,
}

impl std::fmt::Debug for TableRefId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO: print schema id
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
        Ok(TableRefId {
            schema_id,
            table_id,
        })
    }
}

impl TableRefId {
    pub const fn new(schema_id: SchemaId, table_id: TableId) -> Self {
        TableRefId {
            schema_id,
            table_id,
        }
    }
}

/// An unique ID to a column in the query.
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone, Serialize)]
pub struct ColumnRefId {
    pub schema_id: SchemaId,
    pub table_id: TableId,
    /// How many times this table occurs in the query.
    /// This field is used to distinguish the same table in different places.
    pub table_occurrence: u32,
    pub column_id: ColumnId,
}

impl ColumnRefId {
    pub const fn from_table(table: TableRefId, table_occurrence: u32, column_id: ColumnId) -> Self {
        ColumnRefId {
            schema_id: table.schema_id,
            table_id: table.table_id,
            table_occurrence,
            column_id,
        }
    }

    pub const fn new(
        schema_id: SchemaId,
        table_id: TableId,
        table_occurrence: u32,
        column_id: ColumnId,
    ) -> Self {
        ColumnRefId {
            schema_id,
            table_id,
            table_occurrence,
            column_id,
        }
    }

    pub const fn table(&self) -> TableRefId {
        TableRefId {
            schema_id: self.schema_id,
            table_id: self.table_id,
        }
    }
}

impl std::fmt::Debug for ColumnRefId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO: print schema id
        write!(f, "${}.{}", self.table_id, self.column_id)?;
        if self.table_occurrence != 0 {
            write!(f, "({})", self.table_occurrence)?;
        }
        Ok(())
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
        Ok(ColumnRefId {
            schema_id,
            table_id,
            table_occurrence: 0,
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
