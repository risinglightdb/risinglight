//! The metadata of all database objects.
//!
//! The hierarchy of the catalog is: [Root] - [Database] - [Schema] - [Table] - [Column].
//!
//! There is a default database `postgres` and a default schema `postgres` in it.
//!
//! [Root]: RootCatalog
//! [Database]: DatabaseCatalog
//! [Schema]: SchemaCatalog
//! [Table]: TableCatalog
//! [Column]: ColumnCatalog

use std::sync::Arc;

mod column;
mod database;
mod root;
mod schema;
mod table;

pub use self::column::*;
pub use self::database::*;
pub use self::root::*;
pub use self::schema::*;
pub use self::table::*;

/// The type of catalog reference.
pub type RootCatalogRef = Arc<RootCatalog>;
/// The type of database ID.
pub type DatabaseId = u32;
/// The type of schema ID.
pub type SchemaId = u32;
/// The type of table ID.
pub type TableId = u32;
/// The type of column ID.
pub type ColumnId = u32;

/// The name of default database: `postgres`.
pub const DEFAULT_DATABASE_NAME: &str = "postgres";
/// The name of default schema: `postgres`.
pub const DEFAULT_SCHEMA_NAME: &str = "postgres";

/// The reference ID of a table.
#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub struct TableRefId {
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
    pub table_id: TableId,
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
#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
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
