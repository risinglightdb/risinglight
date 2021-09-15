use std::sync::Arc;

pub use self::column::*;
pub use self::database::*;
pub use self::root::*;
pub use self::schema::*;
pub use self::table::*;
use crate::types::*;

pub static DEFAULT_DATABASE_NAME: &str = "postgres";
pub static DEFAULT_SCHEMA_NAME: &str = "postgres";

mod column;
mod database;
mod root;
mod schema;
mod table;

pub type RootCatalogRef = Arc<RootCatalog>;

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub struct TableRefId {
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
    pub table_id: TableId,
}

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub struct ColumnRefId {
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
    pub table_id: TableId,
    pub column_id: ColumnId,
}

#[derive(thiserror::Error, Debug)]
pub enum CatalogError {
    #[error("{0} not found: {1}")]
    NotFound(&'static str, String),
    #[error("duplicated {0}: {1}")]
    Duplicated(&'static str, String),
}
