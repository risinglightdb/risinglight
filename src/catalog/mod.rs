pub use self::column::*;
pub use self::database::*;
pub use self::root::*;
pub use self::schema::*;
pub use self::table::*;

pub static DEFAULT_DATABASE_NAME: &str = "postgres";
pub static DEFAULT_SCHEMA_NAME: &str = "postgres";

mod column;
mod database;
mod root;
mod schema;
mod table;

use crate::types::*;

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub struct TableRefId {
    database_id: DatabaseId,
    schema_id: SchemaId,
    table_id: TableId    
}

#[derive(thiserror::Error, Debug)]
pub enum CatalogError {
    #[error("{0} not found: {1}")]
    NotFound(&'static str, String),
    #[error("duplicated {0}: {1}")]
    Duplicated(&'static str, String),
}
