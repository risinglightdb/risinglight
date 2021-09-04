use std::sync::{Arc, Mutex};

pub(crate) use self::column::*;
pub(crate) use self::database::*;
pub(crate) use self::root::*;
pub(crate) use self::schema::*;
pub(crate) use self::table::*;

pub(crate) type ColumnCatalogRef = Arc<Mutex<ColumnCatalog>>;
pub(crate) type TableCatalogRef = Arc<Mutex<TableCatalog>>;
pub(crate) type SchemaCatalogRef = Arc<Mutex<SchemaCatalog>>;
pub(crate) type DatabaseCatalogRef = Arc<Mutex<DatabaseCatalog>>;
pub(crate) type RootCatalogRef = Arc<Mutex<RootCatalog>>;

pub(crate) static DEFAULT_DATABASE_NAME: &str = "postgres";
pub(crate) static DEFAULT_SCHEMA_NAME: &str = "postgres";

mod column;
mod database;
mod root;
mod schema;
mod table;
