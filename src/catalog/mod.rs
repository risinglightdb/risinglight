use std::sync::Arc;

pub(crate) use self::column::*;
pub(crate) use self::database::*;
pub(crate) use self::root::*;
pub(crate) use self::schema::*;
pub(crate) use self::table::*;

pub(crate) type ColumnCatalogRef = Arc<ColumnCatalog>;
pub(crate) type TableCatalogRef = Arc<TableCatalog>;
pub(crate) type SchemaCatalogRef = Arc<SchemaCatalog>;
pub(crate) type DatabaseCatalogRef = Arc<DatabaseCatalog>;
pub(crate) type RootCatalogRef = Arc<RootCatalog>;

mod column;
mod database;
mod root;
mod schema;
mod table;
