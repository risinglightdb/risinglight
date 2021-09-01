use std::sync::Arc;

pub(crate) use self::column::*;
pub(crate) use self::table::*;
pub(crate) type ColumnCatalogRef = Arc<ColumnCatalog>;

mod column;
mod schema;
mod table;
