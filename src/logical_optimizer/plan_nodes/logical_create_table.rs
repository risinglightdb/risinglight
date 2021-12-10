use crate::{
    catalog::ColumnCatalog,
    types::{DatabaseId, SchemaId},
};
/// The logical plan of `create table`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalCreateTable {
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
    pub table_name: String,
    pub columns: Vec<ColumnCatalog>,
}
