use postgres_parser as pg;
use crate::catalog::TableRefId;
#[derive(Debug, Default, PartialEq, Eq)]
pub struct BaseTableRef {
    pub database_name: Option<String>,
    pub schema_name: Option<String>,
    pub table_name: String,
    pub alias: Option<String>,
    pub table_ref_id: Option<TableRefId>
}

impl From<&pg::nodes::RangeVar> for BaseTableRef {
    fn from(root: &pg::nodes::RangeVar) -> Self {
        BaseTableRef {
            database_name: root.catalogname.as_ref().map(|s| s.to_lowercase()),
            schema_name: root.schemaname.as_ref().map(|s| s.to_lowercase()),
            table_name: root.relname.as_ref().map(|s| s.to_lowercase()).unwrap(),
            alias: root.alias.as_ref().map(|a| a.aliasname.clone().unwrap()),
            table_ref_id: None
        }
    }
}
