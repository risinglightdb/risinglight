use crate::catalog::TableRefId;
use crate::types::ColumnId;
use postgres_parser as pg;
use std::cmp::PartialEq;
use std::sync::{Arc, Mutex};
use std::vec::Vec;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct BaseTableRef {
    pub database_name: Option<String>,
    pub schema_name: Option<String>,
    pub table_name: String,
    pub alias: Option<String>,
    pub table_ref_id: Option<TableRefId>,
    pub column_ids: Vec<ColumnId>,
}

impl From<&pg::nodes::RangeVar> for BaseTableRef {
    fn from(root: &pg::nodes::RangeVar) -> Self {
        BaseTableRef {
            database_name: root.catalogname.as_ref().map(|s| s.to_lowercase()),
            schema_name: root.schemaname.as_ref().map(|s| s.to_lowercase()),
            table_name: root.relname.as_ref().map(|s| s.to_lowercase()).unwrap(),
            alias: root.alias.as_ref().map(|a| a.aliasname.clone().unwrap()),
            table_ref_id: None,
            column_ids: Vec::new(),
        }
    }
}
