use super::*;
use crate::parser::{TableFactor, TableWithJoins};

/// A bound table reference.
#[derive(Debug, PartialEq, Clone)]
pub struct BoundTableRef {
    pub table_ref_id: TableRefId,
    pub column_ids: Vec<ColumnId>,
}

impl Binder {
    pub fn bind_table_with_joins(
        &mut self,
        table: &TableWithJoins,
    ) -> Result<BoundTableRef, BindError> {
        assert!(table.joins.is_empty(), "JOIN is not supported");

        let (name, alias) = match &table.relation {
            TableFactor::Table { name, alias, .. } => (name, alias),
            r => panic!("not supported table factor: {:?}", r),
        };
        let (table_ref_id, _, columns) = self.bind_table_columns(name, &[])?;
        let alias = match alias {
            Some(alias) => &alias.name.value,
            None => split_name(name).unwrap().1,
        };
        if self.tables.contains_key(alias) {
            return Err(BindError::DuplicatedAlias(alias.into()));
        }
        self.tables.insert(alias.into(), table_ref_id);
        Ok(BoundTableRef {
            table_ref_id,
            column_ids: columns.iter().map(|col| col.id()).collect(),
        })
    }
}
