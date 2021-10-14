use super::*;
use crate::parser::TableFactor;
#[derive(Debug, PartialEq, Clone)]
pub enum BoundTableRef {
    BaseTableRef {
        ref_id: TableRefId,
        table_name: String,
        column_ids: Vec<ColumnId>,
    },
}

impl Binder {
    pub fn bind_table_ref(&mut self, table: &TableFactor) -> Result<BoundTableRef, BindError> {
        match table {
            TableFactor::Table { name, alias, .. } => {
                let (database_name, schema_name, mut table_name) = split_name(name)?;
                if let Some(alias) = alias {
                    table_name = &alias.name.value;
                }
                if self.context.regular_tables.contains_key(table_name) {
                    return Err(BindError::DuplicatedTableName(table_name.into()));
                }

                let ref_id = self
                    .catalog
                    .get_table_id_by_name(database_name, schema_name, table_name)
                    .ok_or_else(|| BindError::InvalidTable(table_name.into()))?;
                self.context
                    .regular_tables
                    .insert(table_name.into(), ref_id);
                self.context
                    .column_names
                    .insert(table_name.into(), HashSet::new());
                self.context
                    .column_ids
                    .insert(table_name.into(), Vec::new());
                Ok(BoundTableRef::BaseTableRef {
                    ref_id,
                    table_name: table_name.into(),
                    column_ids: vec![],
                })
            }
            TableFactor::NestedJoin(table_with_joins) => {
                let bounded_table_ref = self.bind_table_ref(&table_with_joins.relation)?;
                // We only support cross join now.
                assert_eq!(table_with_joins.joins.len(), 0);
                Ok(bounded_table_ref)
            }
            _ => panic!("bind table ref"),
        }
    }
}
