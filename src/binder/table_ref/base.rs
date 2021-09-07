use super::*;
use crate::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
use crate::parser::BaseTableRef;

impl Bind for BaseTableRef {
    fn bind(&mut self, binder: &mut Binder) -> Result<(), BindError> {
        self.database_name
            .get_or_insert_with(|| DEFAULT_DATABASE_NAME.to_string());
        self.schema_name
            .get_or_insert_with(|| DEFAULT_SCHEMA_NAME.to_string());

        let table_name = self.alias.as_ref().unwrap_or(&self.table_name).clone();

        if binder.context.regular_tables.contains_key(&table_name) {
            return Err(BindError::DuplicatedTableName(table_name.clone()));
        }

        let table_ref_id_opt = binder.catalog.get_table_id(
            self.database_name.as_ref().unwrap(),
            self.schema_name.as_ref().unwrap(),
            &table_name,
        );

        let id = table_ref_id_opt.ok_or_else(|| BindError::InvalidTable(table_name.clone()))?;
        binder.context.regular_tables.insert(table_name.clone(), id);
        self.table_ref_id = Some(id);
        binder
            .context
            .column_names
            .insert(table_name.clone(), HashSet::new());
        binder
            .context
            .column_ids
            .insert(table_name.clone(), Vec::new());
        Ok(())
    }
}
