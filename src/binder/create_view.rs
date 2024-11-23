use std::collections::HashSet;

use super::*;
use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnId};

impl Binder {
    pub(super) fn bind_create_view(
        &mut self,
        name: ObjectName,
        columns: Vec<ViewColumnDef>,
        query: Query,
    ) -> Result {
        let name = lower_case_name(&name);
        let (schema_name, table_name) = split_name(&name)?;
        let schema = self
            .catalog
            .get_schema_by_name(schema_name)
            .ok_or_else(|| BindError::InvalidSchema(schema_name.into()))?;
        if schema.get_table_by_name(table_name).is_some() {
            return Err(BindError::TableExists(table_name.into()));
        }

        // check duplicated column names
        let mut set = HashSet::new();
        for col in &columns {
            if !set.insert(col.name.value.to_lowercase()) {
                return Err(BindError::ColumnExists(col.name.value.to_lowercase()));
            }
        }

        let (query, _) = self.bind_query(query)?;
        let query_type = self.type_(query)?;
        let output_types = query_type.as_struct();

        // TODO: support inferring column names from query
        if columns.len() != output_types.len() {
            return Err(BindError::ViewAliasesMismatch);
        }

        let columns: Vec<ColumnCatalog> = columns
            .into_iter()
            .zip(output_types)
            .enumerate()
            .map(|(idx, (column, ty))| {
                ColumnCatalog::new(
                    idx as ColumnId,
                    ColumnDesc::new(column.name.value, ty.clone(), true),
                )
            })
            .collect();

        let table_def = self.egraph.add(Node::TableDef(Box::new(TableDef {
            schema_id: schema.id(),
            table_name: table_name.into(),
            columns,
            ordered_pk_ids: vec![],
        })));
        let create_view = self.egraph.add(Node::CreateView([table_def, query]));
        Ok(create_view)
    }
}
