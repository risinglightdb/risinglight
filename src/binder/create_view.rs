use std::collections::HashSet;

use super::*;
use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnId};

impl Binder {
    pub(super) fn bind_create_view(
        &mut self,
        name: ObjectName,
        columns: Vec<Ident>,
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
        for col in columns.iter() {
            if !set.insert(col.value.to_lowercase()) {
                return Err(BindError::ColumnExists(col.value.to_lowercase()));
            }
        }

        let (query, _) = self.bind_query(query)?;
        let query_type = self.type_(query)?;

        let columns: Vec<ColumnCatalog> = columns
            .into_iter()
            .zip(query_type.kind().as_struct())
            .enumerate()
            .map(|(idx, (name, ty))| {
                ColumnCatalog::new(
                    idx as ColumnId,
                    ColumnDesc::new(ty.clone(), name.value, false),
                )
            })
            .collect();

        let table = self.egraph.add(Node::CreateTable(Box::new(CreateTable {
            schema_id: schema.id(),
            table_name: table_name.into(),
            columns,
            ordered_pk_ids: vec![],
        })));
        let create_view = self.egraph.add(Node::CreateView([table, query]));
        Ok(create_view)
    }
}
