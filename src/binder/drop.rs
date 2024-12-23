// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;

impl Binder {
    pub(super) fn bind_drop(
        &mut self,
        object_type: ObjectType,
        if_exists: bool,
        names: Vec<ObjectName>,
        cascade: bool,
    ) -> Result {
        if !matches!(object_type, ObjectType::Table | ObjectType::View) {
            return Err(ErrorKind::Todo(format!("drop {object_type:?}")).into());
        }
        if cascade {
            return Err(ErrorKind::Todo("cascade drop".into()).into());
        }
        let mut table_ids = Vec::with_capacity(names.len());
        for name in names {
            let name = lower_case_name(&name);
            let (schema_name, table_name) = split_name(&name)?;
            let result = self.catalog.get_table_id_by_name(schema_name, table_name);
            if if_exists && result.is_none() {
                continue;
            }
            let table_id = result
                .ok_or_else(|| ErrorKind::InvalidTable(table_name.into()).with_spanned(&name))?;
            let id = self.egraph.add(Node::Table(table_id));
            table_ids.push(id);
        }
        let list = self.egraph.add(Node::List(table_ids.into()));
        let drop = self.egraph.add(Node::Drop(list));
        Ok(drop)
    }
}
