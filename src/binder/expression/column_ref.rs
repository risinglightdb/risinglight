use super::*;
use crate::parser::ColumnRef;

impl Bind for ColumnRef {
    fn bind(&mut self, binder: &mut Binder) -> Result<(), BindError> {
        match &self.table_name {
            Some(name) => {
                if !binder.context.regular_tables.contains_key(name) {
                    return Err(BindError::InvalidTable(name.clone()));
                }

                let table_ref_id = binder.context.regular_tables.get(name).unwrap();
                let table = binder.catalog.get_table(&table_ref_id);
                let col_opt = table.get_column_by_name(&self.column_name);
                if col_opt.is_none() {
                    return Err(BindError::InvalidColumn(self.column_name.clone()));
                }
                let col = col_opt.unwrap();
                self.column_ref_id = Some(ColumnRefId {
                    database_id: table_ref_id.database_id,
                    schema_id: table_ref_id.schema_id,
                    table_id: table_ref_id.table_id,
                    column_id: col.id(),
                });

                self.column_index = Some(record_regular_table_column(
                    binder,
                    name,
                    &self.column_name,
                    col.id(),
                ));
                Ok(())
            }
            None => {
                println!("Binding expression");
                let mut is_matched: bool = false;
                for (name, ref_id) in binder.context.regular_tables.iter() {
                    let table = binder.catalog.get_table(ref_id);
                    let col_opt = table.get_column_by_name(&self.column_name);
                    if let Some(col) = col_opt {
                        if is_matched {
                            return Err(BindError::AmbiguousColumn);
                        }
                        is_matched = true;
                        self.column_ref_id = Some(ColumnRefId {
                            database_id: ref_id.database_id,
                            schema_id: ref_id.schema_id,
                            table_id: ref_id.table_id,
                            column_id: col.id(),
                        });
                        self.table_name = Some(table.name().clone());
                    }
                }

                if !is_matched {
                    return Err(BindError::InvalidColumn(self.column_name.clone()));
                }
                self.column_index = Some(record_regular_table_column(
                    binder,
                    self.table_name.as_ref().unwrap(),
                    &self.column_name,
                    self.column_ref_id.unwrap().column_id,
                ));

                Ok(())
            }
        }
    }
}

fn record_regular_table_column(
    binder: &mut Binder,
    table_name: &str,
    col_name: &str,
    column_id: ColumnId,
) -> ColumnId {
    let mut names = binder.context.column_names.get_mut(table_name).unwrap();
    if !names.contains(col_name) {
        let idx = names.len() as u32;
        names.insert(col_name.to_string());
        let idxs = binder.context.column_ids.get_mut(table_name).unwrap();
        idxs.push(column_id);
        assert_eq!(idxs.len() > 0, true);
        idx
    } else {
        let idxs = binder.context.column_ids.get_mut(table_name).unwrap();
        assert_eq!(idxs.len() > 0, true);
        idxs.iter().position(|&r| r == column_id).unwrap() as u32
    }
}
