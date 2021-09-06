use super::*;
use crate::catalog::ColumnRefId;
use crate::parser::{ExprData, Expression};

impl Bind for Expression {
    fn bind(&mut self, binder: &mut Binder) -> Result<(), BindError> {
        Ok(())
    }
}

impl Bind for ExprData {
    fn bind(&mut self, binder: &mut Binder) -> Result<(), BindError> {
        match self {
            // Binding constant is not necessary
            ExprData::Constant(_) => Ok(()),
            ExprData::ColumnRef {
                table_name,
                column_name,
                column_ref_id,
                column_index,
            } => match table_name {
                Some(name) => {
                    if !binder.context.regular_tables.contains_key(name) {
                        return Err(BindError::InvalidTable(name.clone()));
                    }

                    let table_ref_id = binder.context.regular_tables.get(name).unwrap();
                    let table = binder.catalog.get_table(&table_ref_id);
                    let col_opt = table.get_column_by_name(column_name);
                    if col_opt.is_none() {
                        return Err(BindError::InvalidColumn(column_name.clone()));
                    }
                    let col = col_opt.unwrap();
                    *column_ref_id = Some(ColumnRefId {
                        database_id: table_ref_id.database_id,
                        schema_id: table_ref_id.schema_id,
                        table_id: table_ref_id.table_id,
                        column_id: col.id(),
                    });

                    *column_index = Some(record_regular_table_column(
                        binder,
                        name,
                        column_name,
                        col.id(),
                    ));
                    Ok(())
                }
                None => {
                    let mut is_matched: bool = false;
                    for (name, ref_id) in binder.context.regular_tables.iter() {
                        let table = binder.catalog.get_table(ref_id);
                        let col_opt = table.get_column_by_name(column_name);
                        if col_opt.is_some() {
                            let col = col_opt.unwrap();
                            if !is_matched {
                                is_matched = true;
                                *column_ref_id = Some(ColumnRefId {
                                    database_id: ref_id.database_id,
                                    schema_id: ref_id.schema_id,
                                    table_id: ref_id.table_id,
                                    column_id: col.id(),
                                });
                                *table_name = Some(table.name().clone());
                            } else {
                                return Err(BindError::AmbiguousColumn);
                            }
                        }
                    }

                    if is_matched {
                        *column_index = Some(record_regular_table_column(
                            binder,
                            table_name.as_ref().unwrap(),
                            column_name,
                            column_ref_id.unwrap().column_id,
                        ));
                    } else {
                        return Err(BindError::InvalidColumn(column_name.clone()));
                    }

                    Ok(())
                }
            },
            _ => todo!(),
        }
    }
}

pub fn record_regular_table_column(
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
        idxs.lock().unwrap().push(column_id);
        idx
    } else {
        let idxs = binder.context.column_ids.get_mut(table_name).unwrap();
        idxs.lock()
            .unwrap()
            .iter()
            .position(|&r| r == column_id)
            .unwrap() as u32
    }
}
