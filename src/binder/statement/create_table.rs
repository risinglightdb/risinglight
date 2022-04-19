// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.
use sqlparser::ast::TableConstraint;

use super::*;
use crate::catalog::{ColumnCatalog, ColumnDesc};
use crate::parser::{ColumnDef, ColumnOption, Statement};
use crate::types::{DataType, DatabaseId, SchemaId};

/// A bound `create table` statement.
#[derive(Debug, PartialEq, Clone)]
pub struct BoundCreateTable {
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
    pub table_name: String,
    pub columns: Vec<ColumnCatalog>,
    pub ordered_pk_ids: Vec<ColumnId>,
}

impl Binder {
    pub fn bind_create_table(&mut self, stmt: &Statement) -> Result<BoundCreateTable, BindError> {
        match stmt {
            Statement::CreateTable {
                name,
                columns,
                constraints,
                ..
            } => {
                let name = &lower_case_name(name);
                let (database_name, schema_name, table_name) = split_name(name)?;
                let db = self
                    .catalog
                    .get_database_by_name(database_name)
                    .ok_or_else(|| BindError::InvalidDatabase(database_name.into()))?;
                let schema = db
                    .get_schema_by_name(schema_name)
                    .ok_or_else(|| BindError::InvalidSchema(schema_name.into()))?;
                if schema.get_table_by_name(table_name).is_some() {
                    return Err(BindError::DuplicatedTable(table_name.into()));
                }
                // check duplicated column names
                let mut set = HashSet::new();
                for col in columns.iter() {
                    if !set.insert(col.name.value.to_lowercase()) {
                        return Err(BindError::DuplicatedColumn(col.name.value.clone()));
                    }
                }

                // column name -> index in `ordered_pk_ids` vector
                let ordered_pk_pos = Binder::get_extra_pks(constraints);
                // whether we have primary key declared by "primary key(c1, c2..) syntax"
                let flag = !ordered_pk_pos.is_empty();
                // ordered primary ids declared by "primary key(c1, c2..) syntax"
                let mut ordered_pk_ids = vec![0; ordered_pk_pos.len()];
                let mut col_catalogs = vec![];

                for (idx, colum_def) in columns.iter().enumerate() {
                    let mut is_primary = false;
                    let mut col = ColumnCatalog::from(colum_def, &mut is_primary);
                    if flag && is_primary {
                        // we can't support sql query like
                        // "create table (a int primary key, b int not null, primary key(b));"
                        // declaring pk both in column's option and 'primary key(c1..)' syntax"
                        return Err(BindError::NotSupportedTSQL);
                    } else if is_primary {
                        // primary key is declared only in column's options
                        ordered_pk_ids.push(idx as ColumnId);
                    } else if ordered_pk_pos.contains_key(col.name()) {
                        // primary key is declared only by "primary key(c1, c2) syntax"
                        let pos = *(ordered_pk_pos.get(col.name()).unwrap());
                        ordered_pk_ids[pos] = idx as ColumnId;
                        col.set_primary(true); // TODO: remove this line in the future
                    }
                    col.set_id(idx as ColumnId);
                    col_catalogs.push(col);
                }

                Ok(BoundCreateTable {
                    database_id: db.id(),
                    schema_id: schema.id(),
                    table_name: table_name.into(),
                    columns: col_catalogs,
                    ordered_pk_ids,
                })
            }
            _ => panic!("mismatched statement type"),
        }
    }

    fn get_extra_pks(constraints: &Vec<TableConstraint>) -> HashMap<String, usize> {
        let mut ordered_pk_pos: HashMap<String, usize> = HashMap::new();
        for constraint in constraints {
            match constraint {
                TableConstraint::Unique {
                    is_primary,
                    columns,
                    ..
                } if *is_primary => columns.iter().enumerate().for_each(|(index, indent)| {
                    ordered_pk_pos.insert(indent.value.clone(), index);
                }),
                _ => todo!(),
            }
        }
        ordered_pk_pos
    }
}

impl ColumnCatalog {
    fn from(cdef: &ColumnDef, is_primary_: &mut bool) -> Self {
        let mut is_nullable = true;
        for opt in &cdef.options {
            match opt.option {
                ColumnOption::Null => is_nullable = true,
                ColumnOption::NotNull => is_nullable = false,
                ColumnOption::Unique { is_primary } => *is_primary_ = is_primary,
                _ => todo!("column options"),
            }
        }
        ColumnCatalog::new(
            0,
            ColumnDesc::new(
                DataType::new(cdef.data_type.clone(), is_nullable),
                cdef.name.value.to_lowercase(),
                *is_primary_,
            ),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::catalog::RootCatalog;
    use crate::parser::parse;
    use crate::types::{DataTypeExt, DataTypeKind};

    #[test]
    fn bind_create_table() {
        let catalog = Arc::new(RootCatalog::new());
        let mut binder = Binder::new(catalog.clone());
        let sql = "
            create table t1 (v1 int not null, v2 int);
            create table t2 (a int not null, a int not null);
            create table t3 (v1 int not null);
            create table t4 (a int not null, b int not null, c int, primary key(a, b));
            create table t5 (a int not null, b int not null, c int, primary key(b, a));
            create table t6 (a int primary key, b int not null, c int not null, primary key(b, c));
            create table t7 (a int primary key, b int)";

        let stmts = parse(sql).unwrap();

        assert_eq!(
            binder.bind_create_table(&stmts[0]).unwrap(),
            BoundCreateTable {
                database_id: 0,
                schema_id: 0,
                table_name: "t1".into(),
                columns: vec![
                    ColumnCatalog::new(
                        0,
                        DataTypeKind::Int(None).not_null().to_column("v1".into()),
                    ),
                    ColumnCatalog::new(
                        1,
                        DataTypeKind::Int(None).nullable().to_column("v2".into()),
                    ),
                ],
                ordered_pk_ids: vec![],
            }
        );

        assert_eq!(
            binder.bind_create_table(&stmts[1]),
            Err(BindError::DuplicatedColumn("a".into()))
        );

        let database = catalog.get_database_by_id(0).unwrap();
        let schema = database.get_schema_by_id(0).unwrap();
        schema
            .add_table("t3".into(), vec![], false, vec![])
            .unwrap();
        assert_eq!(
            binder.bind_create_table(&stmts[2]),
            Err(BindError::DuplicatedTable("t3".into()))
        );

        assert_eq!(
            binder.bind_create_table(&stmts[3]).unwrap(),
            BoundCreateTable {
                database_id: 0,
                schema_id: 0,
                table_name: "t4".into(),
                columns: vec![
                    ColumnCatalog::new(
                        0,
                        DataTypeKind::Int(None)
                            .not_null()
                            .to_column_primary_key("a".into()),
                    ),
                    ColumnCatalog::new(
                        1,
                        DataTypeKind::Int(None)
                            .not_null()
                            .to_column_primary_key("b".into()),
                    ),
                    ColumnCatalog::new(2, DataTypeKind::Int(None).nullable().to_column("c".into())),
                ],
                ordered_pk_ids: vec![0, 1],
            }
        );

        assert_eq!(
            binder.bind_create_table(&stmts[4]).unwrap(),
            BoundCreateTable {
                database_id: 0,
                schema_id: 0,
                table_name: "t5".into(),
                columns: vec![
                    ColumnCatalog::new(
                        0,
                        DataTypeKind::Int(None)
                            .not_null()
                            .to_column_primary_key("a".into()),
                    ),
                    ColumnCatalog::new(
                        1,
                        DataTypeKind::Int(None)
                            .not_null()
                            .to_column_primary_key("b".into()),
                    ),
                    ColumnCatalog::new(2, DataTypeKind::Int(None).nullable().to_column("c".into())),
                ],
                ordered_pk_ids: vec![1, 0],
            }
        );

        assert_eq!(
            binder.bind_create_table(&stmts[5]),
            Err(BindError::NotSupportedTSQL)
        );

        assert_eq!(
            binder.bind_create_table(&stmts[6]).unwrap(),
            BoundCreateTable {
                database_id: 0,
                schema_id: 0,
                table_name: "t7".into(),
                columns: vec![
                    ColumnCatalog::new(
                        0,
                        DataTypeKind::Int(None)
                            .nullable()
                            .to_column_primary_key("a".into()),
                    ),
                    ColumnCatalog::new(1, DataTypeKind::Int(None).nullable().to_column("b".into()),),
                ],
                ordered_pk_ids: vec![0],
            }
        );
    }
}
