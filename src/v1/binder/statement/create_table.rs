// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.
use sqlparser::ast::TableConstraint;

use super::*;
use crate::catalog::{ColumnCatalog, ColumnDesc, DatabaseId, SchemaId};
use crate::parser::{ColumnDef, ColumnOption, Statement};
use crate::types::DataType;

/// A bound `create table` statement.
#[derive(Debug, PartialEq, Eq, Clone)]
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

                let mut ordered_pk_ids = Binder::ordered_pks_from_columns(columns);
                let has_pk_from_column = !ordered_pk_ids.is_empty();

                if ordered_pk_ids.len() > 1 {
                    // multi primary key should be declared by "primary key(c1, c2...)" syntax
                    return Err(BindError::NotSupportedTSQL);
                }

                let pks_name_from_constraints = Binder::pks_name_from_constraints(constraints);
                if has_pk_from_column && !pks_name_from_constraints.is_empty() {
                    // can't get primary key both from "primary key(c1, c2...)" syntax and
                    // column's option
                    return Err(BindError::NotSupportedTSQL);
                } else if !has_pk_from_column {
                    for name in &pks_name_from_constraints {
                        if !set.contains(name) {
                            return Err(BindError::InvalidColumn(name.clone()));
                        }
                    }
                    ordered_pk_ids =
                        Binder::ordered_pks_from_constraint(&pks_name_from_constraints, columns);
                }

                let mut columns: Vec<ColumnCatalog> = columns
                    .iter()
                    .enumerate()
                    .map(|(idx, col)| {
                        let mut col = ColumnCatalog::from(col);
                        col.set_id(idx as ColumnId);
                        col
                    })
                    .collect();

                // // TODO: when remove `is_primary` filed in `ColumnDesc`,
                // // Remove this line and change `columns` above to immut.
                for &index in &ordered_pk_ids {
                    columns[index as usize].set_primary(true);
                    columns[index as usize].set_nullable(false);
                }

                Ok(BoundCreateTable {
                    database_id: db.id(),
                    schema_id: schema.id(),
                    table_name: table_name.into(),
                    columns,
                    ordered_pk_ids,
                })
            }
            _ => panic!("mismatched statement type"),
        }
    }

    /// get primary keys' id in declared order。
    /// we use index in columns vector as column id
    fn ordered_pks_from_columns(columns: &[ColumnDef]) -> Vec<ColumnId> {
        let mut ordered_pks = Vec::new();

        for (index, col_def) in columns.iter().enumerate() {
            for option_def in &col_def.options {
                let is_primary_ = if let ColumnOption::Unique { is_primary } = option_def.option {
                    is_primary
                } else {
                    false
                };
                if is_primary_ {
                    ordered_pks.push(index as ColumnId);
                }
            }
        }
        ordered_pks
    }

    /// We have used `pks_name_from_constraints` to get the primary keys' name sorted by declaration
    /// order in "primary key(c1, c2..)" syntax. Now we transfer the name to id to get the sorted
    /// ID
    fn ordered_pks_from_constraint(pks_name: &[String], columns: &[ColumnDef]) -> Vec<ColumnId> {
        let mut ordered_pks = vec![0; pks_name.len()];
        let mut pos_in_ordered_pk = HashMap::new(); // used to get pos from column name
        pks_name.iter().enumerate().for_each(|(pos, name)| {
            pos_in_ordered_pk.insert(name, pos);
        });

        columns.iter().enumerate().for_each(|(index, colum_desc)| {
            let column_name = &colum_desc.name.value;
            if pos_in_ordered_pk.contains_key(column_name) {
                let id = index as ColumnId;
                let pos = *(pos_in_ordered_pk.get(column_name).unwrap());
                ordered_pks[pos] = id;
            }
        });
        ordered_pks
    }
    /// get the primary keys' name sorted by declaration order in "primary key(c1, c2..)" syntax.
    fn pks_name_from_constraints(constraints: &[TableConstraint]) -> Vec<String> {
        let mut pks_name_from_constraints = vec![];

        for constraint in constraints {
            match constraint {
                TableConstraint::Unique {
                    is_primary,
                    columns,
                    ..
                } if *is_primary => columns.iter().for_each(|ident| {
                    pks_name_from_constraints.push(ident.value.clone());
                }),
                _ => continue,
            }
        }
        pks_name_from_constraints
    }
}

impl From<&ColumnDef> for ColumnCatalog {
    fn from(cdef: &ColumnDef) -> Self {
        let mut is_nullable = true;
        let mut is_primary_ = false;
        for opt in &cdef.options {
            match opt.option {
                ColumnOption::Null => is_nullable = true,
                ColumnOption::NotNull => is_nullable = false,
                ColumnOption::Unique { is_primary } => is_primary_ = is_primary,
                _ => todo!("column options"),
            }
        }
        ColumnCatalog::new(
            0,
            ColumnDesc::new(
                DataType::new((&cdef.data_type).into(), is_nullable),
                cdef.name.value.to_lowercase(),
                is_primary_,
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
    use crate::types::DataTypeKind;

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
            create table t7 (a int primary key, b int);
            create table t8 (a int not null, b int, primary key(a));
            create table t9 (v1 int, primary key(a));";

        let stmts = parse(sql).unwrap();

        assert_eq!(
            binder.bind_create_table(&stmts[0]).unwrap(),
            BoundCreateTable {
                database_id: 0,
                schema_id: 0,
                table_name: "t1".into(),
                columns: vec![
                    ColumnCatalog::new(0, DataTypeKind::Int32.not_null().to_column("v1".into()),),
                    ColumnCatalog::new(1, DataTypeKind::Int32.nullable().to_column("v2".into()),),
                ],
                ordered_pk_ids: vec![],
            }
        );

        assert_eq!(
            binder.bind_create_table(&stmts[1]),
            Err(BindError::DuplicatedColumn("a".into()))
        );

        let ref_id = TableRefId::new(0, 0, 0);
        catalog
            .add_table(ref_id, "t3".into(), vec![], false, vec![])
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
                        DataTypeKind::Int32
                            .not_null()
                            .to_column_primary_key("a".into()),
                    ),
                    ColumnCatalog::new(
                        1,
                        DataTypeKind::Int32
                            .not_null()
                            .to_column_primary_key("b".into()),
                    ),
                    ColumnCatalog::new(2, DataTypeKind::Int32.nullable().to_column("c".into())),
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
                        DataTypeKind::Int32
                            .not_null()
                            .to_column_primary_key("a".into()),
                    ),
                    ColumnCatalog::new(
                        1,
                        DataTypeKind::Int32
                            .not_null()
                            .to_column_primary_key("b".into()),
                    ),
                    ColumnCatalog::new(2, DataTypeKind::Int32.nullable().to_column("c".into())),
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
                        DataTypeKind::Int32
                            .not_null()
                            .to_column_primary_key("a".into()),
                    ),
                    ColumnCatalog::new(1, DataTypeKind::Int32.nullable().to_column("b".into())),
                ],
                ordered_pk_ids: vec![0],
            }
        );

        assert_eq!(
            binder.bind_create_table(&stmts[7]).unwrap(),
            BoundCreateTable {
                database_id: 0,
                schema_id: 0,
                table_name: "t8".into(),
                columns: vec![
                    ColumnCatalog::new(
                        0,
                        DataTypeKind::Int32
                            .not_null()
                            .to_column_primary_key("a".into()),
                    ),
                    ColumnCatalog::new(1, DataTypeKind::Int32.nullable().to_column("b".into())),
                ],
                ordered_pk_ids: vec![0],
            }
        );

        assert_eq!(
            binder.bind_create_table(&stmts[8]),
            Err(BindError::InvalidColumn("a".into()))
        );
    }
}
