use std::collections::HashSet;
use std::result::Result as RawResult;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::*;
use crate::catalog::{ColumnCatalog, ColumnId, DatabaseId, SchemaId};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Serialize, Deserialize)]
pub struct CreateTable {
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
    pub table_name: String,
    pub columns: Vec<ColumnCatalog>,
    pub ordered_pk_ids: Vec<ColumnId>,
}

impl std::fmt::Display for CreateTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "databaseId: {}, schemaId: {}, tableName: {}, columns: {:?}, orderedIds: {:?}",
            self.database_id, self.schema_id, self.table_name, self.columns, self.ordered_pk_ids
        )
    }
}

impl FromStr for CreateTable {
    type Err = ();

    fn from_str(_s: &str) -> RawResult<Self, Self::Err> {
        Err(())
    }
}

impl Binder {
    pub(super) fn bind_create_table(
        &mut self,
        name: ObjectName,
        columns: &[ColumnDef],
        constraints: &[TableConstraint],
    ) -> Result {
        let name = lower_case_name(&name);
        let (database_name, schema_name, table_name) = split_name(&name)?;
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

        for &index in &ordered_pk_ids {
            columns[index as usize].set_primary(true);
            columns[index as usize].set_nullable(false);
        }

        let create = self.egraph.add(Node::CreateTable(CreateTable {
            database_id: db.id(),
            schema_id: schema.id(),
            table_name: table_name.into(),
            columns,
            ordered_pk_ids,
        }));
        Ok(create)
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
