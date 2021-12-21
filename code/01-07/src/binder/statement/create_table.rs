use std::collections::HashSet;

use super::*;
use crate::catalog::ColumnDesc;
use crate::parser::{ColumnDef, ColumnOption, Statement};
use crate::types::DataType;

/// A bound `CREATE TABLE` statement.
#[derive(Debug, PartialEq, Clone)]
pub struct BoundCreateTable {
    pub schema_id: SchemaId,
    pub table_name: String,
    pub columns: Vec<(String, ColumnDesc)>,
}

impl Binder {
    pub fn bind_create_table(&mut self, stmt: &Statement) -> Result<BoundCreateTable, BindError> {
        match stmt {
            Statement::CreateTable { name, columns, .. } => {
                // check empty columns
                if columns.is_empty() {
                    return Err(BindError::EmptyColumns);
                }
                let (schema_name, table_name) = split_name(name)?;
                let schema = self
                    .catalog
                    .get_schema_by_name(schema_name)
                    .ok_or_else(|| BindError::SchemaNotFound(schema_name.into()))?;
                // check duplicated table name
                if schema.get_table_by_name(table_name).is_some() {
                    return Err(BindError::DuplicatedTable(table_name.into()));
                }
                // check duplicated column names
                let mut set = HashSet::new();
                for col in columns.iter() {
                    if !set.insert(col.name.value.clone()) {
                        return Err(BindError::DuplicatedColumn(col.name.value.clone()));
                    }
                }
                let columns = columns
                    .iter()
                    .map(|col| (col.name.value.clone(), ColumnDesc::from(col)))
                    .collect();
                Ok(BoundCreateTable {
                    schema_id: schema.id(),
                    table_name: table_name.into(),
                    columns,
                })
            }
            _ => panic!("mismatched statement type"),
        }
    }
}

impl From<&ColumnDef> for ColumnDesc {
    fn from(cdef: &ColumnDef) -> Self {
        let mut is_nullable = true;
        let mut is_primary = false;
        for opt in cdef.options.iter() {
            match opt.option {
                ColumnOption::Null => is_nullable = true,
                ColumnOption::NotNull => is_nullable = false,
                ColumnOption::Unique { is_primary: v } => is_primary = v,
                _ => todo!("column options"),
            }
        }
        ColumnDesc::new(
            DataType::new(cdef.data_type.clone(), is_nullable),
            is_primary,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::catalog::DatabaseCatalog;
    use crate::parser::parse;
    use crate::types::{DataTypeExt, DataTypeKind};

    #[test]
    fn bind_create_table() {
        let catalog = Arc::new(DatabaseCatalog::new());
        let mut binder = Binder::new(catalog.clone());
        let sql = "
            create table t1 (v1 int not null, v2 int); 
            create table t2 (a int not null, a int not null);
            create table t3 (v1 int not null);";
        let stmts = parse(sql).unwrap();

        assert_eq!(
            binder.bind_create_table(&stmts[0]).unwrap(),
            BoundCreateTable {
                schema_id: 0,
                table_name: "t1".into(),
                columns: vec![
                    ("v1".into(), DataTypeKind::Int(None).not_null().to_column()),
                    ("v2".into(), DataTypeKind::Int(None).nullable().to_column()),
                ],
            }
        );

        assert_eq!(
            binder.bind_create_table(&stmts[1]),
            Err(BindError::DuplicatedColumn("a".into()))
        );

        let schema = catalog.get_schema(0).unwrap();
        schema.add_table("t3").unwrap();
        assert_eq!(
            binder.bind_create_table(&stmts[2]),
            Err(BindError::DuplicatedTable("t3".into()))
        );
    }
}
