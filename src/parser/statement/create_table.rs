use super::*;
use crate::{
    catalog::{ColumnCatalog, ColumnDesc},
    types::{DataType, DataTypeKind, DatabaseId, SchemaId},
};
use postgres_parser as pg;
use std::convert::TryFrom;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct CreateTableStmt {
    /// The database name of the entry.
    pub database_name: Option<String>,
    /// The schema name of the entry.
    pub schema_name: Option<String>,
    /// Name of the table we want to create.
    pub table_name: String,
    /// List of columns descriptors in the table. If it's not provided at
    /// transformation time, then we must set it at binding time.
    pub column_descs: Vec<ColumnCatalog>,

    // Binder will fill the following values
    pub database_id: Option<DatabaseId>,
    pub schema_id: Option<SchemaId>,
}

impl TryFrom<&pg::Node> for CreateTableStmt {
    type Error = ParseError;

    fn try_from(node: &pg::Node) -> Result<Self, Self::Error> {
        let stmt = try_match!(node, pg::Node::CreateStmt(s) => s, "create");
        let relation = try_match!(stmt.relation, Some(x) => &**x, "relation");
        let table_name = try_match!(relation.relname, Some(s) => s.clone(), "table name");
        let schema_name = relation.schemaname.clone();
        let database_name = relation.catalogname.clone();

        let columns = try_match!(stmt.tableElts, Some(v) => v, "column");
        let mut column_descs = vec![];
        for column in columns {
            match column {
                pg::Node::ColumnDef(cdef) => {
                    let col = crate::catalog::ColumnCatalog::try_from(cdef)?;
                    column_descs.push(col);
                }
                // pg::Node::Constraint(cons) => match &cons.contype {
                //     pg::sys::ConstrType::CONSTR_PRIMARY => {
                //         for cell in cons.keys.as_ref().unwrap() {
                //             let key = match cell {
                //                 pg::Node::Value(v) => v.string.clone().unwrap(),
                //                 _ => panic!("invalid value type"),
                //             };
                //         }
                //     }
                //     _ => todo!("constraint type"),
                // }
                _ => todo!("tableElt type not supported yet"),
            }
        }
        Ok(CreateTableStmt {
            database_name,
            schema_name,
            table_name,
            column_descs,
            database_id: None,
            schema_id: None,
        })
    }
}

impl TryFrom<&pg::nodes::ColumnDef> for ColumnCatalog {
    type Error = ParseError;

    fn try_from(cdef: &pg::nodes::ColumnDef) -> Result<Self, Self::Error> {
        let type_name = try_match!(cdef.typeName, Some(t) => &**t, "type name");
        let datatype_node =
            try_match!(type_name.names, Some(ns) => ns.last().unwrap(), "datatype name");
        let datatype_name = try_match!(datatype_node, pg::Node::Value(v) => v.string.clone().unwrap(), "datatype name");
        let mut datatype = datatype_name
            .parse::<DataTypeKind>()
            .map_err(|_| ParseError::InvalidInput("datatype"))?;
        if let Some(typmods) = &type_name.typmods {
            let c = try_match!(typmods[0], pg::Node::A_Const(c) => c, "const in typmods");
            let varlen = try_match!(c.val.int, Some(i) => *i as u32, "int value");
            datatype.set_len(varlen);
        }

        let mut is_nullable = true;
        let mut is_primary = false;

        #[allow(unused_variables)]
        let mut is_unique = false;
        for cons in cdef.constraints.iter().flatten() {
            let cons = try_match!(cons, pg::Node::Constraint(c) => c, "constraint");
            match &cons.contype {
                pg::sys::ConstrType::CONSTR_NOTNULL => is_nullable = false,
                pg::sys::ConstrType::CONSTR_NULL => is_nullable = true,
                pg::sys::ConstrType::CONSTR_PRIMARY => {
                    is_primary = true;
                    is_nullable = false;
                }
                #[allow(unused_assignments)]
                pg::sys::ConstrType::CONSTR_UNIQUE => is_unique = true,
                _ => todo!("column constraint"),
            }
        }
        let col_name = try_match!(cdef.colname, Some(s) => s.clone(), "column name");
        Ok(ColumnCatalog::new(
            0, // TODO: id?
            col_name,
            ColumnDesc::new(DataType::new(datatype, is_nullable), is_primary),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_table() {
        let sql = "create table mydatabase.myschema.orders(v1 int not null, v2 double null)";
        let nodes = pg::parse_query(sql).unwrap();
        let stmt = CreateTableStmt::try_from(&nodes[0]).unwrap();
        assert_eq!(
            stmt,
            CreateTableStmt {
                database_name: Some("mydatabase".into()),
                schema_name: Some("myschema".into()),
                table_name: "orders".into(),
                column_descs: vec![
                    ColumnCatalog::new(0, "v1".into(), DataTypeKind::Int32.not_null().to_column()),
                    ColumnCatalog::new(
                        0,
                        "v2".into(),
                        DataTypeKind::Float64.nullable().to_column(),
                    ),
                ],
                database_id: None,
                schema_id: None,
            }
        );
    }

    #[test]
    fn parse_create_table_primary_key() {
        let sql = "create table t1(v1 int primary key, v2 int)";
        let nodes = pg::parse_query(sql).unwrap();
        let stmt = CreateTableStmt::try_from(&nodes[0]).unwrap();
        assert_eq!(
            stmt,
            CreateTableStmt {
                database_name: None,
                schema_name: None,
                table_name: "t1".into(),
                column_descs: vec![
                    ColumnCatalog::new(
                        0,
                        "v1".into(),
                        DataTypeKind::Int32.not_null().to_column_primary_key()
                    ),
                    ColumnCatalog::new(0, "v2".into(), DataTypeKind::Int32.nullable().to_column()),
                ],
                database_id: None,
                schema_id: None,
            }
        );
    }

    #[test]
    fn parse_create_table_char() {
        let sql = "create table t(v1 char, v2 char(2), v3 varchar, v4 varchar(20))";
        let nodes = pg::parse_query(sql).unwrap();
        let stmt = CreateTableStmt::try_from(&nodes[0]).unwrap();
        assert_eq!(
            stmt,
            CreateTableStmt {
                database_name: None,
                schema_name: None,
                table_name: "t".into(),
                column_descs: vec![
                    ColumnCatalog::new(
                        0,
                        "v1".into(),
                        DataTypeKind::Char(1).nullable().to_column(),
                    ),
                    ColumnCatalog::new(
                        0,
                        "v2".into(),
                        DataTypeKind::Char(2).nullable().to_column(),
                    ),
                    ColumnCatalog::new(
                        0,
                        "v3".into(),
                        DataTypeKind::Varchar(256).nullable().to_column(),
                    ),
                    ColumnCatalog::new(
                        0,
                        "v4".into(),
                        DataTypeKind::Varchar(20).nullable().to_column(),
                    ),
                ],
                database_id: None,
                schema_id: None,
            }
        );
    }
}
