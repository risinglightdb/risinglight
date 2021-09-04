use super::*;
use crate::{
    catalog::{ColumnCatalog, ColumnDesc},
    types::{DataType, DataTypeEnum, DatabaseId, SchemaId},
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
        if type_name.typmods.is_some() {
            todo!("parse typmods");
        }
        let datatype_node =
            try_match!(type_name.names, Some(ns) => ns.last().unwrap(), "datatype name");
        let datatype_name = try_match!(datatype_node, pg::Node::Value(v) => v.string.clone().unwrap(), "datatype name");
        let datatype = datatype_name
            .parse::<DataTypeEnum>()
            .map_err(|_| ParseError::InvalidInput("datatype"))?;

        let mut is_nullable = false;
        let mut is_primary = false;
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
                pg::sys::ConstrType::CONSTR_UNIQUE => is_unique = true,
                _ => todo!("column constraint"),
            }
        }
        let col_name = try_match!(cdef.colname, Some(s) => s.clone(), "column name");
        Ok(ColumnCatalog::new(
            0, // TODO: id?
            col_name,
            ColumnDesc::new(DataType::new(datatype, is_nullable), is_primary ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_table() {
        let sql = "create table t1 (v1 int not null, v2 double null)";
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
                        ColumnDesc::new(DataType::new(DataTypeEnum::Int32, false), false)
                    ),
                    ColumnCatalog::new(
                        0,
                        "v2".into(),
                        ColumnDesc::new(DataType::new(DataTypeEnum::Float64, true), false)
                    ),
                ],
                database_id: None,
                schema_id: None
            }
        );
    }
}
