use super::*;
use postgres_parser as pg;
use std::convert::{TryFrom, TryInto};

mod create_database;
mod create_schema;
mod create_table;
mod insert;
mod select;

pub use self::create_database::*;
pub use self::create_schema::*;
pub use self::create_table::*;
pub use self::insert::*;
pub use self::select::*;

#[derive(Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum SQLStatement {
    CreateDatabase(CreateDatabaseStmt),
    CreateSchema(CreateSchemaStmt),
    CreateTable(CreateTableStmt),
    Insert(InsertStmt),
    Select(SelectStmt),
}

impl SQLStatement {
    pub fn parse(sql: &str) -> Result<Vec<Self>, ParseError> {
        pg::parse_query(sql)?.iter().map(Self::try_from).collect()
    }
}

impl TryFrom<&pg::Node> for SQLStatement {
    type Error = ParseError;

    fn try_from(node: &pg::Node) -> Result<Self, Self::Error> {
        match node {
            pg::Node::CreatedbStmt(_) => Ok(Self::CreateDatabase(node.try_into()?)),
            pg::Node::CreateSchemaStmt(_) => Ok(Self::CreateSchema(node.try_into()?)),
            pg::Node::CreateStmt(_) => Ok(Self::CreateTable(node.try_into()?)),
            pg::Node::InsertStmt(_) => Ok(Self::Insert(node.try_into()?)),
            pg::Node::SelectStmt(_) => Ok(Self::Select(node.try_into()?)),
            _ => todo!("parse statement"),
        }
    }
}
