use super::*;

mod create;
mod insert;

pub(crate) use create::*;
pub(crate) use insert::*;

#[derive(Debug, PartialEq)]
pub(crate) struct SQLStatement {
    pub(crate) statement: SQLStatementEnum,
}

#[derive(Debug, PartialEq)]
pub(crate) enum SQLStatementEnum {
    CreateTableStatment(CreateTableStmt),
    InsertStatement(InsertStmt),
}

impl SQLStatement {
    pub(crate) fn new_create_table_stmt(create_stmt: CreateTableStmt) -> SQLStatement {
        SQLStatement {
            statement: SQLStatementEnum::CreateTableStatment(create_stmt),
        }
    }

    pub(crate) fn new_insert_stmt(insert_stmt: InsertStmt) -> SQLStatement {
        SQLStatement {
            statement: SQLStatementEnum::InsertStatement(insert_stmt),
        }
    }
}
