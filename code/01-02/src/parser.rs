//! Parse the SQL string into an Abstract Syntax Tree (AST).
//!
//! The parser module directly uses the [`sqlparser`] crate
//! and re-exports its AST types.

pub use sqlparser::{ast::*, parser::ParserError};
use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

/// Parse the SQL string into a list of ASTs.
pub fn parse(sql: &str) -> Result<Vec<Statement>, ParserError> {
    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, sql)
}
