pub use sqlparser::{ast::*, parser::ParserError};
use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

pub fn parse(sql: &str) -> Result<Vec<Statement>, ParserError> {
    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, sql)
}
