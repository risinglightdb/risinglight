use sqlparser::dialect::{Dialect, PostgreSqlDialect};
use sqlparser::parser::{ Parser, ParserError};
use sqlparser::ast::{Statement};
struct SQLParser {
    dialect: Box<dyn Dialect>
}

impl SQLParser {
    fn new() -> SQLParser {
        SQLParser {
            dialect: Box::new(PostgreSqlDialect{}
            )
        }
    }

    fn parse_sql(&self, sql: &String) ->  Result<Vec<Statement>, ParserError> {
        Parser::parse_sql(&*self.dialect, sql)
    }
}

#[cfg(test)]
mod parser_test {
    use super::*;
    #[test]
    fn basic_parser_test() {
        let parser = SQLParser::new();
        let ast = parser.parse_sql(&String::from("select a from b join c")).unwrap();
        println!("AST: {:?}", ast);
    }
}