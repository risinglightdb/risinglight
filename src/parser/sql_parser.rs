use postgres_parser::{parse_query, Node, PgParserError};

pub(crate) struct Parser {}

impl Parser {
    fn parse_sql(query: &str) -> Result<Vec<Node>, PgParserError> {
        parse_query(query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select() {
        Parser::parse_sql("select a from t1").unwrap();
    }
}
