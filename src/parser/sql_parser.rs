use postgres_parser::{parse_query, PgParserError, Node};

struct Parser {

}

impl Parser {
    fn parse_sql(query: &String) -> Result<Vec<Node>, PgParserError> {
        parse_query(query)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_select() {
        let sql = String::from("select a from t1");
        Parser::parse_sql(&sql).unwrap();
    }
}