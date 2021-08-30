use postgres_parser::{parse_query, PgParserError, Node};

struct Parser {

}

impl Parser {
    fn parse_sql(query: &String) -> Result<Vec<Node>, PgParserError> {
        parse_query(query)
    }
}

