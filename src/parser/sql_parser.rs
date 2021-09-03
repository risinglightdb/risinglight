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
        // let sql = "select a from t1";
        let sql = "create table t1 (v1 int not null, v2 int not null)";
        println!("{}", sql);
        let nodes = Parser::parse_sql(sql).unwrap();
        println!("{:#?}", nodes);
    }
}
