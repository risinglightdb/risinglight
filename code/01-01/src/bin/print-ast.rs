use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

fn main() {
    let mut sql = String::new();
    std::io::stdin().read_line(&mut sql).unwrap();
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, &sql);
    println!("{:#?}", stmts);
}
