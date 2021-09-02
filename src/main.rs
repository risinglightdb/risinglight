mod catalog;
mod parser;
use parser::*;
mod types;

fn main() {
    let sql = String::from("create table t1 (v1 int , v2 int not null)");
    println!("{:#?}", Parser::parse_sql(&sql).unwrap());
}
