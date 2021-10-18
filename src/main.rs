//! A simple interactive shell of the database.

use risinglight::Database;
use std::io::Write;

fn main() {
    env_logger::init();
    let db = Database::new();
    loop {
        print!("> ");
        std::io::stdout().lock().flush().unwrap();
        let mut input = String::new();
        std::io::stdin().read_line(&mut input).unwrap();
        let ret = db.run(&input);
        match ret {
            Ok(chunks) => {
                for chunk in chunks {
                    println!("{}", chunk);
                }
            }
            Err(err) => println!("{}", err),
        }
    }
}
