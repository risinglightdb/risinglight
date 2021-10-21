//! A simple interactive shell of the database.

use std::io::Write;

use risinglight::Database;

#[tokio::main]
async fn main() {
    env_logger::init();
    let db = Database::new_in_memory();
    loop {
        print!("> ");
        std::io::stdout().lock().flush().unwrap();
        let mut input = String::new();
        std::io::stdin().read_line(&mut input).unwrap();
        let ret = db.run(&input).await;
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
