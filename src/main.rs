//! A simple interactive shell of the database.

use std::io::Write;

use log::info;
use risinglight::Database;

#[tokio::main]
async fn main() {
    env_logger::init();

    let db = if let Some(x) = std::env::args().nth(1) {
        if x == "--disk" {
            info!("using Secondary engine");
            Database::new_on_disk().await
        } else {
            Database::new_in_memory()
        }
    } else {
        Database::new_in_memory()
    };

    loop {
        print!("> ");
        std::io::stdout().lock().flush().unwrap();
        let mut input = String::new();
        let cnt = std::io::stdin().read_line(&mut input).unwrap();

        if cnt == 0 {
            // EOF
            break;
        }

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
