//! A simple interactive shell of the database.

use log::info;
use risinglight::storage::SecondaryStorageOptions;
use risinglight::Database;
use rustyline::error::ReadlineError;
use rustyline::Editor;

#[tokio::main]
async fn main() {
    env_logger::init();

    let db = if let Some(x) = std::env::args().nth(1) {
        if x == "--memory" {
            info!("using memory engine");
            Database::new_in_memory()
        } else {
            info!("using Secondary engine");
            Database::new_on_disk(SecondaryStorageOptions::default_for_cli()).await
        }
    } else {
        info!("using Secondary engine");
        Database::new_on_disk(SecondaryStorageOptions::default_for_cli()).await
    };

    let mut rl = Editor::<()>::new();
    loop {
        let readline = rl.readline("> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                let ret = db.run(&line).await;
                match ret {
                    Ok(chunks) => {
                        for chunk in chunks {
                            println!("{}", chunk);
                        }
                    }
                    Err(err) => println!("{}", err),
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("Interrupted");
            }
            Err(ReadlineError::Eof) => {
                println!("Exited");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
}
