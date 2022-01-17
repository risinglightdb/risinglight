//! A simple interactive shell of the database.

use std::fs::File;

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
    let history_path = dirs::cache_dir().and_then(|p| {
        let cache_dir = p.join("risinglight");
        std::fs::create_dir_all(cache_dir.as_path()).ok()?;
        let history_path = cache_dir.join("history.txt");
        if !history_path.as_path().exists() {
            File::create(history_path.as_path()).ok()?;
        }
        Some(history_path.into_boxed_path())
    });

    if let Some(ref history_path) = history_path {
        if let Err(err) = rl.load_history(&history_path) {
            println!("No previous history. {err}");
        }
    }
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

    if let Some(ref history_path) = history_path {
        if let Err(err) = rl.save_history(&history_path) {
            println!("Save history failed, {err}");
        }
    }
}
