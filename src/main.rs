// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

//! A simple interactive shell of the database.

use std::fs::File;
use std::sync::Mutex;

use anyhow::{anyhow, Result};
use clap::Parser;
use risinglight::array::{datachunk_to_sqllogictest_string, DataChunk};
use risinglight::storage::SecondaryStorageOptions;
use risinglight::Database;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use tracing::level_filters::LevelFilter;
use tracing::{info, warn, Level};
use tracing_subscriber::filter;
use tracing_subscriber::prelude::*;

/// RisingLight: an OLAP database system.
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// File to execute. Can be either a SQL `sql` file or sqllogictest `slt` file.
    #[clap(short, long)]
    file: Option<String>,

    /// Whether to use in-memory engine
    #[clap(long)]
    memory: bool,
}

/// Run RisingLight interactive mode
async fn interactive(db: Database) -> Result<()> {
    let mut rl = Editor::<()>::new();
    let history_path = dirs::cache_dir().map(|p| {
        let cache_dir = p.join("risinglight");
        std::fs::create_dir_all(cache_dir.as_path()).ok();
        let history_path = cache_dir.join("history.txt");
        if !history_path.as_path().exists() {
            File::create(history_path.as_path()).ok();
        }
        history_path.into_boxed_path()
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

    Ok(())
}

/// Run a SQL file in RisingLight
async fn run_sql(db: Database, path: &str) -> Result<()> {
    let lines = std::fs::read_to_string(path)?;

    info!("{}", lines);
    let chunks = db.run(&lines).await?;
    for chunk in chunks {
        println!("{}", chunk);
    }

    Ok(())
}

/// Wrapper for sqllogictest
struct DatabaseWrapper {
    tx: tokio::sync::mpsc::Sender<String>,
    rx: Mutex<tokio::sync::mpsc::Receiver<Result<Vec<DataChunk>, risinglight::Error>>>,
}

impl sqllogictest::DB for DatabaseWrapper {
    type Error = risinglight::Error;
    fn run(&self, sql: &str) -> Result<String, Self::Error> {
        info!("{}", sql);
        self.tx.blocking_send(sql.to_string()).unwrap();
        let chunks = self.rx.lock().unwrap().blocking_recv().unwrap()?;
        for chunk in &chunks {
            println!("{:?}", chunk);
        }
        let output = chunks
            .iter()
            .map(datachunk_to_sqllogictest_string)
            .collect();
        Ok(output)
    }
}

/// Run a sqllogictest file in RisingLight
async fn run_sqllogictest(db: Database, path: &str) -> Result<()> {
    let (ttx, mut trx) = tokio::sync::mpsc::channel(1);
    let (dtx, drx) = tokio::sync::mpsc::channel(1);
    let mut tester = sqllogictest::Runner::new(DatabaseWrapper {
        tx: ttx,
        rx: Mutex::new(drx),
    });
    let handle = tokio::spawn(async move {
        while let Some(sql) = trx.recv().await {
            dtx.send(db.run(&sql).await).await.unwrap();
        }
    });

    let path = path.to_string();
    let sqllogictest_handler = tokio::task::spawn_blocking(move || {
        // `ParseError` isn't Send, so we cannot directly use it as anyhow Error.
        tester.run_file(path).map_err(|err| anyhow!("{:?}", err))?;
        Ok::<_, anyhow::Error>(())
    });

    sqllogictest_handler.await.unwrap().unwrap();
    handle.await.unwrap();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let fmt_layer = tracing_subscriber::fmt::layer().compact();
    let filter_layer =
        filter::EnvFilter::from_default_env().add_directive(LevelFilter::INFO.into());

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();

    let db = if args.memory {
        info!("using memory engine");
        Database::new_in_memory()
    } else {
        info!("using Secondary engine");
        Database::new_on_disk(SecondaryStorageOptions::default_for_cli()).await
    };

    if let Some(file) = args.file {
        if file.ends_with(".sql") {
            run_sql(db, &file).await?;
        } else if file.ends_with(".slt") {
            run_sqllogictest(db, &file).await?;
        } else {
            warn!("No suffix detected, assume sql file");
            run_sql(db, &file).await?;
        }
    } else {
        interactive(db).await?;
    }

    Ok(())
}
