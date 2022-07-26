// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

//! A simple interactive shell of the database.

#![feature(div_duration)]

use std::fs::File;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use clap::Parser;
use humantime::format_duration;
use minitrace::prelude::*;
use risinglight::array::{datachunk_to_sqllogictest_string, Chunk};
use risinglight::executor::context::Context;
use risinglight::storage::SecondaryStorageOptions;
use risinglight::utils::time::RoundingDuration;
use risinglight::Database;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use tokio::{select, signal};
use tracing::{info, warn, Level};
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

    /// Control the output format
    /// - `text`: plain text
    /// - `human`: human readable format
    #[clap(long)]
    output_format: Option<String>,

    /// Whether to use minitrace
    #[clap(long)]
    enable_tracing: bool,
}

// human-readable message
fn print_chunk(chunk: &Chunk, output_format: &Option<String>) {
    let output_format = output_format.as_ref().map(|x| x.as_str());
    match output_format {
        Some("human") | None => match chunk.header() {
            Some(header) => match header[0].as_str() {
                "$insert.row_counts" => {
                    println!(
                        "{} rows inserted",
                        chunk.get_first_data_chunk().array_at(0).get_to_string(0)
                    )
                }
                "$delete.row_counts" => {
                    println!(
                        "{} rows deleted",
                        chunk.get_first_data_chunk().array_at(0).get_to_string(0)
                    )
                }
                "$create" => println!("created"),
                "$drop" => println!("dropped"),
                "$explain" => println!(
                    "{}",
                    chunk.get_first_data_chunk().array_at(0).get_to_string(0)
                ),
                _ => println!("{}", chunk),
            },
            None => println!("{}", chunk),
        },
        Some("text") => println!("{}", datachunk_to_sqllogictest_string(chunk)),
        Some(format) => panic!("unsupported output format: {}", format),
    }
}

fn print_execution_time(start_time: Instant) {
    let duration = start_time.elapsed();
    let duration_in_seconds = duration.div_duration_f64(Duration::new(1, 0));
    if duration_in_seconds > 1.0 {
        println!(
            "in {:.3}s ({})",
            duration_in_seconds,
            format_duration(duration.round_to_seconds())
        );
    } else {
        println!("in {:.3}s", duration_in_seconds);
    }
}

async fn run_query_in_background(
    db: Arc<Database>,
    sql: String,
    output_format: Option<String>,
    enable_tracing: bool,
) {
    let context: Arc<Context> = Default::default();
    let start_time = Instant::now();
    let handle = tokio::spawn({
        let context = context.clone();
        async move {
            if enable_tracing {
                let (root, collector) = Span::root("root");
                let result = db.run_with_context(context, &sql).in_span(root).await;
                let records: Vec<SpanRecord> = collector.collect().await;
                println!("{records:#?}");
                result
            } else {
                db.run_with_context(context, &sql).await
            }
        }
    });

    select! {
        _ = signal::ctrl_c() => {
            context.cancel();
            println!("Interrupted");
        }
        ret = handle => {
            match ret.expect("failed to join query thread") {
                Ok(chunks) => {
                    for chunk in chunks {
                        print_chunk(&chunk, &output_format)
                    }

                    print_execution_time(start_time)
                }
                Err(err) => println!("{}", err),
            }
        }
    }

    // Wait detached tasks if cancelled, or do nothing if query ends.
    // Leak is guaranteed not to happen as long as all handles are joined
    // and errors in detached tasks are properly handled.
    context.wait().await;
}

/// Read line by line from STDIN until a line ending with `;`.
///
/// Note that `;` in string literals will also be treated as a terminator
/// as long as it is at the end of a line.
fn read_sql(rl: &mut Editor<()>) -> Result<String, ReadlineError> {
    let mut sql = String::new();
    loop {
        let prompt = if sql.is_empty() { "> " } else { "? " };
        let line = rl.readline(prompt)?;
        if line.is_empty() {
            continue;
        }

        // internal commands starts with "\"
        if line.starts_with('\\') && sql.is_empty() {
            return Ok(line);
        }

        sql.push_str(line.as_str());
        if line.ends_with(';') {
            return Ok(sql);
        } else {
            sql.push('\n');
        }
    }
}

/// Run RisingLight interactive mode
async fn interactive(
    db: Database,
    output_format: Option<String>,
    enable_tracing: bool,
) -> Result<()> {
    let mut rl = Editor::<()>::new()?;
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

    let db = Arc::new(db);

    loop {
        let read_sql = read_sql(&mut rl);
        match read_sql {
            Ok(sql) => {
                if !sql.trim().is_empty() {
                    rl.add_history_entry(sql.as_str());
                    run_query_in_background(db.clone(), sql, output_format.clone(), enable_tracing)
                        .await;
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
async fn run_sql(
    db: Database,
    path: &str,
    output_format: Option<String>,
    enable_tracing: bool,
) -> Result<()> {
    let lines = std::fs::read_to_string(path)?;

    info!("{}", lines);

    let chunks = if enable_tracing {
        let (root, collector) = Span::root("root");
        let chunk = db.run(&lines).in_span(root).await?;
        let records: Vec<SpanRecord> = collector.collect().await;
        println!("{records:#?}");
        chunk
    } else {
        db.run(&lines).await?
    };

    for chunk in chunks {
        print_chunk(&chunk, &output_format);
    }

    Ok(())
}

/// Wrapper for sqllogictest
struct DatabaseWrapper {
    db: Database,
    output_format: Option<String>,
    enable_tracing: bool,
}

#[async_trait]
impl sqllogictest::AsyncDB for DatabaseWrapper {
    type Error = risinglight::Error;
    async fn run(&mut self, sql: &str) -> Result<String, Self::Error> {
        info!("{}", sql);
        let chunks = if self.enable_tracing {
            let (root, collector) = Span::root("root");
            let chunk = self.db.run(sql).in_span(root).await?;
            let records: Vec<SpanRecord> = collector.collect().await;
            println!("{records:#?}");
            chunk
        } else {
            self.db.run(sql).await?
        };

        for chunk in &chunks {
            print_chunk(chunk, &self.output_format);
        }
        Ok(chunks
            .iter()
            .map(datachunk_to_sqllogictest_string)
            .collect())
    }
}

/// Run a sqllogictest file in RisingLight
async fn run_sqllogictest(
    db: Database,
    path: &str,
    output_format: Option<String>,
    enable_tracing: bool,
) -> Result<()> {
    let mut tester = sqllogictest::Runner::new(DatabaseWrapper {
        db,
        output_format,
        enable_tracing,
    });
    let path = path.to_string();

    tester
        .run_file_async(path)
        .await
        // `ParseError` isn't Send, so we cannot directly use it as anyhow Error.
        .map_err(|err| anyhow!("{:?}", err))?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let fmt_layer = tracing_subscriber::fmt::layer().compact();
    let filter_layer =
        tracing_subscriber::EnvFilter::from_default_env().add_directive(Level::INFO.into());

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
            run_sql(db, &file, args.output_format, args.enable_tracing).await?;
        } else if file.ends_with(".slt") {
            run_sqllogictest(db, &file, args.output_format, args.enable_tracing).await?;
        } else {
            warn!("No suffix detected, assume sql file");
            run_sql(db, &file, args.output_format, args.enable_tracing).await?;
        }
    } else {
        interactive(db, args.output_format, args.enable_tracing).await?;
    }

    Ok(())
}
