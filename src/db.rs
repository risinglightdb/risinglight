// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use futures::TryStreamExt;
use minitrace::collector::SpanContext;
use minitrace::Span;
use risinglight_proto::rowset::block_statistics::BlockStatisticsType;

use crate::array::Chunk;
use crate::binder::bind_header;
use crate::catalog::{RootCatalog, RootCatalogRef, TableRefId};
use crate::parser::{parse, ParserError, Statement};
use crate::planner::Statistics;
use crate::storage::{
    InMemoryStorage, SecondaryStorage, SecondaryStorageOptions, Storage, StorageColumnRef,
    StorageImpl, Table,
};

/// The database instance.
pub struct Database {
    catalog: RootCatalogRef,
    storage: StorageImpl,
    config: Mutex<Config>,
}

/// The configuration of the database.
#[derive(Debug, Default)]
struct Config {
    disable_optimizer: bool,
    mock_stat: Option<Statistics>,
}

impl Database {
    /// Create a new in-memory database instance.
    pub fn new_in_memory() -> Self {
        let storage = InMemoryStorage::new();
        Database {
            catalog: storage.catalog().clone(),
            storage: StorageImpl::InMemoryStorage(Arc::new(storage)),
            config: Default::default(),
        }
    }

    /// Create a new database instance with merge-tree engine.
    pub async fn new_on_disk(options: SecondaryStorageOptions) -> Self {
        let storage = Arc::new(SecondaryStorage::open(options).await.unwrap());
        storage.spawn_compactor().await;
        Database {
            catalog: storage.catalog().clone(),
            storage: StorageImpl::SecondaryStorage(storage),
            config: Default::default(),
        }
    }

    pub async fn shutdown(&self) -> Result<(), Error> {
        if let StorageImpl::SecondaryStorage(storage) = &self.storage {
            storage.shutdown().await?;
        }
        Ok(())
    }

    /// Convert a command to SQL.
    fn command_to_sql(&self, cmd: &str) -> Result<String, Error> {
        let tokens = cmd.split_whitespace().collect::<Vec<_>>();
        Ok(match tokens.as_slice() {
            ["dt"] => "SELECT * FROM pg_catalog.pg_tables".to_string(),
            ["d", table] => format!(
                "SELECT * FROM pg_catalog.pg_attribute WHERE table_name = '{table}'",
            ),
            ["stat"] => "SELECT * FROM pg_catalog.pg_stat".to_string(),
            ["stat", table] => format!("SELECT * FROM pg_catalog.pg_stat WHERE table_name = '{table}'"),
            ["stat", table, column] => format!(
                "SELECT * FROM pg_catalog.pg_stat WHERE table_name = '{table}' AND column_name = '{column}'",
            ),
            _ => return Err(Error::Internal("invalid command".into())),
        })
    }

    /// Run SQL queries and return the outputs.
    pub async fn run(&self, sql: &str) -> Result<Vec<Chunk>, Error> {
        let _root = Span::root("run_sql", SpanContext::random());

        let sql = if let Some(cmd) = sql.trim().strip_prefix('\\') {
            self.command_to_sql(cmd)?
        } else {
            sql.to_string()
        };

        let optimizer = crate::planner::Optimizer::new(
            self.catalog.clone(),
            self.get_storage_statistics().await?,
            crate::planner::Config {
                enable_range_filter_scan: self.storage.support_range_filter_scan(),
                table_is_sorted_by_primary_key: self.storage.table_is_sorted_by_primary_key(),
            },
        );

        let stmts = parse(&sql)?;
        let mut outputs: Vec<Chunk> = vec![];
        for stmt in stmts {
            if self.handle_set(&stmt)? {
                continue;
            }

            let mut binder = crate::binder::Binder::new(self.catalog.clone());
            let mut plan = binder.bind(stmt.clone())?;
            if !self.config.lock().unwrap().disable_optimizer {
                plan = optimizer.optimize(plan);
            }
            let executor = match self.storage.clone() {
                StorageImpl::InMemoryStorage(s) => {
                    crate::executor::build(optimizer.clone(), s, &plan)
                }
                StorageImpl::SecondaryStorage(s) => {
                    crate::executor::build(optimizer.clone(), s, &plan)
                }
            };
            let output = executor.try_collect().await?;
            let mut chunk = Chunk::new(output);
            chunk = bind_header(chunk, &stmt);
            outputs.push(chunk);
        }
        Ok(outputs)
    }

    async fn get_storage_statistics(&self) -> Result<Statistics, Error> {
        if let Some(mock) = &self.config.lock().unwrap().mock_stat {
            return Ok(mock.clone());
        }
        let mut stat = Statistics::default();
        // only secondary storage supports statistics
        let StorageImpl::SecondaryStorage(storage) = self.storage.clone() else {
            return Ok(stat);
        };
        for schema in self.catalog.all_schemas().values() {
            // skip internal schema
            if schema.name() == RootCatalog::SYSTEM_SCHEMA_NAME {
                continue;
            }
            for table in schema.all_tables().values() {
                if table.is_view() {
                    continue;
                }
                let table_id = TableRefId::new(schema.id(), table.id());
                let table = storage.get_table(table_id)?;
                let txn = table.read().await?;
                let values = txn.aggreagate_block_stat(&[(
                    BlockStatisticsType::RowCount,
                    StorageColumnRef::Idx(0),
                )]);
                stat.add_row_count(table_id, values[0].as_usize().unwrap().unwrap() as u32);
            }
        }
        Ok(stat)
    }

    /// Mock the row count of a table for planner test.
    fn handle_set(&self, stmt: &Statement) -> Result<bool, Error> {
        if let Statement::Pragma { name, .. } = stmt {
            match name.to_string().as_str() {
                "enable_optimizer" => {
                    self.config.lock().unwrap().disable_optimizer = false;
                    return Ok(true);
                }
                "disable_optimizer" => {
                    self.config.lock().unwrap().disable_optimizer = true;
                    return Ok(true);
                }
                name => {
                    return Err(crate::binder::BindError::NoPragma(name.into()).into());
                }
            }
        }
        let Statement::SetVariable {
            variable, value, ..
        } = stmt
        else {
            return Ok(false);
        };
        let Some(table_name) = variable.0[0].value.strip_prefix("mock_rowcount_") else {
            return Ok(false);
        };
        let count = value[0]
            .to_string()
            .parse::<u32>()
            .map_err(|_| Error::Internal("invalid count".into()))?;
        let table_id = self
            .catalog
            .get_table_id_by_name("postgres", table_name)
            .ok_or_else(|| Error::Internal("table not found".into()))?;
        self.config
            .lock()
            .unwrap()
            .mock_stat
            .get_or_insert_with(Default::default)
            .add_row_count(table_id, count);
        Ok(true)
    }

    /// Return all available pragma options.
    fn pragma_options() -> &'static [&'static str] {
        &["enable_optimizer", "disable_optimizer"]
    }
}

/// The error type of database operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("parse error: {0}")]
    Parse(
        #[source]
        #[from]
        ParserError,
    ),
    #[error("bind error: {0}")]
    Bind(
        #[source]
        #[from]
        crate::binder::BindError,
    ),
    #[error("execute error: {0}")]
    Execute(
        #[source]
        #[from]
        crate::executor::ExecutorError,
    ),
    #[error("Storage error: {0}")]
    Storage(
        #[source]
        #[from]
        #[backtrace]
        crate::storage::TracedStorageError,
    ),
    #[error("Internal error: {0}")]
    Internal(String),
}

impl rustyline::Helper for &Database {}
impl rustyline::validate::Validator for &Database {}
impl rustyline::highlight::Highlighter for &Database {}
impl rustyline::hint::Hinter for &Database {
    type Hint = String;
}

/// Implement SQL completion.
impl rustyline::completion::Completer for &Database {
    type Candidate = rustyline::completion::Pair;
    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Self::Candidate>)> {
        // find the word before cursor
        let (prefix, last_word) = line[..pos].rsplit_once(' ').unwrap_or(("", &line[..pos]));

        // completion for pragma options
        if prefix.trim().eq_ignore_ascii_case("pragma") {
            let candidates = Database::pragma_options()
                .iter()
                .filter(|option| option.starts_with(last_word))
                .map(|option| rustyline::completion::Pair {
                    display: option.to_string(),
                    replacement: option.to_string(),
                })
                .collect();
            return Ok((pos - last_word.len(), candidates));
        }

        // TODO: complete table and column names

        // completion for keywords

        // for a given prefix, all keywords starting with the prefix are returned as candidates
        // they should be ordered in principle that frequently used ones come first
        const KEYWORDS: &[&str] = &[
            "AS", "ALL", "ANALYZE", "CREATE", "COPY", "DELETE", "DROP", "EXPLAIN", "FROM",
            "FUNCTION", "INSERT", "JOIN", "ON", "PRAGMA", "SET", "SELECT", "TABLE", "UNION",
            "VIEW", "WHERE", "WITH",
        ];
        let last_word_upper = last_word.to_uppercase();
        let candidates = KEYWORDS
            .iter()
            .filter(|command| command.starts_with(&last_word_upper))
            .map(|command| rustyline::completion::Pair {
                display: command.to_string(),
                replacement: format!("{command} "),
            })
            .collect();
        Ok((pos - last_word.len(), candidates))
    }
}

#[cfg(test)]
mod tests {
    use rustyline::history::DefaultHistory;

    use super::*;

    #[test]
    fn test_completion() {
        let db = Database::new_in_memory();
        assert_complete(&db, "sel", "SELECT ");
        assert_complete(&db, "sel|ect", "SELECT |ect");
        assert_complete(&db, "select a f", "select a FROM ");
        assert_complete(&db, "pragma en", "pragma enable_optimizer");
    }

    /// Assert that if complete (e.g. press tab) the given `line`, the result will be
    /// `completed_line`.
    ///
    /// Both `line` and `completed_line` can optionally contain a `|` which indicates the cursor
    /// position. If not provided, the cursor is assumed to be at the end of the line.
    #[track_caller]
    fn assert_complete(db: &Database, line: &str, completed_line: &str) {
        /// Find cursor position and remove it from the line.
        fn get_line_and_cursor(line: &str) -> (String, usize) {
            let (before_cursor, after_cursor) = line.split_once('|').unwrap_or((line, ""));
            let pos = before_cursor.len();
            (format!("{before_cursor}{after_cursor}"), pos)
        }
        let (mut line, pos) = get_line_and_cursor(line);

        // complete
        use rustyline::completion::Completer;
        let (start_pos, candidates) = db
            .complete(&line, pos, &rustyline::Context::new(&DefaultHistory::new()))
            .unwrap();
        let replacement = &candidates[0].replacement;
        line.replace_range(start_pos..pos, replacement);

        // assert
        let (completed_line, completed_cursor_pos) = get_line_and_cursor(completed_line);
        assert_eq!(line, completed_line);
        assert_eq!(start_pos + replacement.len(), completed_cursor_pos);
    }
}
