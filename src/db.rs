// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use futures::TryStreamExt;
use risinglight_proto::rowset::block_statistics::BlockStatisticsType;

use crate::array::Chunk;
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
    mock_stat: Mutex<Option<Statistics>>,
}

impl Database {
    /// Create a new in-memory database instance.
    pub fn new_in_memory() -> Self {
        let storage = InMemoryStorage::new();
        Database {
            catalog: storage.catalog().clone(),
            storage: StorageImpl::InMemoryStorage(Arc::new(storage)),
            mock_stat: Default::default(),
        }
    }

    /// Create a new database instance with merge-tree engine.
    pub async fn new_on_disk(options: SecondaryStorageOptions) -> Self {
        let storage = Arc::new(SecondaryStorage::open(options).await.unwrap());
        storage.spawn_compactor().await;
        Database {
            catalog: storage.catalog().clone(),
            storage: StorageImpl::SecondaryStorage(storage),
            mock_stat: Default::default(),
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
            let bound = binder.bind(stmt)?;
            let optimized = optimizer.optimize(bound);
            let executor = match self.storage.clone() {
                StorageImpl::InMemoryStorage(s) => {
                    crate::executor::build(optimizer.clone(), s, &optimized)
                }
                StorageImpl::SecondaryStorage(s) => {
                    crate::executor::build(optimizer.clone(), s, &optimized)
                }
            };
            let output = executor.try_collect().await?;
            let chunk = Chunk::new(output);
            // TODO: set name
            outputs.push(chunk);
        }
        Ok(outputs)
    }

    async fn get_storage_statistics(&self) -> Result<Statistics, Error> {
        if let Some(mock) = &*self.mock_stat.lock().unwrap() {
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
        let Statement::SetVariable { variable, value, .. } = stmt else {
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
        self.mock_stat
            .lock()
            .unwrap()
            .get_or_insert_with(Default::default)
            .add_row_count(table_id, count);
        Ok(true)
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
