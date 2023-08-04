// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use futures::TryStreamExt;
use risinglight_proto::rowset::block_statistics::BlockStatisticsType;

use crate::array::{
    ArrayBuilder, ArrayBuilderImpl, Chunk, DataChunk, I32ArrayBuilder, Utf8ArrayBuilder,
};
use crate::catalog::{RootCatalogRef, TableRefId, SYSTEM_SCHEMA_NAME};
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

    fn run_desc(&self, table_name: &str) -> Result<Vec<Chunk>, Error> {
        let mut column_id = I32ArrayBuilder::new();
        let mut column_name = Utf8ArrayBuilder::new();
        let mut column_type = Utf8ArrayBuilder::new();
        let mut column_is_null = Utf8ArrayBuilder::new();
        let mut column_is_primary = Utf8ArrayBuilder::new();
        let table_catalog = self.catalog.get_table_by_name(table_name).unwrap();

        let all_columns = table_catalog.all_columns();
        for (id, column) in &all_columns {
            let name = column.name();
            let data_type = column.datatype().kind().to_string().to_ascii_lowercase();
            let is_null = column.is_nullable();
            let is_primary = column.is_primary();

            column_id.push(Some(&(*id as i32)));
            column_name.push(Some(name));
            column_type.push(Some(&data_type));
            if is_null {
                column_is_null.push(Some("nullable"));
            } else {
                column_is_null.push(Some("not null"));
            }

            if is_primary {
                column_is_primary.push(Some("primary"));
            } else {
                column_is_primary.push(None);
            }
        }
        let vecs: Vec<ArrayBuilderImpl> = vec![
            column_id.into(),
            column_name.into(),
            column_type.into(),
            column_is_null.into(),
            column_is_primary.into(),
        ];
        Ok(vec![Chunk::new(vec![DataChunk::from_iter(
            vecs.into_iter(),
        )])])
    }

    fn run_dt(&self) -> Result<Vec<Chunk>, Error> {
        let mut schema_id_vec = I32ArrayBuilder::new();
        let mut schema_vec = Utf8ArrayBuilder::new();
        let mut table_id_vec = I32ArrayBuilder::new();
        let mut table_vec = Utf8ArrayBuilder::new();
        for (_, schema) in self.catalog.all_schemas() {
            for (_, table) in schema.all_tables() {
                schema_id_vec.push(Some(&(schema.id() as i32)));
                schema_vec.push(Some(&schema.name()));
                table_id_vec.push(Some(&(table.id() as i32)));
                table_vec.push(Some(&table.name()));
            }
        }
        let vecs: Vec<ArrayBuilderImpl> = vec![
            schema_id_vec.into(),
            schema_vec.into(),
            table_id_vec.into(),
            table_vec.into(),
        ];
        Ok(vec![Chunk::new(vec![DataChunk::from_iter(
            vecs.into_iter(),
        )])])
    }

    async fn run_internal(&self, cmd: &str) -> Result<Vec<Chunk>, Error> {
        if let Some((cmd, arg)) = cmd.split_once(' ') {
            if cmd == "stat" {
                if let StorageImpl::SecondaryStorage(ref storage) = self.storage {
                    let (table, col) = arg.split_once(' ').expect("failed to parse command");
                    let table_id = self
                        .catalog
                        .get_table_id_by_name("postgres", table)
                        .expect("table not found");
                    let col_id = self
                        .catalog
                        .get_table(&table_id)
                        .unwrap()
                        .get_column_id_by_name(col)
                        .expect("column not found");
                    let table = storage.get_table(table_id)?;
                    let txn = table.read().await?;
                    let row_count = txn.aggreagate_block_stat(&[
                        (
                            BlockStatisticsType::RowCount,
                            // Note that `col_id` is the column catalog id instead of storage
                            // column id. This should be fixed in the
                            // future.
                            StorageColumnRef::Idx(col_id),
                        ),
                        (
                            BlockStatisticsType::DistinctValue,
                            StorageColumnRef::Idx(col_id),
                        ),
                    ]);
                    let mut stat_name = Utf8ArrayBuilder::with_capacity(2);
                    let mut stat_value = Utf8ArrayBuilder::with_capacity(2);
                    stat_name.push(Some("RowCount"));
                    stat_value.push(Some(
                        row_count[0]
                            .as_usize()
                            .unwrap()
                            .unwrap()
                            .to_string()
                            .as_str(),
                    ));
                    stat_name.push(Some("DistinctValue"));
                    stat_value.push(Some(
                        row_count[1]
                            .as_usize()
                            .unwrap()
                            .unwrap()
                            .to_string()
                            .as_str(),
                    ));
                    Ok(vec![Chunk::new(vec![DataChunk::from_iter([
                        ArrayBuilderImpl::from(stat_name),
                        ArrayBuilderImpl::from(stat_value),
                    ])])])
                } else {
                    Err(Error::Internal(
                        "this storage engine doesn't support statistics".to_string(),
                    ))
                }
            } else if cmd == "d" {
                self.run_desc(arg)
            } else {
                Err(Error::Internal("unsupported command".to_string()))
            }
        } else if cmd == "dt" {
            self.run_dt()
        } else {
            Err(Error::Internal("unsupported command".to_string()))
        }
    }

    /// Run SQL queries and return the outputs.
    pub async fn run(&self, sql: &str) -> Result<Vec<Chunk>, Error> {
        if let Some(cmdline) = sql.trim().strip_prefix('\\') {
            return self.run_internal(cmdline).await;
        }

        let optimizer = crate::planner::Optimizer::new(
            self.catalog.clone(),
            self.get_storage_statistics().await?,
            crate::planner::Config {
                enable_range_filter_scan: self.storage.support_range_filter_scan(),
                table_is_sorted_by_primary_key: self.storage.table_is_sorted_by_primary_key(),
            },
        );

        let stmts = parse(sql)?;
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
            if schema.name() == SYSTEM_SCHEMA_NAME {
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
