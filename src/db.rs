// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use futures::TryStreamExt;
use risinglight_proto::rowset::block_statistics::BlockStatisticsType;
use tracing::debug;

use crate::array::{ArrayBuilder, ArrayBuilderImpl, DataChunk, I32ArrayBuilder, Utf8ArrayBuilder};
use crate::binder::{BindError, Binder};
use crate::catalog::RootCatalogRef;
use crate::executor::{ExecutorBuilder, ExecutorError};
use crate::logical_planner::{LogicalPlanError, LogicalPlaner};
use crate::optimizer::logical_plan_rewriter::{InputRefResolver, PlanRewriter};
use crate::optimizer::plan_nodes::PlanRef;
use crate::optimizer::Optimizer;
use crate::parser::{parse, ParserError};
use crate::storage::{
    InMemoryStorage, SecondaryStorage, SecondaryStorageOptions, Storage, StorageColumnRef,
    StorageImpl, Table,
};

/// The database instance.
pub struct Database {
    catalog: RootCatalogRef,
    executor_builder: ExecutorBuilder,
    storage: StorageImpl,
}

impl Database {
    /// Create a new in-memory database instance.
    pub fn new_in_memory() -> Self {
        let storage = InMemoryStorage::new();
        let catalog = storage.catalog().clone();
        let storage = StorageImpl::InMemoryStorage(Arc::new(storage));
        let execution_manager = ExecutorBuilder::new(storage.clone());
        Database {
            catalog,
            executor_builder: execution_manager,
            storage,
        }
    }

    /// Create a new database instance with merge-tree engine.
    pub async fn new_on_disk(options: SecondaryStorageOptions) -> Self {
        let storage = Arc::new(SecondaryStorage::open(options).await.unwrap());
        storage.spawn_compactor().await;
        let catalog = storage.catalog().clone();
        let storage = StorageImpl::SecondaryStorage(storage);
        let execution_manager = ExecutorBuilder::new(storage.clone());
        Database {
            catalog,
            executor_builder: execution_manager,
            storage,
        }
    }

    pub async fn shutdown(&self) -> Result<(), Error> {
        if let StorageImpl::SecondaryStorage(storage) = &self.storage {
            storage.shutdown().await?;
        }
        Ok(())
    }

    fn run_dt(&self) -> Result<Vec<DataChunk>, Error> {
        let mut db_id_vec = I32ArrayBuilder::new();
        let mut db_vec = Utf8ArrayBuilder::new();
        let mut schema_id_vec = I32ArrayBuilder::new();
        let mut schema_vec = Utf8ArrayBuilder::new();
        let mut table_id_vec = I32ArrayBuilder::new();
        let mut table_vec = Utf8ArrayBuilder::new();
        for (_, database) in self.catalog.all_databases() {
            for (_, schema) in database.all_schemas() {
                for (_, table) in schema.all_tables() {
                    db_id_vec.push(Some(&(database.id() as i32)));
                    db_vec.push(Some(&database.name()));
                    schema_id_vec.push(Some(&(schema.id() as i32)));
                    schema_vec.push(Some(&schema.name()));
                    table_id_vec.push(Some(&(table.id() as i32)));
                    table_vec.push(Some(&table.name()));
                }
            }
        }
        let vecs: Vec<ArrayBuilderImpl> = vec![
            db_id_vec.into(),
            db_vec.into(),
            schema_id_vec.into(),
            schema_vec.into(),
            table_id_vec.into(),
            table_vec.into(),
        ];
        Ok(vec![DataChunk::from_iter(vecs.into_iter())])
    }

    pub async fn run_internal(&self, cmd: &str) -> Result<Vec<DataChunk>, Error> {
        if let Some((cmd, arg)) = cmd.split_once(' ') {
            if cmd == "stat" {
                if let StorageImpl::SecondaryStorage(ref storage) = self.storage {
                    let (table, col) = arg.split_once(' ').expect("failed to parse command");
                    let table_id = self
                        .catalog
                        .get_table_id_by_name("postgres", "postgres", table)
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
                    Ok(vec![DataChunk::from_iter([
                        ArrayBuilderImpl::from(stat_name),
                        ArrayBuilderImpl::from(stat_value),
                    ])])
                } else {
                    Err(Error::InternalError(
                        "this storage engine doesn't support statistics".to_string(),
                    ))
                }
            } else {
                Err(Error::InternalError("unsupported command".to_string()))
            }
        } else if cmd == "dt" {
            self.run_dt()
        } else {
            Err(Error::InternalError("unsupported command".to_string()))
        }
    }

    /// Run SQL queries and return the outputs.
    pub async fn run(&self, sql: &str) -> Result<Vec<DataChunk>, Error> {
        if let Some(cmdline) = sql.strip_prefix('\\') {
            return self.run_internal(cmdline).await;
        }

        // parse
        let stmts = parse(sql)?;

        let mut binder = Binder::new(self.catalog.clone());
        let logical_planner = LogicalPlaner::default();
        let mut optimizer = Optimizer {
            enable_filter_scan: self.storage.enable_filter_scan(),
        };
        // TODO: parallelize
        let mut outputs = vec![];
        for stmt in stmts {
            let stmt = binder.bind(&stmt)?;
            debug!("{:#?}", stmt);
            let logical_plan = logical_planner.plan(stmt)?;
            debug!("{:#?}", logical_plan);
            // Resolve input reference
            let mut input_ref_resolver = InputRefResolver::default();
            let logical_plan = input_ref_resolver.rewrite(logical_plan);
            let column_names = logical_plan.out_names();
            debug!("{:#?}", logical_plan);
            let optimized_plan = optimizer.optimize(logical_plan);
            debug!("{:#?}", optimized_plan);
            let executor = self.executor_builder.clone().build(optimized_plan);
            let mut output: Vec<DataChunk> = executor.try_collect().await.map_err(|e| {
                debug!("error: {}", e);
                e
            })?;
            for chunk in &output {
                debug!("output:\n{}", chunk);
            }
            if !column_names.is_empty() && !output.is_empty() {
                output[0].set_header(column_names);
            }
            outputs.extend(output);
        }
        Ok(outputs)
    }

    // Generate the execution plans for SQL queries.
    pub fn generate_execution_plan(&self, sql: &str) -> Result<Vec<PlanRef>, Error> {
        let stmts = parse(sql)?;

        let mut binder = Binder::new(self.catalog.clone());
        let logical_planner = LogicalPlaner::default();
        let mut optimizer = Optimizer {
            enable_filter_scan: self.storage.enable_filter_scan(),
        };
        let mut plans = vec![];
        for stmt in stmts {
            let stmt = binder.bind(&stmt)?;
            debug!("{:#?}", stmt);
            let logical_plan = logical_planner.plan(stmt)?;
            debug!("{:#?}", logical_plan);
            // Resolve input reference
            let mut input_ref_resolver = InputRefResolver::default();
            let logical_plan = input_ref_resolver.rewrite(logical_plan);
            debug!("{:#?}", logical_plan);
            let optimized_plan = optimizer.optimize(logical_plan);
            debug!("{:#?}", optimized_plan);
            plans.push(optimized_plan);
        }
        Ok(plans)
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
        BindError,
    ),
    #[error("logical plan error: {0}")]
    Plan(
        #[source]
        #[from]
        LogicalPlanError,
    ),
    #[error("execute error: {0}")]
    Execute(
        #[source]
        #[from]
        ExecutorError,
    ),
    #[error("Storage error: {0}")]
    StorageError(
        #[source]
        #[from]
        #[backtrace]
        crate::storage::TracedStorageError,
    ),
    #[error("Internal error: {0}")]
    InternalError(String),
}
