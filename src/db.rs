use crate::{
    array::{ArrayBuilder, DataChunk, I32ArrayBuilder, Utf8ArrayBuilder},
    binder::{BindError, Binder},
    catalog::RootCatalogRef,
    executor::{ExecutorBuilder, ExecutorError, GlobalEnv},
    logical_optimizer::plan_rewriter::{input_ref_resolver::InputRefResolver, PlanRewriter},
    logical_optimizer::Optimizer,
    logical_planner::{LogicalPlanError, LogicalPlaner},
    parser::{parse, ParserError},
    physical_planner::{PhysicalPlanError, PhysicalPlaner},
    storage::{
        InMemoryStorage, SecondaryStorage, SecondaryStorageOptions, Storage, StorageColumnRef,
        StorageImpl, Table,
    },
};
use futures::TryStreamExt;
use risinglight_proto::rowset::block_statistics::BlockStatisticsType;
use std::sync::Arc;

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
        let env = Arc::new(GlobalEnv {
            storage: storage.clone(),
        });
        let execution_manager = ExecutorBuilder::new(env);
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
        let env = Arc::new(GlobalEnv {
            storage: storage.clone(),
        });
        let execution_manager = ExecutorBuilder::new(env);
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
        let vecs = vec![
            db_id_vec.finish().into(),
            db_vec.finish().into(),
            schema_id_vec.finish().into(),
            schema_vec.finish().into(),
            table_id_vec.finish().into(),
            table_vec.finish().into(),
        ];
        Ok(vec![DataChunk::from_iter(vecs.into_iter())])
    }

    pub async fn run_internal(&self, cmd: &str) -> Result<Vec<DataChunk>, Error> {
        if let Some((cmd, arg)) = cmd.split_once(" ") {
            if cmd == "stat" {
                if let StorageImpl::SecondaryStorage(ref storage) = self.storage {
                    let (table, col) = arg.split_once(" ").expect("failed to parse command");
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
                    stat_value.push(Some(&format!("{:?}", row_count[0])));
                    stat_name.push(Some("DistinctValue"));
                    stat_value.push(Some(&format!("{:?}", row_count[1])));
                    Ok(vec![DataChunk::from_iter([
                        stat_name.finish().into(),
                        stat_value.finish().into(),
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
        let mut optimizer = Optimizer::default();
        let physical_planner = PhysicalPlaner::default();
        // TODO: parallelize
        let mut outputs = vec![];
        for stmt in stmts {
            let stmt = binder.bind(&stmt)?;
            debug!("{:#?}", stmt);
            let logical_plan = logical_planner.plan(stmt)?;
            // Resolve input reference
            let logical_plan = InputRefResolver::default().rewrite_plan(logical_plan.into());
            debug!("{:#?}", logical_plan);
            let optimized_plan = optimizer.optimize(logical_plan);
            let physical_plan = physical_planner.plan(optimized_plan.as_ref().clone())?;
            debug!("{:#?}", physical_plan);
            let executor = self.executor_builder.build(physical_plan);
            let output: Vec<DataChunk> = executor.try_collect().await.map_err(|e| {
                debug!("error: {}", e);
                e
            })?;
            for chunk in &output {
                debug!("output:\n{}", chunk);
            }
            outputs.extend(output);
        }
        Ok(outputs)
    }
}

/// The error type of database operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("parse error: {0}")]
    Parse(#[from] ParserError),
    #[error("bind error: {0}")]
    Bind(#[from] BindError),
    #[error("logical plan error: {0}")]
    LogicalPlan(#[from] LogicalPlanError),
    #[error("physical plan error: {0}")]
    PhysicalPlan(#[from] PhysicalPlanError),
    #[error("execute error: {0}")]
    Execute(#[from] ExecutorError),
    #[error("Storage error: {0}")]
    StorageError(#[from] crate::storage::StorageError),
    #[error("Internal error: {0}")]
    InternalError(String),
}
