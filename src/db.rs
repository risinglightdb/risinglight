use crate::{
    array::DataChunk,
    binder::{BindError, Binder},
    catalog::RootCatalogRef,
    executor::{ExecutorBuilder, ExecutorError, GlobalEnv},
    logical_planner::{LogicalPlanError, LogicalPlaner},
    parser::{parse, ParserError},
    physical_planner::{PhysicalPlanError, PhysicalPlaner},
    storage::{InMemoryStorage, SecondaryStorage, SecondaryStorageOptions, StorageImpl},
};
use futures::TryStreamExt;
use std::sync::Arc;

/// The database instance.
pub struct Database {
    catalog: RootCatalogRef,
    executor_builder: ExecutorBuilder,
}

impl Database {
    /// Create a new in-memory database instance.
    pub fn new_in_memory() -> Self {
        let storage = InMemoryStorage::new();
        let catalog = storage.catalog().clone();
        let env = Arc::new(GlobalEnv {
            storage: StorageImpl::InMemoryStorage(Arc::new(storage)),
        });
        let execution_manager = ExecutorBuilder::new(env);
        Database {
            catalog,
            executor_builder: execution_manager,
        }
    }

    /// Create a new database instance with merge-tree engine.
    pub fn new_on_disk() -> Self {
        let storage = SecondaryStorage::new(SecondaryStorageOptions::default_for_test());
        let catalog = storage.catalog().clone();
        let env = Arc::new(GlobalEnv {
            storage: StorageImpl::SecondaryStorage(Arc::new(storage)),
        });
        let execution_manager = ExecutorBuilder::new(env);
        Database {
            catalog,
            executor_builder: execution_manager,
        }
    }

    /// Run SQL queries and return the outputs.
    pub async fn run(&self, sql: &str) -> Result<Vec<DataChunk>, Error> {
        // parse
        let stmts = parse(sql)?;

        let mut binder = Binder::new(self.catalog.clone());
        let logical_planner = LogicalPlaner::default();
        let physical_planner = PhysicalPlaner::default();
        // TODO: parallelize
        let mut outputs = vec![];
        for stmt in stmts {
            let stmt = binder.bind(&stmt)?;
            debug!("{:#?}", stmt);
            let logical_plan = logical_planner.plan(stmt)?;
            debug!("{:#?}", logical_plan);
            let physical_plan = physical_planner.plan(logical_plan)?;
            debug!("{:#?}", physical_plan);
            let executor = self.executor_builder.build(physical_plan);
            let output: Vec<DataChunk> = executor.try_collect().await.map_err(|e| {
                debug!("error: {}", e);
                e
            })?;
            for chunk in output.iter() {
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
}
