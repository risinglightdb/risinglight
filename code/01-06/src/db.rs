//! Top-level structure of the database.

use std::sync::Arc;

use crate::{
    array::DataChunk,
    binder::{BindError, Binder},
    catalog::{RootCatalog, RootCatalogRef},
    executor::{ExecuteError, ExecutorBuilder},
    logical_planner::{LogicalPlanError, LogicalPlaner},
    parser::{parse, ParserError},
    physical_planner::{PhysicalPlanError, PhysicalPlaner},
    storage::InMemoryStorage,
};

/// The database instance.
pub struct Database {
    catalog: RootCatalogRef,
    executor_builder: ExecutorBuilder,
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

impl Database {
    /// Create a new database instance.
    pub fn new() -> Self {
        let catalog = Arc::new(RootCatalog::new());
        let storage = Arc::new(InMemoryStorage::new());
        Database {
            catalog: catalog.clone(),
            executor_builder: ExecutorBuilder::new(catalog, storage),
        }
    }

    /// Run SQL queries and return the outputs.
    pub fn run(&self, sql: &str) -> Result<Vec<DataChunk>, Error> {
        // parse
        let stmts = parse(sql)?;

        let mut outputs = vec![];
        for stmt in stmts {
            let mut binder = Binder::new(self.catalog.clone());
            let logical_planner = LogicalPlaner::default();
            let physical_planner = PhysicalPlaner::default();

            let bound_stmt = binder.bind(&stmt)?;
            debug!("{:#?}", bound_stmt);
            let logical_plan = logical_planner.plan(bound_stmt)?;
            debug!("{:#?}", logical_plan);
            let physical_plan = physical_planner.plan(&logical_plan)?;
            debug!("{:#?}", physical_plan);
            let mut executor = self.executor_builder.build(physical_plan);
            let output = executor.execute()?;
            outputs.push(output);
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
    Execute(#[from] ExecuteError),
}
