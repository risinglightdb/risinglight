use crate::{
    binder::{BindError, Binder},
    catalog::RootCatalogRef,
    executor::{ExecutorBuilder, ExecutorError, GlobalEnv, GlobalEnvRef},
    logical_plan::{LogicalPlanError, LogicalPlaner},
    parser::{parse, ParserError},
    physical_plan::{PhysicalPlanError, PhysicalPlaner},
    storage::InMemoryStorage,
};
use std::sync::Arc;

pub struct Database {
    env: GlobalEnvRef,
    catalog: RootCatalogRef,
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

impl Database {
    /// Create a new Database instance.
    pub fn new() -> Self {
        let storage = InMemoryStorage::new();
        let catalog = storage.catalog().clone();
        let env = Arc::new(GlobalEnv {
            storage: Arc::new(storage),
        });
        Database { env, catalog }
    }

    /// Run SQL queries.
    pub fn run(&self, sql: &str) -> Result<(), Error> {
        // parse
        let stmts = parse(sql)?;
        // bind
        let mut binder = Binder::new(self.catalog.clone());
        let stmts = stmts
            .iter()
            .map(|s| binder.bind(s))
            .collect::<Result<Vec<_>, _>>()?;
        let logical_planner = LogicalPlaner::default();
        let physical_planner = PhysicalPlaner::default();
        let executor_builder = ExecutorBuilder::new(self.env.clone());
        // TODO: parallelize
        for stmt in stmts {
            let logical_plan = logical_planner.plan(stmt)?;
            let physical_plan = physical_planner.plan(logical_plan)?;
            let executor = executor_builder.build(physical_plan)?;
            futures::executor::block_on(executor).unwrap();
        }
        Ok(())
    }
}

#[derive(thiserror::Error, Debug, PartialEq)]
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
