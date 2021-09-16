use crate::{
    binder::{Bind, BindError, Binder},
    catalog::RootCatalogRef,
    executor::{ExecutorBuilder, ExecutorError, GlobalEnv, GlobalEnvRef},
    logical_plan::{LogicalPlanError, LogicalPlanGenerator},
    parser::{ParseError, SQLStatement},
    physical_plan::{PhysicalPlanError, PhysicalPlanGenerator},
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
        let mut stmts = SQLStatement::parse(sql)?;
        // bind
        let mut binder = Binder::new(self.catalog.clone());
        for stmt in stmts.iter_mut() {
            stmt.bind(&mut binder)?;
        }
        let logical_planner = LogicalPlanGenerator::new();
        let physical_planner = PhysicalPlanGenerator::new();
        let executor_builder = ExecutorBuilder::new(self.env.clone());
        // TODO: parallelize
        for stmt in stmts.iter() {
            let logical_plan = logical_planner.generate_logical_plan(stmt)?;
            let physical_plan = physical_planner.generate_physical_plan(&logical_plan)?;
            let executor = executor_builder.build(physical_plan)?;
            futures::executor::block_on(executor).unwrap();
        }
        Ok(())
    }
}

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum Error {
    #[error("parse error: {0}")]
    Parse(#[from] ParseError),
    #[error("bind error: {0}")]
    Bind(#[from] BindError),
    #[error("logical plan error: {0}")]
    LogicalPlan(#[from] LogicalPlanError),
    #[error("physical plan error: {0}")]
    PhysicalPlan(#[from] PhysicalPlanError),
    #[error("execute error: {0}")]
    Execute(#[from] ExecutorError),
}
