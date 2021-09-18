use crate::{
    binder::{BindError, Binder},
    catalog::RootCatalogRef,
    executor::{ExecutionManager, ExecutorError, GlobalEnv},
    logical_plan::{LogicalPlanError, LogicalPlaner},
    parser::{parse, ParserError},
    physical_plan::{PhysicalPlanError, PhysicalPlaner},
    storage::InMemoryStorage,
};
use std::sync::Arc;

pub struct Database {
    catalog: RootCatalogRef,
    execution_manager: ExecutionManager,
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
        let execution_manager = ExecutionManager::new(env);
        Database {
            catalog,
            execution_manager,
        }
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
        // TODO: parallelize
        for stmt in stmts {
            let logical_plan = logical_planner.plan(stmt)?;
            let physical_plan = physical_planner.plan(logical_plan)?;
            let mut output = self.execution_manager.run(physical_plan);
            self.execution_manager.block_on(output.recv());
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
