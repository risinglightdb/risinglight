use crate::array::DataChunkRef;
use crate::physical_plan::PhysicalPlan;
use crate::server::GlobalEnv;

mod create;
pub use self::create::*;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum ExecutorError {
    #[error("failed to initialize the executor")]
    InitializationError,
    #[error("failed to build executors from the physical plan")]
    BuildingPlanError,
    #[error("failed to create table")]
    CreateTableError,
}

pub enum ExecutionResult {
    Chunk(DataChunkRef),
    Done,
}

pub type BoxedExecutor = Box<dyn Executor>;

pub trait Executor: Send {
    fn init(&mut self) -> Result<(), ExecutorError>;
    fn execute(&mut self, chunk: ExecutionResult) -> Result<ExecutionResult, ExecutorError>;
    fn finish(&mut self) -> Result<(), ExecutorError>;
}

pub struct ExecutorBuilder<'a> {
    plan_node: &'a PhysicalPlan,
    env: GlobalEnv,
}

impl<'a> ExecutorBuilder<'a> {
    pub fn new(plan_node: &'a PhysicalPlan, env: GlobalEnv) -> ExecutorBuilder {
        ExecutorBuilder { plan_node, env }
    }

    pub fn plan_node(&self) -> &PhysicalPlan {
        self.plan_node
    }

    pub fn global_task_env(&self) -> &GlobalEnv {
        &self.env
    }

    pub fn build_plan(&self) -> Result<BoxedExecutor, ExecutorError> {
        match self.plan_node {
            PhysicalPlan::CreateTable(create_plan) => Ok(Box::new(CreateTableExecutor::new(
                self.env.storage_mgr_ref.clone(),
                &create_plan.database_id,
                &create_plan.schema_id,
                &create_plan.table_name,
                &create_plan.column_descs,
            ))),
            _ => Err(ExecutorError::BuildingPlanError),
        }
    }
}
