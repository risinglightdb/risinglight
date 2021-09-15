use crate::physical_plan::PhysicalPlan;
use crate::server::GlobalEnvRef;
use crate::storage::StorageError;
use futures::FutureExt;
use std::future::Future;
use std::pin::Pin;

mod create;
mod evaluator;
mod insert;

pub use self::create::*;
pub use self::insert::*;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum ExecutorError {
    #[error("failed to initialize the executor")]
    InitializationError,
    #[error("failed to build executors from the physical plan")]
    BuildingPlanError,
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

pub type BoxedExecutor = Pin<Box<dyn Future<Output = Result<(), ExecutorError>>>>;

pub struct ExecutorBuilder {
    env: GlobalEnvRef,
}

impl ExecutorBuilder {
    pub fn new(env: GlobalEnvRef) -> ExecutorBuilder {
        ExecutorBuilder { env }
    }

    pub fn build(&self, plan: PhysicalPlan) -> Result<BoxedExecutor, ExecutorError> {
        match plan {
            PhysicalPlan::CreateTable(plan) => Ok(CreateTableExecutor {
                plan,
                env: self.env.clone(),
            }
            .execute()
            .boxed()),
            PhysicalPlan::Insert(plan) => Ok(InsertExecutor {
                plan,
                storage: self.env.storage.clone(),
            }
            .execute()
            .boxed()),
            _ => todo!("execute physical plan"),
        }
    }
}
