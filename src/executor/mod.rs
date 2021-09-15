use crate::physical_plan::PhysicalPlan;
use crate::server::GlobalEnvRef;
use futures::FutureExt;
use std::future::Future;
use std::pin::Pin;

mod create;
mod insert;

pub use self::create::*;
pub use self::insert::*;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum ExecutorError {
    #[error("failed to initialize the executor")]
    InitializationError,
    #[error("failed to build executors from the physical plan")]
    BuildingPlanError,
    #[error("failed to create table")]
    CreateTableError,
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
            _ => todo!("execute physical plan"),
        }
    }
}
