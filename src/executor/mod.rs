use crate::array::DataChunk;
use crate::physical_planner::PhysicalPlan;
use crate::storage::{StorageError, StorageRef};
use async_stream::try_stream;
use futures::stream::{BoxStream, Stream, StreamExt};
use std::sync::Arc;

mod aggregation;
mod create;
mod drop;
mod dummy_scan;
mod evaluator;
mod filter;
mod insert;
mod project;
mod seq_scan;

use self::create::*;
use self::drop::*;
use self::dummy_scan::*;
use self::filter::*;
use self::insert::*;
use self::project::*;
use self::seq_scan::*;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum ExecutorError {
    #[error("failed to build executors from the physical plan")]
    BuildingPlanError,
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("convert error: {0}")]
    Convert(#[from] evaluator::ConvertError),
}

pub type GlobalEnvRef = Arc<GlobalEnv>;

type BoxedExecutor = BoxStream<'static, Result<DataChunk, ExecutorError>>;

/// The global environment for task execution.
/// The instance will be shared by every task.
#[derive(Clone)]
pub struct GlobalEnv {
    pub storage: StorageRef,
}

pub struct ExecutorBuilder {
    env: GlobalEnvRef,
}

impl ExecutorBuilder {
    pub fn new(env: GlobalEnvRef) -> ExecutorBuilder {
        ExecutorBuilder { env }
    }

    pub fn build(&self, plan: PhysicalPlan) -> BoxedExecutor {
        match plan {
            PhysicalPlan::Dummy => DummyScanExecutor.execute().boxed(),
            PhysicalPlan::CreateTable(plan) => CreateTableExecutor {
                plan,
                storage: self.env.storage.clone(),
            }
            .execute()
            .boxed(),
            PhysicalPlan::Drop(plan) => DropExecutor {
                plan,
                storage: self.env.storage.clone(),
            }
            .execute()
            .boxed(),
            PhysicalPlan::Insert(plan) => InsertExecutor {
                plan,
                storage: self.env.storage.clone(),
            }
            .execute()
            .boxed(),

            PhysicalPlan::Projection(plan) => ProjectionExecutor {
                project_expressions: plan.project_expressions,
                child: self.build(*plan.child),
            }
            .execute()
            .boxed(),
            PhysicalPlan::SeqScan(plan) => SeqScanExecutor {
                plan,
                storage: self.env.storage.clone(),
            }
            .execute()
            .boxed(),
            PhysicalPlan::Filter(plan) => FilterExecutor {
                expr: plan.expr,
                child: self.build(*plan.child),
            }
            .execute()
            .boxed(),
        }
    }
}
