use crate::array::DataChunk;
use crate::physical_plan::PhysicalPlan;
use crate::storage::{StorageError, StorageRef};
use std::future::Future;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

mod create;
mod dummy_scan;
mod evaluator;
mod insert;
mod project;
mod seq_scan;

use self::create::*;
use self::dummy_scan::*;
use self::insert::*;
use self::project::*;
use self::seq_scan::*;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum ExecutorError {
    #[error("failed to build executors from the physical plan")]
    BuildingPlanError,
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

pub type GlobalEnvRef = Arc<GlobalEnv>;

/// The global environment for task execution.
/// The instance will be shared by every task.
#[derive(Clone)]
pub struct GlobalEnv {
    pub storage: StorageRef,
}

pub struct ExecutionManager {
    env: GlobalEnvRef,
    runtime: Runtime,
}

impl ExecutionManager {
    pub fn new(env: GlobalEnvRef) -> ExecutionManager {
        ExecutionManager {
            env,
            runtime: tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap(),
        }
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }

    pub fn run(&self, plan: PhysicalPlan) -> mpsc::Receiver<DataChunk> {
        let (sender, recver) = mpsc::channel(1);
        match plan {
            PhysicalPlan::Dummy => self
                .runtime
                .spawn(DummyScanExecutor { output: sender }.execute()),
            PhysicalPlan::CreateTable(plan) => self.runtime.spawn(
                CreateTableExecutor {
                    plan,
                    storage: self.env.storage.clone(),
                    output: sender,
                }
                .execute(),
            ),
            PhysicalPlan::Insert(plan) => self.runtime.spawn(
                InsertExecutor {
                    plan,
                    storage: self.env.storage.clone(),
                    output: sender,
                }
                .execute(),
            ),
            PhysicalPlan::Projection(plan) => self.runtime.spawn(
                ProjectionExecutor {
                    project_expressions: plan.project_expressions,
                    child: self.run(*plan.child),
                    output: sender,
                }
                .execute(),
            ),
            PhysicalPlan::SeqScan(plan) => self.runtime.spawn(
                SeqScanExecutor {
                    plan,
                    storage: self.env.storage.clone(),
                    output: sender,
                }
                .execute(),
            ),
            _ => todo!("execute physical plan: {:?}", plan),
        };
        recver
    }
}
