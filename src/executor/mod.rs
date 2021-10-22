//!
//!
//! # Execution Model
//!
//! The execution engine executes the query in a Vectorized Volcano model.
//!
//! # Async Stream
//!
//! Each executor is an async-stream that produces a stream of values asynchronously.
//!
//! To write async-stream in Rust, we use the [`try_stream`] macro from [`async_stream`] crate.
//!
//! [`try_stream`]: async_stream::try_stream

use crate::array::DataChunk;
use crate::physical_planner::{PhysicalJoinType, PhysicalPlan};
use crate::storage::{Storage, StorageError, StorageImpl};
use async_stream::try_stream;
use futures::stream::{BoxStream, Stream, StreamExt};
use std::sync::Arc;

mod aggregation;
mod create;
mod drop;
mod dummy_scan;
mod evaluator;
mod explain;
mod filter;
mod insert;
mod nested_loop_join;
mod projection;
mod seq_scan;

use self::create::*;
use self::drop::*;
use self::dummy_scan::*;
use self::explain::*;
use self::filter::*;
use self::insert::*;
use self::nested_loop_join::*;
use self::projection::*;
use self::seq_scan::*;

/// The error type of execution.
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum ExecutorError {
    #[error("failed to build executors from the physical plan")]
    BuildingPlanError,
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("convert error: {0}")]
    Convert(#[from] evaluator::ConvertError),
}

/// Reference type of the global environment.
pub type GlobalEnvRef = Arc<GlobalEnv>;

/// A type-erased executor object.
///
/// Logically an executor is a stream of data chunks.
///
/// It consumes one or more streams from its child executors,
/// and produces a stream to its parent.
pub type BoxedExecutor = BoxStream<'static, Result<DataChunk, ExecutorError>>;

/// The global environment for task execution.
/// The instance will be shared by every task.
#[derive(Clone)]
pub struct GlobalEnv {
    pub storage: StorageImpl,
}

/// The builder of executor.
pub struct ExecutorBuilder {
    env: GlobalEnvRef,
}

impl ExecutorBuilder {
    /// Create a new executor builder.
    pub fn new(env: GlobalEnvRef) -> ExecutorBuilder {
        ExecutorBuilder { env }
    }

    /// Build executor from a physical plan with given concrete [`Storage`] type.
    fn build_with_storage(&self, plan: PhysicalPlan, storage: Arc<impl Storage>) -> BoxedExecutor {
        match plan {
            PhysicalPlan::Dummy => DummyScanExecutor.execute().boxed(),
            PhysicalPlan::CreateTable(plan) => {
                CreateTableExecutor { plan, storage }.execute().boxed()
            }
            PhysicalPlan::Drop(plan) => DropExecutor { plan, storage }.execute().boxed(),
            PhysicalPlan::Insert(plan) => InsertExecutor { plan, storage }.execute().boxed(),
            PhysicalPlan::Projection(plan) => ProjectionExecutor {
                project_expressions: plan.project_expressions,
                child: self.build_with_storage(*plan.child, storage),
            }
            .execute()
            .boxed(),
            PhysicalPlan::SeqScan(plan) => SeqScanExecutor { plan, storage }.execute().boxed(),
            PhysicalPlan::Filter(plan) => FilterExecutor {
                expr: plan.expr,
                child: self.build_with_storage(*plan.child, storage),
            }
            .execute()
            .boxed(),
            PhysicalPlan::Explain(plan) => ExplainExecutor { plan }.execute().boxed(),
            PhysicalPlan::Join(plan) => NestedLoopJoinExecutor {
                left_child: self.build_with_storage(*plan.left_plan, storage.clone()),
                right_child: self.build_with_storage(*plan.right_plan, storage.clone()),
                join_op: plan.join_op.clone(),
            }
            .execute()
            .boxed(),
        }
    }

    pub fn build(&self, plan: PhysicalPlan) -> BoxedExecutor {
        use StorageImpl::*;
        match self.env.storage.clone() {
            InMemoryStorage(storage) => self.build_with_storage(plan, storage),
            SecondaryStorage(storage) => self.build_with_storage(plan, storage),
        }
    }
}
