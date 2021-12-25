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

use std::sync::Arc;

use futures::stream::{BoxStream, StreamExt};
use futures_async_stream::try_stream;

use crate::array::DataChunk;
use crate::optimizer::plan_nodes::*;
use crate::storage::{StorageError, StorageImpl};
use crate::types::ConvertError;

mod aggregation;
mod copy_from_file;
mod copy_to_file;
mod create;
mod delete;
mod drop;
mod dummy_scan;
pub mod evaluator;
mod explain;
mod filter;
mod hash_agg;
mod hash_join;
mod insert;
mod limit;
mod nested_loop_join;
mod order;
mod projection;
mod seq_scan;
mod simple_agg;
mod values;

pub use self::aggregation::*;
use self::copy_from_file::*;
use self::copy_to_file::*;
use self::create::*;
use self::delete::*;
use self::drop::*;
use self::dummy_scan::*;
use self::explain::*;
use self::filter::*;
use self::hash_agg::*;
use self::hash_join::*;
use self::insert::*;
use self::limit::*;
use self::nested_loop_join::*;
use self::order::*;
use self::projection::*;
use self::seq_scan::*;
use self::simple_agg::*;
use self::values::*;

/// The error type of execution.
#[derive(thiserror::Error, Debug)]
pub enum ExecutorError {
    #[error("failed to build executors from the physical plan")]
    BuildingPlanError,
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("conversion error: {0}")]
    Convert(#[from] ConvertError),
    #[error("tuple length mismatch: expected {expected} but got {actual}")]
    LengthMismatch { expected: usize, actual: usize },
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("csv error")]
    Csv(#[from] csv::Error),
    #[error("value can not be null")]
    NotNullable,
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
    executor: Option<BoxedExecutor>,
}

impl Visitor for ExecutorBuilder {
    fn visit_dummy(&mut self, _plan: &Dummy) {
        self.executor = Some(DummyScanExecutor.execute());
    }
    fn visit_physical_create_table(&mut self, plan: &PhysicalCreateTable) {
        match &self.env.storage {
            StorageImpl::InMemoryStorage(storage) => {
                self.executor = Some(
                    CreateTableExecutor {
                        plan: plan.clone(),
                        storage: storage.clone(),
                    }
                    .execute(),
                )
            }
            StorageImpl::SecondaryStorage(storage) => {
                self.executor = Some(
                    CreateTableExecutor {
                        plan: plan.clone(),
                        storage: storage.clone(),
                    }
                    .execute(),
                )
            }
        }
    }
    fn visit_physical_drop(&mut self, plan: &PhysicalDrop) {
        self.executor = Some(match &self.env.storage {
            StorageImpl::InMemoryStorage(storage) => DropExecutor {
                plan: plan.clone(),
                storage: storage.clone(),
            }
            .execute(),
            StorageImpl::SecondaryStorage(storage) => DropExecutor {
                plan: plan.clone(),
                storage: storage.clone(),
            }
            .execute(),
        });
    }

    fn visit_physical_insert(&mut self, plan: &PhysicalInsert) {
        self.executor = Some(match &self.env.storage {
            StorageImpl::InMemoryStorage(storage) => InsertExecutor {
                table_ref_id: plan.table_ref_id,
                column_ids: plan.column_ids.clone(),
                storage: storage.clone(),
                child: self.executor.take().unwrap(),
            }
            .execute(),
            StorageImpl::SecondaryStorage(storage) => InsertExecutor {
                table_ref_id: plan.table_ref_id,
                column_ids: plan.column_ids.clone(),
                storage: storage.clone(),
                child: self.executor.take().unwrap(),
            }
            .execute(),
        });
    }

    fn visit_physical_nested_loop_join_is_nested(&mut self) -> bool {
        true
    }
    fn visit_physical_nested_loop_join(&mut self, plan: &PhysicalNestedLoopJoin) {
        plan.left_plan.accept(self);
        let left_child = self.executor.take().unwrap();
        plan.right_plan.accept(self);
        let right_child = self.executor.take().unwrap();
        self.executor = Some(
            NestedLoopJoinExecutor {
                left_child,
                right_child,
                join_op: plan.join_op,
                condition: plan.condition.clone(),
            }
            .execute(),
        );
    }

    fn visit_physical_seq_scan(&mut self, plan: &PhysicalSeqScan) {
        self.executor = Some(match &self.env.storage {
            StorageImpl::InMemoryStorage(storage) => SeqScanExecutor {
                plan: plan.clone(),
                storage: storage.clone(),
            }
            .execute(),
            StorageImpl::SecondaryStorage(storage) => SeqScanExecutor {
                plan: plan.clone(),
                storage: storage.clone(),
            }
            .execute(),
        });
    }

    fn visit_physical_projection(&mut self, plan: &PhysicalProjection) {
        self.executor = Some(
            ProjectionExecutor {
                project_expressions: plan.project_expressions.clone(),
                child: self.executor.take().unwrap(),
            }
            .execute(),
        );
    }

    fn visit_physical_filter(&mut self, plan: &PhysicalFilter) {
        self.executor = Some(
            FilterExecutor {
                expr: plan.expr.clone(),
                child: self.executor.take().unwrap(),
            }
            .execute(),
        );
    }

    fn visit_physical_order(&mut self, plan: &PhysicalOrder) {
        self.executor = Some(
            OrderExecutor {
                comparators: plan.comparators.clone(),
                child: self.executor.take().unwrap(),
            }
            .execute(),
        );
    }

    fn visit_physical_limit(&mut self, plan: &PhysicalLimit) {
        self.executor = Some(
            LimitExecutor {
                child: self.executor.take().unwrap(),
                offset: plan.offset,
                limit: plan.limit,
            }
            .execute(),
        );
    }

    fn visit_physical_explain(&mut self, plan: &PhysicalExplain) {
        self.executor = Some(ExplainExecutor { plan: plan.clone() }.execute());
    }

    fn visit_physical_hash_agg(&mut self, plan: &PhysicalHashAgg) {
        self.executor = Some(
            HashAggExecutor {
                agg_calls: plan.agg_calls.clone(),
                group_keys: plan.group_keys.clone(),
                child: self.executor.take().unwrap(),
            }
            .execute(),
        );
    }

    fn visit_physical_hash_join(&mut self, plan: &PhysicalHashJoin) {
        plan.left_plan.accept(self);
        let left_child = self.executor.take().unwrap();
        plan.right_plan.accept(self);
        let right_child = self.executor.take().unwrap();
        self.executor = Some(
            HashJoinExecutor {
                left_child,
                right_child,
                join_op: plan.join_op,
                condition: plan.condition.clone(),
                left_column_index: plan.left_column_index,
                right_column_index: plan.right_column_index,
            }
            .execute(),
        );
    }

    fn visit_physical_simple_agg(&mut self, plan: &PhysicalSimpleAgg) {
        self.executor = Some(
            SimpleAggExecutor {
                agg_calls: plan.agg_calls.clone(),
                child: self.executor.take().unwrap(),
            }
            .execute(),
        );
    }

    fn visit_physical_delete(&mut self, plan: &PhysicalDelete) {
        self.executor = Some(match &self.env.storage {
            StorageImpl::InMemoryStorage(storage) => DeleteExecutor {
                child: self.executor.take().unwrap(),
                table_ref_id: plan.table_ref_id,
                storage: storage.clone(),
            }
            .execute(),
            StorageImpl::SecondaryStorage(storage) => DeleteExecutor {
                child: self.executor.take().unwrap(),
                table_ref_id: plan.table_ref_id,
                storage: storage.clone(),
            }
            .execute(),
        });
    }

    fn visit_physical_values(&mut self, plan: &PhysicalValues) {
        self.executor = Some(
            ValuesExecutor {
                column_types: plan.column_types.clone(),
                values: plan.values.clone(),
            }
            .execute(),
        );
    }

    fn visit_physical_copy_from_file(&mut self, plan: &PhysicalCopyFromFile) {
        self.executor = Some(CopyFromFileExecutor { plan: plan.clone() }.execute());
    }

    fn visit_physical_copy_to_file(&mut self, plan: &PhysicalCopyToFile) {
        self.executor = Some(
            CopyToFileExecutor {
                child: self.executor.take().unwrap(),
                path: plan.path.clone(),
                format: plan.format.clone(),
            }
            .execute(),
        );
    }
}

impl ExecutorBuilder {
    /// Create a new executor builder.
    pub fn new(env: GlobalEnvRef) -> ExecutorBuilder {
        ExecutorBuilder {
            env,
            executor: None,
        }
    }

    pub fn clone_and_reset(&self) -> Self {
        Self {
            env: self.env.clone(),
            executor: None,
        }
    }

    pub fn build(&mut self, plan: PlanRef) -> BoxedExecutor {
        plan.accept(self);
        self.executor.take().unwrap()
    }
}
