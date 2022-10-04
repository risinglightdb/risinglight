// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

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

use std::future::Future;
use std::sync::Arc;

use futures::stream::{BoxStream, StreamExt};
use futures_async_stream::{for_await, try_stream};
use itertools::Itertools;
use minitrace::prelude::*;
use tokio_util::sync::CancellationToken;

pub use self::aggregation::*;
use self::context::*;
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
use self::internal::*;
use self::limit::*;
use self::nested_loop_join::*;
use self::order::*;
#[allow(unused_imports)]
use self::perfect_hash_agg::*;
use self::projection::*;
use self::simple_agg::*;
#[allow(unused_imports)]
use self::sort_agg::*;
#[allow(unused_imports)]
use self::sort_merge_join::*;
use self::table_scan::*;
use self::top_n::TopNExecutor;
use self::values::*;
use crate::array::DataChunk;
use crate::binder::BoundExpr;
use crate::function::FunctionError;
use crate::optimizer::plan_nodes::*;
use crate::optimizer::PlanVisitor;
use crate::storage::{StorageImpl, TracedStorageError};
use crate::types::{ConvertError, DataValue};

mod aggregation;
pub mod context;
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
mod internal;
mod limit;
mod nested_loop_join;
mod order;
mod perfect_hash_agg;
mod projection;
mod simple_agg;
mod sort_agg;
mod sort_merge_join;
mod table_scan;
mod top_n;
mod values;

/// The error type of execution.
#[derive(thiserror::Error, Debug)]
pub enum ExecutorError {
    #[error("function error: {0}")]
    Function(
        #[from]
        #[backtrace]
        #[source]
        FunctionError,
    ),
    #[error("failed to build executors from the physical plan")]
    BuildingPlanError,
    #[error("storage error: {0}")]
    Storage(
        #[from]
        #[backtrace]
        #[source]
        TracedStorageError,
    ),
    #[error("conversion error: {0}")]
    Convert(#[from] ConvertError),
    #[error("tuple length mismatch: expected {expected} but got {actual}")]
    LengthMismatch { expected: usize, actual: usize },
    #[error("io error")]
    Io(
        #[from]
        #[source]
        std::io::Error,
    ),
    #[error("csv error")]
    Csv(
        #[from]
        #[source]
        csv::Error,
    ),
    #[error("value can not be null")]
    NotNullable,
    #[error("exceed char/varchar length limit: item length {length} > char/varchar width {width}")]
    ExceedLengthLimit { length: u64, width: u64 },
    #[error("abort")]
    Abort,
}

/// The maximum chunk length produced by executor at a time.
const PROCESSING_WINDOW_SIZE: usize = 1024;

/// A type-erased executor object.
///
/// Logically an executor is a stream of data chunks.
///
/// It consumes one or more streams from its child executors,
/// and produces a stream to its parent.
pub type BoxedExecutor = BoxStream<'static, Result<DataChunk, ExecutorError>>;

/// The builder of executor.
#[derive(Clone)]
pub struct ExecutorBuilder {
    context: Arc<Context>,
    storage: StorageImpl,
}

impl ExecutorBuilder {
    /// Create a new executor builder.
    pub fn new(context: Arc<Context>, storage: StorageImpl) -> ExecutorBuilder {
        ExecutorBuilder { context, storage }
    }

    pub fn build(&mut self, plan: PlanRef) -> BoxedExecutor {
        self.visit(plan).unwrap()
    }

    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn trace_execute(mut executor_stream: BoxedExecutor, identifier: &'static str) {
        while let Some(item) = executor_stream
            .next()
            .in_span(Span::enter_with_local_parent(identifier))
            .await
        {
            yield item?
        }
    }
}

/// Helper function to select the given future along with cancellation token.
/// If cancellation is signaled, returns `Err(ExecutorError::Abort)`.
/// Otherwise, the result of the future is returned.
async fn select_with_token<O>(
    token: &CancellationToken,
    f: impl Future<Output = O>,
) -> Result<O, ExecutorError> {
    tokio::select! {
        _ = token.cancelled() => {
            Err(ExecutorError::Abort)
        }
        ret = f => {
            Ok(ret)
        }
    }
}

/// Similar to `select_with_token` but only applies to futures that returns
/// `Result<T, E> where ExecutorError: From<E>` and unifies output to
/// `Result<T, ExecutorError>`.
async fn unified_select_with_token<T, E>(
    token: &CancellationToken,
    f: impl Future<Output = Result<T, E>>,
) -> Result<T, ExecutorError>
where
    ExecutorError: From<E>,
{
    tokio::select! {
        _ = token.cancelled() => {
            Err(ExecutorError::Abort)
        }
        ret = f => {
            Ok(ret?)
        }
    }
}

/// Cancellable executor that is aware of cancellation from cancellation token and
/// short circuit the stream if that happens.
pub struct CancellableExecutor {
    token: CancellationToken,
    child: BoxedExecutor,
}

impl CancellableExecutor {
    pub fn new(token: CancellationToken, child: BoxedExecutor) -> Self {
        Self { token, child }
    }

    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        let mut child = self.child;
        // Short circuit the execution if cancelled.
        while let Some(chunk) = select_with_token(&self.token, child.next()).await? {
            yield chunk?;
        }
    }
}

/// Extension of executors to provide the `cancellable` modifier.
trait ExecutorExt {
    fn cancellable(self, token: CancellationToken) -> BoxedExecutor;
}

impl ExecutorExt for BoxedExecutor {
    fn cancellable(self, token: CancellationToken) -> BoxedExecutor {
        CancellableExecutor::new(token, self).execute()
    }
}

impl PlanVisitor<BoxedExecutor> for ExecutorBuilder {
    fn visit_dummy(&mut self, _plan: &Dummy) -> Option<BoxedExecutor> {
        Some(ExecutorBuilder::trace_execute(
            DummyScanExecutor.execute(),
            "DummyScanExecutor",
        ))
    }

    fn visit_internal(&mut self, plan: &Internal) -> Option<BoxedExecutor> {
        Some(ExecutorBuilder::trace_execute(
            InternalTableExecutor {
                table_name: plan.table_name(),
            }
            .execute(),
            "InternalTableExecutor",
        ))
    }

    fn visit_physical_create_table(&mut self, plan: &PhysicalCreateTable) -> Option<BoxedExecutor> {
        Some(ExecutorBuilder::trace_execute(
            match &self.storage {
                StorageImpl::InMemoryStorage(storage) => CreateTableExecutor {
                    plan: plan.clone(),
                    storage: storage.clone(),
                }
                .execute(),
                StorageImpl::SecondaryStorage(storage) => CreateTableExecutor {
                    plan: plan.clone(),
                    storage: storage.clone(),
                }
                .execute(),
            },
            "CreateTableExecutor",
        ))
    }

    fn visit_physical_drop(&mut self, plan: &PhysicalDrop) -> Option<BoxedExecutor> {
        Some(ExecutorBuilder::trace_execute(
            match &self.storage {
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
            },
            "DropExecutor",
        ))
    }

    fn visit_physical_insert(&mut self, plan: &PhysicalInsert) -> Option<BoxedExecutor> {
        Some(ExecutorBuilder::trace_execute(
            match &self.storage {
                StorageImpl::InMemoryStorage(storage) => InsertExecutor {
                    context: self.context.clone(),
                    table_ref_id: plan.logical().table_ref_id(),
                    column_ids: plan.logical().column_ids().to_vec(),
                    storage: storage.clone(),
                    child: self.visit(plan.child()).unwrap(),
                }
                .execute()
                .cancellable(self.context.token().child_token()),
                StorageImpl::SecondaryStorage(storage) => InsertExecutor {
                    context: self.context.clone(),
                    table_ref_id: plan.logical().table_ref_id(),
                    column_ids: plan.logical().column_ids().to_vec(),
                    storage: storage.clone(),
                    child: self.visit(plan.child()).unwrap(),
                }
                .execute()
                .cancellable(self.context.token().child_token()),
            },
            "InsertExecutor",
        ))
    }

    fn visit_physical_nested_loop_join(
        &mut self,
        plan: &PhysicalNestedLoopJoin,
    ) -> Option<BoxedExecutor> {
        let left_child = self.visit(plan.left()).unwrap();
        let right_child = self.visit(plan.right()).unwrap();
        Some(ExecutorBuilder::trace_execute(
            NestedLoopJoinExecutor {
                left_child,
                right_child,
                join_op: plan.logical().join_op(),
                condition: plan.logical().predicate().to_on_clause(),
                left_types: plan.left().out_types(),
                right_types: plan.right().out_types(),
            }
            .execute(),
            "NestedLoopJoinExecutor",
        ))
    }

    fn visit_physical_table_scan(&mut self, plan: &PhysicalTableScan) -> Option<BoxedExecutor> {
        Some(ExecutorBuilder::trace_execute(
            match &self.storage {
                StorageImpl::InMemoryStorage(storage) => TableScanExecutor {
                    context: self.context.clone(),
                    plan: plan.clone(),
                    expr: None,
                    storage: storage.clone(),
                }
                .execute()
                .cancellable(self.context.token().child_token()),
                StorageImpl::SecondaryStorage(storage) => TableScanExecutor {
                    context: self.context.clone(),
                    plan: plan.clone(),
                    expr: plan.logical().expr().cloned(),
                    storage: storage.clone(),
                }
                .execute()
                .cancellable(self.context.token().child_token()),
            },
            "TableScanExecutor",
        ))
    }

    fn visit_physical_projection(&mut self, plan: &PhysicalProjection) -> Option<BoxedExecutor> {
        Some(ExecutorBuilder::trace_execute(
            ProjectionExecutor {
                project_expressions: plan.logical().project_expressions().to_vec(),
                child: self.visit(plan.child()).unwrap(),
            }
            .execute(),
            "ProjectionExecutor",
        ))
    }

    fn visit_physical_filter(&mut self, plan: &PhysicalFilter) -> Option<BoxedExecutor> {
        Some(ExecutorBuilder::trace_execute(
            FilterExecutor {
                expr: plan.logical().expr().clone(),
                child: self.visit(plan.child()).unwrap(),
            }
            .execute(),
            "FilterExecutor",
        ))
    }

    fn visit_physical_order(&mut self, plan: &PhysicalOrder) -> Option<BoxedExecutor> {
        Some(ExecutorBuilder::trace_execute(
            OrderExecutor {
                comparators: plan.logical().comparators().to_vec(),
                child: self.visit(plan.child()).unwrap(),
                output_types: plan.logical().out_types(),
            }
            .execute(),
            "OrderExecutor",
        ))
    }

    fn visit_physical_limit(&mut self, plan: &PhysicalLimit) -> Option<BoxedExecutor> {
        Some(ExecutorBuilder::trace_execute(
            LimitExecutor {
                child: self.visit(plan.child()).unwrap(),
                offset: plan.logical().offset(),
                limit: plan.logical().limit(),
            }
            .execute(),
            "LimitExecutor",
        ))
    }

    fn visit_physical_top_n(&mut self, plan: &PhysicalTopN) -> Option<BoxedExecutor> {
        Some(ExecutorBuilder::trace_execute(
            TopNExecutor {
                child: self.visit(plan.child()).unwrap(),
                offset: plan.logical().offset(),
                limit: plan.logical().limit(),
                comparators: plan.logical().comparators().to_owned(),
                output_types: plan.logical().out_types(),
            }
            .execute(),
            "TopNExecutor",
        ))
    }

    fn visit_physical_explain(&mut self, plan: &PhysicalExplain) -> Option<BoxedExecutor> {
        Some(ExecutorBuilder::trace_execute(
            ExplainExecutor { plan: plan.clone() }.execute(),
            "ExplainExecutor",
        ))
    }

    fn visit_physical_hash_agg(&mut self, plan: &PhysicalHashAgg) -> Option<BoxedExecutor> {
        Some(ExecutorBuilder::trace_execute(
            HashAggExecutor {
                agg_calls: plan.logical().agg_calls().to_vec(),
                group_keys: plan.logical().group_keys().to_vec(),
                child: self.visit(plan.child()).unwrap(),
            }
            .execute(),
            "HashAggExecutor",
        ))
    }

    fn visit_physical_hash_join(&mut self, plan: &PhysicalHashJoin) -> Option<BoxedExecutor> {
        let left_child = self.visit(plan.left()).unwrap();
        let right_child = self.visit(plan.right()).unwrap();

        let left_col_num = plan.left().out_types().len();
        let (left_column_indexes, right_column_indexes) = plan
            .logical()
            .predicate()
            .eq_keys()
            .iter()
            .map(|(left, right)| (left.index, right.index - left_col_num))
            .unzip();
        Some(ExecutorBuilder::trace_execute(
            HashJoinExecutor {
                left_child,
                right_child,
                join_op: plan.logical().join_op(),
                condition: BoundExpr::Constant(DataValue::Bool(true)),
                left_column_indexes,
                right_column_indexes,
                left_types: plan.left().out_types(),
                right_types: plan.right().out_types(),
            }
            .execute(),
            "HashJoinExecutor",
        ))
    }

    fn visit_physical_simple_agg(&mut self, plan: &PhysicalSimpleAgg) -> Option<BoxedExecutor> {
        Some(ExecutorBuilder::trace_execute(
            SimpleAggExecutor {
                agg_calls: plan.agg_calls().to_vec(),
                child: self.visit(plan.child()).unwrap(),
            }
            .execute(),
            "SimpleAggExecutor",
        ))
    }

    fn visit_physical_delete(&mut self, plan: &PhysicalDelete) -> Option<BoxedExecutor> {
        let child = self.visit(plan.child()).unwrap();
        Some(ExecutorBuilder::trace_execute(
            match &self.storage {
                StorageImpl::InMemoryStorage(storage) => DeleteExecutor {
                    context: self.context.clone(),
                    child,
                    table_ref_id: plan.logical().table_ref_id(),
                    storage: storage.clone(),
                }
                .execute()
                .cancellable(self.context.token().child_token()),
                StorageImpl::SecondaryStorage(storage) => DeleteExecutor {
                    context: self.context.clone(),
                    child,
                    table_ref_id: plan.logical().table_ref_id(),
                    storage: storage.clone(),
                }
                .execute()
                .cancellable(self.context.token().child_token()),
            },
            "DeleteExecutor",
        ))
    }

    fn visit_physical_values(&mut self, plan: &PhysicalValues) -> Option<BoxedExecutor> {
        Some(ExecutorBuilder::trace_execute(
            ValuesExecutor {
                column_types: plan.logical().column_types().to_vec(),
                values: plan.logical().values().to_vec(),
            }
            .execute(),
            "ValuesExecutor",
        ))
    }

    fn visit_physical_copy_from_file(
        &mut self,
        plan: &PhysicalCopyFromFile,
    ) -> Option<BoxedExecutor> {
        Some(ExecutorBuilder::trace_execute(
            CopyFromFileExecutor {
                context: self.context.clone(),
                plan: plan.clone(),
            }
            .execute()
            .cancellable(self.context.token().child_token()),
            "CopyFromFileExecutor",
        ))
    }

    fn visit_physical_copy_to_file(&mut self, plan: &PhysicalCopyToFile) -> Option<BoxedExecutor> {
        Some(ExecutorBuilder::trace_execute(
            CopyToFileExecutor {
                context: self.context.clone(),
                child: self.visit(plan.child()).unwrap(),
                path: plan.logical().path().clone(),
                format: plan.logical().format().clone(),
            }
            .execute()
            .cancellable(self.context.token().child_token()),
            "CopyToFileExecutor",
        ))
    }
}
