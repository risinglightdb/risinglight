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

use std::sync::Arc;

use futures::stream::{BoxStream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;

use crate::array::DataChunk;
use crate::optimizer::plan_nodes::*;
use crate::optimizer::PlanVisitor;
use crate::storage::{StorageImpl, TracedStorageError};
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
mod simple_agg;
mod table_scan;
mod top_n;
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
use self::simple_agg::*;
use self::table_scan::*;
use self::top_n::TopNExecutor;
use self::values::*;

/// The error type of execution.
#[derive(thiserror::Error, Debug)]
pub enum ExecutorError {
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
    storage: StorageImpl,
}

impl ExecutorBuilder {
    /// Create a new executor builder.
    pub fn new(storage: StorageImpl) -> ExecutorBuilder {
        ExecutorBuilder { storage }
    }

    pub fn build(&mut self, plan: PlanRef) -> BoxedExecutor {
        self.visit(plan).unwrap()
    }
}

impl PlanVisitor<BoxedExecutor> for ExecutorBuilder {
    fn visit_dummy(&mut self, _plan: &Dummy) -> Option<BoxedExecutor> {
        Some(DummyScanExecutor.execute())
    }

    fn visit_physical_create_table(&mut self, plan: &PhysicalCreateTable) -> Option<BoxedExecutor> {
        Some(match &self.storage {
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
        })
    }

    fn visit_physical_drop(&mut self, plan: &PhysicalDrop) -> Option<BoxedExecutor> {
        Some(match &self.storage {
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
        })
    }

    fn visit_physical_insert(&mut self, plan: &PhysicalInsert) -> Option<BoxedExecutor> {
        Some(match &self.storage {
            StorageImpl::InMemoryStorage(storage) => InsertExecutor {
                table_ref_id: plan.logical().table_ref_id(),
                column_ids: plan.logical().column_ids().to_vec(),
                storage: storage.clone(),
                child: self.visit(plan.child()).unwrap(),
            }
            .execute(),
            StorageImpl::SecondaryStorage(storage) => InsertExecutor {
                table_ref_id: plan.logical().table_ref_id(),
                column_ids: plan.logical().column_ids().to_vec(),
                storage: storage.clone(),
                child: self.visit(plan.child()).unwrap(),
            }
            .execute(),
        })
    }

    fn visit_physical_nested_loop_join(
        &mut self,
        plan: &PhysicalNestedLoopJoin,
    ) -> Option<BoxedExecutor> {
        let left_child = self.visit(plan.left()).unwrap();
        let right_child = self.visit(plan.right()).unwrap();
        Some(
            NestedLoopJoinExecutor {
                left_child,
                right_child,
                join_op: plan.logical().join_op(),
                condition: plan.logical().predicate().to_on_clause(),
                left_types: plan.left().out_types(),
                right_types: plan.right().out_types(),
            }
            .execute(),
        )
    }

    fn visit_physical_table_scan(&mut self, plan: &PhysicalTableScan) -> Option<BoxedExecutor> {
        Some(match &self.storage {
            StorageImpl::InMemoryStorage(storage) => TableScanExecutor {
                plan: plan.clone(),
                expr: None,
                storage: storage.clone(),
            }
            .execute(),
            StorageImpl::SecondaryStorage(storage) => TableScanExecutor {
                plan: plan.clone(),
                expr: plan.logical().expr().cloned(),
                storage: storage.clone(),
            }
            .execute(),
        })
    }

    fn visit_physical_projection(&mut self, plan: &PhysicalProjection) -> Option<BoxedExecutor> {
        Some(
            ProjectionExecutor {
                project_expressions: plan.logical().project_expressions().to_vec(),
                child: self.visit(plan.child()).unwrap(),
            }
            .execute(),
        )
    }

    fn visit_physical_filter(&mut self, plan: &PhysicalFilter) -> Option<BoxedExecutor> {
        Some(
            FilterExecutor {
                expr: plan.logical().expr().clone(),
                child: self.visit(plan.child()).unwrap(),
            }
            .execute(),
        )
    }

    fn visit_physical_order(&mut self, plan: &PhysicalOrder) -> Option<BoxedExecutor> {
        Some(
            OrderExecutor {
                comparators: plan.logical().comparators().to_vec(),
                child: self.visit(plan.child()).unwrap(),
            }
            .execute(),
        )
    }

    fn visit_physical_limit(&mut self, plan: &PhysicalLimit) -> Option<BoxedExecutor> {
        Some(
            LimitExecutor {
                child: self.visit(plan.child()).unwrap(),
                offset: plan.logical().offset(),
                limit: plan.logical().limit(),
            }
            .execute(),
        )
    }

    fn visit_physical_top_n(&mut self, plan: &PhysicalTopN) -> Option<BoxedExecutor> {
        Some(
            TopNExecutor {
                child: self.visit(plan.child()).unwrap(),
                offset: plan.logical().offset(),
                limit: plan.logical().limit(),
                comparators: plan.logical().comparators().to_owned(),
            }
            .execute(),
        )
    }

    fn visit_physical_explain(&mut self, plan: &PhysicalExplain) -> Option<BoxedExecutor> {
        Some(ExplainExecutor { plan: plan.clone() }.execute())
    }

    fn visit_physical_hash_agg(&mut self, plan: &PhysicalHashAgg) -> Option<BoxedExecutor> {
        Some(
            HashAggExecutor {
                agg_calls: plan.logical().agg_calls().to_vec(),
                group_keys: plan.logical().group_keys().to_vec(),
                child: self.visit(plan.child()).unwrap(),
            }
            .execute(),
        )
    }

    fn visit_physical_hash_join(&mut self, plan: &PhysicalHashJoin) -> Option<BoxedExecutor> {
        let left_child = self.visit(plan.left()).unwrap();
        let right_child = self.visit(plan.right()).unwrap();
        Some(
            HashJoinExecutor {
                left_child,
                right_child,
                join_op: plan.logical().join_op(),
                condition: plan.logical().predicate().to_on_clause(),
                left_column_index: plan.left_column_index(),
                right_column_index: plan.right_column_index(),
                left_types: plan.left().out_types(),
                right_types: plan.right().out_types(),
            }
            .execute(),
        )
    }

    fn visit_physical_simple_agg(&mut self, plan: &PhysicalSimpleAgg) -> Option<BoxedExecutor> {
        Some(
            SimpleAggExecutor {
                agg_calls: plan.agg_calls().to_vec(),
                child: self.visit(plan.child()).unwrap(),
            }
            .execute(),
        )
    }

    fn visit_physical_delete(&mut self, plan: &PhysicalDelete) -> Option<BoxedExecutor> {
        let child = self.visit(plan.child()).unwrap();
        Some(match &self.storage {
            StorageImpl::InMemoryStorage(storage) => DeleteExecutor {
                child,
                table_ref_id: plan.logical().table_ref_id(),
                storage: storage.clone(),
            }
            .execute(),
            StorageImpl::SecondaryStorage(storage) => DeleteExecutor {
                child,
                table_ref_id: plan.logical().table_ref_id(),
                storage: storage.clone(),
            }
            .execute(),
        })
    }

    fn visit_physical_values(&mut self, plan: &PhysicalValues) -> Option<BoxedExecutor> {
        Some(
            ValuesExecutor {
                column_types: plan.logical().column_types().to_vec(),
                values: plan.logical().values().to_vec(),
            }
            .execute(),
        )
    }

    fn visit_physical_copy_from_file(
        &mut self,
        plan: &PhysicalCopyFromFile,
    ) -> Option<BoxedExecutor> {
        Some(CopyFromFileExecutor { plan: plan.clone() }.execute())
    }

    fn visit_physical_copy_to_file(&mut self, plan: &PhysicalCopyToFile) -> Option<BoxedExecutor> {
        Some(
            CopyToFileExecutor {
                child: self.visit(plan.child()).unwrap(),
                path: plan.logical().path().clone(),
                format: plan.logical().format().clone(),
            }
            .execute(),
        )
    }
}
