// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

#![allow(unused)]

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

use egg::{Id, Language};
use futures::stream::{BoxStream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use minitrace::prelude::*;

// use self::copy_from_file::*;
// use self::copy_to_file::*;
use self::create::*;
// use self::delete::*;
use self::drop::*;
use self::evaluator::*;
// use self::dummy_scan::*;
use self::explain::*;
use self::filter::*;
use self::hash_agg::*;
use self::hash_join::*;
use self::insert::*;
// use self::internal::*;
use self::limit::*;
// use self::nested_loop_join::*;
use self::order::*;
// #[allow(unused_imports)]
// use self::perfect_hash_agg::*;
use self::projection::*;
use self::simple_agg::*;
// #[allow(unused_imports)]
// use self::sort_agg::*;
// #[allow(unused_imports)]
// use self::sort_merge_join::*;
use self::table_scan::*;
use self::top_n::TopNExecutor;
use self::values::*;
use crate::array::DataChunk;
use crate::binder::BoundExpr;
use crate::catalog::RootCatalogRef;
use crate::function::FunctionError;
use crate::planner::{ColumnIndexResolver, Expr, RecExpr, TypeSchemaAnalysis};
use crate::storage::{Storage, StorageImpl, TracedStorageError};
use crate::types::{ConvertError, DataType, DataValue};

// mod copy_from_file;
// mod copy_to_file;
mod create;
// mod delete;
mod drop;
// mod dummy_scan;
mod evaluator;
mod explain;
mod filter;
mod hash_agg;
mod hash_join;
mod insert;
// mod internal;
mod limit;
// mod nested_loop_join;
mod order;
// mod perfect_hash_agg;
mod projection;
mod simple_agg;
// mod sort_agg;
// mod sort_merge_join;
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

pub fn build(catalog: RootCatalogRef, storage: Arc<impl Storage>, plan: &RecExpr) -> BoxedExecutor {
    Builder::new(catalog, storage, plan).build()
}

/// The builder of executor.
struct Builder<S: Storage> {
    storage: Arc<S>,
    catalog: RootCatalogRef,
    egraph: egg::EGraph<Expr, TypeSchemaAnalysis>,
    root: Id,
}

impl<S: Storage> Builder<S> {
    /// Create a new executor builder.
    fn new(catalog: RootCatalogRef, storage: Arc<S>, plan: &RecExpr) -> Self {
        let mut egraph = egg::EGraph::new(TypeSchemaAnalysis {
            catalog: catalog.clone(),
        });
        let root = egraph.add_expr(plan);
        Builder {
            storage,
            catalog,
            egraph,
            root,
        }
    }

    fn node(&self, id: Id) -> &Expr {
        &self.egraph[id].nodes[0]
    }

    /// Extract a `RecExpr` from id.
    fn recexpr(&self, id: Id) -> RecExpr {
        self.node(id).build_recexpr(|id| self.node(id).clone())
    }

    /// Returns the output types of a plan node.
    fn plan_types(&self, id: Id) -> &[DataType] {
        let ty = self.egraph[id].data.type_.as_ref().unwrap();
        ty.kind.as_struct()
    }

    /// Resolve the column index of `expr` in `plan`.
    fn resolve_column_index(&self, expr: Id, plan: Id) -> RecExpr {
        let schema = (self.egraph[plan].data.schema.as_ref().expect("no schema"))
            .iter()
            .map(|id| self.recexpr(*id))
            .collect_vec();
        ColumnIndexResolver::new(&schema).resolve(&self.recexpr(expr))
    }

    fn build(self) -> BoxedExecutor {
        self.build_id(self.root)
    }

    fn build_id(&self, id: Id) -> BoxedExecutor {
        use Expr::*;
        match self.node(id).clone() {
            Scan(list) => TableScanExecutor {
                columns: (self.node(list).as_list().iter())
                    .map(|id| self.node(*id).as_column())
                    .collect(),
                storage: self.storage.clone(),
            }
            .execute(),

            Values(rows) => ValuesExecutor {
                column_types: self.plan_types(id).to_vec(),
                values: {
                    rows.iter()
                        .map(|row| {
                            (self.node(*row).as_list().iter())
                                .map(|id| self.recexpr(*id))
                                .collect()
                        })
                        .collect()
                },
            }
            .execute(),

            Proj([projs, child]) => ProjectionExecutor {
                projs: self.resolve_column_index(projs, child),
            }
            .execute(self.build_id(child)),

            Filter([cond, child]) => FilterExecutor {
                condition: self.resolve_column_index(cond, child),
            }
            .execute(self.build_id(child)),

            Order([order_keys, child]) => OrderExecutor {
                order_keys: self.resolve_column_index(order_keys, child),
                types: self.plan_types(id).to_vec(),
            }
            .execute(self.build_id(child)),

            Limit([limit, offset, child]) => LimitExecutor {
                limit: (self.node(limit).as_const().as_usize().unwrap()).unwrap_or(usize::MAX / 2),
                offset: self.node(offset).as_const().as_usize().unwrap().unwrap(),
            }
            .execute(self.build_id(child)),

            TopN([limit, offset, order_keys, child]) => TopNExecutor {
                limit: (self.node(limit).as_const().as_usize().unwrap()).unwrap_or(usize::MAX / 2),
                offset: self.node(offset).as_const().as_usize().unwrap().unwrap(),
                order_keys: self.resolve_column_index(order_keys, child),
                types: self.plan_types(id).to_vec(),
            }
            .execute(self.build_id(child)),

            Join(_) => todo!(),

            HashJoin([op, lkeys, rkeys, left, right]) => HashJoinExecutor {
                op: self.node(op).clone(),
                left_keys: self.resolve_column_index(lkeys, left),
                right_keys: self.resolve_column_index(rkeys, right),
                left_types: self.plan_types(left).to_vec(),
                right_types: self.plan_types(right).to_vec(),
            }
            .execute(self.build_id(left), self.build_id(right)),

            Agg([aggs, group_keys, child]) => {
                let aggs = self.resolve_column_index(aggs, child);
                let group_keys = self.resolve_column_index(group_keys, child);
                if group_keys.as_ref().last().unwrap().as_list().is_empty() {
                    SimpleAggExecutor { aggs }.execute(self.build_id(child))
                } else {
                    HashAggExecutor {
                        aggs,
                        group_keys,
                        types: self.plan_types(id).to_vec(),
                    }
                    .execute(self.build_id(child))
                }
            }

            CreateTable(plan) => CreateTableExecutor {
                plan,
                storage: self.storage.clone(),
            }
            .execute(),

            Drop(plan) => DropExecutor {
                plan,
                storage: self.storage.clone(),
            }
            .execute(),

            Insert([table, cols, child]) => InsertExecutor {
                table_id: self.node(table).as_table(),
                column_ids: (self.node(cols).as_list().iter())
                    .map(|id| self.node(*id).as_column().column_id)
                    .collect(),
                storage: self.storage.clone(),
            }
            .execute(self.build_id(child)),

            Delete(_) => todo!(),
            CopyFrom(_) => todo!(),
            CopyTo(_) => todo!(),

            Explain(plan) => ExplainExecutor {
                plan: self.recexpr(plan),
                catalog: self.catalog.clone(),
            }
            .execute(),

            node => panic!("not a plan: {node:?}"),
        }
    }
}
