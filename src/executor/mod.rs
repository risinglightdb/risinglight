// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

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

use std::collections::HashMap;
use std::sync::Arc;

use egg::{Id, Language};
use futures::stream::{BoxStream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;

// use minitrace::prelude::*;
use self::copy_from_file::*;
use self::copy_to_file::*;
use self::create_function::*;
use self::create_table::*;
use self::create_view::*;
use self::delete::*;
use self::drop::*;
pub use self::error::Error as ExecutorError;
use self::error::*;
use self::evaluator::*;
use self::exchange::*;
use self::explain::*;
use self::filter::*;
use self::hash_agg::*;
use self::hash_join::*;
use self::insert::*;
use self::limit::*;
use self::merge_join::*;
use self::nested_loop_join::*;
use self::order::*;
// #[allow(unused_imports)]
// use self::perfect_hash_agg::*;
use self::projection::*;
use self::simple_agg::*;
use self::sort_agg::*;
use self::system_table_scan::*;
use self::table_scan::*;
use self::top_n::TopNExecutor;
use self::values::*;
use self::window::*;
use crate::array::DataChunk;
use crate::catalog::{RootCatalog, RootCatalogRef, TableRefId};
use crate::planner::{Expr, ExprAnalysis, Optimizer, RecExpr, TypeSchemaAnalysis};
use crate::storage::Storage;
use crate::types::{ColumnIndex, DataType};

mod copy_from_file;
mod copy_to_file;
mod create_function;
mod create_table;
mod create_view;
mod delete;
mod drop;
mod evaluator;
mod exchange;
mod explain;
mod filter;
mod hash_agg;
mod hash_join;
mod insert;
mod limit;
mod nested_loop_join;
mod order;
mod system_table_scan;
// mod perfect_hash_agg;
mod error;
mod merge_join;
mod projection;
mod simple_agg;
mod sort_agg;
mod table_scan;
mod top_n;
mod values;
mod window;

/// The maximum chunk length produced by executor at a time.
const PROCESSING_WINDOW_SIZE: usize = 1024;

/// A type-erased executor object.
///
/// Logically an executor is a stream of data chunks.
///
/// It consumes one or more streams from its child executors,
/// and produces a stream to its parent.
pub type BoxedExecutor = BoxStream<'static, Result<DataChunk>>;
/// A boxed dispatcher that distributes data to multiple partitions.
pub type BoxedDispatcher = BoxStream<'static, Result<(DataChunk, usize)>>;

pub fn build(optimizer: Optimizer, storage: Arc<impl Storage>, plan: &RecExpr) -> BoxedExecutor {
    Builder::new(optimizer, storage, plan).build()
}

/// The builder of executor.
struct Builder<S: Storage> {
    storage: Arc<S>,
    optimizer: Optimizer,
    egraph: egg::EGraph<Expr, TypeSchemaAnalysis>,
    root: Id,
    /// For scans on views, we prebuild their executors and store them here.
    /// Multiple scans on the same view will share the same executor.
    views: HashMap<TableRefId, PartitionedStreamSubscriber>,
}

impl<S: Storage> Builder<S> {
    /// Create a new executor builder.
    fn new(optimizer: Optimizer, storage: Arc<S>, plan: &RecExpr) -> Self {
        let mut egraph = egg::EGraph::new(TypeSchemaAnalysis {
            catalog: optimizer.catalog().clone(),
        });
        let root = egraph.add_expr(plan);

        // recursively build for all views
        let mut views = HashMap::new();
        for node in plan.as_ref() {
            if let Expr::Table(tid) = node
                && let Some(query) = optimizer.catalog().get_table(tid).unwrap().query()
            {
                let builder = Self::new(optimizer.clone(), storage.clone(), query);
                let subscriber = builder.build_subscriber();
                views.insert(*tid, subscriber);
            }
        }

        Builder {
            storage,
            optimizer,
            egraph,
            root,
            views,
        }
    }

    /// Get the node from id.
    fn node(&self, id: Id) -> &Expr {
        // each e-class has exactly one node since there is no rewrite or union.
        &self.egraph[id].nodes[0]
    }

    /// Extract a `RecExpr` from id.
    fn recexpr(&self, id: Id) -> RecExpr {
        self.node(id).build_recexpr(|id| self.node(id).clone())
    }

    /// Returns the output types of a plan node.
    fn plan_types(&self, id: Id) -> &[DataType] {
        let ty = self.egraph[id].data.type_.as_ref().unwrap();
        ty.as_struct()
    }

    /// Resolve the column index of `expr` in `plan`.
    fn resolve_column_index(&self, expr: Id, plan: Id) -> RecExpr {
        let schema = &self.egraph[plan].data.schema;
        self.resolve_column_index_on_schema(expr, schema)
    }

    /// Resolve the column index of `expr` in `left` || `right`.
    fn resolve_column_index2(&self, expr: Id, left: Id, right: Id) -> RecExpr {
        let left = &self.egraph[left].data.schema;
        let right = &self.egraph[right].data.schema;
        let schema = left.iter().chain(right.iter()).cloned().collect_vec();
        self.resolve_column_index_on_schema(expr, &schema)
    }

    /// Resolve the column index of `expr` in `schema`.
    fn resolve_column_index_on_schema(&self, expr: Id, schema: &[Id]) -> RecExpr {
        self.node(expr).build_recexpr(|id| {
            if let Some(idx) = schema.iter().position(|x| *x == id) {
                return Expr::ColumnIndex(ColumnIndex(idx as _));
            }
            match self.node(id) {
                Expr::Column(c) => panic!("column {c} not found from input"),
                e => e.clone(),
            }
        })
    }

    /// Returns the catalog.
    fn catalog(&self) -> &RootCatalogRef {
        self.optimizer.catalog()
    }

    /// Builds the executor.
    fn build(self) -> BoxedExecutor {
        self.build_id(self.root).spawn_merge()
    }

    /// Builds the executor and returns its subscriber.
    fn build_subscriber(self) -> PartitionedStreamSubscriber {
        self.build_id(self.root).spawn()
    }

    /// Builds stream for the given plan.
    fn build_id(&self, id: Id) -> PartitionedStream {
        use Expr::*;
        match self.node(id).clone() {
            Scan([table, list, filter]) => {
                let table_id = self.node(table).as_table();
                let columns = (self.node(list).as_list().iter())
                    .map(|id| self.node(*id).as_column())
                    .collect_vec();
                // analyze range filter
                let filter = {
                    use std::ops::Bound;
                    let mut egraph = egg::EGraph::new(ExprAnalysis::default());
                    let root = egraph.add_expr(&self.recexpr(filter));
                    let expr: Option<crate::storage::KeyRange> =
                        egraph[root].data.range.clone().map(|(_, r)| r);
                    if matches!(
                        expr,
                        Some(crate::storage::KeyRange {
                            start: Bound::Unbounded,
                            end: Bound::Unbounded
                        })
                    ) {
                        None
                    } else {
                        expr
                    }
                };

                if let Some(subscriber) = self.views.get(&table_id) {
                    // scan a view
                    assert!(
                        filter.is_none(),
                        "range filter is not supported in view scan"
                    );

                    // resolve column index
                    // child schema: [$v.0, $v.1, ...]
                    let mut projs = RecExpr::default();
                    let lists = columns
                        .iter()
                        .map(|c| {
                            projs.add(ColumnIndex(crate::types::ColumnIndex(c.column_id as _)))
                        })
                        .collect();
                    projs.add(List(lists));

                    subscriber.subscribe().map(|c| {
                        ProjectionExecutor {
                            projs: projs.clone(),
                        }
                        .execute(c)
                    })
                } else if table_id.schema_id == RootCatalog::SYSTEM_SCHEMA_ID {
                    SystemTableScan {
                        catalog: self.catalog().clone(),
                        storage: self.storage.clone(),
                        table_id,
                        columns,
                    }
                    .execute()
                    .into()
                } else {
                    TableScanExecutor {
                        table_id,
                        columns,
                        filter,
                        storage: self.storage.clone(),
                    }
                    .execute()
                    .into()
                }
            }

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
            .execute()
            .into(),

            Proj([projs, child]) => self.build_id(child).map(|c| {
                ProjectionExecutor {
                    projs: self.resolve_column_index(projs, child),
                }
                .execute(c)
            }),

            Filter([cond, child]) => self.build_id(child).map(|c| {
                FilterExecutor {
                    condition: self.resolve_column_index(cond, child),
                }
                .execute(c)
            }),

            Order([order_keys, child]) => self.build_id(child).map(|c| {
                OrderExecutor {
                    order_keys: self.resolve_column_index(order_keys, child),
                    types: self.plan_types(id).to_vec(),
                }
                .execute(c)
            }),

            Limit([limit, offset, child]) => self.build_id(child).map(|c| {
                LimitExecutor {
                    limit: (self.node(limit).as_const().as_usize().unwrap())
                        .unwrap_or(usize::MAX / 2),
                    offset: self.node(offset).as_const().as_usize().unwrap().unwrap(),
                }
                .execute(c)
            }),

            TopN([limit, offset, order_keys, child]) => self.build_id(child).map(|c| {
                TopNExecutor {
                    limit: (self.node(limit).as_const().as_usize().unwrap())
                        .unwrap_or(usize::MAX / 2),
                    offset: self.node(offset).as_const().as_usize().unwrap().unwrap(),
                    order_keys: self.resolve_column_index(order_keys, child),
                    types: self.plan_types(id).to_vec(),
                }
                .execute(c)
            }),

            Join([op, on, left, right]) => self
                .build_id(left)
                .spawn() // ends the pipeline of left side
                .subscribe()
                .zip(self.build_id(right))
                .map(|l, r| match self.node(op) {
                    Inner | LeftOuter | RightOuter | FullOuter => NestedLoopJoinExecutor {
                        op: self.node(op).clone(),
                        condition: self.resolve_column_index2(on, left, right),
                        left_types: self.plan_types(left).to_vec(),
                        right_types: self.plan_types(right).to_vec(),
                    }
                    .execute(l, r),
                    op @ Semi | op @ Anti => NestedLoopSemiJoinExecutor {
                        anti: matches!(op, Anti),
                        condition: self.resolve_column_index2(on, left, right),
                        left_types: self.plan_types(left).to_vec(),
                    }
                    .execute(l, r),
                    t => panic!("invalid join type: {t:?}"),
                }),

            HashJoin(args @ [op, _, _, _, left, right]) => self
                .build_id(left)
                .spawn() // ends the pipeline of left side
                .subscribe()
                .zip(self.build_id(right))
                .map(|l, r| match self.node(op) {
                    Inner => self.build_hashjoin::<{ JoinType::Inner }>(args, l, r),
                    LeftOuter => self.build_hashjoin::<{ JoinType::LeftOuter }>(args, l, r),
                    RightOuter => self.build_hashjoin::<{ JoinType::RightOuter }>(args, l, r),
                    FullOuter => self.build_hashjoin::<{ JoinType::FullOuter }>(args, l, r),
                    Semi => self.build_hashsemijoin(args, false, l, r),
                    Anti => self.build_hashsemijoin(args, true, l, r),
                    t => panic!("invalid join type: {t:?}"),
                }),

            MergeJoin(args @ [op, _, _, _, left, right]) => self
                .build_id(left)
                .spawn() // ends the pipeline of left side
                .subscribe()
                .zip(self.build_id(right))
                .map(|l, r| match self.node(op) {
                    Inner => self.build_mergejoin::<{ JoinType::Inner }>(args, l, r),
                    LeftOuter => self.build_mergejoin::<{ JoinType::LeftOuter }>(args, l, r),
                    RightOuter => self.build_mergejoin::<{ JoinType::RightOuter }>(args, l, r),
                    FullOuter => self.build_mergejoin::<{ JoinType::FullOuter }>(args, l, r),
                    t => panic!("invalid join type: {t:?}"),
                }),

            Apply(_) => {
                panic!("Apply is not supported in executor. It should be rewritten to join by optimizer.")
            }

            Agg([aggs, child]) => self.build_id(child).map(|c| {
                SimpleAggExecutor {
                    aggs: self.resolve_column_index(aggs, child),
                    types: self.plan_types(id).to_vec(),
                }
                .execute(c)
            }),

            HashAgg([keys, aggs, child]) => self.build_id(child).map(|c| {
                HashAggExecutor {
                    keys: self.resolve_column_index(keys, child),
                    aggs: self.resolve_column_index(aggs, child),
                    types: self.plan_types(id).to_vec(),
                }
                .execute(c)
            }),

            SortAgg([keys, aggs, child]) => self.build_id(child).map(|c| {
                SortAggExecutor {
                    keys: self.resolve_column_index(keys, child),
                    aggs: self.resolve_column_index(aggs, child),
                    types: self.plan_types(id).to_vec(),
                }
                .execute(c)
            }),

            Window([exprs, child]) => self.build_id(child).map(|c| {
                WindowExecutor {
                    exprs: self.resolve_column_index(exprs, child),
                    types: self.plan_types(exprs).to_vec(),
                }
                .execute(c)
            }),

            CreateTable(table) => CreateTableExecutor {
                table,
                storage: self.storage.clone(),
            }
            .execute()
            .into(),

            CreateView([table, query]) => CreateViewExecutor {
                table: self.node(table).as_create_table(),
                query: self.recexpr(query),
                catalog: self.catalog().clone(),
            }
            .execute()
            .into(),

            CreateFunction(f) => CreateFunctionExecutor {
                f,
                catalog: self.optimizer.catalog().clone(),
            }
            .execute()
            .into(),

            Drop(tables) => DropExecutor {
                tables: (self.node(tables).as_list().iter())
                    .map(|id| self.node(*id).as_table())
                    .collect(),
                catalog: self.catalog().clone(),
                storage: self.storage.clone(),
            }
            .execute()
            .into(),

            Insert([table, cols, child]) => InsertExecutor {
                table_id: self.node(table).as_table(),
                column_ids: (self.node(cols).as_list().iter())
                    .map(|id| self.node(*id).as_column().column_id)
                    .collect(),
                storage: self.storage.clone(),
            }
            .execute(self.build_id(child).assume_single())
            .into(),

            Delete([table, child]) => DeleteExecutor {
                table_id: self.node(table).as_table(),
                storage: self.storage.clone(),
            }
            .execute(self.build_id(child).assume_single())
            .into(),

            CopyFrom([src, types]) => CopyFromFileExecutor {
                source: self.node(src).as_ext_source(),
                types: self.node(types).as_type().as_struct().to_vec(),
            }
            .execute()
            .into(),

            CopyTo([src, child]) => CopyToFileExecutor {
                source: self.node(src).as_ext_source(),
            }
            .execute(self.build_id(child).assume_single())
            .into(),

            Explain(plan) => ExplainExecutor {
                plan: self.recexpr(plan),
                optimizer: self.optimizer.clone(),
            }
            .execute()
            .into(),

            Empty(_) => futures::stream::empty().boxed().into(),

            Exchange([dist, child]) => match self.node(dist) {
                Single => self.build_id(child).spawn_merge().into(),
                Broadcast => {
                    let subscriber = self.build_id(child).spawn();
                    let parallism = tokio::runtime::Handle::current().metrics().num_workers();
                    PartitionedStream {
                        streams: (0..parallism)
                            .map(|_| subscriber.subscribe_merge())
                            .collect(),
                    }
                }
                Hash(keys) => {
                    let parallism = tokio::runtime::Handle::current().metrics().num_workers();
                    let hash_key = (self.node(keys).as_list().iter())
                        .map(|id| self.node(*id).as_column().column_id as usize)
                        .collect_vec();
                    self.build_id(child)
                        .spawn_dispatch(|c| {
                            HashPartitionProducer {
                                num_partitions: parallism,
                                hash_key: hash_key.clone(),
                            }
                            .execute(c)
                        })
                        .subscribe()
                }
                node => panic!("invalid exchange type: {node:?}"),
            },

            node => panic!("not a plan: {node:?}"),
        }
    }

    fn build_hashjoin<const T: JoinType>(
        &self,
        [_, cond, lkey, rkey, left, right]: [Id; 6],
        l: BoxedExecutor,
        r: BoxedExecutor,
    ) -> BoxedExecutor {
        assert_eq!(self.node(cond), &Expr::true_());
        HashJoinExecutor::<T> {
            left_keys: self.resolve_column_index(lkey, left),
            right_keys: self.resolve_column_index(rkey, right),
            left_types: self.plan_types(left).to_vec(),
            right_types: self.plan_types(right).to_vec(),
        }
        .execute(l, r)
    }

    fn build_hashsemijoin(
        &self,
        [_, cond, lkeys, rkeys, left, right]: [Id; 6],
        anti: bool,
        l: BoxedExecutor,
        r: BoxedExecutor,
    ) -> BoxedExecutor {
        if self.node(cond) == &Expr::true_() {
            HashSemiJoinExecutor {
                left_keys: self.resolve_column_index(lkeys, left),
                right_keys: self.resolve_column_index(rkeys, right),
                left_types: self.plan_types(left).to_vec(),
                anti,
            }
            .execute(l, r)
        } else {
            HashSemiJoinExecutor2 {
                left_keys: self.resolve_column_index(lkeys, left),
                right_keys: self.resolve_column_index(rkeys, right),
                condition: self.resolve_column_index2(cond, left, right),
                left_types: self.plan_types(left).to_vec(),
                right_types: self.plan_types(right).to_vec(),
                anti,
            }
            .execute(l, r)
        }
    }

    fn build_mergejoin<const T: JoinType>(
        &self,
        [_, cond, lkeys, rkeys, left, right]: [Id; 6],
        l: BoxedExecutor,
        r: BoxedExecutor,
    ) -> BoxedExecutor {
        assert_eq!(self.node(cond), &Expr::true_());
        MergeJoinExecutor::<T> {
            left_keys: self.resolve_column_index(lkeys, left),
            right_keys: self.resolve_column_index(rkeys, right),
            left_types: self.plan_types(left).to_vec(),
            right_types: self.plan_types(right).to_vec(),
        }
        .execute(l, r)
    }
}

/// Spawn a new task to execute the given stream.
fn spawn(mut stream: BoxedExecutor) -> StreamSubscriber {
    let (tx, rx) = async_broadcast::broadcast(16);
    let handle = tokio::task::Builder::default()
        .spawn(async move {
            while let Some(item) = stream.next().await {
                if tx.broadcast(item).await.is_err() {
                    // all receivers are dropped, stop the task.
                    return;
                }
            }
        })
        .expect("failed to spawn task");

    StreamSubscriber {
        rx: rx.deactivate(),
        handle: Arc::new(AbortOnDropHandle(handle)),
    }
}

/// Spawn new tasks to execute the given dispatchers.
/// Dispatch the output to multiple partitions by the associated partition index.
fn spawn_dispatchers(mut streams: Vec<BoxedDispatcher>) -> PartitionedStreamSubscriber {
    let (txs, rxs): (Vec<_>, Vec<_>) = streams
        .into_iter()
        .map(|_| async_broadcast::broadcast(16))
        .unzip();
    let mut handles = Vec::with_capacity(streams.len());
    for stream in streams {
        let txs = txs.clone();
        let handle = tokio::task::Builder::default()
            .spawn(async move {
                while let Some(item) = stream.next().await {
                    match item {
                        // send the chunk to the corresponding partition (ignore error)
                        Ok((chunk, partition)) => _ = txs[partition].broadcast(Ok(chunk)).await,
                        // broadcast the error to all partitions
                        Err(e) => {
                            for tx in txs.iter() {
                                tx.broadcast(Err(e.clone())).await.unwrap();
                            }
                        }
                    }
                }
            })
            .expect("failed to spawn task");
        handles.push(handle);
    }
    PartitionedStreamSubscriber {
        subscribers: (rxs.into_iter().zip(handles))
            .map(|(rx, handle)| StreamSubscriber {
                rx: rx.deactivate(),
                handle: Arc::new(AbortOnDropHandle(handle)),
            })
            .collect(),
    }
}

/// A set of partitioned output streams.
struct PartitionedStream {
    streams: Vec<BoxedExecutor>,
}

/// Creates from a single stream.
impl From<BoxedExecutor> for PartitionedStream {
    fn from(stream: BoxedExecutor) -> Self {
        PartitionedStream {
            streams: vec![stream],
        }
    }
}

impl PartitionedStream {
    /// Assume that there is only one stream and returns it.
    fn assume_single(self) -> BoxedExecutor {
        assert_eq!(self.streams.len(), 1);
        self.streams.into_iter().next().unwrap()
    }

    /// Merges the partitioned streams into a single stream.
    fn spawn_merge(self) -> BoxedExecutor {
        futures::stream::select_all(self.spawn().subscribe().streams).boxed()
    }

    /// Maps each stream with the given function.
    fn map(self, f: impl Fn(BoxedExecutor) -> BoxedExecutor) -> PartitionedStream {
        PartitionedStream {
            streams: self.streams.into_iter().map(f).collect(),
        }
    }

    /// Zips up two sets of partitioned streams.
    fn zip(self, other: PartitionedStream) -> ZippedPartitionedStream {
        ZippedPartitionedStream {
            left: self.streams,
            right: other.streams,
        }
    }

    /// Spawns each stream with the given name.
    fn spawn(self) -> PartitionedStreamSubscriber {
        PartitionedStreamSubscriber {
            subscribers: self.streams.into_iter().map(spawn).collect(),
        }
    }

    /// Spawns each stream and dispatches the output to multiple partitions.
    fn spawn_dispatch(
        self,
        f: impl Fn(BoxedExecutor) -> BoxedDispatcher,
    ) -> PartitionedStreamSubscriber {
        spawn_dispatchers(self.streams.into_iter().map(f).collect())
    }
}

/// The return type of `PartitionedStream::zip`.
struct ZippedPartitionedStream {
    left: Vec<BoxedExecutor>,
    right: Vec<BoxedExecutor>,
}

impl ZippedPartitionedStream {
    /// Maps each stream pair with the given function.
    fn map(self, f: impl Fn(BoxedExecutor, BoxedExecutor) -> BoxedExecutor) -> PartitionedStream {
        assert_eq!(self.left.len(), self.right.len());
        PartitionedStream {
            streams: self
                .left
                .into_iter()
                .zip(self.right.into_iter())
                .map(|(l, r)| f(l, r))
                .collect(),
        }
    }
}

/// A set of partitioned stream subscribers.
struct PartitionedStreamSubscriber {
    subscribers: Vec<StreamSubscriber>,
}

impl PartitionedStreamSubscriber {
    fn subscribe(&self) -> PartitionedStream {
        PartitionedStream {
            streams: self.subscribers.iter().map(|s| s.subscribe()).collect(),
        }
    }

    fn subscribe_merge(&self) -> BoxedExecutor {
        futures::stream::select_all(self.subscribe().streams).boxed()
    }
}

/// A subscriber of an executor's output stream.
///
/// New streams can be created by calling `subscribe`.
struct StreamSubscriber {
    rx: async_broadcast::InactiveReceiver<Result<DataChunk>>,
    handle: Arc<AbortOnDropHandle>,
}

impl StreamSubscriber {
    /// Subscribes an output stream from the executor.
    fn subscribe(&self) -> BoxedExecutor {
        #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
        async fn to_stream(
            rx: async_broadcast::Receiver<Result<DataChunk>>,
            handle: Arc<AbortOnDropHandle>,
        ) {
            #[for_await]
            for chunk in rx {
                yield chunk?;
            }
            drop(handle);
        }
        to_stream(self.rx.activate_cloned(), self.handle.clone())
    }
}

/// A wrapper over `JoinHandle` that aborts the task when dropped.
struct AbortOnDropHandle(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDropHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}
