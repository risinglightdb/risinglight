use std::rc::Rc;

pub use dummy::*;
pub mod dummy;
pub use logical_values::*;
pub mod logical_aggregate;
pub use logical_seq_scan::*;
pub mod logical_copy_from_file;
pub use logical_projection::*;
pub mod logical_copy_to_file;
pub use logical_order::*;
pub mod logical_create_table;
pub use logical_limit::*;
pub mod logical_delete;
pub use logical_join::*;
pub mod logical_drop;
pub use logical_insert::*;
pub mod logical_explain;
pub use logical_filter::*;
pub mod logical_filter;
pub use logical_explain::*;
pub mod logical_insert;
pub use logical_drop::*;
pub mod logical_join;
pub use logical_delete::*;
pub mod logical_limit;
pub use logical_create_table::*;
pub mod logical_order;
pub use logical_copy_to_file::*;
pub mod logical_projection;
pub use logical_copy_from_file::*;
pub mod logical_seq_scan;
pub use logical_aggregate::*;
pub mod logical_values;

pub trait LogicalPlanNode {
    fn children(&self) -> Vec<LogicalPlanRef>;
    fn clone_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef;
}

/// All LogicalPlan nodes
///
/// You can use it as follows:
///
/// ```rust
/// macro_rules! use_logical_plan {
///     ([], $({ $node_name:ident, $node_type:ty }),*) => {};
/// }
/// risinglight::for_all_plan_nodes! { use_logical_plan }
/// ```
#[macro_export]
macro_rules! for_all_plan_nodes {
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*],
            { Dummy, Dummy },
            { LogicalSeqScan, LogicalSeqScan },
            { LogicalInsert, LogicalInsert },
            { LogicalValues, LogicalValues },
            { LogicalCreateTable, LogicalCreateTable },
            { LogicalDrop, LogicalDrop },
            { LogicalProjection, LogicalProjection },
            { LogicalFilter, LogicalFilter },
            { LogicalExplain, LogicalExplain },
            { LogicalJoin, LogicalJoin },
            { LogicalAggregate, LogicalAggregate },
            { LogicalOrder, LogicalOrder },
            { LogicalLimit, LogicalLimit },
            { LogicalDelete, LogicalDelete },
            { LogicalCopyFromFile, LogicalCopyFromFile },
            { LogicalCopyToFile, LogicalCopyToFile }
        }
    };
}

/// An enumeration which record all necessary information of execution plan,
/// which will be used by optimizer and executor.
macro_rules! logical_plan_enum {
    ([], $( { $node_name:ident, $node_type:ty } ),*) => {
        /// `LogicalPlan` embeds all possible plans in `logical_plan` module.
        #[derive(Debug, PartialEq, Clone)]
        pub enum LogicalPlan {
            $( $node_name($node_type) ),*
        }
    };
}

for_all_plan_nodes! {logical_plan_enum}

pub type LogicalPlanRef = Rc<LogicalPlan>;

macro_rules! impl_plan_node {
    ([], $( { $node_name:ident, $node_type:ty } ),*) => {
        /// Implement `LogicalPlan` for the structs.
        impl LogicalPlan {
            pub fn children(&self) -> Vec<LogicalPlanRef> {
                match self {
                    $( Self::$node_name(inner) => inner.children(),)*
                }
            }
            pub fn clone_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
                match self {
                    $( Self::$node_name(inner) => inner.clone_with_children(children),)*
                }
            }
        }

        $(
            impl From<$node_type> for LogicalPlan {
                fn from(plan: $node_type) -> Self {
                    Self::$node_name(plan)
                }
            }

            impl TryFrom<LogicalPlan> for $node_type {
                type Error = ();

                fn try_from(plan: LogicalPlan) -> Result<Self, Self::Error> {
                    match plan {
                        LogicalPlan::$node_name(plan) => Ok(plan),
                        _ => Err(()),
                    }
                }
            }

            impl<'a> TryFrom<&'a LogicalPlan> for &'a $node_type {
                type Error = ();

                fn try_from(plan: &'a LogicalPlan) -> Result<Self, Self::Error> {
                    match plan {
                        LogicalPlan::$node_name(plan) => Ok(plan),
                        _ => Err(()),
                    }
                }
            }
        )*
    }
}

for_all_plan_nodes! { impl_plan_node }

pub(super) trait LeafLogicalPlanNode: Clone {}
macro_rules! impl_plan_node_for_leaf {
    ($leaf_node_type:ident) => {
        impl LogicalPlanNode for $leaf_node_type {
            fn children(&self) -> Vec<LogicalPlanRef> {
                vec![]
            }

            fn clone_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
                assert!(children.is_empty());
                LogicalPlan::$leaf_node_type(self.clone()).into()
            }
        }
    };
}
pub(super) trait UnaryLogicalPlanNode {
    fn child(&self) -> LogicalPlanRef;
    fn clone_with_child(&self, child: LogicalPlanRef) -> LogicalPlanRef;
}
macro_rules! impl_plan_node_for_unary {
    ($unary_node_type:ident) => {
        impl LogicalPlanNode for $unary_node_type {
            fn children(&self) -> Vec<LogicalPlanRef> {
                vec![self.child()]
            }

            fn clone_with_children(&self, mut children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
                assert_eq!(children.len(), 1);
                self.clone_with_child(children.pop().unwrap())
            }
        }
    };
}

pub trait BinaryLogicalPlanNode {
    fn left(&self) -> LogicalPlanRef;
    fn right(&self) -> LogicalPlanRef;
    fn clone_with_left_right(&self, left: LogicalPlanRef, right: LogicalPlanRef) -> LogicalPlanRef;
}

macro_rules! impl_plan_node_for_binary {
    ($binary_node_type:ident) => {
        impl LogicalPlanNode for $binary_node_type {
            fn children(&self) -> Vec<LogicalPlanRef> {
                vec![self.left(), self.right()]
            }

            fn clone_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
                assert_eq!(children.len(), 2);
                let mut iter = children.into_iter();
                self.clone_with_left_right(iter.next().unwrap(), iter.next().unwrap())
            }
        }
    };
}

impl_plan_node_for_leaf! {Dummy}
impl_plan_node_for_leaf! {LogicalCreateTable}
impl_plan_node_for_leaf! {LogicalDrop}
impl_plan_node_for_leaf! {LogicalSeqScan}
impl_plan_node_for_leaf! {LogicalValues}
impl_plan_node_for_leaf! {LogicalCopyFromFile}

impl_plan_node_for_unary! {LogicalInsert}
impl_plan_node_for_unary! {LogicalAggregate}
impl_plan_node_for_unary! {LogicalProjection}
impl_plan_node_for_unary! {LogicalFilter}
impl_plan_node_for_unary! {LogicalOrder}
impl_plan_node_for_unary! {LogicalExplain}
impl_plan_node_for_unary! {LogicalLimit}
impl_plan_node_for_unary! {LogicalDelete}
impl_plan_node_for_unary! {LogicalCopyToFile}

impl_plan_node_for_binary! {LogicalJoin}
