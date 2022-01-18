//! Defines all plan nodes and provides tools to visit plan tree.

use std::fmt::{Debug, Display};
use std::sync::Arc;

use downcast_rs::{impl_downcast, Downcast};
use paste::paste;

use crate::binder::BoundExpr;
use crate::types::DataType;
#[macro_use]
mod plan_tree_node;
pub use plan_tree_node::*;

/// The common trait over all plan nodes.
pub trait PlanNode:
    WithPlanNodeType + PlanTreeNode + Debug + Display + Downcast + Send + Sync
{
    fn out_types(&self) -> Vec<DataType> {
        vec![]
    }
}
impl_downcast!(PlanNode);

/// The type of reference to a plan node.
pub type PlanRef = Arc<dyn PlanNode>;

impl dyn PlanNode {
    /// Write explain string of the plan.
    pub fn explain(&self, level: usize, f: &mut dyn std::fmt::Write) -> std::fmt::Result {
        write!(f, "{}{}", " ".repeat(level * 2), self)?;
        for child in self.children() {
            child.explain(level + 1, f)?;
        }
        Ok(())
    }
}

/// All Plan nodes
///
/// You can use it as follows:
///
/// ```rust
/// macro_rules! use_plan {
///     ([], $($node_name:ty),*) => {};
/// }
/// risinglight::for_all_plan_nodes! { use_plan }
/// ```
#[macro_export]
macro_rules! for_all_plan_nodes {
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*],
            Dummy,
            LogicalTableScan,
            LogicalInsert,
            LogicalValues,
            LogicalCreateTable,
            LogicalDrop,
            LogicalProjection,
            LogicalFilter,
            LogicalExplain,
            LogicalJoin,
            LogicalAggregate,
            LogicalOrder,
            LogicalLimit,
            LogicalDelete,
            LogicalCopyFromFile,
            LogicalCopyToFile,
            PhysicalTableScan,
            PhysicalInsert,
            PhysicalValues,
            PhysicalCreateTable,
            PhysicalDrop,
            PhysicalProjection,
            PhysicalFilter,
            PhysicalExplain,
            PhysicalNestedLoopJoin,
            PhysicalSimpleAgg,
            PhysicalHashAgg,
            PhysicalHashJoin,
            PhysicalOrder,
            PhysicalLimit,
            PhysicalDelete,
            PhysicalCopyFromFile,
            PhysicalCopyToFile
        }
    };
}

/// Define module for each node.
macro_rules! def_mod_and_use {
    ([], $($node_name:ty),*) => {
        $(paste! {
            mod [<$node_name:snake>];
            pub use [<$node_name:snake>]::*;
        })*
    }
}
for_all_plan_nodes! { def_mod_and_use }

pub trait WithPlanNodeType {
    fn node_type(&self) -> PlanNodeType;
}
macro_rules! enum_plan_node_type {
    ([], $($node_name:ident),*) => {
        /// each enum value represent a [`PlanNode`] struct type, help us to dispatch and downcast
        pub enum PlanNodeType {
            $( $node_name ),*
        }

        $(impl WithPlanNodeType for $node_name {
            fn node_type(&self) -> PlanNodeType {
                PlanNodeType::$node_name
            }
        })*
    }
}
for_all_plan_nodes! { enum_plan_node_type }

macro_rules! impl_downcast_utility {
    ([], $($node_name:ident),*) => {
        impl dyn PlanNode {
            $(
                paste! {
                    // TODO: use `Option` or custom error type.
                    #[allow(clippy::result_unit_err)]
                    pub fn [<as_ $node_name:snake>] (&self) -> std::result::Result<&$node_name, ()> {
                        self.downcast_ref::<$node_name>().ok_or_else(|| ())
                    }
                }
            )*
        }
    }
}
for_all_plan_nodes! { impl_downcast_utility }
