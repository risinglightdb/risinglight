use std::fmt;
use std::fmt::Display;
use std::rc::Rc;

pub use dummy::*;
use paste::paste;
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
pub mod physical_aggregate;
pub mod physical_copy;
pub mod physical_create;
pub mod physical_delete;
pub mod physical_drop;
pub mod physical_explain;
pub mod physical_filter;
pub mod physical_insert;
pub mod physical_join;
pub mod physical_limit;
pub mod physical_order;
pub mod physical_projection;
pub mod physical_seq_scan;
pub use physical_aggregate::*;
pub use physical_copy::*;
pub use physical_create::*;
pub use physical_delete::*;
pub use physical_drop::*;
pub use physical_explain::*;
pub use physical_filter::*;
pub use physical_insert::*;
pub use physical_join::*;
pub use physical_limit::*;
pub use physical_order::*;
pub use physical_projection::*;
pub use physical_seq_scan::*;

pub trait PlanTreeNode {
    fn children(&self) -> Vec<PlanRef>;
    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef;
}

pub trait PlanNode: PlanTreeNode + std::fmt::Display {}
/// All Plan nodes
///
/// You can use it as follows:
///
/// ```rust
/// macro_rules! use_plan {
///     ([], $({ $node_name:ident, $node_type:ty }),*) => {};
/// }
/// risinglight::for_all_plan_nodes! { use_plan }
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
            { LogicalCopyToFile, LogicalCopyToFile },
            { PhysicalSeqScan, PhysicalSeqScan},
            { PhysicalInsert, PhysicalInsert},
            { PhysicalValues, PhysicalValues},
            { PhysicalCreateTable, PhysicalCreateTable},
            { PhysicalDrop, PhysicalDrop},
            { PhysicalProjection, PhysicalProjection},
            { PhysicalFilter, PhysicalFilter},
            { PhysicalExplain, PhysicalExplain},
            { PhysicalJoin, PhysicalJoin},
            { PhysicalSimpleAgg, PhysicalSimpleAgg},
            { PhysicalHashAgg, PhysicalHashAgg},
            { PhysicalOrder, PhysicalOrder},
            { PhysicalLimit, PhysicalLimit},
            { PhysicalDelete, PhysicalDelete},
            { PhysicalCopyFromFile, PhysicalCopyFromFile},
            { PhysicalCopyToFile, PhysicalCopyToFile}
        }
    };
}

/// An enumeration which record all necessary information of execution plan,
/// which will be used by optimizer and executor.
macro_rules! plan_enum {
    ([], $( { $node_name:ident, $node_type:ty } ),*) => {
        /// `Plan` embeds all possible plans in `plam_nodes` module.
        #[derive(Debug, PartialEq, Clone)]
        pub enum Plan {
            $( $node_name($node_type) ),*
        }
    };
}

for_all_plan_nodes! {plan_enum}

pub type PlanRef = Rc<Plan>;

macro_rules! impl_plan_node {
    ([], $( { $node_name:ident, $node_type:ty } ),*) => {
        /// Implement `Plan` for the structs.
        impl Plan {
            pub fn fmt_node(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $( Self::$node_name(inner) => inner.fmt(f),)*
                }
            }

            pub fn children(&self) -> Vec<PlanRef> {
                match self {
                    $( Self::$node_name(inner) => inner.children(),)*
                }
            }
            pub fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
                match self {
                    $( Self::$node_name(inner) => inner.clone_with_children(children),)*
                }
            }

            $(
                paste! {
                    #[allow(dead_code)]
                    pub fn [<try_as_ $node_name:lower>]<'a>(&'a self) -> Result<&'a $node_name, ()> {
                        self.try_into() as Result<&'a $node_name, ()>
                    }
                }
            )*

        }

        $(
            impl From<$node_type> for Plan {
                fn from(plan: $node_type) -> Self {
                    Self::$node_name(plan)
                }
            }

            impl From<$node_type> for PlanRef {
                fn from(plan: $node_type) -> PlanRef {
                    Rc::new(plan.into())
                }
            }

            impl TryFrom<Plan> for $node_type {
                type Error = ();

                fn try_from(plan: Plan) -> Result<Self, Self::Error> {
                    match plan {
                        Plan::$node_name(plan) => Ok(plan),
                        _ => Err(()),
                    }
                }
            }

            impl<'a> TryFrom<&'a Plan> for &'a $node_type {
                type Error = ();

                fn try_from(plan: &'a Plan) -> Result<Self, Self::Error> {
                    match plan {
                        Plan::$node_name(plan) => Ok(plan),
                        _ => Err(()),
                    }
                }
            }
        )*
    }
}
for_all_plan_nodes! { impl_plan_node }

impl fmt::Display for Plan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.explain(0, f)
    }
}

impl Plan {
    fn explain(&self, level: usize, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", " ".repeat(level * 2))?;
        self.fmt(f)?;
        for child in self.children() {
            child.explain(level + 1, f)?;
        }
        Ok(())
    }
}

pub(super) trait LeafPlanNode: Clone {}
macro_rules! impl_plan_tree_node_for_leaf {
    ($leaf_node_type:ident) => {
        impl PlanTreeNode for $leaf_node_type {
            fn children(&self) -> Vec<PlanRef> {
                vec![]
            }

            fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
                assert!(children.is_empty());
                Plan::$leaf_node_type(self.clone()).into()
            }
        }
    };
}

use impl_plan_tree_node_for_leaf;

pub(super) trait UnaryPlanNode {
    fn child(&self) -> PlanRef;
    fn clone_with_child(&self, child: PlanRef) -> PlanRef;
}
macro_rules! impl_plan_tree_node_for_unary {
    ($unary_node_type:ident) => {
        impl PlanTreeNode for $unary_node_type {
            fn children(&self) -> Vec<PlanRef> {
                vec![self.child()]
            }

            fn clone_with_children(&self, mut children: Vec<PlanRef>) -> PlanRef {
                assert_eq!(children.len(), 1);
                self.clone_with_child(children.pop().unwrap())
            }
        }
    };
}
use impl_plan_tree_node_for_unary;

pub trait BinaryPlanNode {
    fn left(&self) -> PlanRef;
    fn right(&self) -> PlanRef;
    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> PlanRef;
}

macro_rules! impl_plan_tree_node_for_binary {
    ($binary_node_type:ident) => {
        impl PlanTreeNode for $binary_node_type {
            fn children(&self) -> Vec<PlanRef> {
                vec![self.left(), self.right()]
            }

            fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
                assert_eq!(children.len(), 2);
                let mut iter = children.into_iter();
                self.clone_with_left_right(iter.next().unwrap(), iter.next().unwrap())
            }
        }
    };
}
use impl_plan_tree_node_for_binary;
