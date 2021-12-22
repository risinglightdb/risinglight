//! Defines all plan nodes and provides tools to visit plan tree.

use std::fmt::{Debug, Display};
use std::rc::Rc;

use downcast_rs::{impl_downcast, Downcast};
use paste::paste;
use smallvec::SmallVec;

use crate::binder::BoundExpr;
use crate::types::DataType;

/// The common trait over all plan nodes.
pub trait PlanNode: PlanTreeNode + Debug + Display + Downcast {
    /// Call [`rewrite_expr`] on each expressions of the plan.
    ///
    /// [`rewrite_expr`]: Rewriter::rewrite_expr
    fn rewrite_expr(&mut self, _rewriter: &mut dyn Rewriter) {}
    fn out_types(&self) -> Vec<DataType> {
        vec![]
    }
}

impl_downcast!(PlanNode);

/// The type of reference to a plan node.$
pub type PlanRef = Rc<dyn PlanNode>;

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

pub trait PlanTreeNode {
    /// Get child nodes of the plan.
    fn children(&self) -> SmallVec<[PlanRef; 2]>;

    /// Clone the node with a list of new children.
    fn clone_with_children(&self, children: &[PlanRef]) -> PlanRef;

    /// Walk through the plan tree recursively.
    fn accept(&self, visitor: &mut dyn Visitor);

    /// Rewrite the plan tree recursively.
    fn rewrite(&self, rewriter: &mut dyn Rewriter) -> PlanRef;
}

/// Implement `PlanNode` trait for a plan node structure.
macro_rules! impl_plan_tree_node {
    ($type:ident) => {
        impl_plan_tree_node!($type,[]);
    };
    ($type:ident, [$($child:ident),*] $($fn:tt)*) => {
        impl PlanTreeNode for $type {
            fn children(&self) -> SmallVec<[PlanRef; 2]> {
                smallvec::smallvec![$(self.$child.clone()),*]
            }
            #[allow(unused_mut)]
            fn clone_with_children(&self, children: &[PlanRef]) -> PlanRef {
                let mut iter = children.iter();
                let mut new = self.clone();
                $(
                    new.$child = iter.next().expect("invalid children number").clone();
                )*
                assert!(iter.next().is_none(), "invalid children number");
                Rc::new(new)
            }
            fn accept(&self, visitor: &mut dyn Visitor) {
                if paste! { !visitor.[<visit_ $type:snake _is_nested>]() } {
                    $(
                        self.$child.accept(visitor);
                    )*
                }
                paste! { visitor.[<visit_ $type:snake>](self); }
            }
            fn rewrite(&self, rewriter: &mut dyn Rewriter) -> PlanRef {
                let mut new = self.clone();
                if paste! { !rewriter.[<rewrite_ $type:snake _is_nested>]() } {
                    $(
                        new.$child = self.$child.rewrite(rewriter);
                    )*
                    new.rewrite_expr(rewriter);
                }
                paste! { rewriter.[<rewrite_ $type:snake>](new) }
            }
            $($fn)*
        }
    };
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
            LogicalSeqScan,
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
            PhysicalSeqScan,
            PhysicalInsert,
            PhysicalValues,
            PhysicalCreateTable,
            PhysicalDrop,
            PhysicalProjection,
            PhysicalFilter,
            PhysicalExplain,
            PhysicalJoin,
            PhysicalSimpleAgg,
            PhysicalHashAgg,
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

/// Define `Visitor` trait.
macro_rules! def_visitor {
    ([], $($node_name:ty),*) => {
        /// The visitor for plan nodes.
        ///
        /// Call `plan.walk(&mut visitor)` to walk through the plan tree.
        pub trait Visitor {
            $(paste! {
                #[doc = "Whether the `" [<rewrite_ $node_name:snake>] "` function is nested."]
                ///
                /// If returns `false`, this function will be called after rewriting its children.
                /// If returns `true`, this function should rewrite children by itself.
                fn [<visit_ $node_name:snake _is_nested>](&mut self) -> bool {
                    false
                }
                #[doc = "Visit [`" $node_name "`] itself (is_nested = false) or nested nodes (is_nested = true)."]
                ///
                /// The default implementation is empty.
                fn [<visit_ $node_name:snake>](&mut self, _plan: &$node_name) {}
            })*
        }
    }
}
for_all_plan_nodes! { def_visitor }

/// Define `Rewriter` trait.
macro_rules! def_rewriter {
    ([], $($node_name:ty),*) => {
        /// Rewrites a plan tree into another.
        ///
        /// Call `plan.rewrite(&mut rewriter)` to rewrite the plan tree.
        pub trait Rewriter {
            fn rewrite_expr(&mut self, _expr: &mut BoundExpr) {}
            $(paste! {
                #[doc = "Whether the `" [<rewrite_ $node_name:snake>] "` function is nested."]
                ///
                /// If returns `false`, this function will be called after rewriting its children.
                /// If returns `true`, this function should rewrite children by itself.
                fn [<rewrite_ $node_name:snake _is_nested>](&mut self) -> bool {
                    false
                }
                #[doc = "Visit [`" $node_name "`] and return a new plan node."]
                ///
                /// The default implementation is to return `plan` directly.
                fn [<rewrite_ $node_name:snake>](&mut self, plan: $node_name) -> PlanRef {
                    Rc::new(plan)
                }
            })*
        }
    }
}
for_all_plan_nodes! { def_rewriter }
