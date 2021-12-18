//! Defines all plan nodes and provides tools to visit plan tree.

use std::fmt::{Debug, Display};
use std::rc::Rc;

use downcast_rs::{impl_downcast, Downcast};
use paste::paste;
use smallvec::SmallVec;

use crate::binder::BoundExpr;

pub trait PlanNode: Debug + Display + Downcast {
    fn children(&self) -> SmallVec<[PlanRef; 2]>;
    fn clone_with_children(&self, children: &[PlanRef]) -> PlanRef;
    fn visit(&self, visitor: &mut dyn Visitor);
    fn walk(&self, visitor: &mut dyn Visitor);
    fn rewrite(&self, rewriter: &mut dyn Rewriter) -> PlanRef;
    fn rewrite_expr(&mut self, _rewriter: &mut dyn Rewriter) {}
}

impl_downcast!(PlanNode);

pub type PlanRef = Rc<dyn PlanNode>;

impl dyn PlanNode {
    pub fn explain(&self, level: usize, f: &mut dyn std::fmt::Write) -> std::fmt::Result {
        write!(f, "{}{}", " ".repeat(level * 2), self)?;
        for child in self.children() {
            child.explain(level + 1, f)?;
        }
        Ok(())
    }
}

/// Implement `PlanNode` trait for a plan node structure.
macro_rules! impl_plan_node {
    ($type:ident) => {
        impl_plan_node!($type,[]);
    };
    ($type:ident, [$($child:ident),*] $($fn:tt)*) => {
        impl PlanNode for $type {
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
            fn visit(&self, visitor: &mut dyn Visitor) {
                paste! { visitor.[<visit_ $type:snake>](self); }
            }
            fn walk(&self, visitor: &mut dyn Visitor) {
                if paste! { !visitor.[<visit_ $type:snake _is_nested>]() } {
                    $(
                        self.$child.walk(visitor);
                    )*
                }
                self.visit(visitor);
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

macro_rules! mod_and_use {
    ([], $($node_name:ty),*) => {
        $(paste! {
            mod [<$node_name:snake>];
            pub use [<$node_name:snake>]::*;
        })*
    }
}
for_all_plan_nodes! { mod_and_use }

macro_rules! def_visitor {
    ([], $($node_name:ty),*) => {
        pub trait Visitor {
            $(paste! {
                /// Whether the associated visit function is nested.
                ///
                /// If returns false, the associated visit function will be called after visiting its children.
                /// If returns true, the associated visit function should visit children by itself.
                fn [<visit_ $node_name:snake _is_nested>](&mut self) -> bool {
                    false
                }
                /// Visit [`$node_name`] itself (is_nested = false) or nested nodes (is_nested = true).
                ///
                /// The default implementation is empty.
                fn [<visit_ $node_name:snake>](&mut self, _plan: &$node_name) {}
            })*
        }
    }
}
for_all_plan_nodes! { def_visitor }

macro_rules! def_rewriter {
    ([], $($node_name:ty),*) => {
        pub trait Rewriter {
            fn rewrite_expr(&mut self, _expr: &mut BoundExpr) {}
            $(paste! {
                /// Whether the associated rewrite function is nested.
                ///
                /// If returns false, the associated rewrite function will be called after rewriting its children.
                /// If returns true, the associated rewrite function should rewrite children by itself.
                fn [<rewrite_ $node_name:snake _is_nested>](&mut self) -> bool {
                    false
                }
                /// Visit [`$node_name`] and return a new plan node.
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
