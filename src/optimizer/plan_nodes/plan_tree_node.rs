use smallvec::SmallVec;

use super::PlanRef;

/// the trait [`PlanNode`](super::PlanNode) really need about tree structure and used by optimizer
/// framework. every plan node should impl it.
///
/// the trait [`PlanTreeNodeLeaf`], [`PlanTreeNodeUnary`] and [`PlanTreeNodeBinary`], is just
/// special cases for [`PlanTreeNode`]. as long as you impl these trait for a plan node, we can
/// easily impl the [`PlanTreeNode`] which is really need by framework with helper macros
/// [`impl_plan_tree_node_for_leaf`], [`impl_plan_tree_node_for_unary`] and
/// [`impl_plan_tree_node_for_binary`].
///
/// and due to these three traits need not be used as dyn, it can return `Self` type, which is
/// useful when implement rules and visitors. So we highly recommend not impl the [`PlanTreeNode`]
/// trait directly, instead use these tree trait and impl [`PlanTreeNode`] use these helper
/// macros.
pub trait PlanTreeNode {
    /// Get child nodes of the plan.
    fn children(&self) -> SmallVec<[PlanRef; 2]>;

    /// Clone the node with a list of new children.
    fn clone_with_children(&self, children: &[PlanRef]) -> PlanRef;
}

/// See [`PlanTreeNode`](super)
pub trait PlanTreeNodeLeaf: Clone {}
/// See [`PlanTreeNode`](super)
pub trait PlanTreeNodeUnary {
    fn child(&self) -> PlanRef;
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self;
}
/// See [`PlanTreeNode`](super)
pub trait PlanTreeNodeBinary {
    fn left(&self) -> PlanRef;
    fn right(&self) -> PlanRef;

    #[must_use]
    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self;
}

macro_rules! impl_plan_tree_node_for_leaf {
    ($leaf_node_type:ident) => {
        impl crate::optimizer::plan_nodes::PlanTreeNode for $leaf_node_type {
            fn children(&self) -> smallvec::SmallVec<[crate::optimizer::plan_nodes::PlanRef; 2]> {
                smallvec::smallvec![]
            }

            /// Clone the node with a list of new children.
            fn clone_with_children(
                &self,
                children: &[crate::optimizer::plan_nodes::PlanRef],
            ) -> crate::optimizer::plan_nodes::PlanRef {
                assert_eq!(children.len(), 0);
                self.clone().plan_ref()
            }
        }
    };
}

macro_rules! impl_plan_tree_node_for_unary {
    ($unary_node_type:ident) => {
        impl crate::crate::optimizer::plan_nodes::PlanTreeNode for $unary_node_type {
            fn children(&self) -> smallvec::SmallVec<[crate::optimizer::plan_nodes::PlanRef; 2]> {
                smallvec::smallvec![self.child()]
            }

            /// Clone the node with a list of new children.
            fn clone_with_children(
                &self,
                children: &[crate::optimizer::plan_nodes::PlanRef],
            ) -> crate::optimizer::plan_nodes::PlanRef {
                assert_eq!(children.len(), 1);
                self.clone_with_child(children[0].clone()).plan_ref()
            }
        }
    };
}

macro_rules! impl_plan_tree_node_for_binary {
    ($binary_node_type:ident) => {
        impl crate::crate::optimizer::plan_nodes::PlanTreeNode for $binary_node_type {
            fn children(&self) -> smallvec::SmallVec<[crate::optimizer::plan_nodes::PlanRef; 2]> {
                smallvec::smallvec![self.left(), self.right()]
            }
            fn clone_with_children(
                &self,
                children: &[crate::optimizer::plan_nodes::PlanRef],
            ) -> crate::optimizer::plan_nodes::PlanRef {
                assert_eq!(children.len(), 2);
                self.clone_with_left_right(children[0].clone(), children[1].clone())
                    .plan_ref()
            }
        }
    };
}
