use std::fmt;

use super::{impl_plan_tree_node_for_unary, Plan, PlanRef, PlanTreeNode, UnaryLogicalPlanNode};
use crate::binder::{BoundAggCall, BoundExpr};

/// The physical plan of simple aggregation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalSimpleAgg {
    pub agg_calls: Vec<BoundAggCall>,
    pub child: PlanRef,
}

/// The physical plan of hash aggregation.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalHashAgg {
    pub agg_calls: Vec<BoundAggCall>,
    pub group_keys: Vec<BoundExpr>,
    pub child: PlanRef,
}

impl UnaryLogicalPlanNode for PhysicalSimpleAgg {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }

    fn clone_with_child(&self, child: PlanRef) -> PlanRef {
        Plan::PhysicalSimpleAgg(PhysicalSimpleAgg {
            child,
            agg_calls: self.agg_calls.clone(),
        })
        .into()
    }
}
impl_plan_tree_node_for_unary! {PhysicalSimpleAgg}

impl UnaryLogicalPlanNode for PhysicalHashAgg {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }

    fn clone_with_child(&self, child: PlanRef) -> PlanRef {
        Plan::PhysicalHashAgg(PhysicalHashAgg {
            child,
            agg_calls: self.agg_calls.clone(),
            group_keys: self.group_keys.clone(),
        })
        .into()
    }
}
impl_plan_tree_node_for_unary! {PhysicalHashAgg}

impl fmt::Display for PhysicalHashAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalHashAgg: {} agg calls", self.agg_calls.len(),)
    }
}
impl fmt::Display for PhysicalSimpleAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalHashAgg: {} agg calls", self.agg_calls.len(),)
    }
}
