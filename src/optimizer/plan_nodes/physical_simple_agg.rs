// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;
use serde::{Serialize};
use super::*;
use crate::binder::BoundAggCall;

/// The physical plan of simple aggregation.
#[derive(Debug, Clone, Serialize)]
pub struct PhysicalSimpleAgg {
    agg_calls: Vec<BoundAggCall>,
    child: PlanRef,
    data_types: Vec<DataType>,
}

impl PhysicalSimpleAgg {
    pub fn new(agg_calls: Vec<BoundAggCall>, child: PlanRef) -> Self {
        let data_types = agg_calls
            .iter()
            .map(|agg_call| agg_call.return_type.clone())
            .collect();
        PhysicalSimpleAgg {
            agg_calls,
            child,
            data_types,
        }
    }

    /// Get a reference to the logical aggregate's agg calls.
    pub fn agg_calls(&self) -> &[BoundAggCall] {
        self.agg_calls.as_ref()
    }
}
impl PlanTreeNodeUnary for PhysicalSimpleAgg {
    fn child(&self) -> PlanRef {
        self.child.clone()
    }
    #[must_use]
    fn clone_with_child(&self, child: PlanRef) -> Self {
        Self::new(self.agg_calls().to_vec(), child)
    }
}
impl_plan_tree_node_for_unary!(PhysicalSimpleAgg);
impl PlanNode for PhysicalSimpleAgg {
    fn out_types(&self) -> Vec<DataType> {
        self.data_types.clone()
    }
}

impl fmt::Display for PhysicalSimpleAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalSimpleAgg: {} agg calls", self.agg_calls.len(),)
    }
}
