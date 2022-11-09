// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;

use serde::Serialize;

use super::*;
use crate::v1::binder::BoundAggCall;

/// The physical plan of simple aggregation.
#[derive(Debug, Clone, Serialize)]
pub struct PhysicalSimpleAgg {
    agg_calls: Vec<BoundAggCall>,
    child: PlanRef,
}

impl PhysicalSimpleAgg {
    pub fn new(agg_calls: Vec<BoundAggCall>, child: PlanRef) -> Self {
        PhysicalSimpleAgg { agg_calls, child }
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
    fn schema(&self) -> Vec<ColumnDesc> {
        self.agg_calls
            .iter()
            .map(|agg_call| {
                agg_call
                    .return_type
                    .clone()
                    .to_column(format!("{}", agg_call.kind))
            })
            .collect()
    }

    fn estimated_cardinality(&self) -> usize {
        self.child().estimated_cardinality()
    }
}

impl fmt::Display for PhysicalSimpleAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalSimpleAgg:")?;
        for agg in self.agg_calls().iter() {
            writeln!(f, "  {}", agg)?
        }
        Ok(())
    }
}
