// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::*;
use crate::optimizer::plan_nodes::{LogicalTopN, PlanTreeNodeUnary};

pub struct LimitOrderRule {}

impl Rule for LimitOrderRule {
    fn apply(&self, plan: PlanRef) -> Result<PlanRef, ()> {
        let limit = plan.as_logical_limit()?;
        let child = limit.child();
        let order = child.as_logical_order()?.clone();
        Ok(Arc::new(LogicalTopN::new(
            limit.offset(),
            limit.limit(),
            order.comparators().to_owned(),
            order.child(),
        )))
    }
}
