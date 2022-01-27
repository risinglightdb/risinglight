// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::plan_nodes::PlanRef;

mod exchange_rule;
mod filter_join_rule;
mod filter_scan_rule;

pub use exchange_rule::*;
pub use filter_join_rule::*;
pub use filter_scan_rule::*;

pub trait Rule: Send + Sync + 'static {
    fn apply(&self, plan: PlanRef) -> Result<PlanRef, ()>;
}

pub(super) type BoxedRule = Box<dyn Rule>;
