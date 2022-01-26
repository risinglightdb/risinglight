// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;
use serde::{Serialize};
use super::*;

/// A dummy plan.
#[derive(Debug, Clone, Serialize)]
pub struct Dummy {}
impl PlanTreeNodeLeaf for Dummy {}
impl_plan_tree_node_for_leaf!(Dummy);
impl PlanNode for Dummy {}
impl fmt::Display for Dummy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Dummy:")
    }
}
