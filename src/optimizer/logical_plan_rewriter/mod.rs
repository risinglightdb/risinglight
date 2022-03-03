// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::plan_nodes::*;

mod arith_expr_simplification;
mod bool_expr_simplification;
mod constant_folding;
mod constant_moving;
mod convert_physical;
mod input_ref_resolver;

pub use arith_expr_simplification::*;
pub use bool_expr_simplification::*;
pub use constant_folding::*;
pub use constant_moving::*;
pub use convert_physical::*;
pub use input_ref_resolver::*;
use itertools::Itertools;
use paste::paste;

pub use crate::binder::ExprRewriter;
use crate::for_all_plan_nodes;

macro_rules! def_rewriter {
  ([], $($node:ident),*) => {

    /// it's kind of like a [`PlanVisitor<PlanRef>`](super::PlanVisitor), but with default behaviour of each rewrite method
    pub trait PlanRewriter {
    paste! {
      fn rewrite(&mut self, plan: PlanRef) -> PlanRef{
        match plan.node_type() {
        $(
          PlanNodeType::$node => self.[<rewrite_ $node:snake>](plan.downcast_ref::<$node>().unwrap()),
        )*
        }
      }

      $(
        #[doc = "Visit [`" $node "`] , the function should rewrite the children."]
        fn [<rewrite_ $node:snake>](&mut self, plan: &$node) -> PlanRef {
          let new_children = plan
          .children()
          .into_iter()
          .map(|child| self.rewrite(child.clone()))
          .collect_vec();
          plan.clone_with_children(&new_children)
        }
      )*
      }
    }
  }
}
for_all_plan_nodes! { def_rewriter }
