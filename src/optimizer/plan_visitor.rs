// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use paste::paste;

use super::plan_nodes::*;
use crate::for_all_plan_nodes;

/// Define `PlanVisitor` trait.
macro_rules! def_visitor {
  ([], $($node:ident),*) => {
    /// The visitor for plan nodes. visit all children and return the ret value of the left most child.
    pub trait PlanVisitor<R> {
    paste! {
      fn visit(&mut self, plan: PlanRef) -> Option<R>{
        match plan.node_type() {
        $(
          crate::optimizer::plan_nodes::PlanNodeType::$node => self.[<visit_ $node:snake>](plan.downcast_ref::<$node>().unwrap()),
        )*
        }
      }

      $(
        #[doc = "Visit [`" $node "`] , the function should visit the children."]
        fn [<visit_ $node:snake>](&mut self, plan: &$node) -> Option<R> {
          let children = plan.children();
          if children.is_empty() {
              return None
          }
          let mut iter = plan.children().into_iter();
          let ret = self.visit(iter.next().unwrap());
          iter.for_each(|child| {self.visit(child);});
          ret
        }
      )*
      }
    }
  }
}
for_all_plan_nodes! { def_visitor }
