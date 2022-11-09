// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

//! Defines all plan nodes and provides tools to visit plan tree.
//!
//! This module contains a lot of macros to help generate code. Generally, developers won't need to
//! care too much about the macros, and will only need to focus on implementing new plan nodes. If
//! you have no idea it is like to implement a new plan node, just copy and paste `Dummy` plan node
//! and begin.
//!
//! The `for_all_xxx` macros are a technique in Rust to generate a given pattern of code using a
//! input parameter. There are some articles about it:
//!
//! * [Callbacks](https://adventures.michaelfbryan.com/posts/non-trivial-macros/#callbacks)
//! * [Type Exercise in Rust (Day 4)](https://github.com/skyzh/type-exercise-in-rust/blob/master/archive/day4/src/macros.rs)

use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::ops::Index;
use std::sync::Arc;

use bit_set::BitSet;
use downcast_rs::{impl_downcast, Downcast};
use erased_serde::serialize_trait_object;
use paste::paste;

use crate::types::DataType;
use crate::v1::binder::{BoundExpr, BoundInputRef, ExprVisitor};

mod plan_tree_node;
pub use plan_tree_node::*;
mod join_predicate;
pub use join_predicate::*;

// Import and use all plan nodes

mod dummy;
mod internal;
mod logical_aggregate;
mod logical_copy_from_file;
mod logical_copy_to_file;
mod logical_create_table;
mod logical_delete;
mod logical_drop;
mod logical_explain;
mod logical_filter;
mod logical_insert;
mod logical_join;
mod logical_limit;
mod logical_order;
mod logical_projection;
mod logical_table_scan;
mod logical_top_n;
mod logical_values;
mod physical_copy_from_file;
mod physical_copy_to_file;
mod physical_create_table;
mod physical_delete;
mod physical_drop;
mod physical_explain;
mod physical_filter;
mod physical_hash_agg;
mod physical_hash_join;
mod physical_insert;
mod physical_limit;
mod physical_nested_loop_join;
mod physical_order;
mod physical_projection;
mod physical_simple_agg;
mod physical_table_scan;
mod physical_top_n;
mod physical_values;

pub use dummy::*;
pub use internal::*;
pub use logical_aggregate::*;
pub use logical_copy_from_file::*;
pub use logical_copy_to_file::*;
pub use logical_create_table::*;
pub use logical_delete::*;
pub use logical_drop::*;
pub use logical_explain::*;
pub use logical_filter::*;
pub use logical_insert::*;
pub use logical_join::*;
pub use logical_limit::*;
pub use logical_order::*;
pub use logical_projection::*;
pub use logical_table_scan::*;
pub use logical_top_n::*;
pub use logical_values::*;
pub use physical_copy_from_file::*;
pub use physical_copy_to_file::*;
pub use physical_create_table::*;
pub use physical_delete::*;
pub use physical_drop::*;
pub use physical_explain::*;
pub use physical_filter::*;
pub use physical_hash_agg::*;
pub use physical_hash_join::*;
pub use physical_insert::*;
pub use physical_limit::*;
pub use physical_nested_loop_join::*;
pub use physical_order::*;
pub use physical_projection::*;
pub use physical_simple_agg::*;
pub use physical_table_scan::*;
pub use physical_top_n::*;
pub use physical_values::*;

use super::logical_plan_rewriter::ExprRewriter;
use crate::catalog::ColumnDesc;

/// The upcast trait for `PlanNode`.
pub trait IntoPlanRef {
    fn into_plan_ref(self) -> PlanRef;
    fn clone_as_plan_ref(&self) -> PlanRef;
}
/// The common trait over all plan nodes.
pub trait PlanNode:
    WithPlanNodeType
    + IntoPlanRef
    + PlanTreeNode
    + Debug
    + Display
    + Downcast
    + erased_serde::Serialize
    + Send
    + Sync
{
    /// Get schema of current plan node
    fn schema(&self) -> Vec<ColumnDesc> {
        vec![]
    }

    /// Output column types
    fn out_types(&self) -> Vec<DataType> {
        self.schema()
            .iter()
            .map(|desc| desc.datatype().clone())
            .collect()
    }

    /// Output column names
    fn out_names(&self) -> Vec<String> {
        self.schema()
            .iter()
            .map(|desc| desc.name().to_string())
            .collect()
    }

    /// Esitmated output size of the plan node
    fn estimated_cardinality(&self) -> usize {
        1
    }
    /// transform the plan node to only output the required columns ordered by index number, only
    /// logical plan node will use it, though all plan node impl it.
    fn prune_col(&self, required_cols: BitSet) -> PlanRef {
        let input_types = self.out_types();
        let mut need_prune = false;
        for i in 0..input_types.len() {
            if !required_cols.contains(i) {
                need_prune = true;
            }
        }
        if !need_prune {
            return self.clone_as_plan_ref();
        }
        let exprs = required_cols
            .iter()
            .map(|index| {
                BoundExpr::InputRef(BoundInputRef {
                    index,
                    return_type: input_types[index].clone(),
                })
            })
            .collect();
        LogicalProjection::new(exprs, self.clone_as_plan_ref()).into_plan_ref()
    }
}
impl_downcast!(PlanNode);

/// The type of reference to a plan node.
pub type PlanRef = Arc<dyn PlanNode>;

impl dyn PlanNode {
    /// Write explain string of the plan.
    pub fn explain(&self, level: usize, f: &mut dyn std::fmt::Write) -> std::fmt::Result {
        let indented_self =
            format!("{}", self).replace("\n  ", &format!("\n{}", " ".repeat(level * 2 + 4)));
        write!(f, "{}{}", " ".repeat(level * 2), indented_self)?;
        for child in self.children() {
            child.explain(level + 1, f)?;
        }
        Ok(())
    }
}

serialize_trait_object!(PlanNode);

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
            Internal,
            LogicalTableScan,
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
            LogicalTopN,
            LogicalDelete,
            LogicalCopyFromFile,
            LogicalCopyToFile,
            PhysicalTableScan,
            PhysicalInsert,
            PhysicalValues,
            PhysicalCreateTable,
            PhysicalDrop,
            PhysicalProjection,
            PhysicalFilter,
            PhysicalExplain,
            PhysicalNestedLoopJoin,
            PhysicalSimpleAgg,
            PhysicalHashAgg,
            PhysicalHashJoin,
            PhysicalOrder,
            PhysicalLimit,
            PhysicalTopN,
            PhysicalDelete,
            PhysicalCopyFromFile,
            PhysicalCopyToFile
        }
    };
}

pub trait WithPlanNodeType {
    fn node_type(&self) -> PlanNodeType;
}
macro_rules! enum_plan_node_type {
    ([], $($node_name:ident),*) => {
        /// each enum value represent a [`PlanNode`] struct type, help us to dispatch and downcast
        pub enum PlanNodeType {
            $( $node_name ),*
        }

        $(impl WithPlanNodeType for $node_name {
            fn node_type(&self) -> PlanNodeType {
                PlanNodeType::$node_name
            }
        })*
    }
}
for_all_plan_nodes! { enum_plan_node_type }

macro_rules! impl_downcast_utility {
    ([], $($node_name:ident),*) => {
        impl dyn PlanNode {
            $(
                paste! {
                    // TODO: use `Option` or custom error type.
                    #[allow(clippy::result_unit_err)]
                    pub fn [<as_ $node_name:snake>] (&self) -> std::result::Result<&$node_name, ()> {
                        self.downcast_ref::<$node_name>().ok_or_else(|| ())
                    }
                }
            )*
        }
    }
}
for_all_plan_nodes! { impl_downcast_utility }

/// impl `IntoPlanRef` for each node.
macro_rules! impl_into_plan_ref {
    ([], $($node_name:ident),*) => {
            $(impl IntoPlanRef for $node_name {
                fn into_plan_ref(self) -> PlanRef {
                    std::sync::Arc::new(self)
                }
                fn clone_as_plan_ref(&self) -> PlanRef{
                    self.clone().into_plan_ref()
                }
            })*
    }
}
for_all_plan_nodes! {impl_into_plan_ref }

struct CollectRequiredCols(BitSet);
impl ExprVisitor for CollectRequiredCols {
    fn visit_input_ref(&mut self, expr: &BoundInputRef) {
        self.0.insert(expr.index);
    }
}

struct Mapper(HashMap<usize, usize>);

impl Mapper {
    fn new_with_bitset(bitset: &BitSet) -> Self {
        let mut idx_table = HashMap::with_capacity(bitset.len());
        for (new_idx, old_idx) in bitset.iter().enumerate() {
            idx_table.insert(old_idx, new_idx);
        }
        Self(idx_table)
    }
}

impl ExprRewriter for Mapper {
    fn rewrite_input_ref(&self, expr: &mut BoundExpr) {
        match expr {
            BoundExpr::InputRef(ref mut input_ref) => {
                input_ref.index = self.0[&input_ref.index];
            }
            _ => unreachable!(),
        }
    }
}

impl Index<usize> for Mapper {
    type Output = usize;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[&index]
    }
}
