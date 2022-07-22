// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use sqlparser::ast::BinaryOperator;

use super::*;
use crate::binder::*;
use crate::optimizer::plan_nodes::{
    IntoPlanRef, LogicalAggregate, LogicalProjection, PlanTreeNodeUnary,
};

pub struct ProjectEliminateCSE {}
pub struct AggregateEliminateCSE {}

impl Rule for ProjectEliminateCSE {
    fn apply(&self, plan: PlanRef) -> Result<PlanRef, ()> {
        let projection = plan.as_logical_projection()?;
        let mut cse_eliminator = CSEEliminator::new();
        let mut proj_exprs = projection.project_expressions().to_vec();
        // Search Phase
        cse_eliminator.search(proj_exprs.iter());

        if !cse_eliminator.can_collect() {
            // No common sub-expressions
            return Ok(plan);
        }

        // Collect Phase
        cse_eliminator.collect(proj_exprs.iter());

        // Rewrite Phase
        cse_eliminator.replace(proj_exprs.iter_mut());

        let child =
            LogicalProjection::new(cse_eliminator.new_projection_exprs(), projection.child());
        Ok(LogicalProjection::new(proj_exprs, child.into_plan_ref()).into_plan_ref())
    }
}

impl Rule for AggregateEliminateCSE {
    fn apply(&self, plan: PlanRef) -> Result<PlanRef, ()> {
        let agg = plan.as_logical_aggregate()?;
        let mut cse_eliminator = CSEEliminator::new();
        let mut group_keys = agg.group_keys().to_vec();
        let mut agg_calls = agg.agg_calls().to_vec();
        // Search Phase
        let exprs_iter = group_keys
            .iter()
            .chain(agg_calls.iter().flat_map(|calls| calls.args.iter()));
        cse_eliminator.search(exprs_iter.clone());

        if !cse_eliminator.can_collect() {
            // No common sub-expressions
            return Ok(plan);
        }

        // Collect Phase
        cse_eliminator.collect(exprs_iter);

        let exprs_iter = group_keys
            .iter_mut()
            .chain(agg_calls.iter_mut().flat_map(|calls| calls.args.iter_mut()));
        cse_eliminator.replace(exprs_iter);

        let child = LogicalProjection::new(cse_eliminator.new_projection_exprs(), agg.child());
        Ok(LogicalAggregate::new(agg_calls, group_keys, child.into_plan_ref()).into_plan_ref())
    }
}

#[derive(PartialEq, Debug)]
enum EliminatorState {
    Search,
    Collect,
    Replace,
    End,
}

struct CSEEliminator {
    exprs_maps: Vec<BoundExpr>, // TODO: HashMap
    counts: Vec<(usize, bool)>,
    new_projection_exprs: Vec<BoundExpr>,
    state: EliminatorState,
}

impl CSEEliminator {
    fn new() -> Self {
        Self {
            exprs_maps: vec![],
            counts: vec![],
            new_projection_exprs: vec![],
            state: EliminatorState::Search,
        }
    }

    fn find_common_expressions(&mut self, expr: &BoundExpr) {
        if let Some(idx) = self.exprs_maps.iter().position(|e| e == expr) {
            self.counts[idx].0 += 1;
        } else {
            self.exprs_maps.push(expr.clone());
            self.counts.push((1, false));
        }
    }

    fn collect_new_input_ref_exprs(&mut self, expr: &BoundExpr) {
        if !self
            .new_projection_exprs
            .iter()
            .any(|proj_expr| proj_expr == expr)
        {
            self.new_projection_exprs.push(expr.clone());
        }
    }

    fn collect_new_proj_exprs(&mut self, expr: &BoundExpr) -> bool {
        if let Some(idx) = self
            .exprs_maps
            .iter()
            .position(|proj_expr| proj_expr == expr)
        {
            let (count, occupied) = self.counts[idx];
            if count > 1 && !occupied {
                self.new_projection_exprs.push(expr.clone());
                self.counts[idx].1 = true;
            }
            return count > 1;
        }
        false
    }

    fn replace_expr(&self, expr: &mut BoundExpr) -> bool {
        assert_eq!(self.state, EliminatorState::Replace);
        if let Some(idx) = self
            .new_projection_exprs
            .iter()
            .position(|proj_expr| proj_expr == expr)
        {
            *expr = BoundExpr::InputRef(BoundInputRef {
                index: idx,
                return_type: expr.return_type().unwrap(),
            });
            true
        } else {
            false
        }
    }

    fn advance_state(&mut self) {
        match &mut self.state {
            state @ EliminatorState::Search => {
                if self.counts.iter().any(|(count, _)| count > &1) {
                    *state = EliminatorState::Collect;
                } else {
                    *state = EliminatorState::End;
                }
            }
            state @ EliminatorState::Collect => *state = EliminatorState::Replace,
            state @ EliminatorState::Replace => *state = EliminatorState::End,
            _ => unreachable!(),
        }
    }

    fn new_projection_exprs(&mut self) -> Vec<BoundExpr> {
        assert_eq!(self.state, EliminatorState::End);
        std::mem::take(&mut self.new_projection_exprs)
    }

    fn can_collect(&self) -> bool {
        self.state == EliminatorState::Collect
    }

    // Search and extract common sub-expressions
    fn search<'a>(&mut self, exprs: impl Iterator<Item = &'a BoundExpr>) {
        assert_eq!(self.state, EliminatorState::Search);
        exprs.for_each(|expr| self.visit_expr(expr));
        self.advance_state();
    }

    // Collect new projection expressions for child projection operator
    fn collect<'a>(&mut self, exprs: impl Iterator<Item = &'a BoundExpr>) {
        assert_eq!(self.state, EliminatorState::Collect);
        assert!(self.can_collect());
        exprs.for_each(|expr| self.visit_expr(expr));
        self.advance_state();
    }
    // Rewrite current operator's expressions
    fn replace<'a>(&mut self, exprs: impl Iterator<Item = &'a mut BoundExpr>) {
        assert_eq!(self.state, EliminatorState::Replace);
        exprs.for_each(|expr| self.rewrite_expr(expr));
        self.advance_state();
    }
}

impl ExprVisitor for CSEEliminator {
    fn visit_input_ref(&mut self, expr: &BoundInputRef) {
        if let EliminatorState::Collect = self.state {
            self.collect_new_input_ref_exprs(&BoundExpr::InputRef(expr.clone()))
        }
    }

    fn visit_binary_op(&mut self, expr: &BoundBinaryOp) {
        match expr.op {
            // Keep short-circuit to avoid unnecessary calculations
            BinaryOperator::And | BinaryOperator::Or => {
                if let EliminatorState::Search = self.state {
                    self.visit_expr(expr.left_expr.as_ref());
                    return;
                }
            }
            _ => (),
        }

        match self.state {
            EliminatorState::Search => {
                self.find_common_expressions(&BoundExpr::BinaryOp(expr.clone()))
            }
            EliminatorState::Collect => {
                if self.collect_new_proj_exprs(&BoundExpr::BinaryOp(expr.clone())) {
                    return;
                }
            }
            _ => (),
        }

        self.visit_expr(expr.left_expr.as_ref());
        self.visit_expr(expr.right_expr.as_ref());
    }

    fn visit_unary_op(&mut self, expr: &BoundUnaryOp) {
        match self.state {
            EliminatorState::Search => {
                self.find_common_expressions(&BoundExpr::UnaryOp(expr.clone()))
            }
            EliminatorState::Collect => {
                if self.collect_new_proj_exprs(&BoundExpr::UnaryOp(expr.clone())) {
                    return;
                }
            }
            _ => (),
        }
        self.visit_expr(expr.expr.as_ref());
    }

    fn visit_type_cast(&mut self, expr: &BoundTypeCast) {
        match self.state {
            EliminatorState::Search => {
                self.find_common_expressions(&BoundExpr::TypeCast(expr.clone()))
            }
            EliminatorState::Collect => {
                if self.collect_new_proj_exprs(&BoundExpr::TypeCast(expr.clone())) {
                    return;
                }
            }
            _ => (),
        }
        self.visit_expr(expr.expr.as_ref());
    }

    fn visit_is_null(&mut self, expr: &BoundIsNull) {
        match self.state {
            EliminatorState::Search => {
                self.find_common_expressions(&BoundExpr::IsNull(expr.clone()))
            }
            EliminatorState::Collect => {
                if self.collect_new_proj_exprs(&BoundExpr::IsNull(expr.clone())) {
                    return;
                }
            }
            _ => (),
        }
        self.visit_expr(expr.expr.as_ref());
    }
}

impl ExprRewriter for CSEEliminator {
    fn rewrite_input_ref(&self, expr: &mut BoundExpr) {
        match expr {
            BoundExpr::InputRef(_) => {
                self.replace_expr(expr);
            }
            _ => unreachable!(),
        }
    }

    fn rewrite_binary_op(&self, expr: &mut BoundExpr) {
        if self.replace_expr(expr) {
            return;
        }
        match expr {
            BoundExpr::BinaryOp(e) => {
                self.rewrite_expr(e.left_expr.as_mut());
                self.rewrite_expr(e.right_expr.as_mut());
            }
            _ => unreachable!(),
        }
    }

    fn rewrite_unary_op(&self, expr: &mut BoundExpr) {
        if self.replace_expr(expr) {
            return;
        }
        match expr {
            BoundExpr::UnaryOp(e) => {
                self.rewrite_expr(e.expr.as_mut());
            }
            _ => unreachable!(),
        }
    }

    fn rewrite_type_cast(&self, expr: &mut BoundExpr) {
        if self.replace_expr(expr) {
            return;
        }
        match expr {
            BoundExpr::TypeCast(e) => {
                self.rewrite_expr(e.expr.as_mut());
            }
            _ => unreachable!(),
        }
    }

    fn rewrite_is_null(&self, expr: &mut BoundExpr) {
        if self.replace_expr(expr) {
            return;
        }
        match expr {
            BoundExpr::IsNull(e) => {
                self.rewrite_expr(e.expr.as_mut());
            }
            _ => unreachable!(),
        }
    }
}
