// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use sqlparser::ast::BinaryOperator;

use super::*;
use crate::binder::*;
use crate::optimizer::plan_nodes::{
    IntoPlanRef, LogicalAggregate, LogicalProjection, PlanTreeNodeUnary,
};

/// Eliminate Common Sub-expressions In Projection Operator
pub struct ProjectEliminateCSE {}
/// Eliminate Common Sub-expressions In Aggregate Function Arguments
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

/// Search: Record the number of occurrences of each expression
/// Collect: Create new projection expression list
/// Replace: Replaces the expression binding of the current operator
#[derive(PartialEq, Debug)]
enum EliminatorState {
    Search,
    Collect,
    Replace,
    End,
}

/// `count`: the number of occurrences of the expression.
/// `new_projection_index`: the index of new projection expressions list.
/// an expressions that can be eliminated must be deterministic and have no side effects.
/// like randow(), now() function, it's not satisfied.
struct SubExprNode {
    count: usize,
    // for now, just use is_none(), when we have HashMap, some() will be used.
    new_projection_index: Option<usize>,
}

impl SubExprNode {
    fn new() -> Self {
        SubExprNode {
            count: 1,
            new_projection_index: None,
        }
    }
}

/// This is a helper struct for eliminate common sub-expressions
/// Does not eliminate common aggregate functions
struct CSEEliminator {
    exprs_maps: Vec<BoundExpr>, // TODO(lokax): HashMap<BoundExpr, SubExprNode>
    subexprs: Vec<SubExprNode>,
    new_projection_exprs: Vec<BoundExpr>,
    state: EliminatorState,
}

impl CSEEliminator {
    fn new() -> Self {
        Self {
            exprs_maps: vec![],
            subexprs: vec![],
            new_projection_exprs: vec![],
            state: EliminatorState::Search,
        }
    }

    fn find_common_expressions(&mut self, expr: &BoundExpr) {
        if let Some(idx) = self.exprs_maps.iter().position(|e| e == expr) {
            self.subexprs[idx].count += 1;
        } else {
            self.exprs_maps.push(expr.clone());
            self.subexprs.push(SubExprNode::new());
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
            let subexpr = &mut self.subexprs[idx];
            if subexpr.count > 1 && subexpr.new_projection_index.is_none() {
                self.new_projection_exprs.push(expr.clone());
                subexpr.new_projection_index = Some(self.new_projection_exprs.len() - 1);
                return true;
            }
            return subexpr.count > 1;
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
                if self.subexprs.iter().any(|sub_expr| sub_expr.count > 1) {
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

    fn new_projection_exprs(self) -> Vec<BoundExpr> {
        assert_eq!(self.state, EliminatorState::End);
        self.new_projection_exprs
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
        // the input expression are alawys collected
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
                // example: SELECT expr1 + expr3, expr3
                // first try collect expr1 + expr3.
                // if the return value is false, try collect expr1 and expr3
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
