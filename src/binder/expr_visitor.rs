// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::expression::*;
use crate::types::DataValue;

pub trait ExprVisitor {
    fn visit_expr(&mut self, expr: &BoundExpr) {
        match expr {
            BoundExpr::Constant(v) => self.visit_constant(v),
            BoundExpr::ColumnRef(expr) => self.visit_column_ref(expr),
            BoundExpr::InputRef(expr) => self.visit_input_ref(expr),
            BoundExpr::BinaryOp(expr) => self.visit_binary_op(expr),
            BoundExpr::UnaryOp(expr) => self.visit_unary_op(expr),
            BoundExpr::TypeCast(expr) => self.visit_type_cast(expr),
            BoundExpr::AggCall(expr) => self.visit_agg_call(expr),
            BoundExpr::IsNull(expr) => self.visit_is_null(expr),
            BoundExpr::ExprWithAlias(expr) => self.visit_expr_with_alias(expr),
            BoundExpr::Alias(expr) => self.visit_alias(expr),
        }
    }

    fn visit_constant(&mut self, _: &DataValue) {}

    fn visit_column_ref(&mut self, _: &BoundColumnRef) {}

    fn visit_input_ref(&mut self, _: &BoundInputRef) {}

    fn visit_binary_op(&mut self, expr: &BoundBinaryOp) {
        self.visit_expr(expr.left_expr.as_ref());
        self.visit_expr(expr.right_expr.as_ref());
    }

    fn visit_unary_op(&mut self, expr: &BoundUnaryOp) {
        self.visit_expr(expr.expr.as_ref());
    }

    fn visit_type_cast(&mut self, expr: &BoundTypeCast) {
        self.visit_expr(expr.expr.as_ref());
    }

    fn visit_agg_call(&mut self, agg: &BoundAggCall) {
        for arg in &agg.args {
            self.visit_expr(arg);
        }
    }

    fn visit_is_null(&mut self, expr: &BoundIsNull) {
        self.visit_expr(expr.expr.as_ref());
    }

    fn visit_expr_with_alias(&mut self, expr: &BoundExprWithAlias) {
        self.visit_expr(expr.expr.as_ref());
    }

    fn visit_alias(&mut self, _: &BoundAlias) {}
}

pub trait ExprRewriter {
    fn rewrite_expr(&self, expr: &mut BoundExpr) {
        match expr {
            BoundExpr::Constant(_) => self.rewrite_constant(expr),
            BoundExpr::ColumnRef(_) => self.rewrite_column_ref(expr),
            BoundExpr::InputRef(_) => self.rewrite_input_ref(expr),
            BoundExpr::BinaryOp(_) => self.rewrite_binary_op(expr),
            BoundExpr::UnaryOp(_) => self.rewrite_unary_op(expr),
            BoundExpr::TypeCast(_) => self.rewrite_type_cast(expr),
            BoundExpr::AggCall(_) => self.rewrite_agg_call(expr),
            BoundExpr::IsNull(_) => self.rewrite_is_null(expr),
            BoundExpr::ExprWithAlias(_) => self.rewrite_expr_with_alias(expr),
            BoundExpr::Alias(_) => self.rewrite_alias(expr),
        }
    }

    fn rewrite_constant(&self, _: &mut BoundExpr) {}

    fn rewrite_column_ref(&self, _: &mut BoundExpr) {}

    fn rewrite_input_ref(&self, _: &mut BoundExpr) {}

    fn rewrite_binary_op(&self, expr: &mut BoundExpr) {
        match expr {
            BoundExpr::BinaryOp(expr) => {
                self.rewrite_expr(expr.left_expr.as_mut());
                self.rewrite_expr(expr.right_expr.as_mut());
            }
            _ => unreachable!(),
        }
    }

    fn rewrite_unary_op(&self, expr: &mut BoundExpr) {
        match expr {
            BoundExpr::UnaryOp(expr) => self.rewrite_expr(expr.expr.as_mut()),
            _ => unreachable!(),
        }
    }

    fn rewrite_type_cast(&self, expr: &mut BoundExpr) {
        match expr {
            BoundExpr::TypeCast(expr) => self.rewrite_expr(expr.expr.as_mut()),
            _ => unreachable!(),
        }
    }

    fn rewrite_agg_call(&self, agg: &mut BoundExpr) {
        match agg {
            BoundExpr::AggCall(agg) => {
                for arg in &mut agg.args {
                    self.rewrite_expr(arg);
                }
            }
            _ => unreachable!(),
        }
    }

    fn rewrite_is_null(&self, expr: &mut BoundExpr) {
        match expr {
            BoundExpr::IsNull(expr) => self.rewrite_expr(expr.expr.as_mut()),
            _ => unreachable!(),
        }
    }

    fn rewrite_expr_with_alias(&self, expr: &mut BoundExpr) {
        match expr {
            BoundExpr::ExprWithAlias(expr) => self.rewrite_expr(expr.expr.as_mut()),
            _ => unreachable!(),
        }
    }

    fn rewrite_alias(&self, _: &mut BoundExpr) {}
}
