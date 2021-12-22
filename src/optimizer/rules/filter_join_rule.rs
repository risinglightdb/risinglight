use super::*;
use crate::binder::BoundExpr;
use crate::optimizer::plan_nodes::{LogicalFilter, LogicalJoin};
use crate::optimizer::BoundBinaryOp;
use crate::optimizer::BoundJoinConstraint::On;
use crate::optimizer::BoundJoinOperator::Inner;
use crate::parser::BinaryOperator::And;
use crate::types::{DataTypeExt, DataTypeKind};

pub struct FilterJoinRule {}

impl Rule for FilterJoinRule {
    fn apply(&self, plan: PlanRef) -> Result<PlanRef, ()> {
        let filter = plan.downcast_rc::<LogicalFilter>().map_err(|_| ())?;
        let join = filter
            .child
            .clone()
            .downcast_rc::<LogicalJoin>()
            .map_err(|_| ())?;
        let join_cond = match &join.join_op {
            Inner(On(op)) => op.clone(),
            _ => return Err(()),
        };
        let join_cond = BoundExpr::BinaryOp(BoundBinaryOp {
            op: And,
            left_expr: Box::new(join_cond),
            right_expr: Box::new(filter.expr.clone()),
            return_type: Some(DataTypeKind::Boolean.nullable()),
        });
        Ok(Rc::new(LogicalJoin::new(
            join.left_plan.clone(),
            join.right_plan.clone(),
            Inner(On(join_cond)),
        )))

        // TODO: we need schema of operator to push condition to each side.
        // let filter_conds = to_cnf(filter.expr.clone());
        // let join_cond = match join.join_op {
        //     Inner(On(op)) => op.clone(),
        //     _ => unreachable!(),
        // };
        // let join_conds = to_cnf(join_cond);
        // let left_filter_expr = vec![];
        // let right_filter_expr = vec![];
        // let join_filter_expr = vec![];

        // for cond in filter_conds.into_iter().chain(join_conds.into_iter()) {
        //     let input_refs = input_col_refs(&cond);
        //     let in_left = false;
        //     let in_right = false;
        //     for index in input_refs.iter() {
        //         if(index <=)
        //     }
        // }
    }
}
