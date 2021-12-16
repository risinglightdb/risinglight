use super::{PlanRef, Rule};
use crate::binder::BoundExpr;
use crate::logical_optimizer::plan_nodes::{BinaryPlanNode, LogicalJoin, UnaryPlanNode};
use crate::logical_optimizer::BoundBinaryOp;
use crate::logical_optimizer::BoundJoinConstraint::On;
use crate::logical_optimizer::BoundJoinOperator::Inner;
use crate::parser::BinaryOperator::And;
use crate::types::{DataTypeExt, DataTypeKind};

pub struct FilterJoinRule {}
impl Rule for FilterJoinRule {
    fn matches(&self, plan: PlanRef) -> Result<(), ()> {
        let filter = plan.as_ref().try_as_logicalfilter()?;
        let filter_child = filter.child();
        let join = filter_child.as_ref().try_as_logicaljoin()?;
        // TODO: we just support inner join now.
        match join.join_op {
            Inner(_) => Ok(()),
            _ => Err(()),
        }
    }
    fn apply(&self, plan: PlanRef) -> Result<PlanRef, ()> {
        let filter = plan.as_ref().try_as_logicalfilter()?;
        let filter_child = filter.child();
        let join = filter_child.as_ref().try_as_logicaljoin()?;
        let join_cond = match &join.join_op {
            Inner(On(op)) => op.clone(),
            _ => unreachable!(),
        };
        let join_cond = BoundExpr::BinaryOp(BoundBinaryOp {
            op: And,
            left_expr: Box::new(join_cond),
            right_expr: Box::new(filter.expr.clone()),
            return_type: Some(DataTypeKind::Boolean.nullable()),
        });
        Ok(LogicalJoin {
            left_plan: join.left(),
            right_plan: join.right(),
            join_op: Inner(On(join_cond)),
        }
        .into())

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
