use super::LogicalPlanRef;
use super::Rule;
use crate::binder::BoundExpr;
use crate::logical_optimizer::plan_nodes::{
    try_as_logicalfilter, try_as_logicaljoin, BinaryLogicalPlanNode, LogicalJoin,
    UnaryLogicalPlanNode,
};
use crate::logical_optimizer::BoundBinaryOp;
use crate::logical_optimizer::BoundJoinConstraint::On;
use crate::logical_optimizer::BoundJoinOperator::Inner;
use crate::parser::BinaryOperator::And;
use crate::types::{DataTypeExt, DataTypeKind};

pub struct FilterJoinRule {}
impl Rule for FilterJoinRule {
    fn matches(&self, plan: LogicalPlanRef) -> bool {
        let filter = match try_as_logicalfilter(plan.as_ref()) {
            Some(filter) => filter,
            _ => return false,
        };
        let filter_child = filter.child();
        let join = match try_as_logicaljoin(filter_child.as_ref()) {
            Some(join) => join,
            _ => return false,
        };
        // TODO: we just support inner join now.
        match join.join_op {
            Inner(_) => true,
            _ => false,
        }
    }
    fn apply(&self, plan: LogicalPlanRef) -> LogicalPlanRef {
        let filter = try_as_logicalfilter(plan.as_ref()).unwrap();
        let filter_child = filter.child();
        let join = try_as_logicaljoin(filter_child.as_ref()).unwrap();
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
        LogicalJoin {
            left_plan: join.left(),
            right_plan: join.right(),
            join_op: Inner(On(join_cond)),
        }
        .into()

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
