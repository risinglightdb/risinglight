use itertools::Itertools;

use crate::logical_planner::LogicalAggregate;
use crate::logical_planner::LogicalCopyFromFile;
use crate::logical_planner::LogicalCopyToFile;
use crate::logical_planner::LogicalCreateTable;
use crate::logical_planner::LogicalDelete;
use crate::logical_planner::LogicalDrop;
use crate::logical_planner::LogicalExplain;
use crate::logical_planner::LogicalFilter;
use crate::logical_planner::LogicalInsert;
use crate::logical_planner::LogicalJoin;
use crate::logical_planner::LogicalLimit;
use crate::logical_planner::LogicalOrder;
use crate::logical_planner::LogicalPlan;
use crate::logical_planner::LogicalPlanRef;
use crate::logical_planner::LogicalProjection;
use crate::logical_planner::LogicalSeqScan;
use crate::logical_planner::LogicalValues;
trait LogicalPlanNode {
    fn get_children(&self) -> Vec<LogicalPlanRef>;
    fn copy_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef;
}

// TODO: refactor with macro
impl LogicalPlan {
    fn get_children(&self) -> Vec<LogicalPlanRef> {
        match self {
            LogicalPlan::Dummy => vec![],
            LogicalPlan::CreateTable(plan) => plan.get_children(),
            LogicalPlan::Drop(plan) => plan.get_children(),
            LogicalPlan::Insert(plan) => plan.get_children(),
            LogicalPlan::Join(plan) => plan.get_children(),
            LogicalPlan::SeqScan(plan) => plan.get_children(),
            LogicalPlan::Projection(plan) => plan.get_children(),
            LogicalPlan::Filter(plan) => plan.get_children(),
            LogicalPlan::Order(plan) => plan.get_children(),
            LogicalPlan::Limit(plan) => plan.get_children(),
            LogicalPlan::Explain(plan) => plan.get_children(),
            LogicalPlan::Aggregate(plan) => plan.get_children(),
            LogicalPlan::Delete(plan) => plan.get_children(),
            LogicalPlan::Values(plan) => plan.get_children(),
            LogicalPlan::CopyFromFile(plan) => plan.get_children(),
            LogicalPlan::CopyToFile(plan) => plan.get_children(),
        }
    }

    fn copy_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
        match self {
            LogicalPlan::Dummy => LogicalPlan::Dummy.into(),
            LogicalPlan::CreateTable(plan) => plan.copy_with_children(children),
            LogicalPlan::Drop(plan) => plan.copy_with_children(children),
            LogicalPlan::Insert(plan) => plan.copy_with_children(children),
            LogicalPlan::Join(plan) => plan.copy_with_children(children),
            LogicalPlan::SeqScan(plan) => plan.copy_with_children(children),
            LogicalPlan::Projection(plan) => plan.copy_with_children(children),
            LogicalPlan::Filter(plan) => plan.copy_with_children(children),
            LogicalPlan::Order(plan) => plan.copy_with_children(children),
            LogicalPlan::Limit(plan) => plan.copy_with_children(children),
            LogicalPlan::Explain(plan) => plan.copy_with_children(children),
            LogicalPlan::Aggregate(plan) => plan.copy_with_children(children),
            LogicalPlan::Delete(plan) => plan.copy_with_children(children),
            LogicalPlan::Values(plan) => plan.copy_with_children(children),
            LogicalPlan::CopyFromFile(plan) => plan.copy_with_children(children),
            LogicalPlan::CopyToFile(plan) => plan.copy_with_children(children),
        }
    }
}
impl LogicalPlanNode for LogicalSeqScan {
    fn get_children(&self) -> Vec<LogicalPlanRef> {
        vec![]
    }

    fn copy_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
        assert!(children.is_empty());
        LogicalPlan::SeqScan(self.clone()).into()
    }
}

impl LogicalPlanNode for LogicalInsert {
    fn get_children(&self) -> Vec<LogicalPlanRef> {
        vec![self.child.clone()]
    }

    fn copy_with_children(&self, mut children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
        assert!(children.len() == 1);
        LogicalPlan::Insert(LogicalInsert {
            child: std::mem::take(&mut children[1]),
            table_ref_id: self.table_ref_id,
            column_ids: self.column_ids.clone(),
        })
        .into()
    }
}
impl LogicalPlanNode for LogicalValues {
    fn get_children(&self) -> Vec<LogicalPlanRef> {
        vec![]
    }

    fn copy_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
        assert!(children.is_empty());
        LogicalPlan::Values(self.clone()).into()
    }
}

impl LogicalPlanNode for LogicalCreateTable {
    fn get_children(&self) -> Vec<LogicalPlanRef> {
        vec![]
    }

    fn copy_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
        assert!(children.is_empty());
        LogicalPlan::CreateTable(self.clone()).into()
    }
}

impl LogicalPlanNode for LogicalDrop {
    fn get_children(&self) -> Vec<LogicalPlanRef> {
        vec![]
    }

    fn copy_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
        assert!(children.is_empty());
        LogicalPlan::Drop(self.clone()).into()
    }
}

impl LogicalPlanNode for LogicalProjection {
    fn get_children(&self) -> Vec<LogicalPlanRef> {
        vec![self.child.clone()]
    }

    fn copy_with_children(&self, mut children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
        assert!(children.len() == 1);
        LogicalPlan::Projection(LogicalProjection {
            child: std::mem::take(&mut children[1]),
            project_expressions: self.project_expressions.clone(),
        })
        .into()
    }
}

impl LogicalPlanNode for LogicalFilter {
    fn get_children(&self) -> Vec<LogicalPlanRef> {
        vec![self.child.clone()]
    }

    fn copy_with_children(&self, mut children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
        assert!(children.len() == 1);
        LogicalPlan::Filter(LogicalFilter {
            child: std::mem::take(&mut children[1]),
            expr: self.expr.clone(),
        })
        .into()
    }
}

impl LogicalPlanNode for LogicalExplain {
    fn get_children(&self) -> Vec<LogicalPlanRef> {
        vec![self.plan.clone()]
    }

    fn copy_with_children(&self, mut children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
        assert!(children.len() == 1);
        LogicalPlan::Explain(LogicalExplain {
            plan: std::mem::take(&mut children[1]),
        })
        .into()
    }
}

impl LogicalPlanNode for LogicalJoin {
    fn get_children(&self) -> Vec<LogicalPlanRef> {
        vec![self.relation_plan.clone()]
            .into_iter()
            .chain(
                self.join_table_plans
                    .iter()
                    .map(|join_table| join_table.table_plan.clone()),
            )
            .collect_vec()
    }

    fn copy_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
        let mut children_iter = children.into_iter();
        let relation_plan = children_iter.next().unwrap();
        let mut join_table_plans = self.join_table_plans.clone();
        join_table_plans
            .iter_mut()
            .zip_eq(children_iter)
            .for_each(|(join_table, table_plan)| {
                join_table.table_plan = table_plan;
            });
        LogicalPlan::Join(LogicalJoin {
            relation_plan,
            join_table_plans,
        })
        .into()
    }
}

impl LogicalPlanNode for LogicalAggregate {
    fn get_children(&self) -> Vec<LogicalPlanRef> {
        vec![self.child.clone()]
    }

    fn copy_with_children(&self, mut children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
        assert!(children.len() == 1);
        LogicalPlan::Aggregate(LogicalAggregate {
            child: std::mem::take(&mut children[1]),
            agg_calls: self.agg_calls.clone(),
            group_keys: self.group_keys.clone(),
        })
        .into()
    }
}

impl LogicalPlanNode for LogicalOrder {
    fn get_children(&self) -> Vec<LogicalPlanRef> {
        vec![self.child.clone()]
    }

    fn copy_with_children(&self, mut children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
        assert!(children.len() == 1);
        LogicalPlan::Order(LogicalOrder {
            child: std::mem::take(&mut children[1]),
            comparators: self.comparators.clone(),
        })
        .into()
    }
}

impl LogicalPlanNode for LogicalLimit {
    fn get_children(&self) -> Vec<LogicalPlanRef> {
        vec![self.child.clone()]
    }

    fn copy_with_children(&self, mut children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
        assert!(children.len() == 1);
        LogicalPlan::Limit(LogicalLimit {
            child: std::mem::take(&mut children[1]),
            offset: self.offset,
            limit: self.limit,
        })
        .into()
    }
}

impl LogicalPlanNode for LogicalDelete {
    fn get_children(&self) -> Vec<LogicalPlanRef> {
        vec![self.filter.child.clone()]
    }

    fn copy_with_children(&self, mut children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
        assert!(children.len() == 1);
        LogicalPlan::Delete(LogicalDelete {
            table_ref_id: self.table_ref_id,
            filter: LogicalFilter {
                child: std::mem::take(&mut children[1]),
                expr: self.filter.expr.clone(),
            },
        })
        .into()
    }
}

impl LogicalPlanNode for LogicalCopyFromFile {
    fn get_children(&self) -> Vec<LogicalPlanRef> {
        vec![]
    }

    fn copy_with_children(&self, children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
        assert!(children.is_empty());
        LogicalPlan::CopyFromFile(self.clone()).into()
    }
}

impl LogicalPlanNode for LogicalCopyToFile {
    fn get_children(&self) -> Vec<LogicalPlanRef> {
        vec![self.child.clone()]
    }

    fn copy_with_children(&self, mut children: Vec<LogicalPlanRef>) -> LogicalPlanRef {
        assert!(children.len() == 1);
        LogicalPlan::CopyToFile(LogicalCopyToFile {
            path: self.path.clone(),
            format: self.format.clone(),
            column_types: self.column_types.clone(),
            child: std::mem::take(&mut children[1]),
        })
        .into()
    }
}
