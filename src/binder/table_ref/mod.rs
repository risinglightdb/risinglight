use super::*;
use crate::parser::{JoinConstraint, JoinOperator, TableFactor, TableWithJoins};
use std::vec::Vec;

#[derive(Debug, PartialEq, Clone)]
pub struct BoundedSingleJoinTableRef {
    pub table_ref: Box<BoundTableRef>,
    pub join_op: BoundJoinOperator,
}
/// A bound table reference.
#[derive(Debug, PartialEq, Clone)]
pub enum BoundTableRef {
    BaseTableRef {
        ref_id: TableRefId,
        table_name: String,
        column_ids: Vec<ColumnId>,
    },
    JoinTableRef {
        relation: Box<BoundTableRef>,
        join_tables: Vec<BoundedSingleJoinTableRef>,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum BoundJoinConstraint {
    On(BoundExpr),
}
#[derive(Debug, PartialEq, Clone)]
pub enum BoundJoinOperator {
    Inner(BoundJoinConstraint),
}

impl Binder {
    pub fn bind_table_with_joins(
        &mut self,
        table_with_joins: &TableWithJoins,
    ) -> Result<BoundTableRef, BindError> {
        let relation = self.bind_table_ref(&table_with_joins.relation)?;
        let mut join_tables = vec![];
        for join in table_with_joins.joins.iter() {
            let join_table = self.bind_table_ref(&join.relation)?;
            let join_op = self.bind_join_op(&join.join_operator)?;
            let join_ref = BoundedSingleJoinTableRef {
                table_ref: Box::new(join_table),
                join_op,
            };
            join_tables.push(join_ref);
        }
        Ok(BoundTableRef::JoinTableRef {
            relation: Box::new(relation),
            join_tables,
        })
    }
    pub fn bind_join_op(&mut self, join_op: &JoinOperator) -> Result<BoundJoinOperator, BindError> {
        match join_op {
            JoinOperator::Inner(constraint) => {
                let constraint = self.bind_join_constraint(constraint)?;
                Ok(BoundJoinOperator::Inner(constraint))
            }
            _ => todo!("Support more join types"),
        }
    }

    pub fn bind_join_constraint(
        &mut self,
        join_constraint: &JoinConstraint,
    ) -> Result<BoundJoinConstraint, BindError> {
        match join_constraint {
            JoinConstraint::On(expr) => {
                let expr = self.bind_expr(expr)?;
                Ok(BoundJoinConstraint::On(expr))
            }
            _ => todo!("Support more join constraints"),
        }
    }

    pub fn bind_table_ref(&mut self, table: &TableFactor) -> Result<BoundTableRef, BindError> {
        match table {
            TableFactor::Table { name, alias, .. } => {
                let (database_name, schema_name, mut table_name) = split_name(name)?;
                if let Some(alias) = alias {
                    table_name = &alias.name.value;
                }
                if self.context.regular_tables.contains_key(table_name) {
                    return Err(BindError::DuplicatedTableName(table_name.into()));
                }

                let ref_id = self
                    .catalog
                    .get_table_id_by_name(database_name, schema_name, table_name)
                    .ok_or_else(|| BindError::InvalidTable(table_name.into()))?;
                self.context
                    .regular_tables
                    .insert(table_name.into(), ref_id);
                self.context
                    .column_names
                    .insert(table_name.into(), HashSet::new());
                self.context
                    .column_ids
                    .insert(table_name.into(), Vec::new());
                let base_table_ref = BoundTableRef::BaseTableRef {
                    ref_id,
                    table_name: table_name.into(),
                    column_ids: vec![],
                };
                self.base_table_refs.push(table_name.into());
                Ok(base_table_ref)
            }
            _ => panic!("bind table ref"),
        }
    }
}
