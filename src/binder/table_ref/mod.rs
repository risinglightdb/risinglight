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

#[derive(PartialEq, Clone)]
pub enum BoundJoinConstraint {
    On(BoundExpr),
}

impl std::fmt::Debug for BoundJoinConstraint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::On(expr) => write!(f, "On {:?}", expr),
        }
    }
}

#[derive(PartialEq, Clone)]
pub enum BoundJoinOperator {
    Inner(BoundJoinConstraint),
    LeftOuter(BoundJoinConstraint),
    RightOuter(BoundJoinConstraint),
}

impl std::fmt::Debug for BoundJoinOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inner(constraint) => write!(f, "Inner {:?}", constraint),
            Self::LeftOuter(constraint) => write!(f, "Left Outer {:?}", constraint),
            Self::RightOuter(constraint) => write!(f, "Right Outer {:?}", constraint),
        }
    }
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
                table_ref: (join_table.into()),
                join_op,
            };
            join_tables.push(join_ref);
        }
        Ok(BoundTableRef::JoinTableRef {
            relation: (relation.into()),
            join_tables,
        })
    }
    pub fn bind_join_op(&mut self, join_op: &JoinOperator) -> Result<BoundJoinOperator, BindError> {
        match join_op {
            JoinOperator::Inner(constraint) => {
                let constraint = self.bind_join_constraint(constraint)?;
                Ok(BoundJoinOperator::Inner(constraint))
            }
            JoinOperator::LeftOuter(constraint) => {
                let constraint = self.bind_join_constraint(constraint)?;
                Ok(BoundJoinOperator::LeftOuter(constraint))
            }
            JoinOperator::RightOuter(constraint) => {
                let constraint = self.bind_join_constraint(constraint)?;
                Ok(BoundJoinOperator::RightOuter(constraint))
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

    pub fn bind_table_ref_with_name(
        &mut self,
        database_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<BoundTableRef, BindError> {
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

    pub fn bind_table_ref(&mut self, table: &TableFactor) -> Result<BoundTableRef, BindError> {
        match table {
            TableFactor::Table { name, alias, .. } => {
                let (database_name, schema_name, mut table_name) = split_name(name)?;
                if let Some(alias) = alias {
                    table_name = &alias.name.value;
                }
                self.bind_table_ref_with_name(database_name, schema_name, table_name)
            }
            _ => panic!("bind table ref"),
        }
    }
}
