// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::vec::Vec;

use serde::Serialize;

use super::BoundExpr::*;
use super::*;
use crate::catalog::INTERNAL_SCHEMA_NAME;
use crate::parser::{JoinConstraint, JoinOperator, TableFactor, TableWithJoins};
use crate::types::DataValue::Bool;

#[derive(Debug, PartialEq, Clone)]
pub struct BoundedSingleJoinTableRef {
    pub table_ref: Box<BoundTableRef>,
    pub join_op: BoundJoinOperator,
    pub join_cond: BoundExpr,
}

/// A bound table reference.
#[derive(Debug, PartialEq, Clone)]
pub enum BoundTableRef {
    BaseTableRef {
        ref_id: TableRefId,
        table_name: String,
        column_ids: Vec<ColumnId>,
        column_descs: Vec<ColumnDesc>,
        is_internal: bool,
    },
    JoinTableRef {
        relation: Box<BoundTableRef>,
        join_tables: Vec<BoundedSingleJoinTableRef>,
    },
}

#[derive(PartialEq, Eq, Clone, Copy, Serialize)]
pub enum BoundJoinOperator {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

impl std::fmt::Debug for BoundJoinOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inner => write!(f, "Inner"),
            Self::LeftOuter => write!(f, "Left Outer"),
            Self::RightOuter => write!(f, "Right Outer"),
            Self::FullOuter => write!(f, "Full Outer"),
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
        for join in &table_with_joins.joins {
            let join_table = self.bind_table_ref(&join.relation)?;
            let (join_op, join_cond) = self.bind_join_op(&join.join_operator)?;
            let join_ref = BoundedSingleJoinTableRef {
                table_ref: (join_table.into()),
                join_op,
                join_cond,
            };
            join_tables.push(join_ref);
        }
        Ok(BoundTableRef::JoinTableRef {
            relation: (relation.into()),
            join_tables,
        })
    }
    pub fn bind_join_op(
        &mut self,
        join_op: &JoinOperator,
    ) -> Result<(BoundJoinOperator, BoundExpr), BindError> {
        match join_op {
            JoinOperator::Inner(constraint) => {
                let condition = self.bind_join_constraint(constraint)?;
                Ok((BoundJoinOperator::Inner, condition))
            }
            JoinOperator::LeftOuter(constraint) => {
                let condition = self.bind_join_constraint(constraint)?;
                Ok((BoundJoinOperator::LeftOuter, condition))
            }
            JoinOperator::RightOuter(constraint) => {
                let condition = self.bind_join_constraint(constraint)?;
                Ok((BoundJoinOperator::RightOuter, condition))
            }
            JoinOperator::FullOuter(constraint) => {
                let condition = self.bind_join_constraint(constraint)?;
                Ok((BoundJoinOperator::FullOuter, condition))
            }
            JoinOperator::CrossJoin => Ok((BoundJoinOperator::Inner, Constant(Bool(true)))),
            _ => todo!("Support more join types"),
        }
    }

    pub fn bind_join_constraint(
        &mut self,
        join_constraint: &JoinConstraint,
    ) -> Result<BoundExpr, BindError> {
        match join_constraint {
            JoinConstraint::On(expr) => {
                let expr = self.bind_expr(expr)?;
                Ok(expr)
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
            return Err(BindError::DuplicatedTable(table_name.into()));
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
        self.context
            .column_descs
            .insert(table_name.into(), Vec::new());
        let base_table_ref = BoundTableRef::BaseTableRef {
            ref_id,
            table_name: table_name.into(),
            column_ids: vec![],
            column_descs: vec![],
            is_internal: schema_name == INTERNAL_SCHEMA_NAME,
        };
        self.base_table_refs.push(table_name.into());
        Ok(base_table_ref)
    }

    pub fn bind_table_ref(&mut self, table: &TableFactor) -> Result<BoundTableRef, BindError> {
        match table {
            TableFactor::Table { name, alias, .. } => {
                let name = &lower_case_name(name);
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
