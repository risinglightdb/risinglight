use super::*;
use crate::logical_plan::{
    CreateTableLogicalPlan, InsertLogicalPlan, LogicalPlan, ProjectionLogicalPlan,
    SeqScanLogicalPlan,
};

pub struct PhysicalPlanGenerator {}

impl Default for PhysicalPlanGenerator {
    fn default() -> Self {
        Self::new()
    }
}

// We build the physical plan by copying content from logical plan,
// we may implment moving content in the future.
impl PhysicalPlanGenerator {
    pub fn new() -> PhysicalPlanGenerator {
        PhysicalPlanGenerator {}
    }

    pub fn generate_physical_plan(
        &self,
        plan: &LogicalPlan,
    ) -> Result<PhysicalPlan, PhysicalPlanError> {
        match plan {
            LogicalPlan::CreateTable(create_table) => {
                self.generate_create_table_physical_plan(create_table)
            }
            LogicalPlan::Insert(insert) => self.generate_insert_physical_plan(insert),
            LogicalPlan::Projection(projection) => self.generate_projection_plan(projection),
            LogicalPlan::SeqScan(seq_scan) => self.generate_seq_scan_physical_plan(seq_scan),
            _ => Err(PhysicalPlanError::InvalidLogicalPlan),
        }
    }

    pub fn generate_create_table_physical_plan(
        &self,
        plan: &CreateTableLogicalPlan,
    ) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::CreateTable(CreateTablePhysicalPlan {
            database_id: plan.database_id,
            schema_id: plan.schema_id,
            table_name: plan.table_name.clone(),
            column_descs: plan.column_descs.to_vec(),
        }))
    }

    pub fn generate_insert_physical_plan(
        &self,
        plan: &InsertLogicalPlan,
    ) -> Result<PhysicalPlan, PhysicalPlanError> {
        let mut insert_plan = InsertPhysicalPlan {
            table_ref_id: plan.table_ref_id,
            column_ids: plan.column_ids.clone(),
            values: vec![],
        };

        for val in plan.values.iter() {
            insert_plan.values.push(val.to_vec());
        }

        Ok(PhysicalPlan::Insert(insert_plan))
    }

    pub fn generate_projection_plan(
        &self,
        plan: &ProjectionLogicalPlan,
    ) -> Result<PhysicalPlan, PhysicalPlanError> {
        let child_plan = self.generate_physical_plan(&plan.child)?;

        let proj_plan = ProjectionPhysicalPlan {
            project_expressions: plan.project_expressions.to_vec(),
            child: Box::new(child_plan),
        };

        Ok(PhysicalPlan::Projection(proj_plan))
    }

    pub fn generate_seq_scan_physical_plan(
        &self,
        plan: &SeqScanLogicalPlan,
    ) -> Result<PhysicalPlan, PhysicalPlanError> {
        Ok(PhysicalPlan::SeqScan(SeqScanPhysicalPlan::new(
            &plan.table_ref_id,
            &plan.column_ids,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::{Bind, Binder};
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRefId, RootCatalog, TableRefId};
    use crate::logical_plan::LogicalPlanGenerator;
    use crate::parser::{ColumnRef, ExprKind, Expression, SQLStatement};
    use crate::physical_plan::PhysicalPlanGenerator;
    use crate::types::{DataType, DataTypeKind};
    use std::sync::Arc;

    #[test]
    fn generate_select() {
        let catalog = Arc::new(RootCatalog::new());
        let mut binder = Binder::new(catalog.clone());

        let database = catalog.get_database_by_id(0).unwrap();
        let schema = database.get_schema_by_id(0).unwrap();
        schema
            .add_table(
                "t".into(),
                vec![
                    ColumnCatalog::new(
                        0,
                        "a".into(),
                        ColumnDesc::new(DataType::new(DataTypeKind::Int32, false), false),
                    ),
                    ColumnCatalog::new(
                        1,
                        "b".into(),
                        ColumnDesc::new(DataType::new(DataTypeKind::Int32, false), false),
                    ),
                ],
                false,
            )
            .unwrap();

        let sql = "select a, b from t; ";
        let mut stmts = SQLStatement::parse(sql).unwrap();
        stmts[0].bind(&mut binder).unwrap();
        let logical_planner = LogicalPlanGenerator::new();
        let physical_plan = PhysicalPlanGenerator::new();
        let logical_plan = logical_planner.generate_logical_plan(&stmts[0]).unwrap();
        let physical_plan = physical_plan.generate_physical_plan(&logical_plan).unwrap();
        assert_eq!(
            physical_plan,
            PhysicalPlan::Projection(ProjectionPhysicalPlan {
                project_expressions: vec![
                    Expression {
                        alias: None,
                        return_type: Some(DataType::new(DataTypeKind::Int32, false)),
                        kind: ExprKind::ColumnRef(ColumnRef {
                            table_name: Some("t".to_string()),
                            column_name: "a".to_string(),
                            column_ref_id: Some(ColumnRefId {
                                database_id: 0,
                                schema_id: 0,
                                table_id: 0,
                                column_id: 0
                            }),
                            column_index: Some(0)
                        }),
                    },
                    Expression {
                        alias: None,
                        return_type: Some(DataType::new(DataTypeKind::Int32, false)),
                        kind: ExprKind::ColumnRef(ColumnRef {
                            table_name: Some("t".to_string()),
                            column_name: "b".to_string(),
                            column_ref_id: Some(ColumnRefId {
                                database_id: 0,
                                schema_id: 0,
                                table_id: 0,
                                column_id: 1
                            }),
                            column_index: Some(1)
                        }),
                    }
                ],
                child: Box::new(PhysicalPlan::SeqScan(SeqScanPhysicalPlan::new(
                    &TableRefId {
                        database_id: 0,
                        schema_id: 0,
                        table_id: 0
                    },
                    &vec![0, 1]
                )))
            })
        )
    }
}
