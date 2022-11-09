// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::path::PathBuf;

use super::*;
use crate::parser::CopyTarget;
use crate::v1::binder::BoundCopy;
use crate::v1::optimizer::plan_nodes::{
    LogicalCopyFromFile, LogicalCopyToFile, LogicalInsert, LogicalTableScan,
};

impl LogicalPlaner {
    pub fn plan_copy(&self, stmt: BoundCopy) -> Result<PlanRef, LogicalPlanError> {
        let path = match stmt.target {
            CopyTarget::File { filename } => PathBuf::from(filename),
            t => todo!("unsupported copy target: {:?}", t),
        };
        let column_ids = stmt.columns.iter().map(|col| col.id()).collect();
        let column_types = stmt.columns.iter().map(|col| col.datatype()).collect();
        let column_descs = stmt.columns.iter().map(|col| col.desc().clone()).collect();
        if stmt.to {
            Ok(Arc::new(LogicalCopyToFile::new(
                path,
                stmt.format,
                column_types,
                Arc::new(LogicalTableScan::new(
                    stmt.table_ref_id,
                    column_ids,
                    column_descs,
                    false,
                    false,
                    None,
                )),
            )))
        } else {
            Ok(Arc::new(LogicalInsert::new(
                stmt.table_ref_id,
                column_ids,
                Arc::new(LogicalCopyFromFile::new(
                    path,
                    stmt.format,
                    column_types,
                    column_descs,
                )),
            )))
        }
    }
}
