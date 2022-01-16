use std::path::PathBuf;

use super::*;
use crate::binder::BoundCopy;
use crate::optimizer::plan_nodes::{
    LogicalCopyFromFile, LogicalCopyToFile, LogicalInsert, LogicalTableScan,
};
use crate::parser::CopyTarget;

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
            Ok(Rc::new(LogicalCopyToFile::new(
                path,
                stmt.format,
                column_types,
                Rc::new(LogicalTableScan::new(
                    stmt.table_ref_id,
                    column_ids,
                    column_descs,
                    false,
                    false,
                    None,
                )),
            )))
        } else {
            Ok(Rc::new(LogicalInsert::new(
                stmt.table_ref_id,
                column_ids,
                Rc::new(LogicalCopyFromFile::new(path, stmt.format, column_types)),
            )))
        }
    }
}
