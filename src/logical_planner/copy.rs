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
            Ok(Rc::new(LogicalCopyToFile {
                path,
                format: stmt.format,
                column_types,
                child: Rc::new(LogicalTableScan {
                    table_ref_id: stmt.table_ref_id,
                    column_ids,
                    column_descs,
                    with_row_handler: false,
                    is_sorted: false,
                    expr: None,
                }),
            }))
        } else {
            Ok(Rc::new(LogicalInsert {
                table_ref_id: stmt.table_ref_id,
                column_ids,
                child: Rc::new(LogicalCopyFromFile {
                    path,
                    format: stmt.format,
                    column_types,
                }),
            }))
        }
    }
}
