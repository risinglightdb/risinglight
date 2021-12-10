use super::*;
use crate::{
    binder::BoundCopy,
    logical_optimizer::plan_nodes::{
        logical_copy_from_file::LogicalCopyFromFile, logical_copy_to_file::LogicalCopyToFile,
        logical_insert::LogicalInsert, logical_seq_scan::LogicalSeqScan, LogicalPlan,
    },
    parser::CopyTarget,
};
use std::path::PathBuf;

impl LogicalPlaner {
    pub fn plan_copy(&self, stmt: BoundCopy) -> Result<LogicalPlan, LogicalPlanError> {
        let path = match stmt.target {
            CopyTarget::File { filename } => PathBuf::from(filename),
            t => todo!("unsupported copy target: {:?}", t),
        };
        let column_ids = stmt.columns.iter().map(|col| col.id()).collect();
        let column_types = stmt.columns.iter().map(|col| col.datatype()).collect();
        if stmt.to {
            Ok(LogicalPlan::LogicalCopyToFile(LogicalCopyToFile {
                path,
                format: stmt.format,
                column_types,
                child: LogicalPlan::LogicalSeqScan(LogicalSeqScan {
                    table_ref_id: stmt.table_ref_id,
                    column_ids,
                    with_row_handler: false,
                    is_sorted: false,
                })
                .into(),
            }))
        } else {
            Ok(LogicalPlan::LogicalInsert(LogicalInsert {
                table_ref_id: stmt.table_ref_id,
                column_ids,
                child: LogicalPlan::LogicalCopyFromFile(LogicalCopyFromFile {
                    path,
                    format: stmt.format,
                    column_types,
                })
                .into(),
            }))
        }
    }
}
