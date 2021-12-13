use std::path::PathBuf;

use super::*;
use crate::binder::BoundCopy;
use crate::logical_optimizer::plan_nodes::logical_copy_from_file::LogicalCopyFromFile;
use crate::logical_optimizer::plan_nodes::logical_copy_to_file::LogicalCopyToFile;
use crate::logical_optimizer::plan_nodes::logical_insert::LogicalInsert;
use crate::logical_optimizer::plan_nodes::logical_seq_scan::LogicalSeqScan;
use crate::logical_optimizer::plan_nodes::Plan;
use crate::parser::CopyTarget;

impl LogicalPlaner {
    pub fn plan_copy(&self, stmt: BoundCopy) -> Result<Plan, LogicalPlanError> {
        let path = match stmt.target {
            CopyTarget::File { filename } => PathBuf::from(filename),
            t => todo!("unsupported copy target: {:?}", t),
        };
        let column_ids = stmt.columns.iter().map(|col| col.id()).collect();
        let column_types = stmt.columns.iter().map(|col| col.datatype()).collect();
        if stmt.to {
            Ok(Plan::LogicalCopyToFile(LogicalCopyToFile {
                path,
                format: stmt.format,
                column_types,
                child: Plan::LogicalSeqScan(LogicalSeqScan {
                    table_ref_id: stmt.table_ref_id,
                    column_ids,
                    with_row_handler: false,
                    is_sorted: false,
                })
                .into(),
            }))
        } else {
            Ok(Plan::LogicalInsert(LogicalInsert {
                table_ref_id: stmt.table_ref_id,
                column_ids,
                child: Plan::LogicalCopyFromFile(LogicalCopyFromFile {
                    path,
                    format: stmt.format,
                    column_types,
                })
                .into(),
            }))
        }
    }
}
