mod create;
mod insert;
mod projection;
mod seq_scan;

pub use create::*;
pub use insert::*;
pub use projection::*;
pub use seq_scan::*;

// Plan is an enumeration which record all necessary information of execution plan, which will be used by optimizer and executor.
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum PlanError {}

#[derive(Debug, PartialEq, Clone)]
pub enum Plan {
    SeqScan(SeqScanPlan),
    Insert(InsertPlan),
    CreateTable(CreateTablePlan),
    Projection(ProjectionPlan),
}
