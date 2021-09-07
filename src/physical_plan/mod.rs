mod create;
mod generator;
mod insert;
mod projection;
mod seq_scan;

pub use create::*;
pub use generator::*;
pub use insert::*;
pub use projection::*;
pub use seq_scan::*;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum PhysicalPlanError {
    #[error("invalid SQL")]
    InvalidLogicalPlan,
}

#[derive(Debug, PartialEq, Clone)]
pub enum PhysicalPlan {
    Dummy,
    SeqScan(SeqScanPhysicalPlan),
    Insert(InsertPhysicalPlan),
    CreateTable(CreateTablePhysicalPlan),
    Projection(ProjectionPhysicalPlan),
}
