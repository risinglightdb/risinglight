//! Execute the queries.

use crate::{
    array::DataChunk,
    catalog::CatalogRef,
    physical_planner::PhysicalPlan,
    storage::{StorageError, StorageRef},
};

mod create;
mod explain;
mod insert;
mod values;

use self::create::*;
use self::explain::*;
use self::insert::*;
use self::values::*;

/// The error type of execution.
#[derive(thiserror::Error, Debug)]
pub enum ExecuteError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

pub trait Executor {
    fn execute(&mut self) -> Result<DataChunk, ExecuteError>;
}

/// A type-erased executor object.
pub type BoxedExecutor = Box<dyn Executor>;

/// The builder of executor.
pub struct ExecutorBuilder {
    catalog: CatalogRef,
    storage: StorageRef,
}

impl ExecutorBuilder {
    /// Create a new executor builder.
    pub fn new(catalog: CatalogRef, storage: StorageRef) -> ExecutorBuilder {
        ExecutorBuilder { catalog, storage }
    }

    /// Build executor from a [PhysicalPlan].
    pub fn build(&self, plan: PhysicalPlan) -> BoxedExecutor {
        use PhysicalPlan::*;
        match plan {
            PhysicalCreateTable(plan) => Box::new(CreateTableExecutor {
                plan,
                catalog: self.catalog.clone(),
                storage: self.storage.clone(),
            }),
            PhysicalInsert(plan) => Box::new(InsertExecutor {
                table_ref_id: plan.table_ref_id,
                column_ids: plan.column_ids,
                catalog: self.catalog.clone(),
                storage: self.storage.clone(),
                child: self.build(*plan.child),
            }),
            PhysicalValues(plan) => Box::new(ValuesExecutor {
                column_types: plan.column_types,
                values: plan.values,
            }),
            PhysicalExplain(plan) => Box::new(ExplainExecutor { plan: plan.child }),
        }
    }
}
