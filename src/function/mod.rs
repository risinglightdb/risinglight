
use crate::array::*;
use std::sync::Arc;
use crate::types::PhysicalDataTypeKind;
use std::collections::HashMap;

pub mod abs;

pub use self::abs::*;
#[derive(thiserror::Error, Debug, PartialEq)]
pub enum FunctionError {
    #[error("invalid parameters {0}")]
    InvalidParameters(String),
    #[error("invalid datatypes {0}")]
    InvalidDataTypes(String),
}
// Definition of function.
pub trait Function: Send + Sync {
    // Each function should have an unique name.
    fn name(&self) -> &str;
    // A function could support mutiple kinds of data types.
    // For example, the abosulte value function can get absolute value of integer, float number or
    // double number.
    fn return_types(
        &self,
        input_types: &[PhysicalDataTypeKind],
    ) -> Result<PhysicalDataTypeKind, FunctionError>;
    // The execution logic of function.
    fn execute(&self, input: &DataChunk) -> Result<DataChunk, FunctionError>;
}

pub struct FunctionManager {
    function_map: HashMap<String, Arc<dyn Function>>
}


lazy_static! {
static ref FUNCTION_MANAGER: Arc<FunctionManager> = {
    Arc::new(FunctionManager::new())
};   
}

impl FunctionManager {
    pub fn new() -> FunctionManager {
        FunctionManager {
            function_map: HashMap::new()
        }
    }
    pub fn register(&mut self, func: Arc<dyn Function>) {
        self.function_map.insert(func.name().to_string(), func.clone());
    }
}
