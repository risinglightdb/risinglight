use crate::array::*;
use crate::types::PhysicalDataTypeKind;

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
pub trait Function {
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
