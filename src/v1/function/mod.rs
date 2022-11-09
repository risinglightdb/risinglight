use crate::array::*;
use crate::types::{DataType, NativeType};

pub mod abs;
pub mod add;
pub mod binary;
pub mod ctx;
pub mod repeat;
pub mod unary;

pub use self::abs::*;
pub use self::add::*;
pub use self::binary::*;
pub use self::ctx::*;
pub use self::repeat::*;
pub use self::unary::*;

#[derive(thiserror::Error, Debug, PartialEq, Eq, Clone)]
pub enum FunctionError {
    #[error("invalid parameters {0}")]
    InvalidParameters(String),
    #[error("invalid datatypes {0}")]
    InvalidDataTypes(String),
    #[error("overflow")]
    Overflow,
}
// Definition of function.
pub trait Function: Send + Sync {
    // Each function should have an unique name.
    fn name(&self) -> &str;
    // A function could support mutiple kinds of data types.
    // For example, the abosulte value function can get absolute value of integer, float number or
    // double number.
    fn return_types(&self) -> DataType;
    // The execution logic of function.
    fn execute(&self, input: &[&ArrayImpl]) -> Result<ArrayImpl, FunctionError>;
}
