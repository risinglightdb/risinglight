use super::FunctionError;

#[derive(Debug, Clone)]
pub struct FunctionCtx {
    pub error: Option<FunctionError>,
}
