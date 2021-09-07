use super::*;
use crate::parser::Expression;
use std::convert::TryFrom;
use std::sync::Arc;

#[derive(Debug, PartialEq, Clone)]
pub struct ProjectionLogicalPlan {
    pub project_expressions: Vec<Expression>,
    pub child: Arc<LogicalPlan>,
}
