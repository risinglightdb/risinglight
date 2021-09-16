use super::*;
use crate::parser::Expression;

#[derive(Debug, PartialEq, Clone)]
pub struct ProjectionPhysicalPlan {
    pub project_expressions: Vec<Expression>,
    pub child: Box<PhysicalPlan>,
}
