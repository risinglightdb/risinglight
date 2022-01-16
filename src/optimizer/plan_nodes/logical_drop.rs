use std::fmt;

use super::*;
use crate::binder::statement::drop::Object;

/// The logical plan of `DROP`.
#[derive(Debug, Clone)]
pub struct LogicalDrop {
    object: Object,
}

impl LogicalDrop {
    pub fn new(object: Object) -> Self {
        Self { object }
    }

    /// Get a reference to the logical drop's object.
    pub fn object(&self) -> &Object {
        &self.object
    }
}

impl PlanNode for LogicalDrop {}

impl fmt::Display for LogicalDrop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "{:?}", self)
    }
}
