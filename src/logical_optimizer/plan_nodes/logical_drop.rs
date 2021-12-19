use std::fmt;

use super::*;
use crate::binder::statement::drop::Object;

/// The logical plan of `DROP`.
#[derive(Debug, Clone)]
pub struct LogicalDrop {
    pub object: Object,
}

impl_plan_node!(LogicalDrop);

impl fmt::Display for LogicalDrop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "{:?}", self)
    }
}
