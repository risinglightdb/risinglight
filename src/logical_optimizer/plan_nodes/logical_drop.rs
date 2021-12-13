use std::fmt;

use crate::binder::statement::drop::Object;

/// The logical plan of `drop`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalDrop {
    pub object: Object,
}
impl fmt::Display for LogicalDrop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "{:?}", self)
    }
}
