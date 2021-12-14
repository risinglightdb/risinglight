use std::fmt;

use crate::binder::Object;

/// The physical plan of `drop`.
#[derive(Debug, PartialEq, Clone)]
pub struct PhysicalDrop {
    pub object: Object,
}

impl fmt::Display for PhysicalDrop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "{:?}", self)
    }
}
