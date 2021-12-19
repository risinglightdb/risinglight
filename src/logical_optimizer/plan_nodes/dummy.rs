use std::fmt;

use super::*;

/// A dummy plan.
#[derive(Debug, Clone)]
pub struct Dummy {}

impl_plan_node!(Dummy);

impl fmt::Display for Dummy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Dummy:")
    }
}
