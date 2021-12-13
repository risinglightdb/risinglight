use std::fmt;

#[derive(Debug, PartialEq, Clone)]
pub struct Dummy {}
impl fmt::Display for Dummy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Dummy")
    }
}
