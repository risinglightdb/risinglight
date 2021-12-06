use crate::binder::statement::drop::Object;

/// The logical plan of `drop`.
#[derive(Debug, PartialEq, Clone)]
pub struct LogicalDrop {
    pub object: Object,
}
