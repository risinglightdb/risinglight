use super::*;
use crate::catalog::ColumnDesc;
use crate::types::ColumnId;
use std::collections::HashMap;
use std::sync::Arc;
use std::vec::Vec;

// Each table stores in one or multiple DataTableSlices.
// So far, we only store in one slice.
// Multiple slices could be used for sharding and partitioning in the future.
pub struct DataTableSlice {
    #[allow(dead_code)]
    inner: DataTableSliceInner,
}

pub struct DataTableSliceInner {
    #[allow(dead_code)]
    column_descs: HashMap<ColumnId, ColumnDesc>,
    num_tuples: TupleSize,
    num_segments: SegmentSize,
    table_segments: Vec<Arc<TableSegment>>,
}
