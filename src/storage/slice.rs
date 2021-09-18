use super::*;
use std::sync::{Arc};
use std::collections::HashMap;
use std::vec::Vec;
use crate::types::{ColumnId};
use crate::catalog::{ColumnDesc};

// Each table stores in one or multiple DataTableSlices.
// So far, we only store in one slice. 
// Multiple slices could be used for sharding and partitioning in the future.
pub struct DataTableSlice {
    inner: DataTableSliceInner
}

pub struct DataTableSliceInner {
    column_descs: HashMap<ColumnId, ColumnDesc>,
    num_tuples: TupleSize,
    num_segments: SegmentSize,
    table_segments: Vec<Arc<TableSegment>>
}