use super::*;
use crate::catalog::ColumnDesc;
use crate::types::ColumnId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// A column segment stores data in a list of blocks.
/// Each Block has prev and next block id, so ColumnSegment only stores the first and last block id.
#[allow(dead_code)]
pub struct ColumnSegment {
    inner: Mutex<ColumnSegmentInner>,
}
#[allow(dead_code)]
pub struct ColumnSegmentInner {
    column_desc: ColumnDesc,
    first_block_id: Option<BlockId>,
    last_block_id: Option<BlockId>,
}

// For a table with N columns, a table segment stores fixed number tuples.
#[allow(dead_code)]
pub struct TableSegment {
    id: TableSegmentId,
    num_tuples: TupleSize,
    column_segment_infos: HashMap<ColumnId, Arc<ColumnSegment>>,
    // Used for sequential scan.
    next_table_segment: Option<Arc<TableSegment>>,
}
