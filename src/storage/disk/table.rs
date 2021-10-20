use super::*;
use crate::array::{DataChunk, DataChunkRef};
use crate::catalog::{ColumnDesc, TableRefId};
use crate::types::TableId;
use std::sync::{Arc, RwLock};
use std::vec::Vec;

// Each on disk table stores data in DataTableSlices.
// We only use one slice now.
#[allow(dead_code)]
pub struct OnDiskDataTable {
    table_id: TableId,
    column_descs: HashMap<ColumnId, ColumnDesc>,
    primary_keys: Vec<ColumnId>,
    slices: HashMap<SliceId, Arc<DataTableSlice>>,
}
