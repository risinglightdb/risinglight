use std::sync::Arc;

use crate::catalog::ColumnCatalog;

/// Raw columns.
pub struct ColumnRaw {
    pub index: Vec<u8>,
    pub data: Vec<u8>,
}

/// Raw rowset.
pub struct RowsetRaw {
    /// Size.
    pub size: usize,

    /// Column information.
    pub columns_info: Arc<[ColumnCatalog]>,

    /// Column data.
    pub columns: Vec<ColumnRaw>,
}

impl RowsetRaw {
    #[allow(dead_code)]
    /// Number of rows in the rowset.
    pub fn cardinality(&self) -> usize {
        self.size
    }

    /// Returns `true` if `self` has no rows.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
}
