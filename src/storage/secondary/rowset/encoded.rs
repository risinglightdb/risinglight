use std::sync::Arc;

use crate::catalog::ColumnCatalog;

/// Encoded column.
pub struct EncodedColumn {
    pub index: Vec<u8>,
    pub data: Vec<u8>,
}

/// Encoded rowset.
pub struct EncodedRowset {
    /// Size.
    pub size: usize,

    /// Column information.
    pub columns_info: Arc<[ColumnCatalog]>,

    /// Column data.
    pub columns: Vec<EncodedColumn>,
}

impl EncodedRowset {
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
