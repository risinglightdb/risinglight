// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::StatisticsGlobalAgg;
use crate::storage::secondary::index::ColumnIndex;
use crate::types::DataValue;

/// Gather row count from column index.
pub struct RowCountGlobalAgg {
    cnt: u64,
}

impl RowCountGlobalAgg {
    pub fn create() -> Self {
        Self { cnt: 0 }
    }
}

impl StatisticsGlobalAgg for RowCountGlobalAgg {
    fn apply_batch(&mut self, index: &ColumnIndex) {
        for index in index.indexes() {
            self.cnt += index.row_count as u64;
        }
    }

    fn get_output(&self) -> DataValue {
        DataValue::Int64(self.cnt as i64)
    }
}
