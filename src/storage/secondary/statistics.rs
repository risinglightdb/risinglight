//! Statistics of the storage engine.
//!
//! Secondary supports per-block and per-RowSet statistics.
//!
//! # Currently supported statistics
//!
//! ## RowCount
//!
//! RowCount is NOT a precise statistics. It simply adds up the row counts of all blocks. As there
//! might be rows deleted in deletion vector, the aggregated RowCount is not always accurate.

use risinglight_proto::rowset::block_statistics::BlockStatisticsType;

use super::index::ColumnIndex;
use crate::array::ArrayImpl;
use crate::types::DataValue;

mod row_count;
use row_count::*;
mod distinct_value;
use distinct_value::*;

/// Get the aggregated statistics from pre-aggregated per-block statistics.
pub trait StatisticsGlobalAgg {
    fn apply_batch(&mut self, index: &ColumnIndex);
    fn get_output(&self) -> DataValue;
}

/// Generate per-block statistics from array.
pub trait StatisticsPartialAgg {
    fn apply_batch(&mut self, index: &ArrayImpl);
    fn get_output(&self) -> DataValue;
}

pub fn create_statistics_global_aggregator(
    ty: BlockStatisticsType,
) -> Box<dyn StatisticsGlobalAgg> {
    match ty {
        BlockStatisticsType::RowCount => Box::new(RowCountGlobalAgg::create()),
        BlockStatisticsType::DistinctValue => Box::new(DistinctValueGlobalAgg::create()),
    }
}

#[allow(dead_code)]
pub fn create_statistics_partial_aggregator(
    ty: BlockStatisticsType,
) -> Box<dyn StatisticsPartialAgg> {
    match ty {
        BlockStatisticsType::RowCount =>  panic!("RowCount partial aggregator should not be created, as the row count is already stored in block index"),
        BlockStatisticsType::DistinctValue => Box::new(DistinctValuePartialAgg::create())
    }
}
