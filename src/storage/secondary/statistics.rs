// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

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
use crate::types::DataValue;

mod row_count;
use row_count::*;
mod distinct_value;
use distinct_value::*;
mod statistics_builder;
pub use statistics_builder::*;

/// Get the aggregated statistics from pre-aggregated per-block statistics.
pub trait StatisticsGlobalAgg {
    fn apply_batch(&mut self, index: &ColumnIndex);
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
