// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use risinglight_proto::rowset::block_statistics::BlockStatisticsType;

use super::StatisticsGlobalAgg;
use crate::storage::secondary::index::ColumnIndex;
use crate::types::DataValue;

pub struct DistinctValueGlobalAgg {
    distinct_cnt: u64,
}

impl DistinctValueGlobalAgg {
    pub fn create() -> Self {
        Self { distinct_cnt: 0 }
    }
}

impl StatisticsGlobalAgg for DistinctValueGlobalAgg {
    fn apply_batch(&mut self, index: &ColumnIndex) {
        for index in index.indexes() {
            for stat in &index.stats {
                if stat.block_stat_type() == BlockStatisticsType::DistinctValue {
                    let cnt = u64::from_le_bytes(stat.body.clone().try_into().unwrap());
                    self.distinct_cnt += cnt;
                }
            }
        }
    }

    fn get_output(&self) -> DataValue {
        DataValue::Int64(self.distinct_cnt as i64)
    }
}
