// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::collections::HashSet;

use risinglight_proto::rowset::block_statistics::BlockStatisticsType;
use risinglight_proto::rowset::BlockStatistics;

pub struct StatisticsBuilder<'a> {
    distinct_values: HashSet<&'a [u8]>,
}

impl<'a> StatisticsBuilder<'a> {
    pub fn new() -> Self {
        Self {
            distinct_values: HashSet::<&'a [u8]>::new(),
        }
    }

    pub fn add_item(&mut self, data: Option<&'a [u8]>) {
        if let Some(data) = data {
            self.distinct_values.insert(data);
        }
    }

    pub fn get_statistics(self) -> Vec<BlockStatistics> {
        let distinct_count = self.distinct_values.len() as u64;
        let distinct_stat = BlockStatistics {
            block_stat_type: BlockStatisticsType::DistinctValue as i32,
            body: distinct_count.to_le_bytes().to_vec(),
        };
        vec![distinct_stat]
    }
}

#[cfg(test)]
mod tests {
    use bytes::Buf;

    use super::*;

    #[test]
    fn test_distinct_values() {
        let mut builder = StatisticsBuilder::new();
        builder.add_item(Some(b"2333"));
        builder.add_item(Some(b"2333"));
        builder.add_item(Some(b"2333"));
        builder.add_item(Some(b"2334"));
        builder.add_item(Some(b"2335"));
        let stats = builder.get_statistics();
        assert_eq!(
            stats[0].block_stat_type,
            BlockStatisticsType::DistinctValue as i32
        );
        let mut body = &stats[0].body[..];
        assert_eq!(body.get_u64_le(), 3);
    }
}
