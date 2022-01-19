// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use super::*;

/// State for row count aggregation
pub struct RowCountAggregationState {
    result: DataValue,
}

impl RowCountAggregationState {
    pub fn new(init: DataValue) -> Self {
        Self { result: init }
    }
}

impl AggregationState for RowCountAggregationState {
    fn update(&mut self, array: &ArrayImpl) -> Result<(), ExecutorError> {
        let temp = array.len() as i32;
        // let temp = array.get_valid_bitmap().count_ones() as i32;
        self.result = match &self.result {
            DataValue::Null => DataValue::Int32(temp),
            DataValue::Int32(res) => DataValue::Int32(res + temp),
            _ => panic!("Mismatched type"),
        };
        Ok(())
    }

    fn update_single(&mut self, _: &DataValue) -> Result<(), ExecutorError> {
        self.result = match &self.result {
            DataValue::Null => DataValue::Int32(1),
            DataValue::Int32(res) => DataValue::Int32(res + 1),
            _ => panic!("Mismatched type"),
        };
        Ok(())
    }

    fn output(&self) -> DataValue {
        self.result.clone()
    }
}
