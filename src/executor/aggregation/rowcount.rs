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
    fn update(
        &mut self,
        array: &ArrayImpl,
        visibility: Option<&[bool]>,
    ) -> Result<(), ExecutorError> {
        let temp = match visibility {
            None => array.len(),
            Some(visibility) => visibility.iter().filter(|&&b| b).count(),
        } as i32;
        self.result = match &self.result {
            DataValue::Null => DataValue::Int32(temp),
            DataValue::Int32(res) => DataValue::Int32(res + temp),
            _ => panic!("Mismatched type"),
        };
        Ok(())
    }

    fn output(&self) -> DataValue {
        self.result.clone()
    }
}
