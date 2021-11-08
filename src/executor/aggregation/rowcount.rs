use super::*;

pub struct RowCountAggregationState {
    result: DataValue,
}

impl RowCountAggregationState {
    pub fn new(init: DataValue) -> Box<Self> {
        Box::new(Self { result: init })
    }
}

impl AggregationState for RowCountAggregationState {
    fn update(
        &mut self,
        array: &ArrayImpl,
        visibility: Option<&Vec<bool>>,
    ) -> Result<(), ExecutorError> {
        let array = match visibility {
            None => array.clone(),
            Some(visibility) => {
                array.filter(visibility.iter().copied().collect::<Vec<_>>().into_iter())
            }
        };
        let temp = array.len() as i32;
        match &self.result {
            DataValue::Null => self.result = DataValue::Int32(temp),
            DataValue::Int32(res) => self.result = DataValue::Int32(res + temp),
            _ => panic!("Mismatched type"),
        }
        Ok(())
    }

    fn output(&self) -> DataValue {
        self.result.clone()
    }
}
