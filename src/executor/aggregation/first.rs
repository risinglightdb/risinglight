// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

// use crate::array::ArrayImplValidExt;
use super::*;
use crate::array::Array;

/// State for first/last aggregation
pub struct FirstLastAggregationState {
    result: Option<DataValue>,
    is_first: bool, // or last
}

impl FirstLastAggregationState {
    pub fn new(is_first: bool) -> Self {
        Self {
            result: None,
            is_first,
        }
    }
}

macro_rules! update_func {
    ($( { $Abc:ident, $Value:ident} ),*) => {
        impl FirstLastAggregationState {

            pub fn update_impl(&mut self, array: &ArrayImpl) {
                match array {
                    $(
                        ArrayImpl::$Abc(arr) => {
                            match &self.result {
                                Some(_) => {
                                    if !self.is_first {
                                        // try get last value
                                        let arr_value = arr.iter().last();
                                        if let Some(value) = arr_value {
                                            match value {
                                                Some(_) => self.result = Some(array.get(array.len() - 1)),
                                                None => self.result = Some(DataValue::Null),
                                            }
                                        };
                                    }
                                }
                                None => {
                                    if self.is_first {
                                        let arr_value = arr.iter().next();
                                        if let Some(value) = arr_value {
                                            match value {
                                                Some(_) => self.result = Some(array.get(0)),
                                                None => self.result = Some(DataValue::Null),
                                            }
                                        }
                                    } else {
                                        let arr_value = arr.iter().last();
                                        if let Some(value) = arr_value {
                                            match value {
                                                Some(_) => self.result = Some(array.get(array.len() - 1)),
                                                None => self.result = Some(DataValue::Null),
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    )*
                }
            }
        }
    }
}

update_func!(
    {Null, Null},
    {Bool, Bool},
    {Int32, Int32},
    {Int64, Int64},
    {Float64, Float64},
    {Utf8, String},
    {Blob, Blob},
    {Decimal, Decimal},
    {Date, Date},
    {Interval, Interval}
);

impl AggregationState for FirstLastAggregationState {
    fn update(&mut self, array: &ArrayImpl) -> Result<(), ExecutorError> {
        self.update_impl(array);
        Ok(())
    }

    fn update_single(&mut self, value: &DataValue) -> Result<(), ExecutorError> {
        match &self.result {
            Some(_) => {
                if !self.is_first {
                    self.result = Some(value.clone());
                }
            }
            None => {
                self.result = Some(value.clone());
            }
        }
        Ok(())
    }

    fn output(&self) -> DataValue {
        match &self.result {
            Some(value) => value.clone(),
            None => DataValue::Null,
        }
    }
}
