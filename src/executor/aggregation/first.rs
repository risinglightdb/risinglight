// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

// use crate::array::ArrayImplValidExt;
use super::*;
use crate::array::Array;
use crate::types::DataTypeKind;

/// State for first/last aggregation
pub struct FirstLastAggregationState {
    input_datatype: DataTypeKind,
    result: Option<DataValue>,
    is_first: bool, // or last
}

impl FirstLastAggregationState {
    pub fn new(input_datatype: DataTypeKind, is_first: bool) -> Self {
        Self {
            result: None,
            input_datatype,
            is_first,
        }
    }
}

impl AggregationState for FirstLastAggregationState {
    fn update(&mut self, array: &ArrayImpl) -> Result<(), ExecutorError> {
        match (array, &self.input_datatype) {
            (ArrayImpl::Int32(arr), DataTypeKind::Int(_)) => {
                match &self.result {
                    Some(_) => {
                        if !self.is_first {
                            // try get last value
                            let arr_value = arr.iter().last();
                            if let Some(value) = arr_value {
                                match value {
                                    Some(value) => self.result = Some(DataValue::Int32(*value)),
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
                                    Some(value) => self.result = Some(DataValue::Int32(*value)),
                                    None => self.result = Some(DataValue::Null),
                                }
                            }
                        } else {
                            let arr_value = arr.iter().last();
                            if let Some(value) = arr_value {
                                match value {
                                    Some(value) => self.result = Some(DataValue::Int32(*value)),
                                    None => self.result = Some(DataValue::Null),
                                }
                            }
                        }
                    }
                };
            }
            (ArrayImpl::Int64(arr), DataTypeKind::BigInt(_)) => {
                match &self.result {
                    Some(_) => {
                        if !self.is_first {
                            // try get last value
                            let arr_value = arr.iter().last();
                            if let Some(value) = arr_value {
                                match value {
                                    Some(value) => self.result = Some(DataValue::Int64(*value)),
                                    None => self.result = Some(DataValue::Null),
                                }
                            }
                        }
                    }
                    None => {
                        if self.is_first {
                            let arr_value = arr.iter().next();
                            if let Some(value) = arr_value {
                                match value {
                                    Some(value) => self.result = Some(DataValue::Int64(*value)),
                                    None => self.result = Some(DataValue::Null),
                                }
                            }
                        } else {
                            let arr_value = arr.iter().last();
                            if let Some(value) = arr_value {
                                match value {
                                    Some(value) => self.result = Some(DataValue::Int64(*value)),
                                    None => self.result = Some(DataValue::Null),
                                }
                            }
                        }
                    }
                };
            }
            _ => panic!("Mismatched type"),
        }
        Ok(())
    }

    fn update_single(&mut self, value: &DataValue) -> Result<(), ExecutorError> {
        match (value, &self.input_datatype) {
            (DataValue::Int32(val), DataTypeKind::Int(_)) => {
                match &self.result {
                    Some(_) => {
                        if !self.is_first {
                            self.result = Some(DataValue::Int32(*val));
                        }
                    }
                    None => {
                        self.result = Some(DataValue::Int32(*val));
                    }
                };
            }
            (DataValue::Int64(val), DataTypeKind::BigInt(_)) => {
                match &self.result {
                    Some(_) => {
                        if !self.is_first {
                            self.result = Some(DataValue::Int64(*val));
                        }
                    }
                    None => {
                        self.result = Some(DataValue::Int64(*val));
                    }
                };
            }
            (DataValue::Null, _) => {
                match &self.result {
                    Some(_) => {
                        if !self.is_first {
                            self.result = Some(DataValue::Null);
                        }
                    }
                    None => {
                        self.result = Some(DataValue::Null);
                    }
                };
            }
            _ => panic!("Mismatched type"),
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
