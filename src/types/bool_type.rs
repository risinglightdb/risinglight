use crate::types::{DataType, DataTypeEnum, DataTypeRef};
use std::any::Any;
use std::sync::Arc;

pub(crate) struct BoolType {
    nullable: bool,
}

impl DataType for BoolType {
    fn is_nullable(&self) -> bool {
        self.nullable
    }

    fn get_type(&self) -> DataTypeEnum {
        DataTypeEnum::Bool
    }

    fn get_data_len(&self) -> u32 {
        1
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl BoolType {
    pub(crate) fn new(nullable: bool) -> DataTypeRef {
        Arc::new(Self { nullable }) as DataTypeRef
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_int32() {
        let int32_type = BoolType::new(false);
        assert_eq!(int32_type.as_ref().is_nullable(), false);
        assert_eq!(int32_type.as_ref().get_data_len(), 1);
    }
}
