use crate::types::{DataType, DataTypeEnum};
use std::any::Any;

pub(crate) struct Int32Type {
    pub nullable: bool,
}

impl DataType for Int32Type {
    fn is_nullable(&self) -> bool {
        self.nullable
    }

    fn get_type(&self) -> DataTypeEnum {
        DataTypeEnum::Int32
    }

    fn data_len(&self) -> u32 {
        4
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int32() {
        let int32_type = Int32Type { nullable: false };
        assert_eq!(int32_type.is_nullable(), false);
        assert_eq!(int32_type.data_len(), 4);
    }
}
