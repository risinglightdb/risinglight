use crate::types::{DataType, DataTypeEnum};
use std::any::Any;

pub(crate) struct BoolType {
    pub nullable: bool,
}

impl DataType for BoolType {
    fn is_nullable(&self) -> bool {
        self.nullable
    }

    fn get_type(&self) -> DataTypeEnum {
        DataTypeEnum::Bool
    }

    fn data_len(&self) -> u32 {
        1
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bool() {
        let bool_type = BoolType { nullable: false };
        assert_eq!(bool_type.is_nullable(), false);
        assert_eq!(bool_type.data_len(), 1);
    }
}
