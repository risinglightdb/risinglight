use crate::types::{DataType, DataTypeEnum, DataTypeRef};
use std::any::Any;
use std::sync::Arc;

pub(crate) struct Int32Type {
    nullable: bool,
}

impl DataType for Int32Type {
    fn is_nullable(&self) -> bool {
        self.nullable
    }

    fn get_type() -> DataTypeEnum {
        DataTypeEnum::Int32
    }

    fn get_data_len() -> u32 {
        4
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Int32Type {
    pub(crate) fn new(nullable: bool) -> DataTypeRef {
        Arc::new(Self { nullable }) as DataTypeRef
    }
}
