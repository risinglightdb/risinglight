use super::*;
use crate::parser::TableRef;

mod base;

impl Bind for TableRef {
    fn bind(&mut self, binder: &mut Binder) -> Result<(), BindError> {
        match self {
            TableRef::Base(base) => base.bind(binder),
            _ => todo!("bind table ref"),
        }
    }
}
