use super::*;
use crate::parser::{ExprData, Expression};

impl Bind for Expression {
    fn bind(&mut self, binder:&mut Binder) -> Result<(), BindError> {
        Ok(())
    }
}

impl Bind for ExprData {
    fn bind(&mut self, binder:&mut Binder) -> Result<(), BindError> {
        match self {
            // Binding constant is not necessary
            ExprData::Constant(_) => { Ok(())},
            ExprData::ColumnRef{table_name, column_name, column_ref_id, column_index} => {
                match table_name {
                    Some(name) => { Ok(())}
                    None => ( Ok(()))
                }
            }
            _ => todo!()
        }
    }
}