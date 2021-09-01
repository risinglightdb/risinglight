use crate::types::{column_id_t, DataTypeRef, Int32Type};

pub(crate) struct ColumnDesc {
    column_datatype: DataTypeRef,
    is_primary: bool,
}

impl ColumnDesc {
    pub(crate) fn new(column_datatype: DataTypeRef, is_primary: bool) -> ColumnDesc {
        ColumnDesc {
            column_datatype: column_datatype,
            is_primary: is_primary,
        }
    }

    pub(crate) fn set_primary(&mut self, is_primary: bool) {
        self.is_primary = is_primary;
    }

    pub(crate) fn is_primary(&self) -> bool {
        self.is_primary
    }

    pub(crate) fn is_nullable(&self) -> bool {
        self.column_datatype.is_nullable()
    }
    pub(crate) fn get_datatype(&self) -> DataTypeRef {
        self.column_datatype.clone()
    }
}

pub(crate) struct ColumnCatalog {
    column_id: column_id_t,
    column_name: String,
    column_desc: ColumnDesc,
}

impl ColumnCatalog {
    pub(crate) fn new(column_id: column_id_t, column_name: String, column_desc: ColumnDesc) -> ColumnCatalog {
        ColumnCatalog {
            column_id: column_id,
            column_name: column_name,
            column_desc: column_desc,
        }
    }

    pub(crate) fn get_column_id(&self) -> column_id_t {
        self.column_id
    }

    pub(crate) fn get_column_name(&self) -> String {
        self.column_name.clone()
    }

    pub(crate) fn get_column_datatype(&self) -> DataTypeRef {
        self.column_desc.column_datatype.clone()
    }

    pub(crate) fn set_primary(&mut self, is_primary: bool) {
        self.column_desc.set_primary(is_primary);
    }

    pub(crate) fn is_primary(&self) -> bool {
        self.column_desc.is_primary()
    }

    pub(crate) fn is_nullable(&self) -> bool {
        self.column_desc.is_nullable()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_column_catalog() {
        let col_desc = ColumnDesc::new(Int32Type::new(false), false);
        let mut col_catalog =
            ColumnCatalog::new(0, String::from("grade"), col_desc);
        assert_eq!(col_catalog.get_column_id(), 0);
        assert_eq!(col_catalog.is_primary(), false);
        assert_eq!(col_catalog.get_column_datatype().as_ref().get_data_len(), 4);
        assert_eq!(col_catalog.get_column_name(), String::from("grade"));
        col_catalog.set_primary(true);
        assert_eq!(col_catalog.is_primary(), true);
    }
}
