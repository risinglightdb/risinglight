use crate::types::{ColumnId, DataType};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDesc {
    datatype: DataType,
    is_primary: bool,
}

impl ColumnDesc {
    pub fn new(datatype: DataType, is_primary: bool) -> Self {
        ColumnDesc {
            datatype,
            is_primary,
        }
    }

    pub fn set_primary(&mut self, is_primary: bool) {
        self.is_primary = is_primary;
    }

    pub fn is_primary(&self) -> bool {
        self.is_primary
    }

    pub fn is_nullable(&self) -> bool {
        self.datatype.is_nullable()
    }

    pub fn datatype(&self) -> DataType {
        self.datatype
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnCatalog {
    id: ColumnId,
    name: String,
    desc: ColumnDesc,
}

impl ColumnCatalog {
    pub fn new(id: ColumnId, name: String, desc: ColumnDesc) -> ColumnCatalog {
        ColumnCatalog { id, name, desc }
    }

    pub fn id(&self) -> ColumnId {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn datatype(&self) -> DataType {
        self.desc.datatype
    }

    pub fn set_primary(&mut self, is_primary: bool) {
        self.desc.set_primary(is_primary);
    }

    pub fn is_primary(&self) -> bool {
        self.desc.is_primary()
    }

    pub fn is_nullable(&self) -> bool {
        self.desc.is_nullable()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_catalog() {
        let data_type = DataType::new(DataTypeKind::Int32, false);
        let col_desc = ColumnDesc::new(data_type, false);
        let mut col_catalog = ColumnCatalog::new(0, "grade".into(), col_desc);
        assert_eq!(col_catalog.id(), 0);
        assert_eq!(col_catalog.is_primary(), false);
        assert_eq!(col_catalog.is_nullable(), false);
        assert_eq!(col_catalog.datatype().kind().data_len(), 4);
        assert_eq!(col_catalog.name(), "grade");
        col_catalog.set_primary(true);
        assert_eq!(col_catalog.is_primary(), true);
    }
}
