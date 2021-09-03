use crate::types::{ColumnId, DataType};

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ColumnDesc {
    datatype: DataType,
    is_primary: bool,
    is_nullable: bool,
}

impl ColumnDesc {
    pub(crate) fn new(datatype: DataType, is_primary: bool, is_nullable: bool) -> Self {
        ColumnDesc {
            datatype,
            is_primary,
            is_nullable,
        }
    }

    pub(crate) fn set_primary(&mut self, is_primary: bool) {
        self.is_primary = is_primary;
    }

    pub(crate) fn is_primary(&self) -> bool {
        self.is_primary
    }

    pub(crate) fn is_nullable(&self) -> bool {
        self.is_nullable
    }

    pub(crate) fn datatype(&self) -> DataType {
        self.datatype
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ColumnCatalog {
    id: ColumnId,
    name: String,
    desc: ColumnDesc,
}

impl ColumnCatalog {
    pub(crate) fn new(id: ColumnId, name: String, desc: ColumnDesc) -> ColumnCatalog {
        ColumnCatalog { id, name, desc }
    }

    pub(crate) fn id(&self) -> ColumnId {
        self.id
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn datatype(&self) -> DataType {
        self.desc.datatype
    }

    pub(crate) fn set_primary(&mut self, is_primary: bool) {
        self.desc.set_primary(is_primary);
    }

    pub(crate) fn is_primary(&self) -> bool {
        self.desc.is_primary()
    }

    pub(crate) fn is_nullable(&self) -> bool {
        self.desc.is_nullable()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_catalog() {
        let col_desc = ColumnDesc::new(DataType::Int32, false, false);
        let mut col_catalog = ColumnCatalog::new(0, "grade".into(), col_desc);
        assert_eq!(col_catalog.id(), 0);
        assert_eq!(col_catalog.is_primary(), false);
        assert_eq!(col_catalog.is_nullable(), false);
        assert_eq!(col_catalog.datatype().data_len(), 4);
        assert_eq!(col_catalog.name(), "grade");
        col_catalog.set_primary(true);
        assert_eq!(col_catalog.is_primary(), true);
    }
}
