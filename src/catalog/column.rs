use crate::types::{ColumnId, DataType};
use serde::{Deserialize, Serialize};

/// A descriptor of a column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDesc {
    datatype: DataType,
    is_primary: bool,
}

impl ColumnDesc {
    pub const fn new(datatype: DataType, is_primary: bool) -> Self {
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

    pub fn datatype(&self) -> &DataType {
        &self.datatype
    }
}

impl DataType {
    pub const fn to_column(self) -> ColumnDesc {
        ColumnDesc::new(self, false)
    }

    pub const fn to_column_primary_key(self) -> ColumnDesc {
        ColumnDesc::new(self, true)
    }
}

/// The catalog of a column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

    pub fn set_id(&mut self, id: ColumnId) {
        self.id = id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn desc(&self) -> &ColumnDesc {
        &self.desc
    }

    pub fn datatype(&self) -> DataType {
        self.desc.datatype.clone()
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

/// Find the id of the sort key among column catalogs
pub fn find_sort_key_id(column_infos: &[ColumnCatalog]) -> Option<usize> {
    let mut key = None;
    for (id, column_info) in column_infos.iter().enumerate() {
        if column_info.is_primary() {
            if key.is_some() {
                panic!("only one primary key is supported");
            }
            key = Some(id);
        }
    }
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataTypeExt, DataTypeKind};

    #[test]
    fn test_column_catalog() {
        let col_desc = DataTypeKind::Int(None).not_null().to_column();
        let mut col_catalog = ColumnCatalog::new(0, "grade".into(), col_desc);
        assert_eq!(col_catalog.id(), 0);
        assert!(!col_catalog.is_primary());
        assert!(!col_catalog.is_nullable());
        assert_eq!(col_catalog.name(), "grade");
        col_catalog.set_primary(true);
        assert!(col_catalog.is_primary());
    }
}
