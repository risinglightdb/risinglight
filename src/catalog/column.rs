// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use pretty_xmlish::Pretty;
use serde::{Deserialize, Serialize};

use super::ColumnId;
use crate::types::DataType;

/// A descriptor of a column.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ColumnDesc {
    name: String,
    data_type: DataType,
    is_nullable: bool,
    is_primary: bool,
}

impl ColumnDesc {
    pub fn new(name: impl Into<String>, datatype: DataType, is_nullable: bool) -> Self {
        ColumnDesc {
            name: name.into(),
            data_type: datatype,
            is_nullable,
            is_primary: false,
        }
    }

    pub fn set_primary(&mut self, is_primary: bool) {
        self.is_primary = is_primary;
    }

    pub fn is_primary(&self) -> bool {
        self.is_primary
    }

    pub fn set_nullable(&mut self, is_nullable: bool) {
        self.is_nullable = is_nullable;
    }

    pub fn is_nullable(&self) -> bool {
        self.is_nullable
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn pretty<'a>(&self) -> Pretty<'a> {
        let mut fields = vec![
            ("name", Pretty::display(&self.name)),
            ("type", Pretty::display(&self.data_type)),
        ];
        if self.is_primary {
            fields.push(("primary", Pretty::display(&self.is_primary)));
        }
        if self.is_nullable {
            fields.push(("nullable", Pretty::display(&self.is_nullable)));
        }
        Pretty::childless_record("Column", fields)
    }
}

/// The catalog of a column.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ColumnCatalog {
    id: ColumnId,
    desc: ColumnDesc,
}

impl ColumnCatalog {
    pub fn new(id: ColumnId, desc: ColumnDesc) -> ColumnCatalog {
        ColumnCatalog { id, desc }
    }

    pub fn id(&self) -> ColumnId {
        self.id
    }

    pub fn set_id(&mut self, id: ColumnId) {
        self.id = id
    }

    pub fn name(&self) -> &str {
        &self.desc.name
    }

    pub(crate) fn into_name(self) -> String {
        self.desc.name
    }

    pub fn desc(&self) -> &ColumnDesc {
        &self.desc
    }

    pub fn data_type(&self) -> DataType {
        self.desc.data_type.clone()
    }

    pub fn set_primary(&mut self, is_primary: bool) {
        self.desc.set_primary(is_primary);
    }

    pub fn is_primary(&self) -> bool {
        self.desc.is_primary()
    }

    pub fn set_nullable(&mut self, is_nullable: bool) {
        self.desc.set_nullable(is_nullable);
    }

    pub fn is_nullable(&self) -> bool {
        self.desc.is_nullable()
    }
}

/// Find the id of the sort key among column catalogs
pub fn find_sort_key_id(column_infos: &[ColumnCatalog]) -> Vec<usize> {
    let mut keys = vec![];
    for (id, column_info) in column_infos.iter().enumerate() {
        if column_info.is_primary() {
            keys.push(id);
        }
    }
    keys
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DataType;

    #[test]
    fn test_column_catalog() {
        let mut col_catalog =
            ColumnCatalog::new(0, ColumnDesc::new("grade", DataType::Int32, false));
        assert_eq!(col_catalog.id(), 0);
        assert!(!col_catalog.is_primary());
        assert!(!col_catalog.is_nullable());
        assert_eq!(col_catalog.name(), "grade");
        col_catalog.set_primary(true);
        assert!(col_catalog.is_primary());
    }
}
