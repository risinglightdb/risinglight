//! Defination of data types.

pub use sqlparser::ast::DataType as DataTypeKind;

/// Data type with nullable.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DataType {
    kind: DataTypeKind,
    nullable: bool,
}

impl DataType {
    pub const fn new(kind: DataTypeKind, nullable: bool) -> Self {
        DataType { kind, nullable }
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    pub fn kind(&self) -> DataTypeKind {
        self.kind.clone()
    }
}

/// The extension methods for [`DataTypeKind`].
pub trait DataTypeExt {
    /// Create a nullable [`DataType`] from self.
    fn nullable(self) -> DataType;
    /// Create a non-nullable [`DataType`] from self.
    fn not_null(self) -> DataType;
}

impl DataTypeExt for DataTypeKind {
    fn nullable(self) -> DataType {
        DataType::new(self, true)
    }

    fn not_null(self) -> DataType {
        DataType::new(self, false)
    }
}
