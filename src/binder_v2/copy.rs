use std::result::Result as RawResult;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::*;
use crate::catalog::{ColumnCatalog, ColumnRefId, TableCatalog};

#[derive(Debug, PartialEq, PartialOrd, Ord, Hash, Eq, Clone, Serialize, Deserialize)]
pub struct BoundExtSource {
    pub path: String,
    pub format: FileFormat,
}

/// File format.
#[derive(Debug, PartialEq, PartialOrd, Ord, Hash, Eq, Clone, Serialize, Deserialize)]
pub enum FileFormat {
    Csv {
        /// Delimiter to parse.
        delimiter: char,
        /// Quote to use.
        quote: char,
        /// Escape character to use.
        escape: Option<char>,
        /// Whether or not the file has a header line.
        header: bool,
    },
}

impl std::fmt::Display for BoundExtSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::fmt::Display for FileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl FromStr for BoundExtSource {
    type Err = ();

    fn from_str(_s: &str) -> RawResult<Self, Self::Err> {
        Err(())
    }
}

impl Binder {
    pub(super) fn bind_copy(
        &mut self,
        table_name: ObjectName,
        columns: &[Ident],
        to: bool,
        target: CopyTarget,
        options: &[CopyOption],
    ) -> Result {
        let path = match target {
            CopyTarget::File { filename } => filename,
            t => todo!("unsupported copy target: {:?}", t),
        };

        let (table_id, _, columns) = self.bind_table_columns(&table_name, columns)?;
        let column_ids = columns
            .iter()
            .map(|col| {
                let column_ref_id = ColumnRefId::from_table(table_id, col.id());
                self.egraph.add(Node::Column(column_ref_id))
            })
            .collect();
        let cols_id = self.egraph.add(Node::List(column_ids));

        let ext_source = self.egraph.add(Node::BoundExtSource(BoundExtSource {
            path,
            format: FileFormat::from_options(options),
        }));

        let copy = if to {
            // COPY <source_table> TO <dest_file>
            let scan = self.egraph.add(Node::Scan(cols_id));
            self.egraph.add(Node::CopyTo([ext_source, scan]))
        } else {
            // COPY <dest_table> FROM <source_file>
            let copy = self.egraph.add(Node::CopyFrom(ext_source));
            self.egraph.add(Node::Insert([cols_id, copy]))
        };

        Ok(copy)
    }

    pub(super) fn bind_table_columns(
        &mut self,
        table_name: &ObjectName,
        columns: &[Ident],
    ) -> Result<(TableRefId, Arc<TableCatalog>, Vec<ColumnCatalog>)> {
        let name = lower_case_name(table_name.clone());
        let (database_name, schema_name, table_name) = split_name(&name)?;

        let table_ref_id = self
            .catalog
            .get_table_id_by_name(database_name, schema_name, table_name)
            .ok_or_else(|| BindError::InvalidTable(table_name.into()))?;

        let table = self
            .catalog
            .get_table(&table_ref_id)
            .ok_or_else(|| BindError::InvalidTable(table_name.into()))?;

        let columns = if columns.is_empty() {
            table.all_columns().values().cloned().collect_vec()
        } else {
            let mut column_catalogs = vec![];
            for col in columns.iter() {
                let col_name = col.value.to_lowercase();
                let col = table
                    .get_column_by_name(&col_name)
                    .ok_or_else(|| BindError::InvalidColumn(col_name.clone()))?;
                column_catalogs.push(col);
            }
            column_catalogs
        };
        Ok((table_ref_id, table, columns))
    }
}

impl FileFormat {
    /// Create from copy options.
    pub fn from_options(options: &[CopyOption]) -> Self {
        let mut delimiter = ',';
        let mut quote = '"';
        let mut escape = None;
        let mut header = false;
        for opt in options {
            match opt {
                CopyOption::Format(fmt) => {
                    assert_eq!(fmt.value.to_lowercase(), "csv", "only support CSV format")
                }
                CopyOption::Delimiter(c) => delimiter = *c,
                CopyOption::Header(b) => header = *b,
                CopyOption::Quote(c) => quote = *c,
                CopyOption::Escape(c) => escape = Some(*c),
                o => panic!("unsupported copy option: {:?}", o),
            }
        }
        FileFormat::Csv {
            delimiter,
            quote,
            escape,
            header,
        }
    }
}
