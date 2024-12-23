// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::path::PathBuf;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::*;

#[derive(Debug, PartialEq, PartialOrd, Ord, Hash, Eq, Clone, Serialize, Deserialize)]
pub struct ExtSource {
    pub path: PathBuf,
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

impl std::fmt::Display for ExtSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::fmt::Display for FileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl FromStr for Box<ExtSource> {
    type Err = ();
    fn from_str(_s: &str) -> std::result::Result<Self, Self::Err> {
        Err(())
    }
}

impl Binder {
    pub(super) fn bind_copy(
        &mut self,
        source: CopySource,
        to: bool,
        target: CopyTarget,
        options: &[CopyOption],
    ) -> Result {
        let ext_source = self.egraph.add(Node::ExtSource(Box::new(ExtSource {
            path: match target {
                CopyTarget::File { filename } => filename.into(),
                t => todo!("unsupported copy target: {:?}", t),
            },
            format: FileFormat::from_options(options),
        })));

        let copy = if to {
            // COPY <source_table> TO <dest_file>
            let query = match source {
                CopySource::Table {
                    table_name,
                    columns,
                } => {
                    let (table, _, _) = self.bind_table_id(&table_name)?;
                    let cols = self.bind_table_columns(&table_name, &columns)?;
                    let true_ = self.egraph.add(Node::true_());
                    self.egraph.add(Node::Scan([table, cols, true_]))
                }
                CopySource::Query(query) => self.bind_query(*query)?.0,
            };
            self.egraph.add(Node::CopyTo([ext_source, query]))
        } else {
            // COPY <dest_table> FROM <source_file>
            let (table, cols) = match source {
                CopySource::Table {
                    table_name,
                    columns,
                } => {
                    let (table, is_system, is_view) = self.bind_table_id(&table_name)?;
                    if is_system {
                        return Err(
                            ErrorKind::CopyTo("system table".into()).with_spanned(&table_name)
                        );
                    } else if is_view {
                        return Err(ErrorKind::CopyTo("view".into()).with_spanned(&table_name));
                    }
                    let cols = self.bind_table_columns(&table_name, &columns)?;
                    (table, cols)
                }
                CopySource::Query(query) => {
                    return Err(ErrorKind::CopyTo("query".into()).with_spanned(&*query));
                }
            };
            let types = self.type_(cols)?;
            let types = self.egraph.add(Node::Type(types));
            let copy = self.egraph.add(Node::CopyFrom([ext_source, types]));
            self.egraph.add(Node::Insert([table, cols, copy]))
        };

        Ok(copy)
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
