// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.
use serde::Serialize;

use super::*;
use crate::catalog::ColumnCatalog;
use crate::parser::{CopyOption, CopyTarget, Statement};

/// A bound `COPY` statement.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BoundCopy {
    pub table_ref_id: TableRefId,
    pub columns: Vec<ColumnCatalog>,
    pub to: bool,
    pub target: CopyTarget,
    pub format: FileFormat,
}

/// File format.
#[derive(Debug, PartialEq, Eq, Clone, Serialize)]
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

impl Binder {
    pub fn bind_copy(&mut self, stmt: &Statement) -> Result<BoundCopy, BindError> {
        match stmt {
            Statement::Copy {
                table_name,
                columns,
                to,
                target,
                options,
                ..
            } => {
                let (table_ref_id, _, columns) = self.bind_table_columns(table_name, columns)?;

                Ok(BoundCopy {
                    table_ref_id,
                    columns,
                    to: *to,
                    target: target.clone(),
                    format: FileFormat::from_options(options),
                })
            }
            _ => panic!("mismatched statement type"),
        }
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
