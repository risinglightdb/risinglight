use super::*;
use crate::{array::ArrayBuilderImpl, binder::FileFormat, physical_planner::PhysicalCopyFromFile};
use std::fs::File;

/// The executor of loading file data.
pub struct CopyFromFileExecutor {
    pub plan: PhysicalCopyFromFile,
}

impl CopyFromFileExecutor {
    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream! {
            let chunk = tokio::task::spawn_blocking(|| self.read_file_blocking()).await.unwrap()?;
            yield chunk;
        }
    }

    // TODO(wrj): process a window at a time
    fn read_file_blocking(self) -> Result<DataChunk, ExecutorError> {
        let mut array_builders = self
            .plan
            .column_types
            .iter()
            .map(ArrayBuilderImpl::new)
            .collect::<Vec<ArrayBuilderImpl>>();

        let file = File::open(&self.plan.path)?;
        let mut reader = match self.plan.format {
            FileFormat::Csv {
                delimiter,
                quote,
                escape,
                header,
            } => csv::ReaderBuilder::new()
                .delimiter(delimiter as u8)
                .quote(quote as u8)
                .escape(escape.map(|c| c as u8))
                .has_headers(header)
                .from_reader(file),
        };

        let column_count = array_builders.len();
        for result in reader.records() {
            let record = result?;
            if !(record.len() == column_count
                || record.len() == column_count + 1 && record.get(column_count) == Some(""))
            {
                return Err(ExecutorError::LengthMismatch {
                    expected: column_count,
                    actual: record.len(),
                });
            }
            for ((s, builder), ty) in record
                .iter()
                .zip(&mut array_builders)
                .zip(&self.plan.column_types)
            {
                if !ty.is_nullable() && s.is_empty() {
                    return Err(ExecutorError::NotNullable);
                }
                builder.push_str(s)?;
            }
        }
        let chunk = array_builders
            .into_iter()
            .map(|builder| builder.finish())
            .collect();
        Ok(chunk)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        array::ArrayImpl,
        types::{DataTypeExt, DataTypeKind},
    };
    use std::io::Write;

    #[tokio::test]
    async fn read_csv() {
        let csv = "1,1.5,one\n2,2.5,two\n";

        let mut file = tempfile::NamedTempFile::new().expect("failed to create temp file");
        write!(file, "{}", csv).expect("failed to write file");

        let executor = CopyFromFileExecutor {
            plan: PhysicalCopyFromFile {
                path: file.path().into(),
                format: FileFormat::Csv {
                    delimiter: ',',
                    quote: '"',
                    escape: None,
                    header: false,
                },
                column_types: vec![
                    DataTypeKind::Int(None).not_null(),
                    DataTypeKind::Double.not_null(),
                    DataTypeKind::String.not_null(),
                ],
            },
        };
        let actual = executor.execute().boxed().next().await.unwrap().unwrap();

        let expected: DataChunk = [
            ArrayImpl::Int32([1, 2].into_iter().collect()),
            ArrayImpl::Float64([1.5, 2.5].into_iter().collect()),
            ArrayImpl::UTF8(["one", "two"].iter().map(Some).collect()),
        ]
        .into_iter()
        .collect();
        assert_eq!(actual, expected);
    }
}
