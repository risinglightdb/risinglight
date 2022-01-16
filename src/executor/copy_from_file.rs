use std::fs::File;
use std::io::BufReader;

use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use tokio::sync::mpsc::UnboundedSender;

use super::*;
use crate::array::ArrayBuilderImpl;
use crate::binder::FileFormat;
use crate::optimizer::plan_nodes::PhysicalCopyFromFile;

/// The executor of loading file data.
pub struct CopyFromFileExecutor {
    pub plan: PhysicalCopyFromFile,
}

/// When the source file size is about the limit, we show a progress bar on the screen.
const IMPORT_PROGRESS_BAR_LIMIT: u64 = 1024 * 1024;

/// We produce a batch everytime the DataChunk is larger than this size.
const IMPORT_BATCH_SIZE: u64 = 16 * 1024 * 1024;

impl CopyFromFileExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = tokio::task::spawn_blocking(|| self.read_file_blocking(tx));
        while let Some(chunk) = rx.recv().await {
            yield chunk;
        }
        handle.await.unwrap()?;
    }

    fn read_file_blocking(self, tx: UnboundedSender<DataChunk>) -> Result<(), ExecutorError> {
        let create_array_builders = |plan: &PhysicalCopyFromFile| {
            plan.logical()
                .column_types()
                .iter()
                .map(ArrayBuilderImpl::new)
                .collect_vec()
        };
        let flush_array = |array_builders: Vec<ArrayBuilderImpl>| -> DataChunk {
            array_builders
                .into_iter()
                .map(|builder| builder.finish())
                .collect()
        };

        let mut array_builders = create_array_builders(&self.plan);

        let file = File::open(&self.plan.logical().path())?;
        let file_size = file.metadata()?.len();
        let mut buf_reader = BufReader::new(file);
        let mut reader = match self.plan.logical().format().clone() {
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
                .from_reader(&mut buf_reader),
        };

        let bar = if file_size < IMPORT_PROGRESS_BAR_LIMIT {
            // disable progress bar if file size is < 1MB
            ProgressBar::hidden()
        } else {
            let bar = ProgressBar::new(file_size);
            bar.set_style(
                ProgressStyle::default_bar()
                    .template("[{elapsed_precise}] {bar:40.cyan/blue} {bytes}/{total_bytes}")
                    .progress_chars("=>-"),
            );
            bar
        };

        let column_count = self.plan.logical().column_types().len();
        let mut iter = reader.records();
        let mut round = 0;
        let mut last_pos = 0;
        loop {
            round += 1;
            if round % 1000 == 0 {
                let current_pos = iter.reader().position().byte();
                bar.set_position(current_pos);
                // Produce a chunk of 16MB
                if current_pos - last_pos > IMPORT_BATCH_SIZE {
                    last_pos = current_pos;
                    let chunk = flush_array(array_builders);
                    array_builders = create_array_builders(&self.plan);
                    if chunk.cardinality() > 0 {
                        tx.send(chunk).unwrap();
                    }
                }
            }
            if let Some(record) = iter.next() {
                let record = record?;
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
                    .zip(&self.plan.logical().column_types().to_vec())
                {
                    if !ty.is_nullable() && s.is_empty() {
                        return Err(ExecutorError::NotNullable);
                    }
                    builder.push_str(s)?;
                }
            } else {
                break;
            }
        }
        bar.finish();

        let chunk = flush_array(array_builders);
        if chunk.cardinality() > 0 {
            tx.send(chunk).unwrap();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    use crate::array::ArrayImpl;
    use crate::types::{DataTypeExt, DataTypeKind};

    #[tokio::test]
    async fn read_csv() {
        let csv = "1,1.5,one\n2,2.5,two\n";

        let mut file = tempfile::NamedTempFile::new().expect("failed to create temp file");
        write!(file, "{}", csv).expect("failed to write file");

        let executor = CopyFromFileExecutor {
            plan: PhysicalCopyFromFile {
                logical: LogicalCopyFromFile {
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
            },
        };
        let actual = executor.execute().next().await.unwrap().unwrap();

        let expected: DataChunk = [
            ArrayImpl::Int32([1, 2].into_iter().collect()),
            ArrayImpl::Float64([1.5, 2.5].into_iter().collect()),
            ArrayImpl::Utf8(["one", "two"].iter().map(Some).collect()),
        ]
        .into_iter()
        .collect();
        assert_eq!(actual, expected);
    }
}
