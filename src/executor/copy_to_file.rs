// TODO(wrj): remove this once linked to plan
#![allow(dead_code)]

use super::*;
use crate::physical_planner::FileFormat;
use std::{fs::File, path::PathBuf};
use tokio::sync::mpsc;

/// The executor of saving data to file.
pub struct CopyToFileExecutor {
    pub path: PathBuf,
    pub format: FileFormat,
    pub child: BoxedExecutor,
}

impl CopyToFileExecutor {
    pub fn execute(self) -> impl Stream<Item = Result<DataChunk, ExecutorError>> {
        try_stream! {
            let Self { path, format, child } = self;
            let (sender, recver) = mpsc::channel(1);
            let writer = tokio::task::spawn_blocking(move || Self::write_file_blocking(path, format, recver));
            for await batch in child {
                sender.send(batch?).await.unwrap();
            }
            drop(sender);
            writer.await.unwrap()?;
            yield DataChunk::single(1);
        }
    }

    fn write_file_blocking(
        path: PathBuf,
        format: FileFormat,
        mut recver: mpsc::Receiver<DataChunk>,
    ) -> Result<(), ExecutorError> {
        let file = File::create(&path)?;
        let mut writer = match format {
            FileFormat::Csv {
                delimiter,
                quote,
                escape,
                header,
            } => csv::WriterBuilder::new()
                .delimiter(delimiter)
                .quote(quote)
                .escape(escape.unwrap_or(b'\\'))
                .has_headers(header)
                .from_writer(file),
        };

        while let Some(chunk) = recver.blocking_recv() {
            for i in 0..chunk.cardinality() {
                // TODO(wrj): avoid dynamic memory allocation (String)
                let row = chunk.arrays().iter().map(|a| a.get_to_string(i));
                writer.write_record(row)?;
            }
            writer.flush()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::ArrayImpl;

    #[tokio::test]
    async fn write_csv() {
        let file = tempfile::NamedTempFile::new().expect("failed to create temp file");

        let executor = CopyToFileExecutor {
            path: file.path().into(),
            format: FileFormat::Csv {
                delimiter: b',',
                quote: b'"',
                escape: None,
                header: false,
            },
            child: try_stream! {
                yield [
                    ArrayImpl::Int32([1, 2].into_iter().collect()),
                    ArrayImpl::Float64([1.5, 2.5].into_iter().collect()),
                    ArrayImpl::UTF8(["one", "two"].iter().map(Some).collect()),
                ]
                .into_iter()
                .collect();
            }
            .boxed(),
        };
        executor.execute().boxed().next().await.unwrap().unwrap();

        let actual = std::fs::read_to_string(file.path()).unwrap();
        let expected = "1,1.5,one\n2,2.5,two\n";
        assert_eq!(actual, expected);
    }
}
