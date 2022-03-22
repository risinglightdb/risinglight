// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fs::File;
use std::path::PathBuf;

use tokio::sync::mpsc;

use super::*;
use crate::binder::FileFormat;

/// The executor of saving data to file.
pub struct CopyToFileExecutor {
    pub context: Arc<Context>,
    pub path: PathBuf,
    pub format: FileFormat,
    pub child: BoxedExecutor,
}

impl CopyToFileExecutor {
    #[try_stream(boxed, ok = DataChunk, error = ExecutorError)]
    pub async fn execute(self) {
        let Self {
            path,
            format,
            child,
            ..
        } = self;
        let (sender, recver) = mpsc::channel(1);
        let context = self.context.clone();
        match context
            .spawn_blocking(move |token| Self::write_file_blocking(path, format, recver, token))
        {
            Some(writer) => {
                #[for_await]
                for batch in child {
                    let res = sender.send(batch?).await;
                    if res.is_err() {
                        // send error means the background IO task returns error.
                        break;
                    }
                }
                drop(sender);
                let rows = writer.await.unwrap()?;
                yield DataChunk::single(rows as _);
            }
            None => return Err(ExecutorError::Abort),
        }
    }

    fn write_file_blocking(
        path: PathBuf,
        format: FileFormat,
        mut recver: mpsc::Receiver<DataChunk>,
        token: CancellationToken,
    ) -> Result<usize, ExecutorError> {
        let file = File::create(&path)?;
        let mut writer = match format {
            FileFormat::Csv {
                delimiter,
                quote,
                escape,
                header,
            } => csv::WriterBuilder::new()
                .delimiter(delimiter as u8)
                .quote(quote as u8)
                .escape(escape.unwrap_or(quote) as u8)
                .has_headers(header)
                .from_writer(file),
        };

        let mut rows = 0;

        while let Some(chunk) = recver.blocking_recv() {
            // quit early if cancelled.
            if token.is_cancelled() {
                break;
            }

            for i in 0..chunk.cardinality() {
                // TODO(wrj): avoid dynamic memory allocation (String)
                let row = chunk.arrays().iter().map(|a| a.get_to_string(i));
                writer.write_record(row)?;
            }
            writer.flush()?;
            rows += chunk.cardinality();
        }

        // Delete the file if cancelled.
        if token.is_cancelled() {
            drop(writer);
            std::fs::remove_file(&path)?;
        }

        Ok(rows)
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
            context: Default::default(),
            path: file.path().into(),
            format: FileFormat::Csv {
                delimiter: ',',
                quote: '"',
                escape: None,
                header: false,
            },
            child: async_stream::try_stream! {
                yield [
                    ArrayImpl::new_int32([1, 2].into_iter().collect()),
                    ArrayImpl::new_float64([1.5, 2.5].into_iter().collect()),
                    ArrayImpl::new_utf8(["one", "two"].iter().map(Some).collect()),
                ]
                .into_iter()
                .collect();
            }
            .boxed(),
        };
        executor.execute().next().await.unwrap().unwrap();

        let actual = std::fs::read_to_string(file.path()).unwrap();
        let expected = "1,1.5,one\n2,2.5,two\n";
        assert_eq!(actual, expected);
    }
}
