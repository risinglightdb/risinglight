use std::io::Write;

pub struct Editor<H> {
    _helper: H,
}

impl Editor<()> {
    pub fn new() -> Result<Self> {
        Ok(Self { _helper: () })
    }

    pub fn readline(&mut self, prompt: &str) -> Result<String> {
        print!("{prompt}");
        std::io::stdout().flush()?;
        let mut line = String::new();
        std::io::stdin().read_line(&mut line)?;
        line.pop();
        Ok(line)
    }

    pub fn add_history_entry<S: AsRef<str> + Into<String>>(&mut self, _line: S) -> bool {
        true
    }

    pub fn save_history<P: AsRef<Path> + ?Sized>(&mut self, _path: &P) -> Result<()> {
        Ok(())
    }

    pub fn load_history<P: AsRef<Path> + ?Sized>(&mut self, _path: &P) -> Result<()> {
        Ok(())
    }
}

type Result<T> = std::result::Result<T, ReadlineError>;

use std::path::Path;

use error::*;

pub mod error {
    #[derive(thiserror::Error, Debug)]
    pub enum ReadlineError {
        #[error("interrupted")]
        Interrupted,
        #[error("end of file")]
        Eof,
        #[error("I/O error: {0}")]
        Io(#[from] std::io::Error),
    }
}
