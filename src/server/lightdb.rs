use crate::log::log_manager::LogManager;
use crate::storage::block::Block;
use crate::storage::file_manager::FileManager;
use crate::storage::page::Page;
use std::sync::{Arc, Mutex};

pub struct LightDB {
    file_manager: Arc<Mutex<FileManager>>,
    log_manager: Arc<Mutex<LogManager>>,
}

impl LightDB {
    pub fn new() -> LightDB {
        let file_mgr = Arc::new(Mutex::new(FileManager::new()));
        LightDB {
            file_manager: file_mgr.clone(),
            log_manager: Arc::new(Mutex::new(LogManager::new(
                String::from("log.bin"),
                file_mgr.clone(),
            ))),
        }
    }
}

#[cfg(test)]
mod server_test {
    use super::*;

    #[test]
    fn db_server_test() {
        let db = LightDB::new();
    }
}
