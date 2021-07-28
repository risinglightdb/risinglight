use crate::storage::FileManager;
use crate::log::LogManager
use std::sync::Arc;
pub struct LightdbServer {
    file_manager: Arc<Mutex<FileManager>> 
    log_manager: Arc<Mutex<LogManager>>
}

impl LightdbServer {
    pub fn new() -> LightdbServer {
        let file_mgr = Arc::new(Mutex::new(FileManager::new()));
        LightdbServer {
            file_manager: file_mgr.clone(),
            log_manager: LogManager::new("log.bin")
        }
    }
}

#[cfg(test)]
mod server_test {
    use super::*;

    #[test]
    fn db_server_test() {
        let db = LightdbServer::new();
    }
}