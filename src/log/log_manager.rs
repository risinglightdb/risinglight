struct LogManager {
    current_lsn: u64,
    current_page: Page,
    file_name: String,
    file_manager: Arc<Mutex<FileManager>> 
}

impl LogManager {
    pub fn new(name: String, file_mgr: Arc<Mutex<FileManager>>) -> LogManager {
        LogManager {
            file_name: name.clone(),
            current_lsn: 0,
            current_page: Page::new(Block {
                name: name.clone(),
                id: 0
            }),
            file_manager: file_mgr.clone()
            }
        }
    }
}