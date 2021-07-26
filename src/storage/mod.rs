pub mod block;
pub mod file_manager;
pub mod page;
use file_manager::FileManager;
use lazy_static::*;

lazy_static! {
    static ref FILE_MANAGER: FileManager = FileManager {};
}
