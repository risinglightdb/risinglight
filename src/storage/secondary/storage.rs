use super::{SecondaryStorage, StorageResult};
use tokio::fs;

impl SecondaryStorage {
    pub async fn bootstrap(&mut self) -> StorageResult<()> {
        // create folder if not exist
        fs::create_dir(&self.options.path).await.ok();
        Ok(())
    }
}
