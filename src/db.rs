use std::path::Path;

use std::sync::Arc;

use crate::{
    rocks::{client::RocksClient, new_client},
    Result,
};

pub struct DBInner {
    pub(crate) client: Arc<RocksClient>,
}

impl DBInner {
    pub(crate) async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let client = new_client(path)?;
        let client = Arc::new(client);
        Ok(Self { client })
    }
}
