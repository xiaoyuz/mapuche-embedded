use crate::Frame;

use crate::cmd::Invalid;
use crate::rocks::client::RocksClient;
use crate::rocks::list::ListCommand;

use serde::{Deserialize, Serialize};

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Lindex {
    key: String,
    idx: i64,
    valid: bool,
}

impl Lindex {
    pub fn new(key: impl ToString, idx: i64) -> Lindex {
        Lindex {
            key: key.to_string(),
            idx,
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub async fn execute(&mut self, client: &RocksClient) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        ListCommand::new(client).lindex(&self.key, self.idx).await
    }
}

impl Invalid for Lindex {
    fn new_invalid() -> Lindex {
        Lindex {
            key: "".to_owned(),
            idx: 0,
            valid: false,
        }
    }
}
