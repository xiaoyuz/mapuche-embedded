use crate::Frame;

use crate::cmd::Invalid;
use crate::rocks::client::RocksClient;
use crate::rocks::hash::HashCommand;

use serde::{Deserialize, Serialize};

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Hvals {
    key: String,
    valid: bool,
}

impl Hvals {
    pub fn new(key: impl ToString) -> Hvals {
        Hvals {
            key: key.to_string(),
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub async fn execute(&self, client: &RocksClient) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        HashCommand::new(client)
            .hgetall(&self.key, false, true)
            .await
    }
}

impl Invalid for Hvals {
    fn new_invalid() -> Hvals {
        Hvals {
            key: "".to_owned(),
            valid: false,
        }
    }
}
