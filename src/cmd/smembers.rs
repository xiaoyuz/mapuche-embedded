use crate::Frame;

use crate::cmd::Invalid;
use crate::rocks::client::RocksClient;

use serde::{Deserialize, Serialize};

use crate::rocks::set::SetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Smembers {
    key: String,
    valid: bool,
}

impl Smembers {
    pub fn new(key: impl ToString) -> Smembers {
        Smembers {
            key: key.to_string(),
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub async fn execute(&mut self, client: &RocksClient) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        SetCommand::new(client).smembers(&self.key).await
    }
}

impl Invalid for Smembers {
    fn new_invalid() -> Smembers {
        Smembers {
            key: "".to_owned(),
            valid: false,
        }
    }
}
