use crate::Frame;

use crate::cmd::Invalid;
use crate::rocks::client::RocksClient;
use crate::rocks::hash::HashCommand;

use serde::{Deserialize, Serialize};

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Hincrby {
    key: String,
    field: String,
    step: i64,
    valid: bool,
}

impl Hincrby {
    pub fn new(key: impl ToString, field: impl ToString, step: i64) -> Hincrby {
        Hincrby {
            key: key.to_string(),
            field: field.to_string(),
            step,
            valid: true,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn field(&self) -> &str {
        &self.field
    }

    pub async fn execute(&self, client: &RocksClient) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        HashCommand::new(client)
            .hincrby(&self.key, &self.field, self.step)
            .await
    }
}

impl Invalid for Hincrby {
    fn new_invalid() -> Hincrby {
        Hincrby {
            key: "".to_string(),
            field: "".to_string(),
            step: 0,
            valid: false,
        }
    }
}
