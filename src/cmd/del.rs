use crate::Frame;

use crate::cmd::Invalid;
use crate::rocks::client::RocksClient;

use serde::{Deserialize, Serialize};

use crate::rocks::string::StringCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Del {
    keys: Vec<String>,
    valid: bool,
}

impl Del {
    pub fn new(keys: &[impl ToString]) -> Del {
        Del {
            keys: keys.iter().map(|it| it.to_string()).collect(),
            valid: true,
        }
    }

    /// Get the keys
    pub fn keys(&self) -> &Vec<String> {
        &self.keys
    }

    pub async fn execute(&self, client: &RocksClient) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        StringCommand::new(client).del(&self.keys).await
    }
}

impl Default for Del {
    /// Create a new `Del` command which fetches `key` vector.
    fn default() -> Self {
        Del {
            keys: vec![],
            valid: true,
        }
    }
}

impl Invalid for Del {
    fn new_invalid() -> Self {
        Del {
            keys: vec![],
            valid: false,
        }
    }
}
