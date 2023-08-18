use crate::cmd::Invalid;

use crate::rocks::client::RocksClient;
use crate::Frame;

use serde::{Deserialize, Serialize};

use crate::rocks::string::StringCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Exists {
    keys: Vec<String>,
    valid: bool,
}

impl Exists {
    pub fn new(keys: &[impl ToString]) -> Exists {
        Exists {
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
        StringCommand::new(client).exists(&self.keys).await
    }
}

impl Default for Exists {
    fn default() -> Self {
        Exists {
            keys: vec![],
            valid: true,
        }
    }
}

impl Invalid for Exists {
    fn new_invalid() -> Exists {
        Exists {
            keys: vec![],
            valid: false,
        }
    }
}
