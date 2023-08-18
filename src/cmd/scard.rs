use crate::Frame;

use crate::cmd::Invalid;
use crate::rocks::client::RocksClient;

use serde::{Deserialize, Serialize};

use crate::rocks::set::SetCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Scard {
    key: String,
    valid: bool,
}

impl Scard {
    pub fn new(key: impl ToString) -> Scard {
        Scard {
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
        SetCommand::new(client).scard(&self.key).await
    }
}

impl Invalid for Scard {
    fn new_invalid() -> Scard {
        Scard {
            key: "".to_owned(),
            valid: false,
        }
    }
}
