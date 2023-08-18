use crate::cmd::Invalid;

use crate::rocks::client::RocksClient;
use crate::Frame;

use serde::{Deserialize, Serialize};

use crate::rocks::string::StringCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Strlen {
    key: String,
    valid: bool,
}

impl Strlen {
    pub fn new(key: impl ToString) -> Strlen {
        Strlen {
            key: key.to_string(),
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
        StringCommand::new(client).strlen(&self.key).await
    }
}

impl Invalid for Strlen {
    fn new_invalid() -> Strlen {
        Strlen {
            key: "".to_owned(),
            valid: false,
        }
    }
}
