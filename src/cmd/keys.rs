use crate::rocks::client::RocksClient;
use crate::Frame;
use serde::{Deserialize, Serialize};

use crate::cmd::Invalid;

use crate::rocks::string::StringCommand;
use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Keys {
    regex: String,
    valid: bool,
}

impl Keys {
    pub fn new(regex: impl ToString) -> Keys {
        Keys {
            regex: regex.to_string(),
            valid: true,
        }
    }

    pub fn valid(&self) -> bool {
        self.valid
    }

    pub async fn execute(&mut self, client: &RocksClient) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        StringCommand::new(client).keys(&self.regex).await
    }
}

impl Invalid for Keys {
    fn new_invalid() -> Keys {
        Keys {
            regex: "".to_owned(),
            valid: false,
        }
    }
}
