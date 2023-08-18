use crate::Frame;

use crate::cmd::Invalid;
use crate::rocks::client::RocksClient;
use crate::rocks::hash::HashCommand;

use serde::{Deserialize, Serialize};

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Hdel {
    key: String,
    fields: Vec<String>,
    valid: bool,
}

impl Hdel {
    pub fn new(key: impl ToString, fields: &[impl ToString]) -> Hdel {
        Hdel {
            fields: fields.iter().map(|it| it.to_string()).collect(),
            key: key.to_string(),
            valid: true,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub async fn execute(&self, client: &RocksClient) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        HashCommand::new(client).hdel(&self.key, &self.fields).await
    }
}

impl Invalid for Hdel {
    fn new_invalid() -> Hdel {
        Hdel {
            fields: vec![],
            key: "".to_owned(),
            valid: false,
        }
    }
}
