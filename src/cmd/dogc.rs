use crate::Frame;

use crate::cmd::Invalid;
use crate::rocks::client::RocksClient;
use crate::rocks::gc::GcCommand;

use serde::{Deserialize, Serialize};

use crate::rocks::Result as RocksResult;
use crate::utils::resp_invalid_arguments;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Gc {
    valid: bool,
}

impl Gc {
    pub fn new() -> Gc {
        Gc { valid: true }
    }

    pub async fn execute(&self, client: &RocksClient) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        GcCommand::new(client).run().await
    }
}

impl Default for Gc {
    fn default() -> Self {
        Self::new()
    }
}

impl Invalid for Gc {
    fn new_invalid() -> Self {
        Gc { valid: false }
    }
}
