use crate::rocks::client::RocksClient;
use crate::{cmd::Invalid, rocks::encoding::KeyEncoder};

use crate::rocks::kv::kvpair::KvPair;
use crate::rocks::string::StringCommand;

use crate::utils::resp_invalid_arguments;
use crate::Frame;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::rocks::Result as RocksResult;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Mset {
    keys: Vec<String>,
    vals: Vec<Bytes>,
    valid: bool,
}

impl Mset {
    pub fn new(keys: &[impl ToString], vals: &[impl ToString]) -> Mset {
        Mset {
            keys: keys.iter().map(|it| it.to_string()).collect(),
            vals: vals.iter().map(|it| it.to_string().into()).collect(),
            valid: true,
        }
    }

    /// Get the keys
    pub fn keys(&self) -> &Vec<String> {
        &self.keys
    }

    pub fn vals(&self) -> &Vec<Bytes> {
        &self.vals
    }

    pub async fn execute(&mut self, client: &RocksClient) -> RocksResult<Frame> {
        if !self.valid {
            return Ok(resp_invalid_arguments());
        }
        let mut kvs = Vec::new();
        for (idx, key) in self.keys.iter().enumerate() {
            let val = &self.vals[idx];
            let ekey = KeyEncoder::encode_string(key);
            let eval = KeyEncoder::encode_string_value(&mut val.to_vec(), -1);
            let kvpair = KvPair::from((ekey, eval));
            kvs.push(kvpair);
        }
        StringCommand::new(client).batch_put(kvs).await
    }
}

impl Default for Mset {
    /// Create a new `Mset` command which fetches `key` vector.
    fn default() -> Mset {
        Mset {
            keys: vec![],
            vals: vec![],
            valid: true,
        }
    }
}

impl Invalid for Mset {
    fn new_invalid() -> Mset {
        Mset {
            keys: vec![],
            vals: vec![],
            valid: false,
        }
    }
}
