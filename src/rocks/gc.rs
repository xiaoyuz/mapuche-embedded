use rocksdb::ColumnFamilyRef;

use crate::rocks::Result as RocksResult;
use crate::{frame::Frame, utils::resp_nil};

use super::{
    client::RocksClient,
    encoding::{DataType, KeyDecoder, KeyEncoder},
    hash::HashCommand,
    list::ListCommand,
    set::SetCommand,
    zset::ZsetCommand,
    TxnCommand, CF_NAME_GC, CF_NAME_GC_VERSION,
};

pub struct GcCF<'a> {
    gc_cf: ColumnFamilyRef<'a>,
    gc_version_cf: ColumnFamilyRef<'a>,
}

impl<'a> GcCF<'a> {
    pub fn new(client: &'a RocksClient) -> Self {
        GcCF {
            gc_cf: client.cf_handle(CF_NAME_GC).unwrap(),
            gc_version_cf: client.cf_handle(CF_NAME_GC_VERSION).unwrap(),
        }
    }
}

#[derive(Debug, Clone)]
struct GcTask {
    key_type: DataType,
    user_key: Vec<u8>,
    version: u16,
}

impl GcTask {
    fn new(key_type: DataType, user_key: Vec<u8>, version: u16) -> GcTask {
        GcTask {
            key_type,
            user_key,
            version,
        }
    }
}

pub struct GcCommand<'a> {
    client: &'a RocksClient,
}

impl<'a> GcCommand<'a> {
    pub fn new(client: &'a RocksClient) -> Self {
        Self { client }
    }

    pub async fn run(&self) -> RocksResult<Frame> {
        let client = self.client.clone();
        let gc_cfs = GcCF::new(client);

        let bound_range = KeyEncoder::encode_gc_version_key_range();

        // TODO scan speed throttling
        let iter_res = client.scan(gc_cfs.gc_version_cf.clone(), bound_range, u32::MAX)?;

        for kv in iter_res {
            let (user_key, version) = KeyDecoder::decode_key_gc_userkey_version(kv.0);
            let key_type = match kv.1[0] {
                0 => DataType::String,
                1 => DataType::Hash,
                2 => DataType::List,
                3 => DataType::Set,
                4 => DataType::Zset,
                _ => DataType::Null,
            };
            let task = GcTask::new(key_type, user_key, version);

            client.exec_txn(|txn| {
                let task = task.clone();
                let user_key = String::from_utf8_lossy(&task.user_key);
                let version = task.version;
                match task.key_type {
                    DataType::String => {
                        panic!("string not support async deletion");
                    }
                    DataType::Set => {
                        SetCommand::new(client).txn_gc(txn, &user_key, version)?;
                    }
                    DataType::List => {
                        ListCommand::new(client).txn_gc(txn, &user_key, version)?;
                    }
                    DataType::Hash => {
                        HashCommand::new(client).txn_gc(txn, &user_key, version)?;
                    }
                    DataType::Zset => {
                        ZsetCommand::new(client).txn_gc(txn, &user_key, version)?;
                    }
                    DataType::Null => {
                        panic!("unknown data type to do async deletion");
                    }
                }
                // delete gc version key
                let gc_version_key = KeyEncoder::encode_gc_version_key(&user_key, version);
                txn.del(gc_cfs.gc_version_cf.clone(), gc_version_key)?;
                Ok(())
            })?;

            // check the gc key in a small txn, avoid transaction confliction
            client.exec_txn(|txn| {
                let task = task.clone();
                let user_key = String::from_utf8_lossy(&task.user_key);
                // also delete gc key if version in gc key is same as task.version
                let gc_key = KeyEncoder::encode_gc_key(&user_key);
                let version = task.version;
                if let Some(v) = txn.get(gc_cfs.gc_cf.clone(), gc_key.clone())? {
                    let ver = u16::from_be_bytes(v[..2].try_into().unwrap());
                    if ver == version {
                        txn.del(gc_cfs.gc_cf.clone(), gc_key)?;
                    }
                }
                Ok(())
            })?;
        }
        Ok(resp_nil())
    }
}
