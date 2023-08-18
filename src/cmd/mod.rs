mod get;

use futures::future::BoxFuture;
pub use get::Get;
use serde::{Deserialize, Serialize};

mod set;
pub use set::Set;

mod unknown;
pub use unknown::Unknown;

mod mget;
pub use mget::Mget;

mod mset;
pub use mset::Mset;

mod strlen;
pub use strlen::Strlen;

mod cmdtype;
pub use cmdtype::Type;

mod exists;
pub use exists::Exists;

mod incrdecr;
pub use incrdecr::IncrDecr;

mod expire;
pub use expire::Expire;

mod ttl;
pub use ttl::TTL;

mod del;
pub use del::Del;

mod scan;
pub use scan::Scan;

mod sadd;
pub use sadd::Sadd;

mod scard;
pub use scard::Scard;

mod sismember;
pub use sismember::Sismember;

mod smismember;
pub use smismember::Smismember;

mod srandmember;
pub use srandmember::Srandmember;

mod smembers;
pub use smembers::Smembers;

mod srem;
pub use srem::Srem;

mod spop;
pub use spop::Spop;

mod push;
pub use push::Push;

mod pop;
pub use pop::Pop;

mod ltrim;
pub use ltrim::Ltrim;

mod lrange;
pub use lrange::Lrange;

mod llen;
pub use llen::Llen;

mod lindex;
pub use lindex::Lindex;

mod lset;
pub use lset::Lset;

mod linsert;
pub use linsert::Linsert;

mod lrem;
pub use lrem::Lrem;

mod hset;
pub use hset::Hset;

mod hget;
pub use hget::Hget;

mod hstrlen;
pub use hstrlen::Hstrlen;

mod hexists;
pub use hexists::Hexists;

mod hmget;
pub use hmget::Hmget;

mod hlen;
pub use hlen::Hlen;

mod hgetall;
pub use hgetall::Hgetall;

mod hkeys;
pub use hkeys::Hkeys;

mod hvals;
pub use hvals::Hvals;

mod hdel;
pub use hdel::Hdel;

mod hincrby;
pub use hincrby::Hincrby;

mod zadd;
pub use zadd::Zadd;

mod zcard;
pub use zcard::Zcard;

mod zscore;
pub use zscore::Zscore;

mod zcount;
pub use zcount::Zcount;

mod zrange;
pub use zrange::Zrange;

mod zrevrange;
pub use zrevrange::Zrevrange;

mod zrangebyscore;
pub use zrangebyscore::Zrangebyscore;

mod zpop;
pub use zpop::Zpop;

mod zrank;
pub use zrank::Zrank;

mod zincrby;
pub use zincrby::Zincrby;

mod zrem;
pub use zrem::Zrem;

mod zremrangebyrank;
pub use zremrangebyrank::Zremrangebyrank;

mod zremrangebyscore;
pub use zremrangebyscore::Zremrangebyscore;

mod keys;
pub use keys::Keys;

use crate::config::txn_retry_count;
use crate::db::DBInner;
use crate::Frame;

use crate::rocks::Result as RocksResult;

/// Enumeration of supported Redis commands.
///
/// Methods called on `Command` are delegated to the command implementation.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Command {
    Get(Get),
    Mget(Mget),
    Mset(Mset),
    Set(Set),
    Del(Del),
    Strlen(Strlen),
    Type(Type),
    Exists(Exists),
    Incr(IncrDecr),
    Decr(IncrDecr),
    Expire(Expire),
    ExpireAt(Expire),
    Pexpire(Expire),
    PexpireAt(Expire),
    TTL(TTL),
    PTTL(TTL),
    Scan(Scan),
    Keys(Keys),

    // set
    Sadd(Sadd),
    Scard(Scard),
    Sismember(Sismember),
    Smismember(Smismember),
    Smembers(Smembers),
    Srandmember(Srandmember),
    Spop(Spop),
    Srem(Srem),

    // list
    Lpush(Push),
    Rpush(Push),
    Lpop(Pop),
    Rpop(Pop),
    Lrange(Lrange),
    Ltrim(Ltrim),
    Llen(Llen),
    Lindex(Lindex),
    Lset(Lset),
    Lrem(Lrem),
    Linsert(Linsert),

    // hash
    Hset(Hset),
    Hmset(Hset),
    Hsetnx(Hset),
    Hget(Hget),
    Hmget(Hmget),
    Hlen(Hlen),
    Hgetall(Hgetall),
    Hdel(Hdel),
    Hkeys(Hkeys),
    Hvals(Hvals),
    Hincrby(Hincrby),
    Hexists(Hexists),
    Hstrlen(Hstrlen),

    // sorted set
    Zadd(Zadd),
    Zcard(Zcard),
    Zscore(Zscore),
    Zrem(Zrem),
    Zremrangebyscore(Zremrangebyscore),
    Zremrangebyrank(Zremrangebyrank),
    Zrange(Zrange),
    Zrevrange(Zrevrange),
    Zrangebyscore(Zrangebyscore),
    Zrevrangebyscore(Zrangebyscore),
    Zcount(Zcount),
    Zpopmin(Zpop),
    Zpopmax(Zpop),
    Zrank(Zrank),
    Zincrby(Zincrby),

    Unknown(Unknown),
}

impl Command {
    pub(crate) async fn execute(mut self, inner_db: &DBInner) -> crate::Result<Frame> {
        use Command::*;

        let client = &inner_db.client;

        let response = match &mut self {
            Get(cmd) => cmd.execute(client).await,
            Mget(cmd) => cmd.execute(client).await,
            Mset(cmd) => cmd.execute(client).await,
            Set(cmd) => cmd.execute(client).await,
            Del(cmd) => cmd.execute(client).await,
            Strlen(cmd) => cmd.execute(client).await,
            Type(cmd) => cmd.execute(client).await,
            Exists(cmd) => cmd.execute(client).await,
            Incr(cmd) => cmd.execute(client, true).await,
            Decr(cmd) => cmd.execute(client, false).await,
            Expire(cmd) => cmd.execute(client, false, false).await,
            ExpireAt(cmd) => cmd.execute(client, false, true).await,
            Pexpire(cmd) => cmd.execute(client, true, false).await,
            PexpireAt(cmd) => cmd.execute(client, true, true).await,
            TTL(cmd) => cmd.execute(client, false).await,
            PTTL(cmd) => cmd.execute(client, true).await,
            Scan(cmd) => cmd.execute(client).await,
            Keys(cmd) => cmd.execute(client).await,
            Sadd(cmd) => cmd.execute(client).await,
            Scard(cmd) => cmd.execute(client).await,
            Sismember(cmd) => cmd.execute(client).await,
            Smismember(cmd) => cmd.execute(client).await,
            Smembers(cmd) => cmd.execute(client).await,
            Srandmember(cmd) => cmd.execute(client).await,
            Spop(cmd) => cmd.execute(client).await,
            Srem(cmd) => cmd.execute(client).await,
            Lpush(cmd) => cmd.execute(client, true).await,
            Rpush(cmd) => cmd.execute(client, false).await,
            Lpop(cmd) => cmd.execute(client, true).await,
            Rpop(cmd) => cmd.execute(client, false).await,
            Lrange(cmd) => cmd.execute(client).await,
            Ltrim(cmd) => cmd.execute(client).await,
            Llen(cmd) => cmd.execute(client).await,
            Lindex(cmd) => cmd.execute(client).await,
            Lset(cmd) => cmd.execute(client).await,
            Lrem(cmd) => cmd.execute(client).await,
            Linsert(cmd) => cmd.execute(client).await,
            Hset(cmd) => cmd.execute(client, false, false).await,
            Hmset(cmd) => cmd.execute(client, true, false).await,
            Hsetnx(cmd) => cmd.execute(client, false, true).await,
            Hget(cmd) => cmd.execute(client).await,
            Hmget(cmd) => cmd.execute(client).await,
            Hlen(cmd) => cmd.execute(client).await,
            Hgetall(cmd) => cmd.execute(client).await,
            Hdel(cmd) => cmd.execute(client).await,
            Hkeys(cmd) => cmd.execute(client).await,
            Hvals(cmd) => cmd.execute(client).await,
            Hincrby(cmd) => cmd.execute(client).await,
            Hexists(cmd) => cmd.execute(client).await,
            Hstrlen(cmd) => cmd.execute(client).await,
            Zadd(cmd) => cmd.execute(client).await,
            Zcard(cmd) => cmd.execute(client).await,
            Zscore(cmd) => cmd.execute(client).await,
            Zrem(cmd) => cmd.execute(client).await,
            Zremrangebyscore(cmd) => cmd.execute(client).await,
            Zremrangebyrank(cmd) => cmd.execute(client).await,
            Zrange(cmd) => cmd.execute(client).await,
            Zrevrange(cmd) => cmd.execute(client).await,
            Zrangebyscore(cmd) => cmd.execute(client, false).await,
            Zrevrangebyscore(cmd) => cmd.execute(client, true).await,
            Zcount(cmd) => cmd.execute(client).await,
            Zpopmin(cmd) => cmd.execute(client, true).await,
            Zpopmax(cmd) => cmd.execute(client, false).await,
            Zrank(cmd) => cmd.execute(client).await,
            Zincrby(cmd) => cmd.execute(client).await,

            Unknown(cmd) => cmd.apply().await,
        }?;

        Ok(response)
    }
}

impl From<&str> for Command {
    fn from(value: &str) -> Self {
        serde_json::from_str(value).unwrap()
    }
}

impl From<&Command> for String {
    fn from(value: &Command) -> Self {
        serde_json::to_string(value).unwrap()
    }
}

/// All commands should be implement new_invalid() for invalid check
pub trait Invalid {
    fn new_invalid() -> Self;
}

async fn retry_call<'a, F>(mut f: F) -> RocksResult<Frame>
where
    F: FnMut() -> BoxFuture<'a, RocksResult<Frame>> + Copy,
{
    let mut retry = txn_retry_count();
    let mut res = Frame::Null;
    while retry > 0 {
        res = f().await?;
        if let Frame::TxnFailed(_) = res {
            retry -= 1;
            continue;
        }
        return Ok(res);
    }
    Ok(res)
}
