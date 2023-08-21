pub mod cmd;
pub mod frame;

mod config;
mod db;
mod rocks;
mod utils;

use cmd::{Command, Gc};

use db::DBInner;
use frame::Frame;
use std::{path::Path, sync::Arc, time::Duration};
use tokio::{
    spawn,
    time::{interval, MissedTickBehavior},
};

/// Error returned by most functions.
///
/// When writing a real application, one might want to consider a specialized
/// error handling crate or defining an error type as an `enum` of causes.
/// However, for our example, using a boxed `std::error::Error` is sufficient.
///
/// For performance reasons, boxing is avoided in any hot path. For example, in
/// `parse`, a custom error `enum` is defined. This is because the error is hit
/// and handled during normal execution when a partial frame is received on a
/// socket. `std::error::Error` is implemented for `parse::Error` which allows
/// it to be converted to `Box<dyn std::error::Error>`.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// A specialized `Result` type for mapuche operations.
///
/// This is defined as a convenience.
pub type Result<T> = anyhow::Result<T, Error>;

#[derive(Clone)]
pub struct OpenOptions {
    pub(crate) gc_enabled: bool,
    pub(crate) gc_interval: u64,
}

impl OpenOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn gc_enable(mut self, value: bool) -> Self {
        self.gc_enabled = value;
        self
    }

    pub fn gc_interval(mut self, value: u64) -> Self {
        self.gc_interval = value;
        self
    }

    pub async fn open<P: AsRef<Path>>(self, path: P) -> Result<DB> {
        let inner = DBInner::open(path, self.gc_enabled).await?;
        let inner = Arc::new(inner);
        Ok(DB { inner })
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            gc_enabled: false,
            gc_interval: u64::MAX,
        }
    }
}

#[derive(Clone)]
pub struct DB {
    pub(crate) inner: Arc<DBInner>,
}

impl DB {
    pub async fn open<P: AsRef<Path>>(
        path: P,
        async_deletion_enabled: bool,
        gc_interval: u64,
    ) -> Result<Self> {
        let db = OpenOptions::new().open(path).await?;
        let cloned = db.clone();
        if async_deletion_enabled {
            spawn(async move {
                let mut interval = interval(Duration::from_millis(gc_interval));
                interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
                loop {
                    interval.tick().await;
                    let conn = cloned.conn();
                    let cmd = Command::Gc(Gc::new());
                    let _ = conn.execute(cmd).await;
                }
            });
        }

        Ok(db)
    }

    pub fn conn(&self) -> Conn {
        Conn {
            inner: self.inner.clone(),
        }
    }
}

pub struct Conn {
    pub(crate) inner: Arc<DBInner>,
}

impl Conn {
    pub async fn execute(&self, cmd: Command) -> crate::Result<Frame> {
        cmd.execute(&self.inner).await
    }
}
