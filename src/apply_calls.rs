//! `autobee/lib/apply-calls.js`: the `host` object an apply handler calls back
//! into. The writer grants and revocations PearTube issues go through
//! `add_writer` / `remove_writer`.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use crate::types::{Key, Link, Node};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug)]
pub enum Error {
    /// `hypercore-id-encoding` rejected the key text.
    InvalidKey(hypercore_id_encoding::InvalidKey),
    /// The failure the Autobee returned; forwarded unchanged, as in the
    /// reference.
    Host(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::InvalidKey(e) => e.fmt(f),
            Error::Host(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for Error {}

impl From<hypercore_id_encoding::InvalidKey> for Error {
    fn from(e: hypercore_id_encoding::InvalidKey) -> Self {
        Error::InvalidKey(e)
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for Error {
    fn from(e: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Error::Host(e)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// `addWriter(key, { isIndexer, weight })`. The weight defaults from
/// `isIndexer`, so it is kept unset until used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AddWriterOptions {
    pub is_indexer: bool,
    pub weight: Option<u64>,
}

impl AddWriterOptions {
    pub fn indexer() -> Self {
        Self {
            is_indexer: true,
            weight: None,
        }
    }

    pub fn writer() -> Self {
        Self {
            is_indexer: false,
            weight: None,
        }
    }

    fn weight(&self) -> u64 {
        self.weight.unwrap_or(if self.is_indexer { 2 } else { 1 })
    }
}

impl Default for AddWriterOptions {
    fn default() -> Self {
        Self::indexer()
    }
}

/// What `system.addWriter` receives after the apply context is folded in.
#[derive(Debug, Clone, PartialEq)]
pub struct SystemAddWriter {
    pub weight: u64,
    pub is_genesis: bool,
    pub coord: Option<Link>,
    /// `applying[0].weight`, or -1 outside apply.
    pub carrier: i64,
}

/// The `Autobee` side `ApplyCalls` reads and forwards to.
pub trait Auto: Send + Sync {
    fn id(&self) -> Option<&str>;
    fn key(&self) -> Key;
    fn discovery_key(&self) -> Option<Key>;
    fn name(&self) -> Option<&str>;
    fn flushes(&self) -> u64;
    fn is_genesis(&self) -> bool;
    fn add_writer<'a>(
        &'a self,
        key: Key,
        opts: SystemAddWriter,
    ) -> BoxFuture<'a, std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>>;
    fn remove_writer<'a>(
        &'a self,
        key: Key,
    ) -> BoxFuture<'a, std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>>;
    fn interrupt(&self, reason: Option<&str>);
    fn create_anchor<'a>(
        &'a self,
        key: Key,
        length: u64,
    ) -> BoxFuture<'a, std::result::Result<Link, Box<dyn std::error::Error + Send + Sync>>>;
}

pub struct ApplyCalls<'a, 'b> {
    auto: &'a dyn Auto,
    /// `host.applying`: the batch being applied. The reference sets and clears
    /// it around the handler call; a host built per apply carries it directly.
    applying: Option<&'b [Node]>,
}

impl<'a, 'b> ApplyCalls<'a, 'b> {
    pub fn new(auto: &'a dyn Auto, applying: Option<&'b [Node]>) -> Self {
        Self { auto, applying }
    }

    pub fn applying(&self) -> Option<&'b [Node]> {
        self.applying
    }

    pub fn id(&self) -> Option<&str> {
        self.auto.id()
    }

    pub fn key(&self) -> Key {
        self.auto.key()
    }

    pub fn discovery_key(&self) -> Option<Key> {
        self.auto.discovery_key()
    }

    pub fn name(&self) -> Option<&str> {
        self.auto.name()
    }

    pub fn clock(&self) -> u64 {
        self.auto.flushes()
    }

    pub fn genesis(&self) -> bool {
        self.auto.is_genesis()
    }

    /// `host.addWriter(key, opts)`.
    pub async fn add_writer(&self, key: Key, opts: AddWriterOptions) -> Result<()> {
        // no witness needed for genesis
        let applying = self.applying();
        let (coord, carrier) = match applying {
            Some(batch) => {
                let coord = batch.first().map(|node| Link {
                    key: node.key,
                    length: node.length,
                });
                (coord, batch[0].weight)
            }
            None => (None, -1),
        };
        self.auto
            .add_writer(
                key,
                SystemAddWriter {
                    weight: opts.weight(),
                    is_genesis: applying.is_none(),
                    coord,
                    carrier,
                },
            )
            .await
            .map_err(Error::Host)
    }

    /// `host.addWriter(id)` with a string key, as handlers pass `op.writer`.
    pub async fn add_writer_id(&self, id: &str, opts: AddWriterOptions) -> Result<()> {
        let key = hypercore_id_encoding::decode(id)?;
        self.add_writer(key, opts).await
    }

    /// Deprecated in the reference: every applied node is recorded whether or
    /// not apply acks it, so this has nothing left to do. Kept so handlers
    /// written against older versions keep applying.
    pub fn ack_writer(&self) {}

    pub async fn remove_writer(&self, key: Key) -> Result<()> {
        self.auto.remove_writer(key).await.map_err(Error::Host)
    }

    pub async fn remove_writer_id(&self, id: &str) -> Result<()> {
        let key = hypercore_id_encoding::decode(id)?;
        self.remove_writer(key).await
    }

    pub fn prefer_fast_forward(&self) {}

    pub fn interrupt(&self, reason: Option<&str>) {
        self.auto.interrupt(reason)
    }

    pub fn removeable(&self, _key: &Key) -> bool {
        true
    }

    pub async fn create_anchor(&self, key: Key, length: u64) -> Result<Link> {
        self.auto
            .create_anchor(key, length)
            .await
            .map_err(Error::Host)
    }
}
