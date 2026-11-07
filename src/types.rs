//! Shared in-memory records, mirroring the JS objects the modules pass around:
//! `createNode` in `lib/writers.js`, the batches `lib/topo.js` returns, and the
//! system `info` record.

use std::cmp::Ordering;
use std::fmt;

use bytes::Bytes;
use hypercore::Core;

pub use crate::encoding::{Approval, Batch as BatchCoords, GrantWitness, Link, TrustedHead, Views};
use crate::encoding::{DecodedOplog, SystemInfoV3};

/// A hypercore public key.
pub type Key = [u8; 32];

/// `advanceHash`'s eight-byte result.
pub type Hash = [u8; 8];

/// A topo batch: `null` holes are kept so an incomplete replay stays visible.
pub type Batch = Vec<Option<Node>>;

/// `EMPTY_HEAD`: the head with no core behind it.
pub const EMPTY_HEAD: Head = Head {
    key: None,
    length: 0,
};

/// `{ key, length }`. `key` is `None` where the reference has `null`
/// (`EMPTY_HEAD`); every head that names blocks has a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Head {
    pub key: Option<Key>,
    pub length: u64,
}

impl Head {
    /// The `Link` form, when a core backs the head.
    pub fn link(self) -> Option<Link> {
        self.key.map(|key| Link {
            key,
            length: self.length,
        })
    }
}

impl From<Link> for Head {
    fn from(link: Link) -> Self {
        Head {
            key: Some(link.key),
            length: link.length,
        }
    }
}

impl From<&Link> for Head {
    fn from(link: &Link) -> Self {
        link.clone().into()
    }
}

impl From<&Node> for Head {
    fn from(node: &Node) -> Self {
        Head {
            key: Some(node.key),
            length: node.length,
        }
    }
}

/// The writer core a node was read from (`node.core`, `node.from` in the
/// legacy paths). A handle, so nodes stay cheap to clone.
#[derive(Clone)]
pub struct WriterRef(pub Core);

impl WriterRef {
    pub fn key(&self) -> &Key {
        self.0.key()
    }
}

impl fmt::Debug for WriterRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("WriterRef").field(self.key()).finish()
    }
}

impl PartialEq for WriterRef {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for WriterRef {}

/// The coordinates `topo.sort` compares: what a batch head and a pinned oplog
/// reference have in common.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WeightedLink {
    pub key: Key,
    pub length: u64,
    pub weight: i64,
    pub timestamp: u64,
}

impl From<Link> for WeightedLink {
    fn from(link: Link) -> Self {
        WeightedLink {
            key: link.key,
            length: link.length,
            weight: 0,
            timestamp: 0,
        }
    }
}

impl From<&Node> for WeightedLink {
    fn from(node: &Node) -> Self {
        WeightedLink {
            key: node.key,
            length: node.length,
            weight: node.weight,
            timestamp: node.timestamp,
        }
    }
}

/// `topo.cmp`: heavier first, then older, then lower key, then shorter.
///
/// The reference's length compare returns 1 instead of 0 for equal lengths, so
/// identical coordinates order as `Greater`; that never reaches `Equal`.
pub fn cmp_node(a: &WeightedLink, b: &WeightedLink) -> Ordering {
    b.weight
        .cmp(&a.weight)
        .then(a.timestamp.cmp(&b.timestamp))
        .then(a.key.cmp(&b.key))
        .then_with(|| {
            if a.length < b.length {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        })
}

/// One `system.bee` changes-stream entry: the oplog reference pinned by that
/// flush and the head its batch occupied before.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub oplog: WeightedLink,
    pub undo: Option<Head>,
}

/// `createNode`: an oplog block read back at `length`, with the weight the
/// caller resolved for it.
#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    pub core: Option<WriterRef>,
    pub key: Key,
    pub length: u64,
    pub weight: i64,
    pub version: u64,
    pub timestamp: u64,
    pub links: Vec<Link>,
    pub batch: BatchCoords,
    pub views: Option<Views>,
    pub trusted: Vec<TrustedHead>,
    pub witness: Option<GrantWitness>,
    pub approvals: Option<Vec<Approval>>,
    pub optimistic: bool,
    pub value: Option<Bytes>,
    pub hash: Option<Hash>,
    pub weight_hint: u64,
    pub drift: u64,
}

impl Node {
    /// `createNode(core, length, weight, oplog)`.
    pub fn from_oplog(
        key: Key,
        length: u64,
        weight: i64,
        oplog: &DecodedOplog,
        core: Option<WriterRef>,
    ) -> Self {
        match oplog {
            DecodedOplog::Mapped(m) => Node {
                core,
                key,
                length,
                weight,
                version: m.version,
                timestamp: m.timestamp,
                links: m.links.clone(),
                batch: m.batch.clone(),
                views: m.views.clone(),
                trusted: m.trusted.clone().unwrap_or_default(),
                witness: m.witness.clone(),
                approvals: m.approvals.clone(),
                optimistic: m.optimistic,
                value: m.value.clone(),
                hash: None,
                weight_hint: m.weight_hint,
                drift: m.drift,
            },
            DecodedOplog::V4(m) => Node {
                core,
                key,
                length,
                weight,
                version: crate::constants::OPLOG_VERSION,
                timestamp: m.timestamp,
                links: m.links.clone(),
                batch: m.batch.clone().unwrap_or(BatchCoords { start: 0, end: 0 }),
                views: m.views.clone(),
                trusted: link_trusted(m.trusted.clone()),
                witness: m.witness.clone(),
                approvals: m.approvals.clone(),
                optimistic: m.optimistic,
                value: m.value.clone(),
                hash: m.hash,
                weight_hint: m.weight_hint.unwrap_or(0),
                drift: m.drift.unwrap_or(0),
            },
        }
    }
}

/// Version 4 trusted heads are plain links; the node shape keeps the legacy
/// record, whose flush count only legacy replays fill in.
fn link_trusted(links: Option<Vec<Link>>) -> Vec<TrustedHead> {
    links
        .unwrap_or_default()
        .into_iter()
        .map(|link| TrustedHead {
            key: link.key,
            length: link.length,
            flushes: 0,
        })
        .collect()
}

/// The version 3 system `info` record `system.flush` writes and reads back.
#[derive(Debug, Clone, PartialEq)]
pub struct Info {
    pub version: u64,
    pub timestamp: u64,
    pub flushes: u64,
    pub view: Head,
    pub heads: Vec<Head>,
    pub indexers: Option<Vec<Head>>,
    pub pending: Option<Vec<bool>>,
    pub hash: Option<Hash>,
}

impl From<SystemInfoV3> for Info {
    fn from(m: SystemInfoV3) -> Self {
        Info {
            version: crate::constants::AUTOBEE_VERSION,
            timestamp: m.timestamp,
            flushes: m.flushes,
            view: m.view.into(),
            heads: m.heads.into_iter().map(Head::from).collect(),
            indexers: m
                .indexers
                .map(|heads| heads.into_iter().map(Head::from).collect()),
            pending: m.pending,
            hash: m.hash,
        }
    }
}

impl From<&SystemInfoV3> for Info {
    fn from(m: &SystemInfoV3) -> Self {
        m.clone().into()
    }
}

/// `topo.sort`'s return: the head to rewind to, the view to move to, and the
/// batches that still have to be applied.
#[derive(Debug, Clone, PartialEq)]
pub struct SortResult {
    pub undo: Option<Head>,
    pub view: Option<Head>,
    pub tip: Vec<Batch>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(length: u64, weight: i64, timestamp: u64, key: u8) -> WeightedLink {
        WeightedLink {
            key: [key; 32],
            length,
            weight,
            timestamp,
        }
    }

    #[test]
    fn cmp_matches_topo_order() {
        let a = node(1, 2, 10, 0);
        // heavier first
        assert_eq!(cmp_node(&a, &node(1, 1, 0, 0)), Ordering::Less);
        // then older
        assert_eq!(cmp_node(&a, &node(1, 2, 11, 0)), Ordering::Less);
        // then lower key
        assert_eq!(cmp_node(&a, &node(1, 2, 10, 1)), Ordering::Less);
        // then shorter; equal coordinates are not Equal in the reference
        assert_eq!(cmp_node(&a, &node(2, 2, 10, 0)), Ordering::Less);
        assert_eq!(cmp_node(&a, &a), Ordering::Greater);
    }

    #[test]
    fn head_link_round_trip() {
        let link = Link {
            key: [7; 32],
            length: 3,
        };
        assert_eq!(Head::from(link.clone()).link(), Some(link.clone()));
        assert_eq!(EMPTY_HEAD.link(), None);
        let w = node(3, 0, 0, 7);
        assert_eq!(
            Head {
                key: Some(w.key),
                length: w.length
            },
            Head::from(link)
        );
    }
}
