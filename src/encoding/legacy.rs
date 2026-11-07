//! The four `@autobee` external types, ported from `autobee/encoding/legacy.js`.
//!
//! The reference only decodes them: `preencode`/`encode` throw, because a
//! record written in a format newer than the reader must never be produced.
//! Encoding panics here for the same reason.

use bytes::Bytes;
use compact_encoding::{Codec, Reader, Result, Writer};

use super::schema::autobase_compat::{Checkout, Clock, Node};

fn decode_only() -> ! {
    panic!("legacy records are decode-only")
}

/// `BootRecordV0`.
#[derive(Debug, Clone, PartialEq)]
pub struct BootRecordV0 {
    pub key: [u8; 32],
    pub system_length: u64,
    pub indexers_updated: bool,
    pub fast_forwarding: bool,
    pub recoveries: u64,
    pub migrating: bool,
    pub heads: Clock,
}

impl Codec for BootRecordV0 {
    fn encoded_len(&self) -> usize {
        decode_only()
    }

    fn encode(&self, _: &mut Writer) {
        decode_only()
    }

    fn decode(r: &mut Reader) -> Result<Self> {
        let indexed = Checkout::decode(r)?;
        let heads = Clock::decode(r)?;
        Ok(Self {
            key: indexed.key,
            system_length: indexed.length,
            indexers_updated: false,
            fast_forwarding: false,
            // the initial recovery is not a fast-forward recovery
            recoveries: 1,
            migrating: false,
            heads,
        })
    }
}

/// `IndexCheckpoint`.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexCheckpoint {
    pub signature: [u8; 64],
    pub length: u64,
}

impl Codec for IndexCheckpoint {
    fn encoded_len(&self) -> usize {
        decode_only()
    }

    fn encode(&self, _: &mut Writer) {
        decode_only()
    }

    fn decode(r: &mut Reader) -> Result<Self> {
        Ok(Self {
            signature: r.fixed::<64>()?,
            length: r.uint()?,
        })
    }
}

/// `Checkpointer`.
#[derive(Debug, Clone, PartialEq)]
pub struct Checkpointer {
    pub checkpointer: u64,
    pub checkpoint: Option<IndexCheckpoint>,
}

impl Codec for Checkpointer {
    fn encoded_len(&self) -> usize {
        decode_only()
    }

    fn encode(&self, _: &mut Writer) {
        decode_only()
    }

    fn decode(r: &mut Reader) -> Result<Self> {
        let checkpointer = r.uint()?;
        let checkpoint = if checkpointer == 0 {
            Some(IndexCheckpoint::decode(r)?)
        } else {
            None
        };
        Ok(Self {
            checkpointer,
            checkpoint,
        })
    }
}

/// `DigestV0` carries the indexer list of a version 0 oplog.
struct DigestV0;

impl DigestV0 {
    fn decode(r: &mut Reader) -> Result<()> {
        let pointer = r.uint()?;
        if pointer == 0 {
            for _ in 0..r.uint()? {
                let _ = r.uint()?;
                let _ = r.fixed::<32>()?;
                let _ = r.fixed::<32>()?;
            }
        }
        Ok(())
    }
}

/// `Digest` (the version 1 shape).
#[derive(Debug, Clone, PartialEq)]
pub struct Digest {
    pub pointer: u64,
    pub key: Option<[u8; 32]>,
}

impl Codec for Digest {
    fn encoded_len(&self) -> usize {
        decode_only()
    }

    fn encode(&self, _: &mut Writer) {
        decode_only()
    }

    fn decode(r: &mut Reader) -> Result<Self> {
        let pointer = r.uint()?;
        Ok(Self {
            pointer,
            key: if pointer == 0 {
                Some(r.fixed::<32>()?)
            } else {
                None
            },
        })
    }
}

/// The trailing `Additional` record of a version 0 oplog.
struct Additional;

impl Additional {
    fn decode(r: &mut Reader) -> Result<()> {
        if r.uint()? == 0 {
            let flags = r.uint()?;
            if flags & 1 != 0 {
                let _ = r.fixed::<32>()?;
            }
            if flags & 2 != 0 {
                let _ = r.uint()?;
            }
        }
        Ok(())
    }
}

/// `OplogMessageV0`.
#[derive(Debug, Clone, PartialEq)]
pub struct OplogMessageV0 {
    pub max_supported_version: u64,
    pub checkpoint: Option<Vec<Checkpointer>>,
    pub node: Node,
}

impl Codec for OplogMessageV0 {
    fn encoded_len(&self) -> usize {
        decode_only()
    }

    fn encode(&self, _: &mut Writer) {
        decode_only()
    }

    fn decode(r: &mut Reader) -> Result<Self> {
        let flags = r.uint()?;
        let is_checkpointer = flags & 1 != 0;
        if is_checkpointer {
            DigestV0::decode(r)?;
        }
        let checkpoint = if is_checkpointer {
            Some(r.array::<Checkpointer>()?)
        } else {
            None
        };
        let node = Node::decode(r)?;
        Additional::decode(r)?;
        let max_supported_version = if r.remaining() > 0 { r.uint()? } else { 0 };
        Ok(Self {
            max_supported_version,
            checkpoint,
            node,
        })
    }
}

/// `OplogMessageV1`.
#[derive(Debug, Clone, PartialEq)]
pub struct OplogMessageV1 {
    pub max_supported_version: u64,
    pub digest: Option<Digest>,
    pub checkpoint: Option<Vec<Checkpointer>>,
    pub optimistic: bool,
    pub node: Node,
}

impl Codec for OplogMessageV1 {
    fn encoded_len(&self) -> usize {
        decode_only()
    }

    fn encode(&self, _: &mut Writer) {
        decode_only()
    }

    fn decode(r: &mut Reader) -> Result<Self> {
        let max_supported_version = r.uint()?;
        let flags = r.uint()?;
        let is_checkpointer = flags & 1 != 0;
        let checkpoint = if is_checkpointer {
            Some(r.array::<Checkpointer>()?)
        } else {
            None
        };
        let digest = if is_checkpointer {
            Some(Digest::decode(r)?)
        } else {
            None
        };
        Ok(Self {
            max_supported_version,
            digest,
            checkpoint,
            optimistic: flags & 2 != 0,
            node: Node::decode(r)?,
        })
    }
}

/// `SystemWriterV0`, the member record of system-writer versions 0 to 3.
#[derive(Debug, Clone, PartialEq)]
pub struct SystemWriterV0 {
    pub is_indexer: bool,
    pub is_removed: bool,
    pub length: u64,
}

impl Codec for SystemWriterV0 {
    fn encoded_len(&self) -> usize {
        decode_only()
    }

    fn encode(&self, _: &mut Writer) {
        decode_only()
    }

    fn decode(r: &mut Reader) -> Result<Self> {
        // The version byte doubles as the flags word; rewind and read it again.
        r.unread(1)?;
        let flags = r.uint()?;
        Ok(Self {
            is_indexer: flags & 1 != 0,
            is_removed: flags & 2 != 0,
            length: r.uint()?,
        })
    }
}

#[allow(dead_code)]
fn _unused(_: Bytes) {}
