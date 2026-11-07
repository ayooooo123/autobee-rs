//! Autobee encodings, mirroring `autobee/lib/encoding.js`.
//!
//! The schema codecs are generated from `spec/autobee/schema.json` by
//! [`hyperschema`] in `build.rs`; the helpers below are the hand-written
//! wrappers around them. The three `map` functions of the schema are applied
//! here because the generator leaves mapped versions raw.

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use compact_encoding::{Codec, Error, Reader, Result, encode};

pub mod legacy;

pub mod schema {
    include!(concat!(env!("OUT_DIR"), "/autobee_schema.rs"));
}

pub use schema::autobase_compat::{
    BootRecord, BootRecordRaw, Checkout, Clock, InfoV1, InfoV2, OplogMessageV2,
};
pub use schema::autobee::{
    Approval, Batch, ExternalBatch, GrantWitness, Link, ManifestData, OplogMessageV3,
    OplogMessageV4, SystemInfo, SystemInfoV3, SystemWriter, SystemWriterV4, SystemWriterV5,
    TrustedHead, Views,
};

pub type Oplog = schema::autobee::Oplog;

/// `OPLOG_VERSION`.
pub const OPLOG_VERSION: u64 = 4;
/// The version `encodeValue({ legacy: true })` writes.
pub const LEGACY_OPLOG_VERSION: u64 = 2;
/// `LEGACY_TIME`: the timestamp the reference stamps on legacy oplogs.
pub const LEGACY_TIME: u64 = 1_789_460_100_000;
/// `AutobeeEncryption.PADDING`: the encrypted envelope's leading block.
pub const PADDING: usize = 8;

// ---------------------------------------------------------------------------
// boot records

pub fn encode_boot_record(r: &Link) -> Bytes {
    encode(r)
}

pub fn decode_boot_record(value: impl Into<Bytes>) -> Result<Link> {
    compact_encoding::decode(value)
}

pub fn encode_previous_drain(d: u64) -> Bytes {
    encode(&d)
}

pub fn decode_previous_drain(value: impl Into<Bytes>) -> Result<u64> {
    compact_encoding::decode(value)
}

pub fn decode_autobase_boot_record(value: impl Into<Bytes>) -> Result<BootRecord> {
    compact_encoding::decode(value)
}

// ---------------------------------------------------------------------------
// system keys

/// `[1, 0, key]`.
pub fn encode_system_writer_key(key: &[u8; 32]) -> [u8; 34] {
    let mut buf = [0u8; 34];
    buf[0] = 1;
    buf[2..].copy_from_slice(key);
    buf
}

/// `[5, key]`.
pub fn encode_system_pending_key(key: &[u8; 32]) -> [u8; 33] {
    let mut buf = [0u8; 33];
    buf[0] = 5;
    buf[1..].copy_from_slice(key);
    buf
}

pub fn decode_system_pending_key(buf: &[u8]) -> Result<&[u8; 32]> {
    buf.get(1..33)
        .and_then(|key| key.try_into().ok())
        .ok_or(Error::OutOfBounds)
}

/// `{ gte: [5], lt: [6] }`.
pub fn encode_system_pending_range() -> ([u8; 1], [u8; 1]) {
    ([5], [6])
}

pub fn encode_system_pending(weight: u64) -> Bytes {
    encode(&weight)
}

pub fn decode_system_pending(value: impl Into<Bytes>) -> Result<u64> {
    compact_encoding::decode(value)
}

/// `[4, key]`.
pub fn encode_system_grant_key(key: &[u8; 32]) -> [u8; 33] {
    let mut buf = [0u8; 33];
    buf[0] = 4;
    buf[1..].copy_from_slice(key);
    buf
}

/// The grant hint the reference encodes as `{ key, length, weight }`.
#[derive(Debug, Clone, PartialEq)]
pub struct GrantHint {
    pub key: [u8; 32],
    pub length: u64,
    pub weight: u64,
}

pub fn encode_system_grant_hint(hint: &GrantHint) -> Bytes {
    let mut w = compact_encoding::Writer::with_capacity(32 + 10 + 10);
    w.raw(&hint.key);
    w.uint(hint.length);
    w.uint(hint.weight);
    w.finish()
}

pub fn decode_system_grant_hint(value: impl Into<Bytes>) -> Result<GrantHint> {
    let mut r = Reader::new(value.into());
    Ok(GrantHint {
        key: r.fixed::<32>()?,
        length: r.uint()?,
        weight: r.uint()?,
    })
}

// ---------------------------------------------------------------------------
// system writers

pub fn encode_system_writer(m: &SystemWriter) -> Bytes {
    encode(m)
}

/// `decodeSystemWriter`'s return: the writer key (from the key prefix) and the
/// version-mapped record.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedSystemWriter {
    pub key: Bytes,
    pub value: SystemWriterInfo,
}

/// The record shapes the versioned `system-writer` decodes to.
#[derive(Debug, Clone, PartialEq)]
pub enum SystemWriterInfo {
    /// The `memberLegacyMap` shape of versions 0 to 3.
    Legacy(LegacySystemWriter),
    V4(SystemWriterV4),
    V5(SystemWriterV5),
}

/// `memberLegacyMap`.
#[derive(Debug, Clone, PartialEq)]
pub struct LegacySystemWriter {
    pub version: u64,
    pub is_removed: bool,
    pub is_oplog: bool,
    pub is_genesis: bool,
    pub weight: u64,
    pub max_weight: u64,
    pub length: u64,
    pub timestamp: i64,
}

pub fn decode_system_writer(key: &[u8], value: impl Into<Bytes>) -> Result<DecodedSystemWriter> {
    let writer = decode_system_writer_value(value)?;
    Ok(DecodedSystemWriter {
        key: key.get(2..).map_or_else(Bytes::new, Bytes::copy_from_slice),
        value: writer,
    })
}

fn decode_system_writer_value(value: impl Into<Bytes>) -> Result<SystemWriterInfo> {
    Ok(match compact_encoding::decode::<SystemWriter>(value)? {
        SystemWriter::V3(m) => {
            let weight = if m.is_indexer { 2 } else { 1 };
            SystemWriterInfo::Legacy(LegacySystemWriter {
                // memberLegacyMap pins the version, it does not carry the wire one
                version: 0,
                is_removed: m.is_removed,
                is_oplog: false,
                is_genesis: false,
                weight,
                max_weight: weight,
                length: m.length,
                timestamp: 0,
            })
        }
        SystemWriter::V4(m) => SystemWriterInfo::V4(m),
        SystemWriter::V5(m) => SystemWriterInfo::V5(m),
    })
}

// ---------------------------------------------------------------------------
// system info

pub fn encode_system_info(m: &SystemInfo) -> Bytes {
    encode(m)
}

/// The record shapes the versioned `system-info` decodes to.
#[derive(Debug, Clone, PartialEq)]
pub enum SystemInfoValue {
    /// The `infoLegacyMap` shape of versions 0 and 1.
    Legacy(LegacySystemInfo),
    V2(InfoV2),
    V3(SystemInfoV3),
}

/// `infoLegacyMap`.
#[derive(Debug, Clone, PartialEq)]
pub struct LegacySystemInfo {
    pub version: u64,
    pub timestamp: u64,
    pub flushes: u64,
    pub view: Option<Link>,
    pub views: Clock,
    pub heads: Clock,
    pub indexers: Clock,
}

pub fn decode_system_info(buf: impl Into<Bytes>) -> Result<SystemInfoValue> {
    let mut r = Reader::new(buf.into());
    // The wire version survives the map (versions 0 and 1 both decode to V1).
    let start = r.position();
    let version = r.uint()?;
    // rewind the whole varint, not one byte: a padded/non-minimal version must
    // still decode as what it is rather than as the legacy map
    r.unread(r.position() - start)?;
    Ok(match SystemInfo::decode(&mut r)? {
        SystemInfo::V1(info) => SystemInfoValue::Legacy(LegacySystemInfo {
            version,
            timestamp: 0,
            flushes: 0,
            view: None,
            views: info.views,
            heads: info.heads,
            indexers: info.indexers,
        }),
        SystemInfo::V2(info) => SystemInfoValue::V2(info),
        SystemInfo::V3(info) => SystemInfoValue::V3(info),
    })
}

// ---------------------------------------------------------------------------
// oplogs

/// The result of `decodeRawOplog`: the generated decode, which still applies
/// `oplogLegacyMap` to version 3 but leaves the legacy versions untouched.
#[derive(Debug, Clone, PartialEq)]
pub enum RawOplog {
    Raw(Oplog),
    Mapped(OplogMapped),
}

pub fn decode_raw_oplog(buf: impl Into<Bytes>) -> Result<RawOplog> {
    Ok(match compact_encoding::decode::<Oplog>(buf)? {
        Oplog::V3(m) => RawOplog::Mapped(map_v3(m)),
        other => RawOplog::Raw(other),
    })
}

/// `encodeOplog`: a `{ start: 0, end: 0 }` batch is written as absent.
pub fn encode_oplog(m: &mut Oplog) -> Bytes {
    match m {
        Oplog::V3(v) if v.batch.as_ref().is_some_and(|b| b.start == 0 && b.end == 0) => {
            v.batch = None;
        }
        Oplog::V4(v) if v.batch.as_ref().is_some_and(|b| b.start == 0 && b.end == 0) => {
            v.batch = None;
        }
        _ => {}
    }
    encode(m)
}

/// The shape `decodeOplog` returns for versions 0 to 3.
#[derive(Debug, Clone, PartialEq)]
pub struct OplogMapped {
    pub version: u64,
    pub timestamp: u64,
    pub links: Vec<Link>,
    pub batch: Batch,
    pub views: Option<Views>,
    pub trusted: Option<Vec<TrustedHead>>,
    pub witness: Option<GrantWitness>,
    pub approvals: Option<Vec<Approval>>,
    pub optimistic: bool,
    pub value: Option<Bytes>,
    pub weight_hint: u64,
    pub drift: u64,
}

/// `decodeOplog`'s return: the mapped legacy shape, or the raw version 4 record
/// with a null batch normalised to `{ start: 0, end: 0 }`.
#[derive(Debug, Clone, PartialEq)]
pub enum DecodedOplog {
    Mapped(OplogMapped),
    V4(OplogMessageV4),
}

impl DecodedOplog {
    pub fn value(&self) -> Option<&Bytes> {
        match self {
            Self::Mapped(m) => m.value.as_ref(),
            Self::V4(m) => m.value.as_ref(),
        }
    }
}

/// `oplogLegacyMap`: the version 3 witness has no v4 standing, and weight
/// hints do not exist yet.
fn map_v3(m: OplogMessageV3) -> OplogMapped {
    OplogMapped {
        version: 3,
        timestamp: m.timestamp,
        links: m.links,
        batch: mapped_batch(m.batch),
        views: m.views,
        trusted: m.trusted,
        witness: None,
        approvals: None,
        optimistic: m.optimistic,
        value: m.value,
        weight_hint: 0,
        drift: 0,
    }
}

fn to_links(clock: Clock) -> Vec<Link> {
    clock
        .into_iter()
        .map(|c| Link {
            key: c.key,
            length: c.length,
        })
        .collect()
}

fn mapped_batch(batch: Option<Batch>) -> Batch {
    batch.unwrap_or(Batch { start: 0, end: 0 })
}

pub fn decode_oplog(buf: impl Into<Bytes>) -> Result<DecodedOplog> {
    Ok(match compact_encoding::decode::<Oplog>(buf)? {
        Oplog::V0(m) => DecodedOplog::Mapped(OplogMapped {
            version: 0,
            timestamp: LEGACY_TIME,
            links: to_links(m.node.heads),
            batch: Batch {
                start: 0,
                // a versioned batch is one-based; JS's `batch - 1` never sees 0
                end: m.node.batch.saturating_sub(1),
            },
            views: None,
            trusted: None,
            witness: None,
            approvals: None,
            optimistic: false,
            value: m.node.value,
            weight_hint: 0,
            drift: 0,
        }),
        Oplog::V1(m) => DecodedOplog::Mapped(OplogMapped {
            version: 1,
            timestamp: LEGACY_TIME,
            links: to_links(m.node.heads),
            batch: Batch {
                start: 0,
                end: m.node.batch.saturating_sub(1),
            },
            views: None,
            trusted: None,
            witness: None,
            approvals: None,
            optimistic: m.optimistic,
            value: m.node.value,
            weight_hint: 0,
            drift: 0,
        }),
        Oplog::V2(m) => DecodedOplog::Mapped(OplogMapped {
            version: 2,
            timestamp: LEGACY_TIME,
            links: to_links(m.node.heads),
            batch: Batch {
                start: 0,
                end: m.node.batch.saturating_sub(1),
            },
            views: None,
            trusted: None,
            witness: None,
            approvals: None,
            optimistic: m.optimistic,
            value: m.node.value,
            weight_hint: 0,
            drift: 0,
        }),
        Oplog::V3(m) => DecodedOplog::Mapped(map_v3(m)),
        Oplog::V4(mut m) => {
            m.batch = Some(mapped_batch(m.batch));
            DecodedOplog::V4(m)
        }
    })
}

// ---------------------------------------------------------------------------
// values

/// The options of `createValue`/`encodeValue`.
#[derive(Debug, Clone)]
pub struct ValueOptions {
    /// Defaults to 4, or 2 with `legacy`.
    pub version: Option<u64>,
    pub optimistic: bool,
    pub timestamp: u64,
    pub links: Vec<Link>,
    pub views: Option<Views>,
    /// Bytes of zero padding written before the record.
    pub padding: usize,
    /// Wrap the record in the encrypted envelope (requires `optimistic`).
    pub encrypted: bool,
    /// Write a version 2 node instead of a version 4 record.
    pub legacy: bool,
    pub heads: Clock,
}

impl Default for ValueOptions {
    fn default() -> Self {
        Self {
            version: None,
            optimistic: false,
            timestamp: now_millis(),
            links: Vec::new(),
            views: None,
            padding: 0,
            encrypted: false,
            legacy: false,
            heads: Vec::new(),
        }
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

fn create_value(value: Bytes, opts: &ValueOptions) -> Oplog {
    if opts.legacy {
        // the reference uses `opts.version || LEGACY_OPLOG_VERSION`
        let version = opts
            .version
            .filter(|v| *v != 0)
            .unwrap_or(LEGACY_OPLOG_VERSION);
        return match version {
            2 => Oplog::V2(OplogMessageV2 {
                node: schema::autobase_compat::Node {
                    heads: opts.heads.clone(),
                    batch: 1,
                    value: Some(value),
                },
                checkpoint: None,
                digest: None,
                optimistic: opts.optimistic,
                trace: None,
            }),
            other => panic!("encoding a legacy value with version {other} is not supported"),
        };
    }

    match opts.version.unwrap_or(OPLOG_VERSION) {
        3 => Oplog::V3(OplogMessageV3 {
            timestamp: opts.timestamp,
            links: opts.links.clone(),
            batch: None,
            views: opts.views.clone(),
            optimistic: opts.optimistic,
            value: Some(value),
            witness: None,
            attestations: None,
            trusted: None,
        }),
        4 => Oplog::V4(OplogMessageV4 {
            timestamp: opts.timestamp,
            links: opts.links.clone(),
            batch: None,
            views: opts.views.clone(),
            trusted: None,
            witness: None,
            approvals: None,
            optimistic: opts.optimistic,
            value: Some(value),
            hash: None,
            weight_hint: None,
            drift: None,
        }),
        other => panic!("encoding a value with version {other} is not supported"),
    }
}

/// `encodeValue`: optionally padded and/or wrapped in the version 4 encrypted
/// envelope. Padding bytes are the first 8 bytes of a BLAKE2b-128 of the record
/// with the first byte zeroed, as the reference only needs them to be skipped.
pub fn encode_value(value: Bytes, opts: &ValueOptions) -> Bytes {
    let oplog = create_value(value, opts);
    let body = encode(&oplog);

    let mut record = Vec::with_capacity(opts.padding + body.len());
    record.resize(opts.padding, 0);
    record.extend_from_slice(&body);

    if !opts.encrypted {
        return Bytes::from(record);
    }
    if !opts.optimistic {
        panic!("encoding an encrypted value is not supported");
    }

    let hash = blake2b_simd::Params::new()
        .hash_length(16)
        .hash(&record)
        .as_bytes()
        .to_vec();
    let mut out = vec![0u8; PADDING];
    out.copy_from_slice(&hash[..PADDING]);
    out[0] = 0;
    out.extend_from_slice(&record);
    Bytes::from(out)
}

/// `decodeValue` without an encryption key: the oplog's value.
pub fn decode_value(buf: impl Into<Bytes>) -> Result<Option<Bytes>> {
    Ok(decode_oplog(buf)?.value().cloned())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The version varint may be encoded in more bytes than it needs; rewinding
    /// one byte instead of the whole varint read the padding as the version and
    /// decoded a version 3 record as legacy.
    #[test]
    fn a_padded_version_varint_decodes_the_versioned_record() {
        let info = SystemInfo::V3(SystemInfoV3 {
            timestamp: 7,
            flushes: 1,
            view: Link {
                key: [1; 32],
                length: 1,
            },
            heads: vec![Link {
                key: [2; 32],
                length: 3,
            }],
            indexers: None,
            pending: None,
            hash: Some([9; 8]),
        });
        let encoded = encode_system_info(&info);
        assert_eq!(encoded[0], 3, "minimal version prefix");

        let mut padded = bytes::BytesMut::from(&[0xfd, 0x03, 0x00][..]);
        padded.extend_from_slice(&encoded[1..]);
        assert_eq!(
            decode_system_info(padded.freeze()).expect("decode"),
            SystemInfoValue::V3(SystemInfoV3 {
                timestamp: 7,
                flushes: 1,
                view: Link {
                    key: [1; 32],
                    length: 1,
                },
                heads: vec![Link {
                    key: [2; 32],
                    length: 3,
                }],
                indexers: None,
                pending: None,
                hash: Some([9; 8]),
            })
        );
    }
}
