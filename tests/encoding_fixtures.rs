//! Byte-for-byte fixtures for `autobee/encoding`.
//!
//! `fixtures/gen/encoding.mjs` records, with the npm reference, the bytes and
//! decoded values of every schema type plus every exported encode/decode
//! helper. Schema types are compared against Rust literals emitted by `build.rs`;
//! the helper results are compared field by field against the reference JSON.

use bytes::Bytes;
use compact_encoding::Codec;
use serde_json::{Value, json};

use autobee::encoding::legacy::Checkpointer;
use autobee::encoding::schema::autobase_compat::{Checkpoint, Digest, Node, Trace, UserViewTrace};
use autobee::encoding::schema::autobee::{OplogMessageV3, OplogMessageV4};
use autobee::encoding::{
    self, Batch, BootRecord, Clock, DecodedOplog, DecodedSystemWriter, ExternalBatch, GrantHint,
    Link, Oplog, OplogMapped, RawOplog, SystemInfo, SystemInfoValue, SystemWriter,
    SystemWriterInfo, ValueOptions, Views,
};

#[allow(clippy::all)]
mod cases_types {
    include!(concat!(env!("OUT_DIR"), "/cases_types.rs"));
}

pub struct TypeCase {
    pub name: &'static str,
    pub run: Box<dyn Fn()>,
}

pub fn hex(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "odd hex length: {s}");
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).expect("hex digit"))
        .collect()
}

fn hx(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Decode the reference bytes into `T`, re-encode and compare both ways.
pub fn check<T>(expected: &T, bytes: Vec<u8>)
where
    T: Codec + PartialEq + std::fmt::Debug,
{
    assert_eq!(
        compact_encoding::encode(expected).as_ref(),
        &bytes[..],
        "encode"
    );
    let decoded = compact_encoding::decode::<T>(Bytes::from(bytes)).expect("decode");
    assert_eq!(&decoded, expected, "decode");
}

#[test]
fn schema_type_fixtures() {
    let cases = cases_types::cases();
    assert!(!cases.is_empty());
    let mut failed = Vec::new();
    for case in &cases {
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (case.run)())).is_err() {
            failed.push(case.name);
        }
    }
    assert!(failed.is_empty(), "failed cases: {failed:?}");
}

#[test]
fn exported_function_fixtures() {
    let fixtures: Value = serde_json::from_str(include_str!("../fixtures/encoding-reference.json"))
        .expect("fixture json");
    let calls = fixtures["calls"].as_array().expect("calls");
    assert!(!calls.is_empty());
    for call in calls {
        let name = call["fn"].as_str().expect("fn");
        check_call(name, call);
    }
}

fn check_call(name: &str, call: &Value) {
    match name {
        "encode_boot_record" => {
            let expected = link(&call["value"]);
            assert_eq!(
                encoding::encode_boot_record(&expected).as_ref(),
                &hex(call["hex"].as_str().unwrap())[..],
                "{name}"
            );
        }
        "decode_boot_record" => {
            let decoded = encoding::decode_boot_record(bytes(&call["hex"])).unwrap();
            assert_eq!(link_json(&decoded), call["decoded"], "{name}");
        }
        "encode_previous_drain" => {
            let expected = call["value"].as_u64().unwrap();
            assert_eq!(
                encoding::encode_previous_drain(expected).as_ref(),
                &hex(call["hex"].as_str().unwrap())[..],
                "{name}"
            );
        }
        "decode_previous_drain" => {
            let decoded = encoding::decode_previous_drain(bytes(&call["hex"])).unwrap();
            assert_eq!(decoded, call["decoded"].as_u64().unwrap(), "{name}");
        }
        "decode_autobase_boot_record" => {
            let decoded = encoding::decode_autobase_boot_record(bytes(&call["hex"])).unwrap();
            assert_eq!(boot_record_json(&decoded), call["decoded"], "{name}");
        }
        "encode_system_writer_key" => {
            let key = fixed32(&call["key"]);
            assert_eq!(
                hx(&encoding::encode_system_writer_key(&key)),
                call["hex"].as_str().unwrap(),
                "{name}"
            );
        }
        "encode_system_pending_key" => {
            let key = fixed32(&call["key"]);
            assert_eq!(
                hx(&encoding::encode_system_pending_key(&key)),
                call["hex"].as_str().unwrap(),
                "{name}"
            );
        }
        "decode_system_pending_key" => {
            let buf = bytes(&call["hex"]);
            let key = encoding::decode_system_pending_key(&buf).unwrap();
            assert_eq!(hx(key), call["decoded"].as_str().unwrap(), "{name}");
        }
        "encode_system_pending_range" => {
            let (gte, lt) = encoding::encode_system_pending_range();
            assert_eq!(hx(&gte), call["gte"].as_str().unwrap(), "{name} gte");
            assert_eq!(hx(&lt), call["lt"].as_str().unwrap(), "{name} lt");
        }
        "encode_system_pending" => {
            let expected = call["value"].as_u64().unwrap();
            assert_eq!(
                encoding::encode_system_pending(expected).as_ref(),
                &hex(call["hex"].as_str().unwrap())[..],
                "{name}"
            );
        }
        "decode_system_pending" => {
            let decoded = encoding::decode_system_pending(bytes(&call["hex"])).unwrap();
            assert_eq!(json!({ "weight": decoded }), call["decoded"], "{name}");
        }
        "encode_system_grant_key" => {
            let key = fixed32(&call["key"]);
            assert_eq!(
                hx(&encoding::encode_system_grant_key(&key)),
                call["hex"].as_str().unwrap(),
                "{name}"
            );
        }
        "encode_system_grant_hint" => {
            let expected = GrantHint {
                key: fixed32(&call["value"]["key"]),
                length: call["value"]["length"].as_u64().unwrap(),
                weight: call["value"]["weight"].as_u64().unwrap(),
            };
            assert_eq!(
                encoding::encode_system_grant_hint(&expected).as_ref(),
                &hex(call["hex"].as_str().unwrap())[..],
                "{name}"
            );
        }
        "decode_system_grant_hint" => {
            let decoded = encoding::decode_system_grant_hint(bytes(&call["hex"])).unwrap();
            assert_eq!(
                json!({
                    "key": hx(&decoded.key),
                    "length": decoded.length,
                    "weight": decoded.weight,
                }),
                call["decoded"],
                "{name}"
            );
        }
        "encode_system_writer" => {
            let writer = compact_encoding::decode::<SystemWriter>(bytes(&call["hex"])).unwrap();
            assert_eq!(
                encoding::encode_system_writer(&writer).as_ref(),
                &hex(call["hex"].as_str().unwrap())[..],
                "{name}"
            );
        }
        "decode_system_writer" => {
            let key = hex(call["key"].as_str().unwrap());
            let decoded = encoding::decode_system_writer(&key, bytes(&call["hex"])).unwrap();
            assert_eq!(
                decoded_system_writer_json(&decoded),
                call["decoded"],
                "{name}"
            );
        }
        "encode_system_info" => {
            let info = compact_encoding::decode::<SystemInfo>(bytes(&call["hex"])).unwrap();
            assert_eq!(
                encoding::encode_system_info(&info).as_ref(),
                &hex(call["hex"].as_str().unwrap())[..],
                "{name}"
            );
        }
        "decode_system_info" => {
            let decoded = encoding::decode_system_info(bytes(&call["hex"])).unwrap();
            assert_eq!(system_info_json(&decoded), call["decoded"], "{name}");
        }
        "encode_oplog" => {
            let mut oplog = compact_encoding::decode::<Oplog>(bytes(&call["hex"])).unwrap();
            assert_eq!(
                encoding::encode_oplog(&mut oplog).as_ref(),
                &hex(call["hex"].as_str().unwrap())[..],
                "{name}"
            );
        }
        "decode_oplog" => {
            let decoded = encoding::decode_oplog(bytes(&call["hex"])).unwrap();
            assert_eq!(decoded_oplog_json(&decoded), call["decoded"], "{name}");
        }
        "decode_raw_oplog" => {
            let decoded = encoding::decode_raw_oplog(bytes(&call["hex"])).unwrap();
            let actual = match &decoded {
                RawOplog::Raw(o) => oplog_json(o),
                RawOplog::Mapped(m) => mapped_oplog_json(m),
            };
            assert_eq!(actual, call["decoded"], "{name}");
        }
        "encode_value" => {
            let value = bytes(&call["value"]);
            let expected = value_options(&call["opts"]);
            assert_eq!(
                encoding::encode_value(value, &expected).as_ref(),
                &hex(call["hex"].as_str().unwrap())[..],
                "{name}"
            );
        }
        "decode_value" => {
            let decoded = encoding::decode_value(bytes(&call["hex"])).unwrap();
            assert_eq!(
                decoded.map(|v| hx(&v)),
                call["decoded"].as_str().map(str::to_string),
                "{name}"
            );
        }
        other => panic!("unknown fixture call {other}"),
    }
}

// ---------------------------------------------------------------------------
// fixture arguments

fn bytes(v: &Value) -> Bytes {
    Bytes::from(hex(v.as_str().expect("hex string")))
}

fn fixed32(v: &Value) -> [u8; 32] {
    hex(v.as_str().unwrap()).try_into().expect("fixed32")
}

fn link(v: &Value) -> Link {
    Link {
        key: fixed32(&v["key"]),
        length: v["length"].as_u64().unwrap(),
    }
}

fn clock(v: &Value) -> Clock {
    v.as_array()
        .unwrap()
        .iter()
        .map(|c| autobee::encoding::Checkout {
            key: fixed32(&c["key"]),
            length: c["length"].as_u64().unwrap(),
        })
        .collect()
}

fn value_options(v: &Value) -> ValueOptions {
    ValueOptions {
        version: v["version"].as_u64(),
        optimistic: v["optimistic"].as_bool().unwrap_or(false),
        timestamp: v["timestamp"].as_u64().unwrap_or(0),
        links: v["links"]
            .as_array()
            .map(|links| links.iter().map(link).collect())
            .unwrap_or_default(),
        views: None,
        padding: v["padding"].as_u64().unwrap_or(0) as usize,
        encrypted: v["encrypted"].as_bool().unwrap_or(false),
        legacy: v["legacy"].as_bool().unwrap_or(false),
        heads: v["heads"]
            .as_array()
            .map(|c| clock(&Value::Array(c.clone())))
            .unwrap_or_default(),
    }
}

// ---------------------------------------------------------------------------
// decoded values -> JSON

fn link_json(l: &Link) -> Value {
    json!({ "key": hx(&l.key), "length": l.length })
}

fn checkout_json(c: &autobee::encoding::Checkout) -> Value {
    json!({ "key": hx(&c.key), "length": c.length })
}

fn clock_json(clock: &[autobee::encoding::Checkout]) -> Value {
    Value::Array(clock.iter().map(checkout_json).collect())
}

fn opt_hex_bytes(v: Option<&Bytes>) -> Value {
    v.map_or(Value::Null, |b| json!(hx(b)))
}

fn opt_hex_array(v: Option<&[u8; 32]>) -> Value {
    v.map_or(Value::Null, |b| json!(hx(b)))
}

fn batch_json(b: &Batch) -> Value {
    json!({ "start": b.start, "end": b.end })
}

fn external_batch_json(b: &ExternalBatch) -> Value {
    json!({ "key": hx(&b.key), "start": b.start, "length": b.length })
}

fn views_json(v: &Views) -> Value {
    json!({
        "flushes": v.flushes,
        "system": external_batch_json(&v.system),
        "view": v.view.as_ref().map_or(Value::Null, external_batch_json),
    })
}

fn node_json(n: &Node) -> Value {
    json!({
        "heads": clock_json(&n.heads),
        "batch": n.batch,
        "value": opt_hex_bytes(n.value.as_ref()),
    })
}

fn index_checkpoint_json(c: &autobee::encoding::schema::autobase_compat::IndexCheckpoint) -> Value {
    json!({ "signature": hx(&c.signature), "length": c.length })
}

fn legacy_index_checkpoint_json(c: &autobee::encoding::legacy::IndexCheckpoint) -> Value {
    json!({ "signature": hx(&c.signature), "length": c.length })
}

fn checkpointer_json(c: &Checkpointer) -> Value {
    json!({
        "checkpointer": c.checkpointer,
        "checkpoint": c.checkpoint.as_ref().map_or(Value::Null, legacy_index_checkpoint_json),
    })
}

/// The reference wraps a checkpointer list as `{ system, encryption, user }`.
fn legacy_checkpoint_json(checkpointer: Option<&Vec<Checkpointer>>) -> Value {
    match checkpointer {
        None => Value::Null,
        Some(list) => json!({
            "system": list.first().map_or(Value::Null, checkpointer_json),
            "encryption": Value::Null,
            "user": Value::Array(list.iter().skip(1).map(checkpointer_json).collect()),
        }),
    }
}

fn generated_checkpointer_json(
    c: &autobee::encoding::schema::autobase_compat::Checkpointer,
) -> Value {
    json!({
        "checkpointer": c.checkpointer,
        "checkpoint": c.checkpoint.as_ref().map_or(Value::Null, index_checkpoint_json),
    })
}

fn checkpoint_json(c: &Checkpoint) -> Value {
    json!({
        "system": c.system.as_ref().map_or(Value::Null, generated_checkpointer_json),
        "encryption": c.encryption.as_ref().map_or(Value::Null, generated_checkpointer_json),
        "user": c.user.as_ref().map_or(Value::Null, |list| Value::Array(
            list.iter().map(generated_checkpointer_json).collect()
        )),
    })
}

fn legacy_digest_json(d: &autobee::encoding::legacy::Digest) -> Value {
    json!({ "pointer": d.pointer, "key": opt_hex_array(d.key.as_ref()) })
}

fn digest_json(d: &Digest) -> Value {
    json!({ "pointer": d.pointer, "key": opt_hex_array(d.key.as_ref()) })
}

fn trace_json(t: &Trace) -> Value {
    let user = |u: &UserViewTrace| json!({ "view": u.view, "blocks": u.blocks });
    json!({
        "system": t.system,
        "encryption": t.encryption,
        "user": t.user.iter().map(user).collect::<Vec<_>>(),
    })
}

fn witness_json(w: &autobee::encoding::schema::autobee::Witness) -> Value {
    json!({
        "weight": w.weight,
        "backer": {
            "key": hx(&w.backer.key),
            "length": w.backer.length,
            "signature": hx(&w.backer.signature),
            "manifest": hx(&w.backer.manifest),
        },
    })
}

fn attestation_json(a: &autobee::encoding::schema::autobee::Attestation) -> Value {
    json!({ "key": hx(&a.key), "weight": a.weight, "signature": hx(&a.signature) })
}

fn trusted_head_json(t: &autobee::encoding::schema::autobee::TrustedHead) -> Value {
    json!({ "key": hx(&t.key), "length": t.length, "flushes": t.flushes })
}

fn grant_witness_json(w: &autobee::encoding::schema::autobee::GrantWitness) -> Value {
    json!({ "weight": w.weight, "link": link_json(&w.link) })
}

fn approval_json(a: &autobee::encoding::schema::autobee::Approval) -> Value {
    json!({ "key": hx(&a.key), "weight": a.weight })
}

fn boot_record_json(b: &BootRecord) -> Value {
    match b {
        BootRecord::V0(m) => json!({
            "version": 0,
            "key": hx(&m.key),
            "systemLength": m.system_length,
            "indexersUpdated": m.indexers_updated,
            "fastForwarding": m.fast_forwarding,
            "recoveries": m.recoveries,
            "migrating": m.migrating,
            "heads": clock_json(&m.heads),
        }),
        BootRecord::V3(m) => json!({
            "version": 3,
            "key": hx(&m.key),
            "systemLength": m.system_length,
            "indexersUpdated": m.indexers_updated,
            "fastForwarding": m.fast_forwarding,
            "recoveries": m.recoveries.unwrap_or(0),
        }),
    }
}

fn decoded_system_writer_json(d: &DecodedSystemWriter) -> Value {
    let mut out = json!({ "key": hx(&d.key) });
    let object = out.as_object_mut().unwrap();
    match &d.value {
        SystemWriterInfo::Legacy(m) => {
            object.insert("version".into(), json!(m.version));
            object.insert("isRemoved".into(), json!(m.is_removed));
            object.insert("isOplog".into(), json!(m.is_oplog));
            object.insert("isGenesis".into(), json!(m.is_genesis));
            object.insert("weight".into(), json!(m.weight));
            object.insert("maxWeight".into(), json!(m.max_weight));
            object.insert("length".into(), json!(m.length));
            object.insert("timestamp".into(), json!(m.timestamp));
        }
        SystemWriterInfo::V4(m) => {
            object.insert("version".into(), json!(4));
            object.insert("isRemoved".into(), json!(m.is_removed));
            object.insert("isOplog".into(), json!(m.is_oplog));
            object.insert("weight".into(), json!(m.weight));
            object.insert("maxWeight".into(), json!(m.max_weight));
            object.insert("length".into(), json!(m.length));
            object.insert("clock".into(), json!(m.clock));
            object.insert("isGenesis".into(), json!(m.is_genesis));
            object.insert("isAnchor".into(), json!(m.is_anchor));
            object.insert("timestamp".into(), json!(m.timestamp.unwrap_or(0)));
        }
        SystemWriterInfo::V5(m) => {
            object.insert("version".into(), json!(5));
            object.insert("isRemoved".into(), json!(m.is_removed));
            object.insert("isOplog".into(), json!(m.is_oplog));
            object.insert("isGenesis".into(), json!(m.is_genesis));
            object.insert("weight".into(), json!(m.weight));
            object.insert("maxWeight".into(), json!(m.max_weight));
            object.insert("length".into(), json!(m.length));
            object.insert("timestamp".into(), json!(m.timestamp));
        }
    }
    out
}

fn system_info_json(v: &SystemInfoValue) -> Value {
    match v {
        SystemInfoValue::Legacy(m) => json!({
            "version": m.version,
            "timestamp": m.timestamp,
            "flushes": m.flushes,
            "view": Value::Null,
            "views": clock_json(&m.views),
            "heads": clock_json(&m.heads),
            "indexers": clock_json(&m.indexers),
        }),
        SystemInfoValue::V2(m) => json!({
            "version": 2,
            "members": m.members,
            "pendingIndexers": m.pending_indexers.iter().map(|k| hx(k)).collect::<Vec<_>>(),
            "indexers": clock_json(&m.indexers),
            "heads": clock_json(&m.heads),
            "views": clock_json(&m.views),
            "encryptionLength": m.encryption_length,
            "entropy": opt_hex_array(m.entropy.as_ref()),
        }),
        SystemInfoValue::V3(m) => json!({
            "version": 3,
            "timestamp": m.timestamp,
            "flushes": m.flushes,
            "view": link_json(&m.view),
            "heads": m.heads.iter().map(link_json).collect::<Vec<_>>(),
            "indexers": m.indexers.as_ref().map_or(Value::Null, |list| {
                Value::Array(list.iter().map(link_json).collect())
            }),
            "pending": m.pending.as_ref().map_or(Value::Null, |list| json!(list)),
            "hash": m.hash.as_ref().map_or(Value::Null, |h| json!(hx(h))),
        }),
    }
}

fn oplog_json(o: &Oplog) -> Value {
    match o {
        Oplog::V0(m) => json!({
            "version": 0,
            "maxSupportedVersion": m.max_supported_version,
            "digest": Value::Null,
            "checkpoint": legacy_checkpoint_json(m.checkpoint.as_ref()),
            "optimistic": Value::Null,
            "node": node_json(&m.node),
        }),
        Oplog::V1(m) => json!({
            "version": 1,
            "maxSupportedVersion": m.max_supported_version,
            "digest": m.digest.as_ref().map_or(Value::Null, legacy_digest_json),
            "checkpoint": legacy_checkpoint_json(m.checkpoint.as_ref()),
            "optimistic": m.optimistic,
            "node": node_json(&m.node),
        }),
        Oplog::V2(m) => json!({
            "version": 2,
            "node": node_json(&m.node),
            "checkpoint": m.checkpoint.as_ref().map_or(Value::Null, checkpoint_json),
            "digest": m.digest.as_ref().map_or(Value::Null, digest_json),
            "optimistic": m.optimistic,
            "trace": m.trace.as_ref().map_or(Value::Null, trace_json),
        }),
        Oplog::V3(m) => oplog_v3_json(m),
        Oplog::V4(m) => oplog_v4_json(m),
    }
}

fn oplog_v3_json(m: &OplogMessageV3) -> Value {
    json!({
        "version": 3,
        "timestamp": m.timestamp,
        "links": m.links.iter().map(link_json).collect::<Vec<_>>(),
        "batch": m.batch.as_ref().map_or(Value::Null, batch_json),
        "views": m.views.as_ref().map_or(Value::Null, views_json),
        "optimistic": m.optimistic,
        "value": opt_hex_bytes(m.value.as_ref()),
        "witness": m.witness.as_ref().map_or(Value::Null, witness_json),
        "attestations": m.attestations.as_ref().map_or(Value::Null, |list| {
            Value::Array(list.iter().map(attestation_json).collect())
        }),
        "trusted": m.trusted.as_ref().map_or(Value::Null, |list| {
            Value::Array(list.iter().map(trusted_head_json).collect())
        }),
    })
}

fn oplog_v4_json(m: &OplogMessageV4) -> Value {
    json!({
        "version": 4,
        "timestamp": m.timestamp,
        "links": m.links.iter().map(link_json).collect::<Vec<_>>(),
        "batch": m.batch.as_ref().map_or(Value::Null, batch_json),
        "views": m.views.as_ref().map_or(Value::Null, views_json),
        "trusted": m.trusted.as_ref().map_or(Value::Null, |list| {
            Value::Array(list.iter().map(link_json).collect())
        }),
        "witness": m.witness.as_ref().map_or(Value::Null, grant_witness_json),
        "approvals": m.approvals.as_ref().map_or(Value::Null, |list| {
            Value::Array(list.iter().map(approval_json).collect())
        }),
        "optimistic": m.optimistic,
        "value": opt_hex_bytes(m.value.as_ref()),
        "hash": m.hash.as_ref().map_or(Value::Null, |h| json!(hx(h))),
        // the reference decodes an absent hint/drift as 0
        "weightHint": m.weight_hint.unwrap_or(0),
        "drift": m.drift.unwrap_or(0),
    })
}

fn decoded_oplog_json(d: &DecodedOplog) -> Value {
    match d {
        DecodedOplog::Mapped(m) => mapped_oplog_json(m),
        DecodedOplog::V4(m) => oplog_v4_json(m),
    }
}

fn mapped_oplog_json(m: &OplogMapped) -> Value {
    json!({
        "version": m.version,
        "timestamp": m.timestamp,
        "links": m.links.iter().map(link_json).collect::<Vec<_>>(),
        "batch": batch_json(&m.batch),
        "views": m.views.as_ref().map_or(Value::Null, views_json),
        "trusted": m.trusted.as_ref().map_or(Value::Null, |list| {
            Value::Array(list.iter().map(trusted_head_json).collect())
        }),
        "witness": m.witness.as_ref().map_or(Value::Null, grant_witness_json),
        "approvals": m.approvals.as_ref().map_or(Value::Null, |list| {
            Value::Array(list.iter().map(approval_json).collect())
        }),
        "optimistic": m.optimistic,
        "value": opt_hex_bytes(m.value.as_ref()),
        "weightHint": m.weight_hint,
        "drift": m.drift,
    })
}
