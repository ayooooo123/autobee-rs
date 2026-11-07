//! Every node, weighted reference and system info object in the recorded
//! traces loads into `autobee::types` and writes back without loss on the
//! fields the trace carries.

use std::fs;
use std::path::PathBuf;

use bytes::Bytes;
use serde_json::{Map, Value, json};

use autobee::encoding::{Approval, Batch as BatchCoords, ExternalBatch, GrantWitness, Link};
use autobee::encoding::{SystemInfoV3, TrustedHead, Views};
use autobee::types::{Head, Info, Key, Node, WeightedLink};

fn hex(s: &str) -> Vec<u8> {
    assert!(s.len().is_multiple_of(2), "odd hex length: {s}");
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).expect("hex digit"))
        .collect()
}

fn hx(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn key(v: &Value) -> Key {
    hex(v.as_str().expect("key string"))
        .try_into()
        .expect("32-byte key")
}

fn maybe_key(v: &Value) -> Option<Key> {
    v.as_str().map(|s| hex(s).try_into().expect("32-byte key"))
}

fn hash(v: &Value) -> [u8; 8] {
    hex(v.as_str().expect("hash string"))
        .try_into()
        .expect("8-byte hash")
}

fn link(v: &Value) -> Link {
    Link {
        key: key(&v["key"]),
        length: v["length"].as_u64().expect("length"),
    }
}

fn link_json(l: &Link) -> Value {
    json!({ "key": hx(&l.key), "length": l.length })
}

fn head(v: &Value) -> Head {
    Head {
        key: maybe_key(&v["key"]),
        length: v["length"].as_u64().expect("length"),
    }
}

fn head_json(h: &Head) -> Value {
    json!({ "key": h.key.as_ref().map(|k| hx(k)), "length": h.length })
}

fn external_batch(v: &Value) -> ExternalBatch {
    ExternalBatch {
        key: key(&v["key"]),
        start: v["start"].as_u64().expect("start"),
        length: v["length"].as_u64().expect("length"),
    }
}

fn external_batch_json(b: &ExternalBatch) -> Value {
    json!({ "key": hx(&b.key), "start": b.start, "length": b.length })
}

fn views(v: &Value) -> Views {
    Views {
        flushes: v["flushes"].as_u64().expect("flushes"),
        system: external_batch(&v["system"]),
        view: v.get("view").filter(|v| v.is_object()).map(external_batch),
    }
}

fn views_json(v: &Views) -> Value {
    json!({
        "flushes": v.flushes,
        "system": external_batch_json(&v.system),
        "view": v.view.as_ref().map(external_batch_json),
    })
}

fn approval(v: &Value) -> Approval {
    Approval {
        key: key(&v["key"]),
        weight: v["weight"].as_u64().expect("weight"),
    }
}

fn approval_json(a: &Approval) -> Value {
    json!({ "key": hx(&a.key), "weight": a.weight })
}

fn witness(v: &Value) -> GrantWitness {
    GrantWitness {
        weight: v["weight"].as_u64().expect("weight"),
        link: link(&v["link"]),
    }
}

fn witness_json(w: &GrantWitness) -> Value {
    json!({ "weight": w.weight, "link": link_json(&w.link) })
}

fn trusted(v: &Value) -> TrustedHead {
    TrustedHead {
        key: key(&v["key"]),
        length: v["length"].as_u64().expect("length"),
        flushes: v.get("flushes").and_then(|v| v.as_u64()).unwrap_or(0),
    }
}

fn node(v: &Value) -> Node {
    Node {
        core: None,
        key: key(&v["key"]),
        length: v["length"].as_u64().expect("length"),
        weight: v["weight"].as_i64().expect("weight"),
        version: v["version"].as_u64().expect("version"),
        timestamp: v["timestamp"].as_u64().expect("timestamp"),
        links: v["links"]
            .as_array()
            .expect("links")
            .iter()
            .map(link)
            .collect(),
        batch: BatchCoords {
            start: v["batch"]["start"].as_u64().expect("batch start"),
            end: v["batch"]["end"].as_u64().expect("batch end"),
        },
        views: v.get("views").filter(|v| v.is_object()).map(views),
        trusted: v
            .get("trusted")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().map(trusted).collect())
            .unwrap_or_default(),
        witness: v.get("witness").filter(|v| v.is_object()).map(witness),
        approvals: v
            .get("approvals")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().map(approval).collect()),
        optimistic: v["optimistic"].as_bool().expect("optimistic"),
        value: v
            .get("value")
            .and_then(|v| v.as_str())
            .map(|s| Bytes::from(hex(s))),
        hash: v
            .get("hash")
            .and_then(|v| v.as_str())
            .map(|s| hash(&Value::from(s))),
        weight_hint: v.get("weightHint").and_then(|v| v.as_u64()).unwrap_or(0),
        drift: v.get("drift").and_then(|v| v.as_u64()).unwrap_or(0),
    }
}

fn field_json(n: &Node, name: &str) -> Value {
    match name {
        "key" => json!(hx(&n.key)),
        "length" => json!(n.length),
        "weight" => json!(n.weight),
        "version" => json!(n.version),
        "timestamp" => json!(n.timestamp),
        "links" => Value::Array(n.links.iter().map(link_json).collect()),
        "batch" => json!({ "start": n.batch.start, "end": n.batch.end }),
        "views" => n.views.as_ref().map(views_json).unwrap_or(Value::Null),
        "trusted" => Value::Array(
            n.trusted
                .iter()
                .map(|t| {
                    let mut o = Map::new();
                    o.insert("key".into(), json!(hx(&t.key)));
                    o.insert("length".into(), json!(t.length));
                    if t.flushes != 0 {
                        o.insert("flushes".into(), json!(t.flushes));
                    }
                    Value::Object(o)
                })
                .collect(),
        ),
        "witness" => n.witness.as_ref().map(witness_json).unwrap_or(Value::Null),
        "approvals" => n.approvals.as_ref().map_or(Value::Null, |a| {
            Value::Array(a.iter().map(approval_json).collect())
        }),
        "optimistic" => json!(n.optimistic),
        "value" => n
            .value
            .as_ref()
            .map(|v| json!(hx(v)))
            .unwrap_or(Value::Null),
        "hash" => n.hash.as_ref().map(|h| json!(hx(h))).unwrap_or(Value::Null),
        "weightHint" => json!(n.weight_hint),
        "drift" => json!(n.drift),
        other => panic!("unexpected node field {other}"),
    }
}

fn is_hex_key(m: &Map<String, Value>) -> bool {
    m.get("key")
        .and_then(|v| v.as_str())
        .is_some_and(|s| s.len() == 64)
}

#[derive(Default)]
struct Found {
    nodes: usize,
    weighted: usize,
    heads: usize,
}

fn walk(v: &Value, found: &mut Found) {
    match v {
        Value::Object(m) => {
            if is_hex_key(m) && m.contains_key("length") {
                if m.contains_key("optimistic") {
                    let n = node(v);
                    let mut out = Map::new();
                    for k in m.keys() {
                        out.insert(k.clone(), field_json(&n, k));
                    }
                    assert_eq!(Value::Object(out), *v, "node round-trip failed");
                    found.nodes += 1;
                    return;
                }
                if m.contains_key("weight") {
                    let w = WeightedLink {
                        key: key(&m["key"]),
                        length: m["length"].as_u64().expect("length"),
                        weight: m["weight"].as_i64().expect("weight"),
                        timestamp: m.get("timestamp").and_then(|v| v.as_u64()).unwrap_or(0),
                    };
                    assert_eq!(json!(hx(&w.key)), m["key"]);
                    assert_eq!(json!(w.length), m["length"]);
                    assert_eq!(json!(w.weight), m["weight"]);
                    found.weighted += 1;
                    return;
                }
                if m.len() == 2 {
                    let h = head(v);
                    assert_eq!(head_json(&h), *v, "head round-trip failed");
                    found.heads += 1;
                    return;
                }
            }
            for v in m.values() {
                walk(v, found);
            }
        }
        Value::Array(a) => {
            for v in a {
                walk(v, found);
            }
        }
        _ => {}
    }
}

fn system_info(v: &Value) -> SystemInfoV3 {
    SystemInfoV3 {
        timestamp: v["timestamp"].as_u64().expect("timestamp"),
        flushes: v["flushes"].as_u64().expect("flushes"),
        view: link(&v["view"]),
        heads: v["heads"]
            .as_array()
            .expect("heads")
            .iter()
            .map(link)
            .collect(),
        indexers: v["indexers"]
            .as_array()
            .map(|a| a.iter().map(link).collect()),
        pending: v["pending"].as_array().map(|a| {
            a.iter()
                .map(|b| b.as_bool().expect("pending flag"))
                .collect()
        }),
        hash: v["hash"].as_str().map(|s| hash(&Value::from(s))),
    }
}

fn info_field(i: &Info, name: &str) -> Value {
    match name {
        "version" => json!(i.version),
        "timestamp" => json!(i.timestamp),
        "flushes" => json!(i.flushes),
        "view" => head_json(&i.view),
        "heads" => Value::Array(i.heads.iter().map(head_json).collect()),
        "indexers" => i
            .indexers
            .as_ref()
            .map(|h| Value::Array(h.iter().map(head_json).collect()))
            .unwrap_or(Value::Null),
        "pending" => json!(i.pending),
        "hash" => i.hash.as_ref().map(|h| json!(hx(h))).unwrap_or(Value::Null),
        other => panic!("unexpected info field {other}"),
    }
}

fn traces() -> Vec<PathBuf> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/traces");
    let mut files: Vec<PathBuf> = fs::read_dir(&dir)
        .expect("traces dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|e| e == "json"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no traces in {}", dir.display());
    files
}

#[test]
fn trace_nodes_round_trip() {
    let mut found = Found::default();
    let mut infos = 0;

    for path in traces() {
        let data: Value =
            serde_json::from_slice(&fs::read(&path).expect("read trace")).expect("parse trace");
        walk(&data, &mut found);

        for flush in data["flushes"].as_array().expect("flushes") {
            let original = &flush["info"];
            let info = Info::from(system_info(original));
            let mut out = Map::new();
            for k in original.as_object().expect("info object").keys() {
                out.insert(k.clone(), info_field(&info, k));
            }
            assert_eq!(
                Value::Object(out),
                *original,
                "info round-trip failed in {}",
                path.display()
            );
            infos += 1;
        }
    }

    assert!(found.nodes > 500, "only {} nodes", found.nodes);
    assert!(found.weighted > 0, "no weighted references");
    assert!(found.heads > 0, "no heads");
    assert!(infos > 0, "no system info records");
    println!(
        "{} nodes, {} weighted refs, {} heads, {} info records",
        found.nodes, found.weighted, found.heads, infos
    );
}
