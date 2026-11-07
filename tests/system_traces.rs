use autobee::encoding::{Batch, GrantHint, Link, SystemInfoValue};
use autobee::order_hash::advance_hash;
use autobee::system::{AddWriterOptions, SystemBee, WriterUpdate};
use autobee::types::Node;
use corestore::{Corestore, GetOptions};
use hyperbee::{Bee, Head};
use serde_json::Value;
use std::path::Path;

fn bytes(s: &str) -> Vec<u8> {
    assert_eq!(s.len() % 2, 0);
    s.as_bytes()
        .chunks_exact(2)
        .map(|c| u8::from_str_radix(std::str::from_utf8(c).unwrap(), 16).unwrap())
        .collect()
}
fn fixed<const N: usize>(s: &str) -> [u8; N] {
    bytes(s).try_into().unwrap()
}
fn key(v: &Value) -> [u8; 32] {
    fixed(v.as_str().unwrap())
}
fn number(v: &Value) -> Option<u64> {
    v.as_i64().and_then(|n| u64::try_from(n).ok())
}
fn hex(b: &[u8]) -> String {
    b.iter().map(|b| format!("{b:02x}")).collect()
}
fn link(v: &Value) -> Link {
    Link {
        key: key(&v["key"]),
        length: v["length"].as_u64().unwrap(),
    }
}
fn head(v: &Value) -> Head {
    Head {
        key: key(&v["key"]),
        length: v["length"].as_u64().unwrap(),
    }
}
fn node(v: &Value) -> Node {
    Node {
        core: None,
        key: key(&v["key"]),
        length: v["length"].as_u64().unwrap(),
        weight: v["weight"].as_i64().unwrap(),
        version: v["version"].as_u64().unwrap(),
        timestamp: v["timestamp"].as_u64().unwrap(),
        links: v["links"].as_array().unwrap().iter().map(link).collect(),
        batch: Batch {
            start: v["batch"]["start"].as_u64().unwrap(),
            end: v["batch"]["end"].as_u64().unwrap(),
        },
        views: None,
        trusted: Vec::new(),
        witness: None,
        approvals: None,
        optimistic: v["optimistic"].as_bool().unwrap_or(false),
        value: None,
        hash: None,
        weight_hint: 0,
        drift: 0,
    }
}
fn update(v: &Value) -> WriterUpdate {
    WriterUpdate {
        key: key(&v["key"]),
        length: v["length"].as_u64().unwrap(),
        weight: number(&v["weight"]),
        max_weight: number(&v["maxWeight"]),
        is_genesis: v["isGenesis"].as_bool().unwrap(),
        is_added: v["isAdded"].as_bool().unwrap(),
        is_removed: v["isRemoved"].as_bool().unwrap(),
        timestamp: number(&v["timestamp"]),
        pending: number(&v["pending"]),
        grant: v["grant"].as_object().map(|_| GrantHint {
            key: key(&v["grant"]["key"]),
            length: v["grant"]["length"].as_u64().unwrap(),
            weight: v["grant"]["weight"].as_u64().unwrap(),
        }),
    }
}
fn traces() -> Vec<(String, Value)> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/traces");
    let mut paths: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|p| p.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "json"))
        .collect();
    paths.sort();
    paths
        .into_iter()
        .map(|p| {
            (
                p.file_stem().unwrap().to_string_lossy().into_owned(),
                serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap(),
            )
        })
        .collect()
}

#[test]
fn every_recorded_hash_transition_matches() {
    for (name, trace) in traces() {
        for event in trace["hashes"].as_array().unwrap() {
            let n = &event["node"];
            let previous = event["prev"].as_str().map(fixed::<8>);
            let result = advance_hash(
                previous.as_ref(),
                &key(&n["key"]),
                n["length"].as_u64().unwrap(),
                n["weight"].as_u64().unwrap(),
            );
            assert_eq!(
                hex(&result),
                event["result"].as_str().unwrap(),
                "{name} peer {}",
                event["peer"]
            );
        }
    }
}

async fn replay_peer(name: &str, peer: &str, writer: [u8; 32], events: &[&Value]) {
    let dir = tempfile::tempdir().unwrap();
    let store = Corestore::open_with_primary_key(dir.path(), [peer.as_bytes()[0]; 32]).unwrap();
    let id = hypercore_id_encoding::encode(&writer);
    let mut system_opts = GetOptions::name(format!("autobee/{id}/view/system"));
    system_opts.manifest_version = Some(3);
    let core = std::sync::Arc::new(store.get(system_opts).await.unwrap());
    assert_eq!(
        core.key(),
        &key(&events[0]["system"]["key"]),
        "{name}/{peer} system core key"
    );
    let mut view_opts = GetOptions::name(format!("autobee/{id}/view/view"));
    view_opts.manifest_version = Some(3);
    let view_core = store.get(view_opts).await.unwrap();
    assert_eq!(
        view_core.key(),
        &key(&events[0]["view"]["key"]),
        "{name}/{peer} view core key"
    );
    let bee = Bee::from_session(&store, core.clone()).await.unwrap();
    let mut system = SystemBee::new(bee);
    for (i, event) in events.iter().enumerate() {
        assert_eq!(
            system.bee.head().unwrap(),
            head(&event["preHead"]),
            "{name}/{peer} pre-head {i}"
        );
        let nodes: Vec<_> = event["batch"]
            .as_array()
            .unwrap()
            .iter()
            .map(node)
            .collect();
        for n in &nodes {
            system.add_node(n).unwrap();
        }
        // The recorder captures the merged updates, including handler grants.
        system.updates.clear();
        for upd in event["updates"].as_array().unwrap() {
            system.update(update(upd));
        }
        let (_, ops) = system
            .flush(&nodes, head(&event["viewHead"]))
            .await
            .unwrap();
        let expected = event["puts"].as_array().unwrap();
        assert_eq!(ops.len(), expected.len(), "operation count {i}");
        for (actual, expected) in ops.iter().zip(expected) {
            assert_eq!(
                hex(&actual.key),
                expected["key"].as_str().unwrap(),
                "operation key {i}"
            );
            assert_eq!(
                actual.value.as_ref().map(|v| hex(v)),
                expected["value"].as_str().map(str::to_owned),
                "operation value {i}"
            );
            assert_eq!(
                actual.value.is_some(),
                expected["op"] == "put",
                "operation kind {i}"
            );
        }
        let info = match system.get_info().await.unwrap().unwrap() {
            SystemInfoValue::V3(v) => v,
            _ => panic!("unexpected info version"),
        };
        let want = &event["info"];
        assert_eq!(want["version"], 3);
        assert!(info.indexers.is_none() && want["indexers"].is_null());
        assert_eq!(
            info.pending.as_ref().unwrap(),
            &want["pending"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_bool().unwrap())
                .collect::<Vec<_>>()
        );
        assert_eq!(info.timestamp, want["timestamp"].as_u64().unwrap());
        assert_eq!(info.flushes, want["flushes"].as_u64().unwrap());
        assert_eq!(info.view, link(&want["view"]));
        assert_eq!(
            info.heads,
            want["heads"]
                .as_array()
                .unwrap()
                .iter()
                .map(link)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            info.hash.map(|v| hex(&v)),
            want["hash"].as_str().map(str::to_owned)
        );
        assert_eq!(
            system.bee.head().unwrap(),
            head(&event["systemHead"]),
            "{name}/{peer} system head {i}"
        );
        let blocks = event["system"]["blocks"].as_array().unwrap();
        assert_eq!(core.length() as usize, blocks.len());
        for (seq, block) in blocks.iter().enumerate() {
            let actual = core
                .get(seq as u64, Default::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                hex(&actual),
                block.as_str().unwrap(),
                "physical block {seq} after flush {i}"
            );
        }
    }
    let last_head = system.bee.head().unwrap();
    let last_hash = system.hash;
    let snapshot = system.snapshot(None).await.unwrap();
    let latest_writer = snapshot.get(&writer).await.unwrap();
    system.undo(head(&events[0]["systemHead"])).await.unwrap();
    assert_eq!(
        system.hash.map(|h| hex(&h)),
        events[0]["info"]["hash"].as_str().map(str::to_owned)
    );
    assert_eq!(snapshot.get(&writer).await.unwrap(), latest_writer);
    system.boot(last_head).await.unwrap();
    assert_eq!(system.hash, last_hash);
}
fn is_single_core_history(events: &[&Value]) -> bool {
    let Some(first) = events.first() else {
        return false;
    };
    let local = &first["system"]["key"];
    let mut previous = 0;
    for event in events {
        if &event["preHead"]["key"] != local
            || &event["systemHead"]["key"] != local
            || event["preHead"]["length"].as_u64() != Some(previous)
        {
            return false;
        }
        previous = event["systemHead"]["length"].as_u64().unwrap();
    }
    true
}

#[tokio::test]
async fn replay_single_core_system_flushes() {
    let mut count = 0;
    for (name, trace) in traces() {
        for peer in trace["peers"].as_object().unwrap().keys() {
            let events: Vec<_> = trace["flushes"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|f| f["peer"] == *peer)
                .collect();
            if is_single_core_history(&events) {
                replay_peer(&name, peer, key(&trace["peers"][peer]["writer"]), &events).await;
                count += 1;
            }
        }
    }
    assert!(count > 0);
}

#[tokio::test]
#[ignore = "needs hyperbee-rs move/checkout(key)"]
async fn replay_multicore_and_undo_traces() {
    for (name, trace) in traces() {
        for peer in trace["peers"].as_object().unwrap().keys() {
            let events: Vec<_> = trace["flushes"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|f| f["peer"] == *peer)
                .collect();
            if !events.is_empty() && !is_single_core_history(&events) {
                replay_peer(&name, peer, key(&trace["peers"][peer]["writer"]), &events).await;
            }
        }
    }
}

#[tokio::test]
async fn promotion_partial_then_satisfied() {
    let dir = tempfile::tempdir().unwrap();
    let store = Corestore::open_with_primary_key(dir.path(), [42; 32]).unwrap();
    let bee = Bee::open(&store, None).await.unwrap();
    let view = bee.head().unwrap();
    let mut system = SystemBee::new(bee);
    let (_, trace) = traces()
        .into_iter()
        .find(|(n, _)| n == "single-writer")
        .unwrap();
    let n = node(&trace["flushes"][0]["batch"][0]);
    let writer = [17; 32];
    let coord = Link {
        key: n.key,
        length: n.length,
    };
    system
        .add_writer(
            writer,
            AddWriterOptions {
                weight: 3,
                coord: Some(coord.clone()),
                carrier: 1,
                anchor: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    system.add_node(&n).unwrap();
    system.flush(std::slice::from_ref(&n), view).await.unwrap();
    assert_eq!(system.promotions.pending(&writer).await.unwrap(), 3);
    assert_eq!(system.promotions.anchored(&writer).await.unwrap(), 1);
    assert_eq!(system.promotions.digest, [false, true, true]);
    system
        .add_writer(
            writer,
            AddWriterOptions {
                weight: 3,
                coord: Some(coord),
                carrier: 3,
                anchor: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    system.add_node(&n).unwrap();
    system.flush(std::slice::from_ref(&n), view).await.unwrap();
    assert_eq!(system.promotions.pending(&writer).await.unwrap(), 0);
    assert_eq!(system.promotions.anchored(&writer).await.unwrap(), 3);
    assert!(system.promotions.digest.is_empty());
    system.remove_writer(writer, 0).await.unwrap();
    system.add_node(&n).unwrap();
    system.flush(std::slice::from_ref(&n), view).await.unwrap();
    assert_eq!(system.promotions.anchored(&writer).await.unwrap(), 0);
}
