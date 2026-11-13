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
        .as_chunks::<2>()
        .0
        .iter()
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

fn replay_store(dir: &Path, peer: &str) -> Corestore {
    Corestore::open_with_primary_key(dir, [peer.as_bytes()[0]; 32]).unwrap()
}

/// Copies a core another peer owns in the recording into `store` by replicating
/// from a store opened with that peer's primary key and the same core name.
async fn seed_core(
    store: &Corestore,
    root: &Path,
    owner: &str,
    writer: &[u8; 32],
    blocks: &[String],
) {
    let dir = root.join(format!("seed-{owner}"));
    std::fs::create_dir_all(&dir).unwrap();
    let seeds = replay_store(&dir, owner);
    let id = hypercore_id_encoding::encode(writer);
    let mut opts = GetOptions::name(format!("autobee/{id}/view/system"));
    opts.manifest_version = Some(3);
    let core = seeds.get(opts).await.unwrap();
    let blocks: Vec<Vec<u8>> = blocks.iter().map(|block| bytes(block)).collect();
    let slices: Vec<&[u8]> = blocks.iter().map(|block| block.as_slice()).collect();
    core.append(&slices).await.unwrap();

    let ((ar, aw), (br, bw)) = protomux::memory::duplex(64);
    let handshake = Some([11u8; 64]);
    seeds.replicate(&protomux::Mux::new(
        ar,
        aw,
        protomux::StreamInfo {
            is_initiator: true,
            handshake_hash: handshake,
            remote_public_key: None,
        },
    ));
    store.replicate(&protomux::Mux::new(
        br,
        bw,
        protomux::StreamInfo {
            is_initiator: false,
            handshake_hash: handshake,
            remote_public_key: None,
        },
    ));
    let reader = store.get(GetOptions::key(*core.key())).await.unwrap();
    let complete = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        reader.download(0, Some(blocks.len() as u64), false),
    )
    .await
    .expect("the seeded download settles")
    .unwrap();
    assert!(complete);
    core.close();
    reader.close();
}

async fn replay_peer(
    store: &Corestore,
    name: &str,
    peer: &str,
    writer: [u8; 32],
    events: &[&Value],
) {
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
    let bee = Bee::from_session(store, core.clone()).await.unwrap();
    let mut system = SystemBee::new(bee);
    for (i, event) in events.iter().enumerate() {
        let pre = head(&event["preHead"]);
        // A recorded undo, or a boot onto another peer's core, leaves the head
        // where the trace says the flush started.
        if system.bee.head().as_ref() != Some(&pre) {
            system.undo(pre).await.unwrap();
        }
        assert_eq!(
            system.bee.head().unwrap(),
            pre,
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
        let info = match system.get_info(None).await.unwrap().unwrap() {
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
    system.boot(last_head, None).await.unwrap();
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
                let dir = tempfile::tempdir().unwrap();
                let store = replay_store(dir.path(), peer);
                replay_peer(
                    &store,
                    &name,
                    peer,
                    key(&trace["peers"][peer]["writer"]),
                    &events,
                )
                .await;
                count += 1;
            }
        }
    }
    assert!(count > 0);
}

/// Peers whose system tree was undone or booted onto another peer's core: the
/// undo target is the flush's recorded pre-head, and a foreign pre-head is
/// seeded from the blocks its owner recorded.
#[tokio::test]
async fn replay_undo_and_cross_core_system_flushes() {
    let mut count = 0;
    for (name, trace) in traces() {
        let peers = trace["peers"].as_object().unwrap();
        for peer in peers.keys() {
            let events: Vec<_> = trace["flushes"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|f| f["peer"] == *peer)
                .collect();
            if events.is_empty() || is_single_core_history(&events) {
                continue;
            }
            let dir = tempfile::tempdir().unwrap();
            let store = replay_store(dir.path(), peer);
            let local = key(&events[0]["system"]["key"]);
            let mut seeded: Vec<[u8; 32]> = Vec::new();
            for event in &events {
                let pre = head(&event["preHead"]);
                if pre.key == local || seeded.contains(&pre.key) {
                    continue;
                }
                let Some((owner, blocks)) = peers.iter().find_map(|(owner, _)| {
                    let owner_events: Vec<_> = trace["flushes"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter(|f| f["peer"] == *owner)
                        .collect();
                    if !owner_events
                        .iter()
                        .any(|e| key(&e["system"]["key"]) == pre.key)
                    {
                        return None;
                    }
                    owner_events
                        .iter()
                        .rev()
                        .find(|e| {
                            e["system"]["blocks"].as_array().unwrap().len() == pre.length as usize
                        })
                        .map(|e| (owner, e["system"]["blocks"].clone()))
                }) else {
                    panic!("{name}: {peer} pre-head on a core nobody recorded");
                };
                let blocks: Vec<String> = blocks
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|block| block.as_str().unwrap().to_owned())
                    .collect();
                seed_core(
                    &store,
                    dir.path(),
                    owner,
                    &key(&peers[owner]["writer"]),
                    &blocks,
                )
                .await;
                seeded.push(pre.key);
            }
            replay_peer(&store, &name, peer, key(&peers[peer]["writer"]), &events).await;
            count += 1;
        }
    }
    assert!(count > 0);
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
