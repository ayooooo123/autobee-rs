//! Replays `fixtures/traces/*.json`: every recorded autobee 2.12.1 session
//! runs again through Rust `Autobee` instances over in-process stores and
//! memory-duplex replication, and the bytes are compared back to the trace.
//!
//! Compared per scenario: each peer's oplog blocks, the local system and view
//! core blocks as they stood after every system flush (a prefix of the final
//! core, since hyperbee only appends), the heads at each flush, the apply
//! calls (nodes and host calls) and the undo/fast-forward counters. The
//! recording's `topology`/`hashes`/`puts`/`updates` internals are already
//! covered by the module-level trace tests.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use autobee::apply_calls::{AddWriterOptions, ApplyCalls};
use autobee::apply_view::{FastForwardOptions, Handlers};
use autobee::base::{Autobee, FlushObserver};
use autobee::system::SystemBee;
use autobee::types::{Key, Link, Node};
use bytes::Bytes;
use corestore::{Corestore, GetOptions};
use hyperbee::Bee;
use hypercore::core::Session;
use protomux::{Mux, StreamInfo, memory};
use serde_json::{Value, json};
use tokio::sync::Notify;
use tokio::time::{Instant, sleep};

const TRACES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/traces");

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn key_hex(key: &Key) -> String {
    hex(key)
}

fn decode_hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&text[index..index + 2], 16).unwrap())
        .collect()
}

fn link_json(link: &Link) -> Value {
    json!({ "key": key_hex(&link.key), "length": link.length })
}

fn views_json(views: &autobee::types::Views) -> Value {
    let batch = |batch: &autobee::encoding::ExternalBatch| json!({ "key": key_hex(&batch.key), "start": batch.start, "length": batch.length });
    json!({
        "flushes": views.flushes,
        "system": batch(&views.system),
        "view": views.view.as_ref().map(batch),
    })
}

/// The node projection the recorder stores (`node(n)` in `run.mjs`).
fn node_json(node: &Node) -> Value {
    json!({
        "key": key_hex(&node.key),
        "length": node.length,
        "weight": node.weight,
        "version": node.version,
        "timestamp": node.timestamp,
        "links": node.links.iter().map(link_json).collect::<Vec<_>>(),
        "batch": { "start": node.batch.start, "end": node.batch.end },
        "views": node.views.as_ref().map(views_json),
        "value": node.value.as_ref().map(|value| hex(value)),
        "hash": node.hash.as_ref().map(|hash| hex(hash)),
        "witness": node.witness.as_ref().map(|witness| json!({
            "weight": witness.weight,
            "link": link_json(&witness.link),
        })),
        "optimistic": node.optimistic,
        "approvals": node.approvals.as_ref().map(|approvals| approvals.iter().map(|approval| json!({
            "key": key_hex(&approval.key),
            "weight": approval.weight,
        })).collect::<Vec<_>>()),
    })
}

fn bee_head_json(head: Option<hyperbee::Head>) -> Value {
    head.map_or(
        Value::Null,
        |head| json!({ "key": key_hex(&head.key), "length": head.length }),
    )
}

#[derive(Default)]
struct Recorded {
    applies: StdMutex<Vec<Value>>,
    flushes: StdMutex<Vec<Value>>,
}

/// One flush's shape: the physical lengths and the logical heads.
struct Observer {
    peer: String,
    recorded: Arc<Recorded>,
}

impl FlushObserver for Observer {
    fn flushed(
        &self,
        flushes: u64,
        batch: &[Node],
        system: &SystemBee,
        system_length: u64,
        view: &Bee,
        view_length: u64,
    ) {
        let entry = json!({
            "peer": self.peer,
            "flushes": flushes,
            "batch": batch.iter().map(node_json).collect::<Vec<_>>(),
            "systemLength": system_length,
            "viewLength": view_length,
            "systemHead": bee_head_json(system.bee.head()),
            "viewHead": bee_head_json(view.head()),
        });
        self.recorded
            .flushes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(entry);
    }
}

/// The one-shot gate the fast-forward scenario uses to hold the drain until a
/// landing has been scheduled.
struct Gate {
    entered: AtomicBool,
    release: Notify,
}

struct Handler {
    peer: String,
    clock: Arc<AtomicU64>,
    db_key: Arc<StdMutex<Option<Key>>>,
    recorded: Arc<Recorded>,
    fast_forward: bool,
    trust_owner: bool,
    gate: Option<Arc<Gate>>,
}

impl Handler {
    fn key(&self) -> Key {
        self.db_key
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .expect("bootstrap key is set after open")
    }
}

impl Handlers for Handler {
    fn apply<'a, 'b>(
        &'a self,
        nodes: &'a [Node],
        view: &'a Bee,
        host: &'a ApplyCalls<'a, 'b>,
    ) -> autobee::apply_calls::BoxFuture<'a, autobee::apply_calls::Result<()>> {
        Box::pin(async move {
            let owner = key_hex(&self.key());
            let mut writes: Vec<(Vec<u8>, Option<Bytes>)> = Vec::new();
            let mut host_calls = Vec::new();

            for node in nodes {
                let Some(value) = node.value.as_ref() else {
                    continue;
                };
                if value.is_empty() {
                    continue;
                }
                let Ok(op) = serde_json::from_slice::<Value>(value) else {
                    continue;
                };
                let kind = op["type"].as_str().unwrap_or_default();
                let is_owner = key_hex(&node.key) == owner;
                match kind {
                    "grant" if is_owner => {
                        let writer = op["writer"].as_str().expect("grant writer");
                        host_calls.push(json!({
                            "method": "addWriter",
                            "key": writer,
                            "options": { "isIndexer": false },
                        }));
                        host.add_writer_id(
                            writer,
                            AddWriterOptions {
                                is_indexer: false,
                                weight: None,
                            },
                        )
                        .await?;
                        writes.push((
                            format!("writer/{writer}").into_bytes(),
                            Some(Bytes::from_static(b"writer")),
                        ));
                    }
                    "revoke" if is_owner => {
                        let writer = op["writer"].as_str().expect("revoke writer");
                        host_calls.push(json!({ "method": "removeWriter", "key": writer }));
                        host.remove_writer_id(writer).await?;
                        writes.push((format!("writer/{writer}").into_bytes(), None));
                    }
                    "put" => {
                        let key = op["key"].as_str().expect("put key");
                        let value = op["value"].as_str().expect("put value");
                        writes.push((
                            key.as_bytes().to_vec(),
                            Some(Bytes::copy_from_slice(value.as_bytes())),
                        ));
                    }
                    _ => {}
                }
            }

            let mut batch = view.batch();
            for (key, value) in writes {
                match value {
                    Some(value) => batch.put(key, value),
                    None => batch.del(key),
                }
            }
            batch
                .flush()
                .await
                .map_err(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>)?;

            self.recorded
                .applies
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(json!({
                    "peer": self.peer,
                    "nodes": nodes.iter().map(node_json).collect::<Vec<_>>(),
                    "host": host_calls,
                }));
            Ok(())
        })
    }

    fn update<'a>(
        &'a self,
        _view: &'a Bee,
        _changes: &'a autobee::updates::Changes,
    ) -> autobee::apply_calls::BoxFuture<'a, ()> {
        Box::pin(async {})
    }

    fn now(&self) -> u64 {
        self.clock.load(Ordering::Acquire)
    }

    fn fast_forward(&self) -> Option<FastForwardOptions> {
        self.fast_forward.then(FastForwardOptions::default)
    }

    fn pre_apply<'a>(&'a self, _view: &'a Bee) -> autobee::apply_calls::BoxFuture<'a, ()> {
        Box::pin(async move {
            let Some(gate) = &self.gate else {
                return;
            };
            gate.entered.store(true, Ordering::Release);
            gate.release.notified().await;
        })
    }

    fn is_trusted<'a>(
        &'a self,
        key: &'a Key,
        reference: Option<&'a Bee>,
    ) -> autobee::apply_calls::BoxFuture<'a, bool> {
        Box::pin(async move {
            if !self.trust_owner {
                return true;
            }
            let writer = key_hex(key);
            let mut trusted = writer == key_hex(&self.key());
            if !trusted && let Some(reference) = reference {
                let revoked = reference
                    .get(format!("revoked/{writer}").as_bytes())
                    .await
                    .ok()
                    .flatten();
                let record = reference
                    .get(format!("writer/{writer}").as_bytes())
                    .await
                    .ok()
                    .flatten();
                trusted = revoked.is_none()
                    && record.is_some_and(|record| record.value.as_ref() == b"owner");
            }
            trusted
        })
    }
}

struct Peer {
    id: String,
    store: Corestore,
    db: Arc<Autobee>,
    recorded: Arc<Recorded>,
    handler: Arc<Handler>,
}

async fn open_peer(
    root: &std::path::Path,
    id: &str,
    bootstrap: Option<Key>,
    fast_forward: bool,
    trust_owner: bool,
    gate: Option<Arc<Gate>>,
    clock: Arc<AtomicU64>,
) -> Peer {
    let dir = root.join(id);
    std::fs::create_dir_all(&dir).unwrap();
    let first = id.as_bytes()[0];
    let store = Corestore::open_with_primary_key(&dir, [first; 32]).unwrap();
    let recorded = Arc::new(Recorded::default());
    // a joiner's channel key is known before the bee boots; the owner's is not
    let db_key = Arc::new(StdMutex::new(bootstrap));
    let handler = Arc::new(Handler {
        peer: id.to_string(),
        clock: Arc::clone(&clock),
        db_key: Arc::clone(&db_key),
        recorded: Arc::clone(&recorded),
        fast_forward,
        trust_owner,
        gate,
    });
    let db = Autobee::open(&store, bootstrap, handler.clone() as Arc<dyn Handlers>)
        .await
        .unwrap();
    *db_key.lock().unwrap_or_else(|e| e.into_inner()) = Some(db.key);
    db.set_flush_observer(Arc::new(Observer {
        peer: id.to_string(),
        recorded: Arc::clone(&recorded),
    }));
    Peer {
        id: id.to_string(),
        store,
        db,
        recorded,
        handler,
    }
}

fn mux_pair() -> (Mux, Mux) {
    let ((ar, aw), (br, bw)) = memory::duplex(1024);
    let info = |is_initiator| StreamInfo {
        is_initiator,
        handshake_hash: Some([7u8; 64]),
        remote_public_key: None,
    };
    (Mux::new(ar, aw, info(true)), Mux::new(br, bw, info(false)))
}

async fn core_blocks(session: &Session) -> Vec<String> {
    let mut blocks = Vec::new();
    for index in 0..session.length() {
        let block = session
            .get(index, hypercore::GetOptions::default())
            .await
            .unwrap()
            .expect("block present");
        blocks.push(hex(&block));
    }
    blocks
}

fn assert_hex_list(label: &str, actual: &[String], expected: &[String]) {
    if let Some(index) = (0..actual.len().max(expected.len()))
        .find(|index| actual.get(*index) != expected.get(*index))
    {
        panic!(
            "{label}: entry {index} differs (actual {} entries, expected {})\nactual:   {:?}\nexpected: {:?}",
            actual.len(),
            expected.len(),
            actual.get(index),
            expected.get(index)
        );
    }
}

fn expected_blocks(value: &Value) -> Vec<String> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|block| block.as_str().unwrap().to_string())
        .collect()
}

async fn run_scenario(name: &str) {
    let path = format!("{TRACES}/{name}.json");
    let trace: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    let root = tempfile::tempdir().unwrap();
    let clock = Arc::new(AtomicU64::new(1_700_000_000_000));
    let mut peers: Vec<Peer> = Vec::new();
    let mut streams: Vec<(Mux, Mux)> = Vec::new();
    let fast_forward_scenario = trace.get("fastForward").is_some();

    // the recorder creates `a` first and every other peer bootstraps from it
    let ids: Vec<String> = trace["peers"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    let mut bootstrap: Option<Key> = None;
    for id in &ids {
        let is_last_joiner = fast_forward_scenario && id == "b";
        let gate = is_last_joiner.then(|| {
            Arc::new(Gate {
                entered: AtomicBool::new(false),
                release: Notify::new(),
            })
        });
        let peer = open_peer(
            root.path(),
            id,
            bootstrap,
            is_last_joiner,
            is_last_joiner,
            gate,
            Arc::clone(&clock),
        )
        .await;
        bootstrap.get_or_insert(peer.db.key);
        if std::env::var_os("AUTOBEE_DEBUG").is_some() {
            eprintln!(
                "peer {id}: key={} local={} id={}",
                key_hex(&peer.db.key),
                key_hex(&peer.db.local_key),
                peer.db.id()
            );
        }
        peers.push(peer);
    }

    let find = |peers: &[Peer], id: &str| {
        peers
            .iter()
            .position(|peer| peer.id == id)
            .unwrap_or_else(|| panic!("{name}: unknown peer {id}"))
    };

    for step in trace["steps"].as_array().unwrap() {
        let action = step["action"].as_str().unwrap().to_string();
        let result = tokio::time::timeout(Duration::from_secs(120), async {
            match action.as_str() {
                "append" => {
                    let peer_id = step["peer"].as_str().unwrap();
                    let index = find(&peers, peer_id);
                    let op = serde_json::to_string(&step["op"]).unwrap();
                    let optimistic = step["optimistic"].as_bool().unwrap_or(false);
                    clock.fetch_add(1000, Ordering::AcqRel);
                    peers[index]
                        .db
                        .append(vec![Bytes::from(op.into_bytes())], optimistic)
                        .await
                        .unwrap_or_else(|err| panic!("{name}/{peer_id} append: {err}"));
                }
                "connect" => {
                    let a = step["peers"][0].as_str().unwrap();
                    let b = step["peers"][1].as_str().unwrap();
                    let (mux_a, mux_b) = mux_pair();
                    peers[find(&peers, a)].store.replicate(&mux_a);
                    peers[find(&peers, b)].store.replicate(&mux_b);
                    streams.push((mux_a, mux_b));
                }
                "sync" => {
                    let a = step["peers"][0].as_str().unwrap();
                    let b = step["peers"][1].as_str().unwrap();
                    for (source, target) in [(a, b), (b, a)] {
                        let source_index = find(&peers, source);
                        let target_index = find(&peers, target);
                        let key = peers[source_index].db.local_key;
                        let length = peers[source_index].db.local.length();
                        if length == 0 {
                            continue;
                        }
                        let core = peers[target_index]
                            .store
                            .get(GetOptions::key(key))
                            .await
                            .unwrap();
                        core.get(
                            length - 1,
                            hypercore::GetOptions {
                                wait: true,
                                timeout: Some(Duration::from_secs(10)),
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap();
                        core.close();
                        peers[target_index].db.wakeup(key, length).await.unwrap();
                    }
                    for peer in &peers {
                        peer.db.update().await.unwrap();
                    }
                    for peer in &peers {
                        peer.db.flush().await.unwrap();
                    }
                    // the recorder's pulls land on a running event loop; give the
                    // replicated reads it leaves in flight the same chance here
                    let mut last: Vec<(u64, u64)> = Vec::new();
                    let mut stable = 0;
                    for _ in 0..400 {
                        for peer in &peers {
                            peer.db.update().await.unwrap();
                        }
                        let mut snapshot = Vec::new();
                        for peer in &peers {
                            let flushes = peer.db.system.lock().await.flushes;
                            snapshot.push((flushes, peer.db.local.length()));
                        }
                        if snapshot == last {
                            stable += 1;
                            if stable >= 20 {
                                break;
                            }
                        } else {
                            stable = 0;
                            last = snapshot;
                        }
                        sleep(Duration::from_millis(5)).await;
                    }
                }
                "disconnect" => {
                    for (a, b) in streams.drain(..) {
                        a.destroy();
                        b.destroy();
                    }
                }
                "fast-forward-search" => {
                    let peer_id = step["peer"].as_str().unwrap();
                    let index = find(&peers, peer_id);
                    let heads: Vec<Link> = step["heads"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|head| Link {
                            key: decode_hex(head["key"].as_str().unwrap())
                                .try_into()
                                .unwrap(),
                            length: head["length"].as_u64().unwrap(),
                        })
                        .collect();
                    let peer = &peers[index];
                    peer.db.queue_fast_forward(&heads);

                    let deadline = Instant::now() + Duration::from_secs(120);
                    loop {
                        let entered = peer
                            .handler
                            .gate
                            .as_ref()
                            .expect("gate for a fast-forward scenario")
                            .entered
                            .load(Ordering::Acquire);
                        if entered && peer.db.fast_forward_to().is_some() {
                            break;
                        }
                        assert!(
                            Instant::now() < deadline,
                            "{name}: no fast-forward landing was scheduled"
                        );
                        sleep(Duration::from_millis(5)).await;
                    }
                    peer.handler.gate.as_ref().unwrap().release.notify_waiters();

                    let deadline = Instant::now() + Duration::from_secs(120);
                    while peer.db.stats.fast_forwards.load(Ordering::Acquire) == 0 {
                        assert!(
                            Instant::now() < deadline,
                            "{name}: fast-forward landing did not apply"
                        );
                        sleep(Duration::from_millis(5)).await;
                    }
                }
                other => panic!("{name}: unknown step {other}"),
            }
        })
        .await;
        if result.is_err() {
            panic!("{name}: step timed out: {step}");
        }
    }
    for (a, b) in streams.drain(..) {
        a.destroy();
        b.destroy();
    }

    for id in &ids {
        let trace_peer = &trace["peers"][id];
        let peer = &peers[find(&peers, id)];

        // statistics
        assert_eq!(
            peer.db.stats.undos.load(Ordering::Acquire),
            trace_peer["stats"]["undos"].as_u64().unwrap(),
            "{name}/{id}: undo count"
        );
        assert_eq!(
            peer.db.stats.fast_forwards.load(Ordering::Acquire),
            trace_peer["stats"]["fastForwards"].as_u64().unwrap(),
            "{name}/{id}: fast-forward count"
        );

        // the local oplog
        let oplog_session = peer.db.local.session(false);
        let oplog = core_blocks(&oplog_session).await;
        let expected_oplog = expected_blocks(&trace_peer["oplog"]["blocks"]);
        assert_hex_list(&format!("{name}/{id} oplog"), &oplog, &expected_oplog);

        // the flushed system and view cores, as prefixes of the final cores
        let recorded_flushes = peer
            .recorded
            .flushes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let trace_flushes: Vec<&Value> = trace["flushes"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|flush| flush["peer"].as_str() == Some(id.as_str()))
            .collect();
        assert_eq!(
            recorded_flushes.len(),
            trace_flushes.len(),
            "{name}/{id}: flush count\nactual:   {:?}\nexpected: {:?}",
            recorded_flushes
                .iter()
                .map(|flush| flush["batch"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|node| format!(
                        "{}:{}",
                        &node["key"].as_str().unwrap()[..6],
                        node["length"]
                    ))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            trace_flushes
                .iter()
                .map(|flush| flush["batch"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|node| format!(
                        "{}:{}",
                        &node["key"].as_str().unwrap()[..6],
                        node["length"]
                    ))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
        );

        if !recorded_flushes.is_empty() {
            let system_key = {
                let system = peer.db.system.lock().await;
                system.bee.key()
            };
            let view_key = peer.db.working_view.bee().key();
            let system_core = peer.store.get(GetOptions::key(system_key)).await.unwrap();
            let view_core = peer.store.get(GetOptions::key(view_key)).await.unwrap();
            let system_blocks = core_blocks(&system_core).await;
            let view_blocks = core_blocks(&view_core).await;
            system_core.close();
            view_core.close();

            for (index, (recorded, expected)) in
                recorded_flushes.iter().zip(&trace_flushes).enumerate()
            {
                assert_eq!(
                    recorded["batch"], expected["batch"],
                    "{name}/{id}: batch of flush {index}"
                );
                assert_eq!(
                    recorded["systemHead"], expected["systemHead"],
                    "{name}/{id}: system head after flush {index}"
                );
                assert_eq!(
                    recorded["viewHead"], expected["viewHead"],
                    "{name}/{id}: view head after flush {index}"
                );

                let system_length = recorded["systemLength"].as_u64().unwrap() as usize;
                let view_length = recorded["viewLength"].as_u64().unwrap() as usize;
                let expected_system = expected_blocks(&expected["system"]["blocks"]);
                let expected_view = expected_blocks(&expected["view"]["blocks"]);
                assert_eq!(
                    system_length,
                    expected_system.len(),
                    "{name}/{id}: system length after flush {index}"
                );
                assert_eq!(
                    view_length,
                    expected_view.len(),
                    "{name}/{id}: view length after flush {index}"
                );
                assert_hex_list(
                    &format!("{name}/{id} system blocks after flush {index}"),
                    &system_blocks[..system_length],
                    &expected_system,
                );
                assert_hex_list(
                    &format!("{name}/{id} view blocks after flush {index}"),
                    &view_blocks[..view_length],
                    &expected_view,
                );
            }
        }

        // the apply calls
        let recorded_applies = peer
            .recorded
            .applies
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let trace_applies: Vec<&Value> = trace["apply"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|call| call["peer"].as_str() == Some(id.as_str()))
            .collect();
        assert_eq!(
            recorded_applies.len(),
            trace_applies.len(),
            "{name}/{id}: apply count"
        );
        for (index, (recorded, expected)) in recorded_applies.iter().zip(&trace_applies).enumerate()
        {
            assert_eq!(
                recorded["nodes"], expected["nodes"],
                "{name}/{id}: apply nodes of call {index}"
            );
            assert_eq!(
                recorded["host"], expected["host"],
                "{name}/{id}: apply host calls of call {index}"
            );
        }
    }

    for peer in &peers {
        peer.db.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn single_writer_trace() {
    run_scenario("single-writer").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_writers_trace() {
    run_scenario("concurrent-writers").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn add_remove_trace() {
    run_scenario("add-remove").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn optimistic_trace() {
    run_scenario("optimistic").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn reorder_trace() {
    run_scenario("reorder").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn late_joiner_trace() {
    run_scenario("late-joiner").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn fast_forward_landing_trace() {
    run_scenario("fast-forward-landing").await;
}

// ---------------------------------------------------------------------------
// multi-value append

/// `fixtures/gen/append-batch.mjs` records npm autobee appending three values in
/// one call; a multi-value append must stamp the same coordinates JS does
/// (`end: values.length - 1 - i`) and flush the same oplog bytes.
const MULTI_APPEND: &str = include_str!("../fixtures/append-batch-reference.json");

#[tokio::test(flavor = "multi_thread")]
async fn multi_value_append_matches_js() {
    let fixture: Value = serde_json::from_str(MULTI_APPEND).unwrap();
    let clock = Arc::new(AtomicU64::new(0));
    let dir = tempfile::tempdir().unwrap();
    let peer = open_peer(
        dir.path(),
        "a",
        None,
        false,
        false,
        None,
        Arc::clone(&clock),
    )
    .await;
    assert_eq!(
        key_hex(&peer.db.local_key),
        fixture["writer"].as_str().unwrap(),
        "fixture writer key"
    );
    clock.store(fixture["clock"].as_u64().unwrap(), Ordering::Release);

    let values: Vec<Bytes> = fixture["apply"][0]
        .as_array()
        .unwrap()
        .iter()
        .map(|node| Bytes::from(decode_hex(node["value"].as_str().unwrap())))
        .collect();
    peer.db.append(values, false).await.unwrap();

    let mut blocks = Vec::new();
    for index in 0..peer.db.local.length() {
        let block = peer
            .db
            .local
            .get(index, hypercore::GetOptions::default())
            .await
            .unwrap()
            .expect("local block");
        blocks.push(hex(&block));
    }
    let expected: Vec<String> = fixture["oplog"]["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|block| block.as_str().unwrap().to_string())
        .collect();
    assert_eq!(blocks, expected, "multi-value append oplog bytes");

    let applies = peer
        .recorded
        .applies
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    let mut nodes: Vec<Value> = Vec::new();
    for call in &applies {
        nodes.extend(call["nodes"].as_array().unwrap().iter().cloned());
    }
    let shapes: Vec<Value> = nodes
        .iter()
        .map(|node| {
            json!({
                "key": node["key"],
                "length": node["length"],
                "batch": node["batch"],
                "value": node["value"],
            })
        })
        .collect();
    assert_eq!(
        Value::Array(shapes),
        Value::Array(fixture["apply"][0].as_array().unwrap().clone()),
        "multi-value append batch shape"
    );
    peer.db.close().await.unwrap();
}
