//! Replays `fixtures/traces/fast-forward-landing.json` through the
//! `fast_forward` module and exercises the search rules the trace cannot
//! produce: untrusted candidates, a revoked owner, the legacy crawl bounds and
//! cancellation.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autobee::apply_calls::{ApplyCalls, BoxFuture as CallFuture, Result as CallResult};
use autobee::apply_view::Handlers;
use autobee::encoding::schema::autobee::ExternalBatch;
use autobee::encoding::{self, Batch, GrantHint, Link, SystemInfoValue, TrustedHead, Views};
use autobee::fast_forward::{BoxFuture, FastForward, Host, Landing, MIN_FF_GAP, Options};
use autobee::system::SystemBee;
use autobee::trusted::{Genesis, TrustedPeers};
use autobee::types::{EMPTY_HEAD, Head, Info, Key, Node};
use autobee::updates::Changes;
use corestore::{Corestore, GetOptions as StoreGetOptions};
use hyperbee::{Bee, Head as BeeHead, RangeOptions};
use hypercore::core::Session;
use hypercore::{GetOptions, RequestBag};
use serde_json::Value;

const TRACE: &str = include_str!("../fixtures/traces/fast-forward-landing.json");
const OWNER_CORE: &str = "f4ca17163325eda4c80ed9f00cd8a5600901a63b22b9e28ba058b2c5ba0659e6";
const JOINER_CORE: &str = "ad497a97cdf56c57bcb706a349406539df5fc27d85d4c9bc492ada7731ddb39f";
const SYSTEM_CORE: &str = "b22de2d7dd4d80c072f709183551311a24bec15fe5c701f4e8b97b41e6f600d6";
const VIEW_CORE: &str = "f6aa1b1e758a6e9b9407d32da8888402f5ba4b189a72d167b0909d82402366dc";

// --- trace helpers ---------------------------------------------------------

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

fn key(v: &Value) -> Key {
    fixed(v.as_str().unwrap())
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|b| format!("{b:02x}")).collect()
}

fn number(v: &Value) -> Option<u64> {
    v.as_i64().and_then(|n| u64::try_from(n).ok())
}

fn link(v: &Value) -> Link {
    Link {
        key: key(&v["key"]),
        length: v["length"].as_u64().unwrap(),
    }
}

fn head(v: &Value) -> Head {
    Head {
        key: Some(key(&v["key"])),
        length: v["length"].as_u64().unwrap(),
    }
}

fn bee_head(h: Head) -> BeeHead {
    BeeHead {
        key: h.key.unwrap(),
        length: h.length,
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

fn update(v: &Value) -> autobee::system::WriterUpdate {
    autobee::system::WriterUpdate {
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

fn trace() -> Value {
    serde_json::from_str(TRACE).unwrap()
}

fn peer_events<'a>(trace: &'a Value, peer: &str) -> Vec<&'a Value> {
    trace["flushes"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["peer"] == peer)
        .collect()
}

fn peer_applies<'a>(trace: &'a Value, peer: &str) -> Vec<&'a Value> {
    trace["apply"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["peer"] == peer)
        .collect()
}

fn core_name(local: &Key, kind: &str) -> Vec<u8> {
    format!(
        "autobee/{}/view/{kind}",
        hypercore_id_encoding::encode(local)
    )
    .into_bytes()
}

/// The local writer core uses the store's manifest version (1); the system and
/// view cores are opened with `DEFAULT_MANIFEST_VERSION` (3).
async fn open_writer(store: &Corestore, name: Vec<u8>) -> Session {
    store
        .get(StoreGetOptions {
            name: Some(name),
            ..Default::default()
        })
        .await
        .unwrap()
}

async fn open_named(store: &Corestore, name: Vec<u8>) -> Session {
    store
        .get(StoreGetOptions {
            name: Some(name),
            manifest_version: Some(3),
            ..Default::default()
        })
        .await
        .unwrap()
}

async fn scan(bee: &Bee) -> Vec<(String, String)> {
    let mut stream = bee.range(RangeOptions::default());
    let mut entries = Vec::new();
    while let Some(entry) = stream.next().await.unwrap() {
        entries.push((hex(&entry.key), hex(&entry.value)));
    }
    entries
}

// --- the channel's trust handler -------------------------------------------

/// `isTrusted` as `src/apply.js#makeIsTrusted` defines it for a channel: the
/// channel key, or a linked owner device that was not revoked.
struct OwnerTrust {
    owner: Key,
    decisions: Mutex<Vec<(Key, bool)>>,
}

impl OwnerTrust {
    fn new(owner: Key) -> Arc<Self> {
        Arc::new(Self {
            owner,
            decisions: Mutex::new(Vec::new()),
        })
    }

    fn decided(&self) -> Vec<(Key, bool)> {
        self.decisions.lock().unwrap().clone()
    }

    async fn trusted(owner: Key, key: Key, view: Option<Bee>) -> bool {
        if key == owner {
            return true;
        }
        let Some(view) = view else {
            return false;
        };
        let options = GetOptions {
            timeout: Some(Duration::from_secs(5)),
            ..Default::default()
        };
        let revoked = format!("revoked/{}", hex(&key)).into_bytes();
        if let Ok(Some(_)) = view.get_with_options(&revoked, options.clone()).await {
            return false;
        }
        let record = format!("writer/{}", hex(&key)).into_bytes();
        let Ok(Some(entry)) = view.get_with_options(&record, options).await else {
            return false;
        };
        serde_json::from_slice::<Value>(&entry.value)
            .ok()
            .is_some_and(|value| value["role"] == "owner")
    }
}

impl Handlers for OwnerTrust {
    fn apply<'a, 'b>(
        &'a self,
        _nodes: &'a [Node],
        _view: &'a Bee,
        _host: &'a ApplyCalls<'a, 'b>,
    ) -> CallFuture<'a, CallResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn update<'a>(&'a self, _view: &'a Bee, _changes: &'a Changes) -> CallFuture<'a, ()> {
        Box::pin(async {})
    }

    fn is_trusted<'a>(&'a self, key: &'a Key, reference: Option<&'a Bee>) -> CallFuture<'a, bool> {
        Box::pin(async move {
            let trusted = OwnerTrust::trusted(self.owner, *key, reference.cloned()).await;
            self.decisions.lock().unwrap().push((*key, trusted));
            trusted
        })
    }
}

// --- the joiner's host for the recorded session -----------------------------

struct ReplayHost {
    store: Corestore,
    reboot: Corestore,
    reference: Mutex<Option<Bee>>,
    trusted: Arc<TrustedPeers>,
    owner: Key,
    local: Key,
    flushes: AtomicU64,
    forwarding: AtomicBool,
}

impl Host for ReplayHost {
    fn open_core(&self, key: Key) -> BoxFuture<'_, Option<Arc<Session>>> {
        Box::pin(async move {
            self.store
                .get(StoreGetOptions::key(key))
                .await
                .ok()
                .map(Arc::new)
        })
    }

    fn open_legacy_core(&self, key: Key) -> BoxFuture<'_, Option<Arc<Session>>> {
        self.open_core(key)
    }

    fn read_oplog(
        &self,
        core: Arc<Session>,
        length: u64,
        timeout: u64,
    ) -> BoxFuture<'_, Option<Node>> {
        Box::pin(async move {
            if length == 0 {
                return None;
            }
            // `timeout: 0` is no timeout, as `if (timeout)` in the reference
            let options = GetOptions {
                timeout: (timeout != 0).then(|| Duration::from_millis(timeout)),
                ..Default::default()
            };
            let block = core.get(length - 1, options).await.ok()??;
            let oplog = encoding::decode_oplog(block).ok()?;
            Some(Node::from_oplog(*core.key(), length, 0, &oplog, None))
        })
    }

    fn open_view(&self, head: Head, _timeout: u64) -> BoxFuture<'_, Option<Bee>> {
        Box::pin(async move {
            let core = self.store.get(StoreGetOptions::key(head.key?)).await.ok()?;
            let bee = Bee::from_session(&self.store, Arc::new(core)).await.ok()?;
            bee.checkout(bee_head(head)).await.ok()
        })
    }

    fn close_view(&self, _view: Bee) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn is_trusted(&self, key: Key) -> BoxFuture<'_, bool> {
        Box::pin(async move {
            let reference = self.reference.lock().unwrap().clone();
            let genesis = Genesis {
                is_genesis: false,
                key: &self.owner,
                indexers: &[],
            };
            self.trusted
                .is_trusted(&key, reference.as_ref(), genesis)
                .await
        })
    }

    fn most_recent_trusted(&self, view: Bee) -> BoxFuture<'_, Option<Head>> {
        Box::pin(async move {
            let reference = self.reference.lock().unwrap().clone();
            self.trusted
                .most_recent_trusted(&view, reference.as_ref())
                .await
        })
    }

    fn should_reindex(&self, key: Key, length: u64, timeout: u64) -> BoxFuture<'_, bool> {
        Box::pin(async move {
            let Ok(core) = self.store.get(StoreGetOptions::key(key)).await else {
                return true;
            };
            if core.manifest().is_none() && length > 0 {
                let options = GetOptions {
                    timeout: (timeout != 0).then(|| Duration::from_millis(timeout)),
                    ..Default::default()
                };
                let _ = core.get(length - 1, options).await;
            }
            let reindex = match core.manifest() {
                None => true,
                Some(manifest) if manifest.version >= 3 => false,
                Some(manifest) => manifest.version > 1,
            };
            core.close();
            reindex
        })
    }

    fn system_flushes(&self) -> u64 {
        self.flushes.load(Ordering::SeqCst)
    }

    fn local_key(&self) -> Key {
        self.local
    }

    fn conservative(&self) -> bool {
        false
    }

    fn forwarding(&self) -> bool {
        self.forwarding.load(Ordering::SeqCst)
    }

    fn open_system(&self) -> BoxFuture<'_, Option<SystemBee>> {
        Box::pin(async move {
            let core = self
                .reboot
                .get(StoreGetOptions {
                    name: Some(core_name(&self.local, "system")),
                    manifest_version: Some(3),
                    ..Default::default()
                })
                .await
                .ok()?;
            let bee = Bee::from_session(&self.reboot, Arc::new(core)).await.ok()?;
            Some(SystemBee::new(bee))
        })
    }

    fn warmup(
        &self,
        _view: Bee,
        _requests: RequestBag,
    ) -> Option<BoxFuture<'static, Result<(), ()>>> {
        // the recorded handler supplied no warmup hook
        None
    }

    fn core_length(&self, core: Arc<Session>, timeout: u64) -> BoxFuture<'_, u64> {
        Box::pin(async move {
            if core.length() == 0 {
                let mut heads = core.subscribe();
                let _ = tokio::time::timeout(
                    Duration::from_millis(timeout),
                    heads.wait_for(|head| head.length > 0),
                )
                .await;
            }
            core.length()
        })
    }

    fn read_legacy_system_info(
        &self,
        _core: Arc<Session>,
        _length: u64,
        _timeout: u64,
    ) -> BoxFuture<'_, Option<Info>> {
        Box::pin(async { None })
    }
}

fn info(value: SystemInfoValue) -> Info {
    match value {
        SystemInfoValue::V3(info) => info.into(),
        _ => panic!("expected a version 3 system info"),
    }
}

async fn observed(store: &Corestore, key: Key, target: u64) -> (u64, u64, Vec<u64>) {
    let core = store.get(StoreGetOptions::key(key)).await.unwrap();
    let length = core.length();
    let mut present = Vec::new();
    for index in 0..target {
        if core.has(index) {
            present.push(index);
        }
    }
    core.close();
    (length, target, present)
}

fn expected_download(landing: &Value, kind: &str) -> (u64, u64, Vec<u64>) {
    let row = landing["downloaded"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["kind"] == kind)
        .unwrap();
    (
        row["length"].as_u64().unwrap(),
        row["targetLength"].as_u64().unwrap(),
        row["present"]
            .as_array()
            .unwrap()
            .iter()
            .map(|index| index.as_u64().unwrap())
            .collect(),
    )
}

#[tokio::test]
async fn replays_the_recorded_landing() {
    let trace = trace();
    let landing = &trace["fastForward"]["landing"];
    let owner: Key = fixed(OWNER_CORE);
    let system_key: Key = fixed(SYSTEM_CORE);
    let view_key: Key = fixed(VIEW_CORE);

    // --- peer a, the owner: its cores rebuilt from the trace -----------------
    let dir_a = tempfile::tempdir().unwrap();
    let store_a = Corestore::open_with_primary_key(dir_a.path(), [0x61; 32]).unwrap();

    let local = open_writer(&store_a, b"local".to_vec()).await;
    assert_eq!(hex(local.key()), OWNER_CORE, "owner local core");

    let blocks: Vec<Vec<u8>> = trace["peers"]["a"]["oplog"]["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|block| bytes(block.as_str().unwrap()))
        .collect();
    assert_eq!(blocks.len() as u64, 25);
    local.append(&blocks).await.unwrap();

    let flushes: Vec<&Value> = peer_events(&trace, "a")
        .into_iter()
        .filter(|event| event["systemHead"]["length"].as_u64().unwrap() <= 25)
        .collect();
    assert_eq!(flushes.len(), 25);

    let system_core = Arc::new(open_named(&store_a, core_name(&owner, "system")).await);
    assert_eq!(hex(system_core.key()), SYSTEM_CORE, "owner system core");
    let mut system = SystemBee::new(
        Bee::from_session(&store_a, Arc::clone(&system_core))
            .await
            .unwrap(),
    );
    for event in &flushes {
        assert_eq!(
            system.bee.head().unwrap(),
            bee_head(head(&event["preHead"]))
        );
        let nodes: Vec<Node> = event["batch"]
            .as_array()
            .unwrap()
            .iter()
            .map(node)
            .collect();
        for n in &nodes {
            system.add_node(n).unwrap();
        }
        system.updates.clear();
        for upd in event["updates"].as_array().unwrap() {
            system.update(update(upd));
        }
        system
            .flush(&nodes, bee_head(head(&event["viewHead"])))
            .await
            .unwrap();
    }
    assert_eq!(
        system.bee.head().unwrap(),
        bee_head(head(&landing["systemHead"]))
    );
    assert_eq!(system.flushes, landing["info"]["flushes"].as_u64().unwrap());
    assert_eq!(
        system.heads,
        landing["info"]["heads"]
            .as_array()
            .unwrap()
            .iter()
            .map(link)
            .collect::<Vec<_>>()
    );
    {
        let last = flushes.last().unwrap();
        let expected = last["system"]["blocks"].as_array().unwrap();
        assert_eq!(system_core.length() as usize, expected.len());
        for (seq, block) in expected.iter().enumerate() {
            let actual = system_core
                .get(seq as u64, GetOptions::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(hex(&actual), block.as_str().unwrap(), "system block {seq}");
        }
    }

    // the view bee: the recorded handler's puts, one flush per apply
    let view_core = Arc::new(open_named(&store_a, core_name(&owner, "view")).await);
    assert_eq!(hex(view_core.key()), VIEW_CORE, "owner view core");
    let view = Bee::from_session(&store_a, Arc::clone(&view_core))
        .await
        .unwrap();
    for (index, event) in peer_applies(&trace, "a").iter().take(25).enumerate() {
        let mut write = view.batch();
        for n in event["nodes"].as_array().unwrap() {
            let op: Value = serde_json::from_slice(&bytes(n["value"].as_str().unwrap())).unwrap();
            match op["type"].as_str().unwrap() {
                "grant" => write.put(
                    format!("writer/{}", op["writer"].as_str().unwrap()).into_bytes(),
                    b"writer".to_vec(),
                ),
                "revoke" => {
                    write.del(format!("writer/{}", op["writer"].as_str().unwrap()).into_bytes())
                }
                "put" => write.put(
                    op["key"].as_str().unwrap().as_bytes().to_vec(),
                    op["value"].as_str().unwrap().as_bytes().to_vec(),
                ),
                other => panic!("unknown op {other}"),
            }
        }
        write.flush().await.unwrap();
        assert_eq!(
            view.head().unwrap(),
            bee_head(head(&flushes[index]["viewHead"])),
            "view head after apply {index}"
        );
        let expected = flushes[index]["view"]["blocks"].as_array().unwrap();
        assert_eq!(view_core.length() as usize, expected.len());
        for (seq, block) in expected.iter().enumerate() {
            let actual = view_core
                .get(seq as u64, GetOptions::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(hex(&actual), block.as_str().unwrap(), "view block {seq}");
        }
    }
    assert_eq!(view.head().unwrap(), bee_head(head(&landing["viewHead"])));

    // --- peer b, the joiner: an empty store that replicates a -----------------
    let dir_b = tempfile::tempdir().unwrap();
    let store_b = Corestore::open_with_primary_key(dir_b.path(), [0x62; 32]).unwrap();
    let ((ar, aw), (br, bw)) = protomux::memory::duplex(64);
    let handshake = Some([7u8; 64]);
    let mux_a = protomux::Mux::new(
        ar,
        aw,
        protomux::StreamInfo {
            is_initiator: true,
            handshake_hash: handshake,
            remote_public_key: None,
        },
    );
    let mux_b = protomux::Mux::new(
        br,
        bw,
        protomux::StreamInfo {
            is_initiator: false,
            handshake_hash: handshake,
            remote_public_key: None,
        },
    );
    store_a.replicate(&mux_a);
    store_b.replicate(&mux_b);

    let joiner = open_writer(&store_b, b"local".to_vec()).await;
    assert_eq!(hex(joiner.key()), JOINER_CORE, "joiner local core");

    // the joiner had already replicated the owner's whole view before landing
    // (its bounded pre-applied drain); seed that so the recorded presence holds
    {
        let seeded = store_b.get(StoreGetOptions::key(view_key)).await.unwrap();
        let complete =
            tokio::time::timeout(Duration::from_secs(10), seeded.download(0, Some(25), false))
                .await
                .expect("the seeded download settles")
                .unwrap();
        assert!(complete);
        seeded.close();
    }

    let reference_core = store_b.get(StoreGetOptions::key(view_key)).await.unwrap();
    assert_eq!(hex(reference_core.key()), VIEW_CORE);
    let reference = Bee::from_session(&store_b, Arc::new(reference_core))
        .await
        .unwrap()
        .checkout(bee_head(head(&landing["viewHead"])))
        .await
        .unwrap();

    let trust = OwnerTrust::new(owner);
    let host = ReplayHost {
        store: store_b.clone(),
        reboot: store_b.namespace("reboot"),
        reference: Mutex::new(Some(reference)),
        trusted: Arc::new(TrustedPeers::new(Arc::clone(&trust) as Arc<dyn Handlers>)),
        owner,
        local: *joiner.key(),
        // the joiner had applied the grant (one flush) before the search
        flushes: AtomicU64::new(1),
        forwarding: AtomicBool::new(false),
    };

    let heads = [Head {
        key: Some(owner),
        length: 25,
    }];
    assert!(
        25 - host.flushes.load(Ordering::SeqCst) >= MIN_FF_GAP,
        "the recorded landing clears the gap"
    );

    let mut chosen =
        FastForward::from_heads(&host, &heads, autobee::fast_forward::FromOptions::default())
            .await
            .expect("a trusted candidate lands");
    assert_eq!(
        chosen.head,
        head(&trace["fastForward"]["candidates"][0]["selected"]),
        "the selected head is the verified system head"
    );
    assert_eq!(
        trust.decided(),
        trace["fastForward"]["candidates"][0]["decisions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|decision| (
                key(&decision["key"]),
                decision["trusted"].as_bool().unwrap()
            ))
            .collect::<Vec<_>>(),
        "the same trust decisions in the same order"
    );

    let landing_result: Landing = chosen.run(&host).await.expect("the landing runs");
    assert_eq!(landing_result.head, head(&landing["systemHead"]));
    assert_eq!(
        chosen.system.view,
        Some(bee_head(head(&landing["viewHead"])))
    );
    assert_eq!(chosen.system.flushes, 25);

    let system_info = info(chosen.system.get_info().await.unwrap().unwrap());
    assert_eq!(
        system_info.flushes,
        landing["info"]["flushes"].as_u64().unwrap()
    );
    assert_eq!(
        system_info.timestamp,
        landing["info"]["timestamp"].as_u64().unwrap()
    );
    assert_eq!(system_info.view, head(&landing["info"]["view"]));
    assert_eq!(
        system_info.heads,
        landing["info"]["heads"]
            .as_array()
            .unwrap()
            .iter()
            .map(head)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        system_info.pending,
        Some(
            landing["info"]["pending"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_bool().unwrap())
                .collect()
        )
    );
    assert_eq!(
        system_info.hash.map(|hash| hex(&hash)),
        landing["info"]["hash"].as_str().map(str::to_owned)
    );

    // downloads, before any KV scan: the same cores at the same lengths with
    // the same blocks present
    assert_eq!(
        observed(&store_b, owner, 25).await,
        expected_download(landing, "oplog")
    );
    assert_eq!(
        observed(&store_b, system_key, 25).await,
        expected_download(landing, "system")
    );
    assert_eq!(
        observed(&store_b, view_key, 25).await,
        expected_download(landing, "view")
    );

    // the landed KV state, after the scans that may fetch more blocks
    let expected_system: Vec<(String, String)> = landing["system"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["key"].as_str().unwrap().to_owned(),
                row["value"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    assert_eq!(scan(&chosen.system.bee).await, expected_system);

    let expected_view: Vec<(String, String)> = landing["view"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["key"].as_str().unwrap().to_owned(),
                row["value"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    let view_bee = host
        .open_view(head(&landing["viewHead"]), 5_000)
        .await
        .unwrap();
    assert_eq!(scan(&view_bee).await, expected_view);

    // a search while the landing is still pending decides nothing at all,
    // exactly like the recorded second candidate
    host.forwarding.store(true, Ordering::SeqCst);
    host.flushes.store(25, Ordering::SeqCst);
    let decided = trust.decided().len();
    assert!(
        FastForward::from_heads(&host, &heads, autobee::fast_forward::FromOptions::default())
            .await
            .is_none(),
        "a pending landing queues no second search"
    );
    assert_eq!(trust.decided().len(), decided, "no trust decision is made");
    assert!(trace["fastForward"]["candidates"][1]["selected"].is_null());
}

// --- stub host -------------------------------------------------------------

#[derive(Default)]
struct StubHost {
    core: Option<Arc<Session>>,
    /// The key each `open_core` call was made for, most recent last.
    opened: Mutex<Vec<Key>>,
    oplogs: Mutex<HashMap<(Key, u64), Node>>,
    legacy: Mutex<HashMap<Key, Info>>,
    legacy_calls: Mutex<Vec<Key>>,
    trust: Mutex<HashMap<Key, bool>>,
    trusted_peers: Option<Arc<TrustedPeers>>,
    reference: Mutex<Option<Bee>>,
    decisions: Mutex<Vec<Key>>,
    flushes: AtomicU64,
    length: u64,
    forwarding: AtomicBool,
}

impl Host for StubHost {
    fn open_core(&self, key: Key) -> BoxFuture<'_, Option<Arc<Session>>> {
        Box::pin(async move {
            self.opened.lock().unwrap().push(key);
            self.core.clone()
        })
    }

    fn open_legacy_core(&self, key: Key) -> BoxFuture<'_, Option<Arc<Session>>> {
        Box::pin(async move {
            self.legacy_calls.lock().unwrap().push(key);
            self.core.clone()
        })
    }

    fn read_oplog(
        &self,
        _core: Arc<Session>,
        length: u64,
        _timeout: u64,
    ) -> BoxFuture<'_, Option<Node>> {
        Box::pin(async move {
            let key = self.opened.lock().unwrap().pop()?;
            self.oplogs.lock().unwrap().get(&(key, length)).cloned()
        })
    }

    fn open_view(&self, _head: Head, _timeout: u64) -> BoxFuture<'_, Option<Bee>> {
        Box::pin(async { None })
    }

    fn close_view(&self, _view: Bee) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn is_trusted(&self, key: Key) -> BoxFuture<'_, bool> {
        Box::pin(async move {
            self.decisions.lock().unwrap().push(key);
            if let Some(trusted) = &self.trusted_peers {
                let reference = self.reference.lock().unwrap().clone();
                let genesis = Genesis {
                    is_genesis: false,
                    key: &key,
                    indexers: &[],
                };
                return trusted.is_trusted(&key, reference.as_ref(), genesis).await;
            }
            *self.trust.lock().unwrap().get(&key).unwrap_or(&false)
        })
    }

    fn most_recent_trusted(&self, _view: Bee) -> BoxFuture<'_, Option<Head>> {
        Box::pin(async { None })
    }

    fn should_reindex(&self, _key: Key, _length: u64, _timeout: u64) -> BoxFuture<'_, bool> {
        Box::pin(async { false })
    }

    fn system_flushes(&self) -> u64 {
        self.flushes.load(Ordering::SeqCst)
    }

    fn local_key(&self) -> Key {
        [0; 32]
    }

    fn conservative(&self) -> bool {
        false
    }

    fn forwarding(&self) -> bool {
        self.forwarding.load(Ordering::SeqCst)
    }

    fn open_system(&self) -> BoxFuture<'_, Option<SystemBee>> {
        Box::pin(async { None })
    }

    fn warmup(
        &self,
        _view: Bee,
        _requests: RequestBag,
    ) -> Option<BoxFuture<'static, Result<(), ()>>> {
        None
    }

    fn core_length(&self, core: Arc<Session>, _timeout: u64) -> BoxFuture<'_, u64> {
        Box::pin(async move { self.length.max(core.length()) })
    }

    fn read_legacy_system_info(
        &self,
        _core: Arc<Session>,
        _length: u64,
        _timeout: u64,
    ) -> BoxFuture<'_, Option<Info>> {
        Box::pin(async move {
            let key = self.legacy_calls.lock().unwrap().last().copied();
            key.and_then(|key| self.legacy.lock().unwrap().get(&key).cloned())
        })
    }
}

fn stub_node(key: Key, length: u64, flushes: u64, system: Key) -> Node {
    Node {
        core: None,
        key,
        length,
        weight: 1,
        version: 4,
        timestamp: 0,
        links: Vec::new(),
        batch: Batch { start: 0, end: 0 },
        views: Some(Views {
            flushes,
            system: ExternalBatch {
                key: system,
                start: 0,
                length: 1,
            },
            view: None,
        }),
        trusted: Vec::new(),
        witness: None,
        approvals: None,
        optimistic: false,
        value: None,
        hash: None,
        weight_hint: 0,
        drift: 0,
    }
}

fn legacy_info(indexer: Key) -> Info {
    Info {
        version: 1,
        timestamp: 0,
        flushes: 0,
        view: EMPTY_HEAD,
        heads: Vec::new(),
        indexers: Some(vec![Head {
            key: Some(indexer),
            length: 1,
        }]),
        pending: None,
        hash: None,
    }
}

async fn scratch() -> (tempfile::TempDir, Arc<Session>, SystemBee) {
    let dir = tempfile::tempdir().unwrap();
    let store = Corestore::open_with_primary_key(dir.path(), [0x62; 32]).unwrap();
    let core = Arc::new(open_writer(&store, b"scratch".to_vec()).await);
    let bee = Bee::from_session(
        &store,
        Arc::new(open_named(&store, b"system".to_vec()).await),
    )
    .await
    .unwrap();
    (dir, core, SystemBee::new(bee))
}

#[tokio::test]
async fn untrusted_candidates_do_not_land() {
    let (_dir, core, _system) = scratch().await;
    let candidate: Key = [3; 32];
    let signalled: Key = [4; 32];

    let host = StubHost {
        core: Some(core),
        flushes: AtomicU64::new(1),
        ..Default::default()
    };
    host.oplogs.lock().unwrap().insert(
        (candidate, 25),
        Node {
            trusted: vec![TrustedHead {
                key: signalled,
                length: 5,
                flushes: 0,
            }],
            ..stub_node(candidate, 25, 25, [5; 32])
        },
    );
    host.oplogs
        .lock()
        .unwrap()
        .insert((signalled, 5), stub_node(signalled, 5, 3, [6; 32]));

    let heads = [Head {
        key: Some(candidate),
        length: 25,
    }];
    assert!(
        FastForward::from_heads(&host, &heads, Default::default())
            .await
            .is_none(),
        "no candidate is trusted"
    );
    assert_eq!(
        *host.decisions.lock().unwrap(),
        vec![candidate, signalled],
        "the direct candidate and its signalled head are both probed"
    );
}

#[tokio::test]
async fn a_revoked_owner_is_not_trusted() {
    let dir = tempfile::tempdir().unwrap();
    let store = Corestore::open_with_primary_key(dir.path(), [0x63; 32]).unwrap();
    let view_core = Arc::new(open_named(&store, b"view".to_vec()).await);
    let view = Bee::from_session(&store, Arc::clone(&view_core))
        .await
        .unwrap();

    let owner: Key = [9; 32];
    let candidate: Key = [7; 32];
    let mut write = view.batch();
    write.put(
        format!("writer/{}", hex(&candidate)).into_bytes(),
        br#"{"role":"owner"}"#.to_vec(),
    );
    write.put(
        format!("revoked/{}", hex(&candidate)).into_bytes(),
        b"1".to_vec(),
    );
    write.flush().await.unwrap();

    let trust = OwnerTrust::new(owner);
    let trusted = Arc::new(TrustedPeers::new(Arc::clone(&trust) as Arc<dyn Handlers>));

    let host = StubHost {
        core: Some(Arc::new(open_writer(&store, b"scratch".to_vec()).await)),
        trusted_peers: Some(trusted),
        reference: Mutex::new(Some(view)),
        flushes: AtomicU64::new(1),
        ..Default::default()
    };
    host.oplogs
        .lock()
        .unwrap()
        .insert((candidate, 25), stub_node(candidate, 25, 25, [8; 32]));

    let heads = [Head {
        key: Some(candidate),
        length: 25,
    }];
    assert!(
        FastForward::from_heads(&host, &heads, Default::default())
            .await
            .is_none(),
        "a revoked owner device cannot vouch for a landing"
    );
    assert_eq!(trust.decided(), vec![(candidate, false)]);
}

#[tokio::test]
async fn the_legacy_crawl_stops_at_five_hops() {
    let (_dir, core, system) = scratch().await;
    let host = StubHost {
        core: Some(core),
        length: 5,
        ..Default::default()
    };
    // every system's indexer reports the next system as its flushed successor
    let systems: Vec<Key> = (0..=6u8).map(|i| [0x10 + i; 32]).collect();
    for index in 0..6usize {
        let indexer = [0x40 + index as u8; 32];
        host.legacy
            .lock()
            .unwrap()
            .insert(systems[index], legacy_info(indexer));
        host.oplogs.lock().unwrap().insert(
            (indexer, 5),
            Node {
                version: 2,
                ..stub_node(indexer, 5, 1, systems[index + 1])
            },
        );
    }

    let mut ff = FastForward::new(
        system,
        Head {
            key: Some(systems[0]),
            length: 5,
        },
        Options::default(),
    );
    let (head, autobee) = ff.resolve_legacy(&host).await.unwrap();
    assert!(autobee.is_none());
    assert_eq!(
        head.key,
        Some(systems[5]),
        "the crawl stops after five hops"
    );
    assert_eq!(
        *host.legacy_calls.lock().unwrap(),
        systems[..6].to_vec(),
        "six systems are opened, the sixth successor is not followed"
    );
}

#[tokio::test]
async fn the_legacy_crawl_stops_on_a_cycle() {
    let (_dir, core, system) = scratch().await;
    let host = StubHost {
        core: Some(core),
        length: 5,
        ..Default::default()
    };
    let a: Key = [0x21; 32];
    let b: Key = [0x22; 32];
    for (peer, indexer) in [(a, [0x31; 32]), (b, [0x32; 32])] {
        host.legacy
            .lock()
            .unwrap()
            .insert(peer, legacy_info(indexer));
    }
    host.oplogs.lock().unwrap().insert(
        ([0x31; 32], 5),
        Node {
            version: 2,
            ..stub_node([0x31; 32], 5, 1, b)
        },
    );
    host.oplogs.lock().unwrap().insert(
        ([0x32; 32], 5),
        Node {
            version: 2,
            ..stub_node([0x32; 32], 5, 1, a)
        },
    );

    let mut ff = FastForward::new(
        system,
        Head {
            key: Some(a),
            length: 5,
        },
        Options::default(),
    );
    let (head, _) = ff.resolve_legacy(&host).await.unwrap();
    assert_eq!(head.key, Some(b), "the cycle ends on the last unseen head");
    assert_eq!(*host.legacy_calls.lock().unwrap(), vec![a, b]);
}

// --- cancellation ----------------------------------------------------------

struct CancelHost {
    store: Corestore,
    view_key: Key,
    started: Arc<AtomicBool>,
}

impl Host for CancelHost {
    fn open_core(&self, key: Key) -> BoxFuture<'_, Option<Arc<Session>>> {
        Box::pin(async move {
            self.store
                .get(StoreGetOptions::key(key))
                .await
                .ok()
                .map(Arc::new)
        })
    }

    fn open_legacy_core(&self, key: Key) -> BoxFuture<'_, Option<Arc<Session>>> {
        self.open_core(key)
    }

    fn read_oplog(
        &self,
        _core: Arc<Session>,
        _length: u64,
        _timeout: u64,
    ) -> BoxFuture<'_, Option<Node>> {
        Box::pin(async { None })
    }

    fn open_view(&self, head: Head, _timeout: u64) -> BoxFuture<'_, Option<Bee>> {
        Box::pin(async move {
            let core = self.store.get(StoreGetOptions::key(head.key?)).await.ok()?;
            let bee = Bee::from_session(&self.store, Arc::new(core)).await.ok()?;
            bee.checkout(bee_head(head)).await.ok()
        })
    }

    fn close_view(&self, _view: Bee) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn is_trusted(&self, _key: Key) -> BoxFuture<'_, bool> {
        Box::pin(async { false })
    }

    fn most_recent_trusted(&self, _view: Bee) -> BoxFuture<'_, Option<Head>> {
        Box::pin(async { None })
    }

    fn should_reindex(&self, _key: Key, _length: u64, _timeout: u64) -> BoxFuture<'_, bool> {
        Box::pin(async { false })
    }

    fn system_flushes(&self) -> u64 {
        0
    }

    fn local_key(&self) -> Key {
        [0; 32]
    }

    fn conservative(&self) -> bool {
        false
    }

    fn forwarding(&self) -> bool {
        false
    }

    fn open_system(&self) -> BoxFuture<'_, Option<SystemBee>> {
        Box::pin(async { None })
    }

    fn warmup(
        &self,
        _view: Bee,
        requests: RequestBag,
    ) -> Option<BoxFuture<'static, Result<(), ()>>> {
        let store = self.store.clone();
        let key = self.view_key;
        let started = Arc::clone(&self.started);
        Some(Box::pin(async move {
            started.store(true, Ordering::SeqCst);
            let Ok(core) = store.get(StoreGetOptions::key(key)).await else {
                return Err(());
            };
            // block 5 does not exist: this waits for the bag or forever
            let options = GetOptions {
                bag: Some(requests),
                ..Default::default()
            };
            let result = core.get(5, options).await;
            core.close();
            result.map(|_| ()).map_err(|_| ())
        }))
    }

    fn core_length(&self, core: Arc<Session>, _timeout: u64) -> BoxFuture<'_, u64> {
        Box::pin(async move { core.length() })
    }

    fn read_legacy_system_info(
        &self,
        _core: Arc<Session>,
        _length: u64,
        _timeout: u64,
    ) -> BoxFuture<'_, Option<Info>> {
        Box::pin(async { None })
    }
}

#[tokio::test]
async fn cancelling_the_bag_during_warmup_releases_every_read() {
    let dir = tempfile::tempdir().unwrap();
    let store = Corestore::open_with_primary_key(dir.path(), [0x64; 32]).unwrap();
    let system_core = Arc::new(open_named(&store, b"system".to_vec()).await);
    let mut system = SystemBee::new(
        Bee::from_session(&store, Arc::clone(&system_core))
            .await
            .unwrap(),
    );
    let view_core = open_named(&store, b"view".to_vec()).await;
    view_core.append(&[b"block".as_slice()]).await.unwrap();
    let view_key = *view_core.key();
    let view_head = BeeHead {
        key: view_key,
        length: 1,
    };
    let batch = vec![stub_node([0x70; 32], 1, 1, [0x71; 32])];
    system.add_node(&batch[0]).unwrap();
    system.flush(&batch, view_head).await.unwrap();
    let system_head = system.bee.head().unwrap();
    assert_eq!(system.version, 3);

    let warmup_started = Arc::new(AtomicBool::new(false));
    let host = Arc::new(CancelHost {
        store: store.clone(),
        view_key,
        started: Arc::clone(&warmup_started),
    });
    let mut ff = FastForward::new(
        system,
        Head {
            key: Some(system_head.key),
            length: system_head.length,
        },
        Options {
            timeout: 500,
            conservative: false,
        },
    );
    let bag = ff.active_requests.clone();
    let runner = {
        let host = Arc::clone(&host);
        tokio::spawn(async move {
            let landing = ff.run(&*host).await;
            (ff, landing)
        })
    };

    for _ in 0..200 {
        if warmup_started.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(warmup_started.load(Ordering::SeqCst), "warmup is in flight");

    bag.destroy("cancelled");
    let (ff, landing) = tokio::time::timeout(Duration::from_secs(5), runner)
        .await
        .expect("cancellation settles the fast forward")
        .unwrap();
    assert!(landing.is_none(), "a cancelled fast forward does not land");
    assert!(ff.destroyed && ff.failed);
    assert!(ff.cores.is_empty(), "every core it opened is closed");

    // every read through the bag fails from here on
    let core = store.get(StoreGetOptions::key(view_key)).await.unwrap();
    assert!(
        core.get(
            0,
            GetOptions {
                bag: Some(bag),
                ..Default::default()
            }
        )
        .await
        .is_err(),
        "the bag fails reads that come after"
    );
}
