//! Live interop: npm autobee 2.12.1 and this crate on one local HyperDHT
//! testnet. The JS peer and the Rust peer share a channel with PearTube-like
//! handlers; the owner grants the other writer, both append, and both sides
//! must converge to the same system heads/hash, the same view rows and the
//! same oplog bytes. The Rust peer then restarts on the same storage and
//! resumes, and finally the roles reverse.
//!
//! Needs node and `npm install` in `tests/js`; run with `-- --include-ignored`.
//! The swarm comes from the peeroxide fork (`[patch.crates-io]`).

use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use autobee::apply_calls::{AddWriterOptions, ApplyCalls};
use autobee::apply_view::Handlers;
use autobee::base::Autobee;
use autobee::types::{Key, Node};
use bytes::Bytes;
use corestore::{Corestore, GetOptions};
use hyperbee::Bee;
use peeroxide::{JoinOpts, SwarmConfig, SwarmConnection, SwarmHandle, spawn};
use peeroxide_dht::hyperdht::HyperDhtConfig;
use peeroxide_dht::rpc::DhtConfig;
use protomux::Mux;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep, timeout};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn key_hex(key: &Key) -> String {
    hex(key)
}

fn unhex32(text: &str) -> Key {
    let mut key = [0u8; 32];
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16).expect("hex key");
    }
    key
}

fn js_dir() -> &'static Path {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/js"))
}

// ---------------------------------------------------------------------------
// the Rust side

struct Handler {
    key: Arc<StdMutex<Option<Key>>>,
    clock: Arc<AtomicU64>,
}

impl Handler {
    fn key(&self) -> Key {
        self.key
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .expect("bootstrap key")
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
                let is_owner = key_hex(&node.key) == owner;
                match (op["type"].as_str().unwrap_or_default(), is_owner) {
                    ("grant", true) => {
                        let writer = op["writer"].as_str().expect("grant writer");
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
                    ("revoke", true) => {
                        let writer = op["writer"].as_str().expect("revoke writer");
                        host.remove_writer_id(writer).await?;
                        writes.push((format!("writer/{writer}").into_bytes(), None));
                    }
                    ("put", _) => {
                        writes.push((
                            op["key"].as_str().expect("put key").as_bytes().to_vec(),
                            Some(Bytes::from(
                                op["value"].as_str().expect("put value").as_bytes().to_vec(),
                            )),
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
}

struct RustPeer {
    store: Corestore,
    auto: Arc<Autobee>,
    dir: std::path::PathBuf,
    primary: [u8; 32],
    clock: Arc<AtomicU64>,
}

impl RustPeer {
    async fn open(dir: &Path, primary: [u8; 32], channel: Option<Key>) -> RustPeer {
        std::fs::create_dir_all(dir).unwrap();
        let store = Corestore::open_with_primary_key(dir, primary).unwrap();
        Self::reopen(store, dir, primary, channel).await
    }

    async fn reopen(
        store: Corestore,
        dir: &Path,
        primary: [u8; 32],
        channel: Option<Key>,
    ) -> RustPeer {
        let key_slot = Arc::new(StdMutex::new(None));
        let clock = Arc::new(AtomicU64::new(now_millis()));
        let handler = Arc::new(Handler {
            key: Arc::clone(&key_slot),
            clock: Arc::clone(&clock),
        });
        let auto = Autobee::open(&store, channel, handler as Arc<dyn Handlers>)
            .await
            .expect("autobee open");
        *key_slot.lock().unwrap_or_else(|e| e.into_inner()) = Some(auto.key);
        RustPeer {
            store,
            auto,
            dir: dir.to_path_buf(),
            primary,
            clock,
        }
    }

    /// Closes the bee and the store, then opens the same directory again.
    async fn restart(self) -> RustPeer {
        let RustPeer {
            store,
            auto,
            dir,
            primary,
            ..
        } = self;
        auto.close().await.unwrap();
        store.close().await;
        drop(store);
        Self::open(&dir, primary, Some(auto.key)).await
    }

    async fn append(&self, op: &Value) {
        self.clock.store(now_millis(), Ordering::Release);
        self.auto
            .append(vec![Bytes::from(serde_json::to_vec(op).unwrap())], false)
            .await
            .unwrap();
    }

    async fn view_rows(&self) -> Vec<(String, String)> {
        let bee = self.auto.working_view.view().clone();
        let mut rows = Vec::new();
        let mut stream = bee.range(hyperbee::RangeOptions {
            gte: Some(Bytes::new()),
            ..Default::default()
        });
        while let Some(entry) = stream.next().await.unwrap() {
            rows.push((hex(&entry.key), hex(&entry.value)));
        }
        rows
    }

    async fn state(&self) -> Value {
        let (info, heads, system_head) = {
            let system = self.auto.system.lock().await;
            let info = system.get_info(None).await.unwrap();
            (info, system.heads.clone(), system.bee.head())
        };
        let info = match info {
            Some(autobee::encoding::SystemInfoValue::V3(info)) => json!({
                "version": 3,
                "timestamp": info.timestamp,
                "flushes": info.flushes,
                "heads": heads.iter().map(|head| json!({
                    "key": key_hex(&head.key),
                    "length": head.length,
                })).collect::<Vec<_>>(),
                "hash": info.hash.map(|hash| hex(&hash)),
                "pending": info.pending,
            }),
            other => json!({ "unexpected": format!("{other:?}") }),
        };
        json!({
            "key": key_hex(&self.auto.key),
            "localKey": key_hex(&self.auto.local_key),
            "localLength": self.auto.local.length(),
            "systemHead": system_head.map(|head| json!({
                "key": key_hex(&head.key),
                "length": head.length,
            })),
            "info": info,
            "view": self
                .view_rows()
                .await
                .into_iter()
                .map(|(key, value)| json!({ "key": key, "value": value }))
                .collect::<Vec<_>>(),
        })
    }

    async fn blocks(&self, key: Key) -> Vec<String> {
        let core = self.store.get(GetOptions::key(key)).await.unwrap();
        let mut blocks = Vec::new();
        for index in 0..core.length() {
            let block = core
                .get(index, hypercore::GetOptions::default())
                .await
                .unwrap()
                .unwrap();
            blocks.push(hex(&block));
        }
        core.close();
        blocks
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

// ---------------------------------------------------------------------------
// the JS side

struct JsPeer {
    _child: Child,
    stdin: ChildStdin,
    lines: mpsc::UnboundedReceiver<String>,
    id: String,
}

impl JsPeer {
    fn spawn(id: &str, args: &[&str]) -> JsPeer {
        assert!(
            js_dir().join("node_modules").exists(),
            "run `npm install` in {} first",
            js_dir().display()
        );
        let mut child = Command::new("node")
            .arg("autobee_peer.js")
            .args(args)
            .current_dir(js_dir())
            .stdout(Stdio::piped())
            .stdin(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("node on PATH");
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, lines) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut reader = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                let _ = tx.send(line);
            }
        });
        JsPeer {
            _child: child,
            stdin,
            lines,
            id: id.to_string(),
        }
    }

    async fn send(&mut self, command: &str) {
        self.stdin.write_all(command.as_bytes()).await.unwrap();
        self.stdin.write_all(b"\n").await.unwrap();
        self.stdin.flush().await.unwrap();
    }

    async fn writable(&mut self) -> bool {
        let line = self.call("writable", "writable ").await;
        line.trim_start_matches("writable ").trim() == "true"
    }

    /// Sends one command and returns its `prefix ...` answer line.
    async fn call(&mut self, command: &str, prefix: &str) -> String {
        self.send(command).await;
        self.wait(prefix).await
    }

    async fn wait(&mut self, prefix: &str) -> String {
        timeout(Duration::from_secs(120), async {
            loop {
                let line = self.lines.recv().await.expect("js peer exited");
                if line.starts_with("error") {
                    panic!("{}: {line}", self.id);
                }
                if line.starts_with(prefix) {
                    return line;
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{}: no {prefix:?} line", self.id))
    }

    async fn ready(&mut self) -> (Key, Key) {
        let line = self.wait("ready ").await;
        let payload: Value =
            serde_json::from_str(line.trim_start_matches("ready ").trim()).unwrap();
        (
            unhex32(payload["key"].as_str().unwrap()),
            unhex32(payload["localKey"].as_str().unwrap()),
        )
    }

    async fn state(&mut self) -> Value {
        let line = self.call("state", "state ").await;
        serde_json::from_str(line.trim_start_matches("state ").trim()).unwrap()
    }

    async fn blocks(&mut self, key: &Key) -> Vec<String> {
        let line = self
            .call(&format!("blocks {}", key_hex(key)), "blocks ")
            .await;
        let payload: Value =
            serde_json::from_str(line.trim_start_matches("blocks ").trim()).unwrap();
        payload["blocks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|block| block.as_str().unwrap().to_string())
            .collect()
    }

    async fn append(&mut self, op: &Value) {
        self.call(&format!("append {}", op), "appended ").await;
    }

    async fn hint(&mut self, key: &Key, length: u64) {
        self.call(&format!("hint {} {length}", key_hex(key)), "hinted")
            .await;
    }

    async fn close(&mut self) {
        self.send("close").await;
        let _ = timeout(Duration::from_secs(30), self.wait("closed")).await;
    }
}

async fn js_testnet() -> (Child, ChildStdin, String) {
    let mut child = Command::new("node")
        .arg("peer.js")
        .arg("testnet")
        .current_dir(js_dir())
        .stdout(Stdio::piped())
        .stdin(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("node on PATH");
    let stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut lines = BufReader::new(stdout).lines();
    let line = timeout(Duration::from_secs(120), async {
        loop {
            let line = lines.next_line().await.unwrap().expect("testnet exited");
            if let Some(port) = line.strip_prefix("bootstrap ") {
                return port.trim().to_string();
            }
        }
    })
    .await
    .expect("testnet bootstrap");
    (child, stdin, line)
}

async fn rust_swarm(bootstrap_port: &str) -> (SwarmHandle, mpsc::Receiver<SwarmConnection>) {
    let mut dht = DhtConfig::default();
    dht.bootstrap = vec![format!("127.0.0.1:{bootstrap_port}")];
    dht.host = "127.0.0.1".to_string();
    dht.port = 0;
    let mut hyper = HyperDhtConfig::default();
    hyper.dht = dht;
    let mut config = SwarmConfig::default();
    config.dht = hyper;
    let (_task, handle, conns) = spawn(config).await.expect("peeroxide swarm");
    (handle, conns)
}

/// Wires every incoming connection into the store and the wakeup session.
fn replicate_connections(
    auto: Arc<Autobee>,
    store: Corestore,
    mut conns: mpsc::Receiver<SwarmConnection>,
) {
    tokio::spawn(async move {
        while let Some(conn) = conns.recv().await {
            let mux = Mux::from_secret_stream(conn.peer.stream);
            store.replicate(&mux);
            auto.wakeup.add_stream(&mux);
        }
    });
}

async fn rust_join(handle: &SwarmHandle, capability: &Key) {
    let mut opts = JoinOpts::default();
    opts.server = true;
    opts.client = true;
    handle
        .join(hypercore::crypto::discovery_key(capability), opts)
        .await
        .expect("join topic");
    handle.flush().await.expect("flush");
}

fn normalise_info(state: &Value) -> Value {
    let mut info = state["info"].clone();
    if let Some(heads) = info.get_mut("heads").and_then(Value::as_array_mut) {
        heads.sort_by_key(|head| head["key"].as_str().unwrap_or_default().to_string());
    }
    info
}

fn normalise_rows(rows: &Value) -> Vec<(String, String)> {
    let Some(rows) = rows.as_array() else {
        panic!("expected a view row array, got {rows}");
    };
    let mut rows: Vec<(String, String)> = rows
        .iter()
        .map(|row| {
            let Some(key) = row["key"].as_str() else {
                panic!("row without a key: {row}");
            };
            let Some(value) = row["value"].as_str() else {
                panic!("row {key} without a value: {row}");
            };
            (key.to_string(), value.to_string())
        })
        .collect();
    rows.sort();
    rows
}

fn converged_views(js: &Value, rust: &Value, expected: &[(&str, &str)]) -> bool {
    let rows = normalise_rows(&rust["view"]);
    rows == normalise_rows(&js["view"])
        && expected
            .iter()
            .all(|(key, value)| rows.contains(&(hex(key.as_bytes()), hex(value.as_bytes()))))
}

#[test]
fn equal_stale_views_have_not_converged() {
    let empty = json!({"view": []});
    let current = json!({"view": [{"key": hex(b"key"), "value": hex(b"value")}]});
    let expected = &[("key", "value")];
    assert!(!converged_views(&empty, &empty, expected));
    assert!(!converged_views(&current, &empty, expected));
    assert!(converged_views(&current, &current, expected));
}

/// One round of hint exchange between the JS owner and the Rust writer.
async fn poke(js: &mut JsPeer, rust: &RustPeer, js_local: Key) {
    let rust_state = rust.state().await;
    for head in rust_state["info"]["heads"].as_array().into_iter().flatten() {
        let key = unhex32(head["key"].as_str().unwrap());
        let length = head["length"].as_u64().unwrap();
        js.hint(&key, length).await;
    }
    js.hint(&rust.auto.local_key, rust.auto.local.length())
        .await;
    let js_state = js.state().await;
    for head in js_state["info"]["heads"].as_array().into_iter().flatten() {
        let key = unhex32(head["key"].as_str().unwrap());
        let length = head["length"].as_u64().unwrap();
        rust.auto.wakeup(key, length).await.unwrap();
    }
    rust.auto
        .wakeup(js_local, js_state["localLength"].as_u64().unwrap())
        .await
        .unwrap();
    rust.auto.update().await.unwrap();
}

/// One round of hint exchange in the reversed roles.
async fn poke_js_writer(js: &mut JsPeer, rust: &RustPeer, js_local: Key) {
    let rust_state = rust.state().await;
    for head in rust_state["info"]["heads"].as_array().into_iter().flatten() {
        let key = unhex32(head["key"].as_str().unwrap());
        let length = head["length"].as_u64().unwrap();
        js.hint(&key, length).await;
    }
    let js_state = js.state().await;
    for head in js_state["info"]["heads"].as_array().into_iter().flatten() {
        let key = unhex32(head["key"].as_str().unwrap());
        let length = head["length"].as_u64().unwrap();
        rust.auto.wakeup(key, length).await.unwrap();
    }
    rust.auto
        .wakeup(js_local, js_state["localLength"].as_u64().unwrap())
        .await
        .unwrap();
    rust.auto.update().await.unwrap();
}

/// Exchanges hints in both directions until the two peers agree on the system
/// info, the view rows and every oplog either side knows about.
async fn converge(
    js: &mut JsPeer,
    rust: &RustPeer,
    js_local: Key,
    round: &str,
    expected: &[(&str, &str)],
) -> (Value, Value) {
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let js_state = js.state().await;
        let rust_state = rust.state().await;

        for head in rust_state["info"]["heads"].as_array().into_iter().flatten() {
            let key = unhex32(head["key"].as_str().unwrap());
            let length = head["length"].as_u64().unwrap();
            js.hint(&key, length).await;
        }
        js.hint(&rust.auto.local_key, rust.auto.local.length())
            .await;
        if let Some(heads) = js_state["info"]["heads"].as_array() {
            for head in heads {
                let key = unhex32(head["key"].as_str().unwrap());
                let length = head["length"].as_u64().unwrap();
                rust.auto.wakeup(key, length).await.unwrap();
            }
        }
        rust.auto
            .wakeup(js_local, js_state["localLength"].as_u64().unwrap())
            .await
            .unwrap();
        rust.auto.update().await.unwrap();

        let js_state = js.state().await;
        let rust_state = rust.state().await;

        let same_info = normalise_info(&js_state) == normalise_info(&rust_state);
        let same_view = converged_views(&js_state, &rust_state, expected);

        let mut same_blocks = true;
        if same_info && same_view {
            let js_key = unhex32(js_state["localKey"].as_str().unwrap());
            for key in [js_key, rust.auto.local_key] {
                let from_js = js.blocks(&key).await;
                let from_rust = rust.blocks(key).await;
                if from_js != from_rust {
                    same_blocks = false;
                    break;
                }
            }
        }

        if same_info && same_view && same_blocks {
            return (js_state, rust_state);
        }
        assert!(
            Instant::now() < deadline,
            "{round}: peers did not converge\njs: {js_state}\nrust: {rust_state}"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs node and `npm install` in tests/js"]
async fn js_owner_and_rust_writer_converge_restart_and_swap() {
    let (_testnet, _testnet_in, port) = js_testnet().await;
    let dir = tempfile::tempdir().unwrap();

    // --- JS owner, Rust writer
    let mut js = JsPeer::spawn("js-owner", &["owner", dir.path().to_str().unwrap(), &port]);
    let (channel, js_local) = js.ready().await;
    js.wait("joined").await;

    let mut rust = RustPeer::open(&dir.path().join("rust-owner"), [0x51; 32], Some(channel)).await;
    let rust_local = rust.auto.local_key;
    let (handle, conns) = rust_swarm(&port).await;
    replicate_connections(Arc::clone(&rust.auto), rust.store.clone(), conns);
    rust_join(&handle, &channel).await;

    // the owner grants the Rust writer; the writer appends only once it has
    // applied its own grant (the reference throws `Not writable` before that)
    js.call(&format!("grant {}", key_hex(&rust_local)), "appended ")
        .await;
    let deadline = Instant::now() + Duration::from_secs(120);
    while !rust.auto.writers.lock().await.writable {
        assert!(
            Instant::now() < deadline,
            "the grant never reached the Rust writer"
        );
        poke(&mut js, &rust, js_local).await;
        sleep(Duration::from_millis(50)).await;
    }
    js.append(&json!({ "type": "put", "key": "js/1", "value": "from-js" }))
        .await;
    rust.append(&json!({ "type": "put", "key": "rust/1", "value": "from-rust" }))
        .await;

    let (js_state, rust_state) = converge(
        &mut js,
        &rust,
        js_local,
        "owner-grant",
        &[("js/1", "from-js"), ("rust/1", "from-rust")],
    )
    .await;

    assert_eq!(
        normalise_info(&js_state),
        normalise_info(&rust_state),
        "system info diverged"
    );
    assert_eq!(
        normalise_rows(&js_state["view"]),
        normalise_rows(&rust_state["view"]),
        "view rows diverged"
    );
    let view = normalise_rows(&rust_state["view"]);
    assert!(
        view.contains(&(hex(b"js/1"), hex(b"from-js"))),
        "js put missing"
    );
    assert!(
        view.contains(&(hex(b"rust/1"), hex(b"from-rust"))),
        "rust put missing"
    );
    assert!(
        view.contains(&(
            hex(format!("writer/{}", key_hex(&rust_local)).as_bytes()),
            hex(b"writer")
        )),
        "grant was not applied for {}; view = {view:?}",
        key_hex(&rust_local)
    );
    assert_eq!(
        js.blocks(&js_local).await,
        rust.blocks(js_local).await,
        "owner oplog bytes diverged"
    );
    assert_eq!(
        js.blocks(&rust_local).await,
        rust.blocks(rust_local).await,
        "writer oplog bytes diverged"
    );

    // --- restart the Rust peer on the same storage
    let before = rust.state().await;
    rust = rust.restart().await;
    let (restart_handle, restart_conns) = rust_swarm(&port).await;
    replicate_connections(Arc::clone(&rust.auto), rust.store.clone(), restart_conns);
    rust_join(&restart_handle, &channel).await;

    let after = rust.state().await;
    assert_eq!(
        normalise_info(&before),
        normalise_info(&after),
        "the restarted peer lost its system state"
    );
    assert_eq!(
        normalise_rows(&before["view"]),
        normalise_rows(&after["view"]),
        "the restarted peer lost its view"
    );

    rust.append(&json!({ "type": "put", "key": "rust/2", "value": "after-restart" }))
        .await;
    let (js_state, rust_state) = converge(
        &mut js,
        &rust,
        js_local,
        "restart",
        &[("rust/2", "after-restart")],
    )
    .await;
    assert_eq!(
        normalise_info(&js_state),
        normalise_info(&rust_state),
        "system info diverged after the restart"
    );
    assert!(
        normalise_rows(&rust_state["view"]).contains(&(hex(b"rust/2"), hex(b"after-restart"))),
        "post-restart append did not reach the owner"
    );
    drop(handle);
    drop(restart_handle);
    js.close().await;
    rust.auto.close().await.unwrap();
    rust.store.close().await;

    // --- roles reversed: Rust owner, JS writer
    let owner = RustPeer::open(&dir.path().join("rust-owner-2"), [0x52; 32], None).await;
    let owner_channel = owner.auto.key;
    let (owner_handle, owner_conns) = rust_swarm(&port).await;
    replicate_connections(Arc::clone(&owner.auto), owner.store.clone(), owner_conns);
    rust_join(&owner_handle, &owner_channel).await;

    let mut writer = JsPeer::spawn(
        "js-writer",
        &[
            "peer",
            dir.path().to_str().unwrap(),
            &port,
            &key_hex(&owner_channel),
        ],
    );
    let (_js_channel, js_writer_local) = writer.ready().await;
    writer.wait("joined").await;

    owner
        .append(&json!({
            "type": "grant",
            "writer": key_hex(&js_writer_local),
        }))
        .await;
    owner
        .append(&json!({ "type": "put", "key": "owner/1", "value": "from-owner" }))
        .await;
    let deadline = Instant::now() + Duration::from_secs(120);
    while !writer.writable().await {
        assert!(
            Instant::now() < deadline,
            "the grant never reached the JS writer"
        );
        poke_js_writer(&mut writer, &owner, js_writer_local).await;
        sleep(Duration::from_millis(50)).await;
    }
    writer
        .append(&json!({ "type": "put", "key": "writer/1", "value": "from-writer" }))
        .await;

    let (js_state, owner_state) = converge_js_writer(&mut writer, &owner, js_writer_local).await;
    assert_eq!(
        normalise_info(&js_state),
        normalise_info(&owner_state),
        "system info diverged with the roles reversed"
    );
    assert_eq!(
        normalise_rows(&js_state["view"]),
        normalise_rows(&owner_state["view"]),
        "view rows diverged with the roles reversed"
    );
    let view = normalise_rows(&owner_state["view"]);
    assert!(view.contains(&(hex(b"owner/1"), hex(b"from-owner"))));
    assert!(view.contains(&(hex(b"writer/1"), hex(b"from-writer"))));

    writer.close().await;
    owner.auto.close().await.unwrap();
    owner.store.close().await;
}

/// `converge` with the JS side as the writer and the Rust side as the owner.
async fn converge_js_writer(js: &mut JsPeer, rust: &RustPeer, js_local: Key) -> (Value, Value) {
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let js_state = js.state().await;
        let rust_state = rust.state().await;

        for head in rust_state["info"]["heads"].as_array().into_iter().flatten() {
            let key = unhex32(head["key"].as_str().unwrap());
            let length = head["length"].as_u64().unwrap();
            js.hint(&key, length).await;
        }
        if let Some(heads) = js_state["info"]["heads"].as_array() {
            for head in heads {
                let key = unhex32(head["key"].as_str().unwrap());
                let length = head["length"].as_u64().unwrap();
                rust.auto.wakeup(key, length).await.unwrap();
            }
        }
        rust.auto
            .wakeup(js_local, js_state["localLength"].as_u64().unwrap())
            .await
            .unwrap();
        rust.auto.update().await.unwrap();

        let js_state = js.state().await;
        let rust_state = rust.state().await;
        if normalise_info(&js_state) == normalise_info(&rust_state)
            && converged_views(
                &js_state,
                &rust_state,
                &[("owner/1", "from-owner"), ("writer/1", "from-writer")],
            )
            && js.blocks(&js_local).await == rust.blocks(js_local).await
        {
            return (js_state, rust_state);
        }
        assert!(
            Instant::now() < deadline,
            "reversed roles did not converge\njs: {js_state}\nrust: {rust_state}"
        );
        sleep(Duration::from_millis(50)).await;
    }
}
