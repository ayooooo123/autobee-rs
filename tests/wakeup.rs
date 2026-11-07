//! autobee wakeup: core coupling over hypercore-rs cores, and interop with
//! npm `autobee-wakeup` on a local HyperDHT testnet.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autobee::types::Link;
use autobee::wakeup::{AutobeeWakeup, UNKNOWN_LENGTH, WakeupBase};
use hypercore::crypto::{KeyPair, discovery_key};
use hypercore::storage::Store;
use hypercore::{Core, CoreOptions};
use peeroxide::{JoinOpts, SwarmConfig, SwarmConnection, SwarmHandle, spawn};
use peeroxide_dht::hyperdht::HyperDhtConfig;
use peeroxide_dht::rpc::DhtConfig;
use protomux::{Mux, StreamInfo, memory};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{Notify, mpsc};
use tokio::time::timeout;

/// A stand-in bee: a bootstrap core, a fixed writer head list and bump counting.
struct Base {
    bootstrap: Core,
    writers: Mutex<Vec<Link>>,
    bumps: AtomicUsize,
}

impl Base {
    fn new(bootstrap: Core, writers: Vec<Link>) -> Arc<Self> {
        Arc::new(Self {
            bootstrap,
            writers: Mutex::new(writers),
            bumps: AtomicUsize::new(0),
        })
    }
}

impl WakeupBase for Base {
    fn bootstrap(&self) -> Core {
        self.bootstrap.clone()
    }

    fn wakeup_writers(&self) -> Vec<Link> {
        self.writers.lock().unwrap().clone()
    }

    fn bump_soon(&self) {
        self.bumps.fetch_add(1, Ordering::Relaxed);
    }
}

fn writer(store: &Store, seed: u8) -> Core {
    Core::open(
        store,
        CoreOptions {
            key_pair: Some(KeyPair::from_seed(&[seed; 32])),
            ..Default::default()
        },
    )
    .unwrap()
}

fn reader(store: &Store, key: &[u8; 32]) -> Core {
    Core::open(
        store,
        CoreOptions {
            key: Some(*key),
            ..Default::default()
        },
    )
    .unwrap()
}

fn link(core: &Core) -> Link {
    Link {
        key: *core.key(),
        length: core.length(),
    }
}

/// Two muxes over an in-memory link that look like one secret-stream session.
fn mux_pair() -> (Mux, Mux) {
    let ((ar, aw), (br, bw)) = memory::duplex(64);
    let info = |is_initiator| StreamInfo {
        is_initiator,
        handshake_hash: Some([3u8; 64]),
        remote_public_key: None,
    };
    (Mux::new(ar, aw, info(true)), Mux::new(br, bw, info(false)))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The hints a wakeup has flushed so far, longest length per key.
#[derive(Default)]
struct Hints {
    links: Vec<Link>,
}

impl Hints {
    fn take(&mut self, wakeup: &AutobeeWakeup) {
        for link in wakeup.flush() {
            match self.links.iter_mut().find(|h| h.key == link.key) {
                Some(prev) => {
                    if link.length == UNKNOWN_LENGTH || prev.length < link.length {
                        *prev = link;
                    }
                }
                None => self.links.push(link),
            }
        }
    }

    fn get(&self, key: &[u8; 32]) -> Option<u64> {
        self.links.iter().find(|h| &h.key == key).map(|h| h.length)
    }

    async fn wait(&mut self, wakeup: &AutobeeWakeup, key: &[u8; 32], time: Duration) -> u64 {
        timeout(time, async {
            loop {
                self.take(wakeup);
                if let Some(length) = self.get(key) {
                    return length;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("no hint for {} in {:?}", hex(key), self.links))
    }
}

#[tokio::test]
async fn hints_keep_the_longest_length() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let wakeup = AutobeeWakeup::new(Base::new(writer(&store, 5), vec![]));
    let key = [0x77; 32];
    let hint = |length| Link { key, length };

    wakeup.hint(&[hint(3), hint(1)]);
    assert_eq!(wakeup.flush(), vec![hint(3)], "a shorter length is ignored");
    wakeup.hint(&[hint(2)]);
    assert_eq!(wakeup.flush(), vec![hint(2)]);
    wakeup.hint(&[hint(4), hint(UNKNOWN_LENGTH)]);
    assert_eq!(wakeup.flush(), vec![hint(UNKNOWN_LENGTH)]);
}

/// A coupled writer the bootstrap core's peer does not replicate is announced
/// when that peer appears; lookups are answered from the base's writers.
#[tokio::test]
async fn peer_add_couples_wakeup_and_lookups_are_answered() {
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (sa, sb) = (
        Store::open(da.path()).unwrap(),
        Store::open(db.path()).unwrap(),
    );

    let boot_a = writer(&sa, 9);
    boot_a.append(&[b"boot".as_slice()]).await.unwrap();
    let boot_b = reader(&sb, boot_a.key());
    // Keep the channels open for the run instead of letting idle sync close them.
    let _active_a = boot_a.session(true);
    let _active_b = boot_b.session(true);

    // Coupled but never replicated over the stream: the peer cannot know it.
    let coupled = writer(&sa, 1);
    coupled
        .append(&[b"one".as_slice(), b"two".as_slice()])
        .await
        .unwrap();
    let wa = writer(&sa, 2);
    wa.append(&[b"w".as_slice()]).await.unwrap();
    let wb = writer(&sb, 3);
    wb.append(&[b"b".as_slice()]).await.unwrap();

    let capability = [0x41; 32];
    let (ma, mb) = mux_pair();

    let a = AutobeeWakeup::new(Base::new(boot_a.clone(), vec![link(&wa)]));
    a.add_stream(&ma);
    a.set_capability(capability, None);
    a.recouple();
    assert!(a.add_core(coupled.clone()));

    let b = AutobeeWakeup::new(Base::new(boot_b.clone(), vec![link(&wb)]));
    b.add_stream(&mb);
    b.set_capability(capability, None);

    boot_a.replicate(&ma);
    boot_b.replicate(&mb);

    let mut hints = Hints::default();
    assert_eq!(
        hints.wait(&b, coupled.key(), Duration::from_secs(10)).await,
        2,
        "peer add announces the coupled writer"
    );

    b.request_lookup();
    assert_eq!(
        hints.wait(&b, wa.key(), Duration::from_secs(10)).await,
        1,
        "a lookup is answered from the base's writers"
    );

    let mut from_b = Hints::default();
    a.request_lookup();
    assert_eq!(
        from_b.wait(&a, wb.key(), Duration::from_secs(10)).await,
        1,
        "the other side answers too"
    );

    a.close();
    b.close();
}

fn js_dir() -> &'static std::path::Path {
    std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/js"))
}

#[derive(Default)]
struct Log {
    events: Mutex<Vec<String>>,
    notify: Notify,
}

impl Log {
    fn push(&self, event: String) {
        self.events.lock().unwrap().push(event);
        self.notify.notify_waiters();
    }

    async fn wait_for(&self, prefix: &str) -> String {
        timeout(Duration::from_secs(60), async {
            loop {
                let notified = self.notify.notified();
                let found = {
                    let events = self.events.lock().unwrap();
                    if let Some(error) = events.iter().find(|e| e.starts_with("error")) {
                        panic!("node error: {error}");
                    }
                    events.iter().find(|e| e.starts_with(prefix)).cloned()
                };
                if let Some(event) = found {
                    return event;
                }
                notified.await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("no {prefix:?} in {:?}", self.events.lock().unwrap()))
    }
}

fn node(args: &[&str]) -> (Child, ChildStdin, Arc<Log>) {
    assert!(
        js_dir().join("node_modules").exists(),
        "run `npm install` in {} first",
        js_dir().display()
    );
    let mut child = Command::new("node")
        .arg("peer.js")
        .args(args)
        .current_dir(js_dir())
        .stdout(std::process::Stdio::piped())
        .stdin(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("node on PATH");
    let stdin = child.stdin.take().unwrap();
    let log = Arc::new(Log::default());
    let reader = log.clone();
    let stdout: ChildStdout = child.stdout.take().unwrap();
    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            reader.push(line);
        }
    });
    (child, stdin, log)
}

async fn send(stdin: &mut ChildStdin, command: &str) {
    stdin.write_all(command.as_bytes()).await.unwrap();
    stdin.write_all(b"\n").await.unwrap();
    stdin.flush().await.unwrap();
}

async fn testnet() -> (Child, ChildStdin, String) {
    let (child, stdin, log) = node(&["testnet"]);
    let line = log.wait_for("bootstrap ").await;
    let port = line.trim_start_matches("bootstrap ").trim().to_owned();
    (child, stdin, port)
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

fn unhex(text: &str) -> [u8; 32] {
    let mut key = [0u8; 32];
    for (i, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).expect("hex key");
    }
    key
}

/// npm `autobee-wakeup` and this crate on one capability: each side answers the
/// other's lookup with its writers and the answer lands in the hint sink.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs node and `npm install` in tests/js"]
async fn js_autobee_wakeup_exchanges_announces_and_lookups() {
    let (_testnet, _testnet_in, port) = testnet().await;
    let capability = [0x51; 32];
    let (_js, mut js_in, js) = node(&["peer", &port, &hex(&capability)]);
    let joined = js.wait_for("joined writers ").await;
    let listed = joined
        .trim_start_matches("joined writers ")
        .trim()
        .to_owned();
    let (js_key, js_length) = listed.split_once(':').expect("writers list");
    let js_key = unhex(js_key);
    let js_length: u64 = js_length.parse().expect("writer length");

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let rust_writer = Link {
        key: [0xa1; 32],
        length: 4,
    };
    let wakeup = Arc::new(AutobeeWakeup::new(Base::new(
        writer(&store, 7),
        vec![rust_writer.clone()],
    )));
    wakeup.set_capability(capability, None);

    let (handle, mut conns) = rust_swarm(&port).await;
    let add = wakeup.clone();
    tokio::spawn(async move {
        while let Some(conn) = conns.recv().await {
            add.add_stream(&Mux::from_secret_stream(conn.peer.stream));
        }
    });

    let mut opts = JoinOpts::default();
    opts.server = false;
    opts.client = true;
    handle
        .join(discovery_key(&capability), opts)
        .await
        .expect("join topic");
    handle.flush().await.expect("flush");

    // Rust asks; the JS answer must land in the Rust hint sink.
    let mut hints = Hints::default();
    let answered = timeout(Duration::from_secs(60), async {
        loop {
            wakeup.request_lookup();
            hints.take(&wakeup);
            if let Some(length) = hints.get(&js_key) {
                return length;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("js answer");
    assert_eq!(answered, js_length);

    // JS asks; its hint sink must see the Rust writers.
    send(&mut js_in, "lookup").await;
    let line = js.wait_for("hints ").await;
    assert_eq!(
        line.trim_start_matches("hints ").trim(),
        format!("{}:{}", hex(&rust_writer.key), rust_writer.length)
    );
}
