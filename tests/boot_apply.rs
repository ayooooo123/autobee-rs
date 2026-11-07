//! Replays the recorded JS fixtures for `boot`, `trusted`, `triggers`,
//! `updates` and `apply_calls`:
//! `fixtures/gen/boot.mjs` writes `fixtures/boot-reference.json`,
//! `fixtures/gen/units.mjs` writes `fixtures/units-reference.json`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use autobee::apply_calls::{
    AddWriterOptions, ApplyCalls, Auto, BoxFuture, Result as HandlerResult, SystemAddWriter,
};
use autobee::apply_view::Handlers;
use autobee::triggers::Writer as _;
use autobee::types::{Head, Key, Link, Node};
use autobee::{boot, encoding, migrations, triggers, trusted, updates};
use corestore::{Corestore, GetOptions};
use hyperbee::Bee;
use serde_json::Value;

// --- fixture helpers -----------------------------------------------------

fn hex_to_bytes(text: &str) -> Vec<u8> {
    assert!(text.len().is_multiple_of(2), "odd hex length");
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).expect("hex"))
        .collect()
}

fn key_of(value: &Value) -> Key {
    let text = value.as_str().expect("hex string");
    hex_to_bytes(text).try_into().expect("32-byte key")
}

fn opt_head(value: &Value) -> Option<Head> {
    value.as_object().map(|object| Head {
        key: Some(key_of(&object["key"])),
        length: object["length"].as_u64().unwrap(),
    })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn fixture(text: &str) -> Value {
    serde_json::from_str(text).expect("fixture json")
}

const BOOT: &str = include_str!("../fixtures/boot-reference.json");
const UNITS: &str = include_str!("../fixtures/units-reference.json");

// --- boot ----------------------------------------------------------------

async fn open_preset(
    store: &Corestore,
    preset: &Value,
    sessions: &mut Vec<hypercore::core::Session>,
) {
    let session = if let Some(name) = preset.get("keyPairName").and_then(Value::as_str) {
        store
            .get(GetOptions {
                key_pair: Some(store.create_key_pair(name)),
                inactive: true,
                ..Default::default()
            })
            .await
            .expect("preset key pair core")
    } else if let Some(name) = preset.get("name").and_then(Value::as_str) {
        store
            .get(GetOptions {
                name: Some(name.as_bytes().to_vec()),
                inactive: true,
                ..Default::default()
            })
            .await
            .expect("preset named core")
    } else {
        store
            .get(GetOptions {
                key: Some(key_of(&preset["key"])),
                inactive: true,
                ..Default::default()
            })
            .await
            .expect("preset keyed core")
    };
    assert_eq!(
        hex(session.core().key()),
        preset["key"].as_str().unwrap(),
        "preset core key derived differently"
    );

    for (name, value) in preset["userData"].as_object().unwrap() {
        let bytes = value.as_str().map(hex_to_bytes);
        session
            .set_user_data(name, bytes.as_deref())
            .await
            .expect("preset user data");
    }

    sessions.push(session);
}

async fn check_user_data(scenario: &str, session: &hypercore::core::Session, expected: &Value) {
    for (name, value) in expected.as_object().unwrap() {
        let actual = session.user_data(name).expect("user data read");
        let expected = value.as_str().map(hex_to_bytes);
        assert_eq!(
            actual.as_ref().map(|bytes| hex(bytes)),
            expected.as_ref().map(|bytes| hex(bytes)),
            "{scenario}: user data {name}"
        );
    }
}

#[tokio::test]
async fn boot_fixtures() {
    let fixtures = fixture(BOOT);
    for scenario in fixtures["scenarios"].as_array().unwrap() {
        let name = scenario["name"].as_str().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let mut store =
            Corestore::open_with_primary_key(dir.path(), key_of(&scenario["primaryKey"]))
                .expect("corestore");
        store.manifest_version = scenario["manifestVersion"].as_u64().unwrap();

        let mut sessions = Vec::new();
        for preset in scenario["preset"].as_array().unwrap() {
            open_preset(&store, preset, &mut sessions).await;
        }

        if name == "reboot" {
            let first = boot::boot(&store, None, boot::Options::default())
                .await
                .expect("first boot");
            drop(first);
        }

        let recorded = &scenario["boot"];
        let key_pair = recorded["keyPairName"]
            .as_str()
            .map(|name| store.create_key_pair(name));
        if let Some(public_key) = recorded["keyPairPublicKey"].as_str() {
            assert_eq!(
                hex(&key_pair.as_ref().unwrap().public_key),
                public_key,
                "{name}: key pair derived differently"
            );
        }
        let options = boot::Options {
            encrypt: recorded["encrypt"].as_bool().unwrap_or(false),
            encryption_key: recorded["encryptionKey"].as_str().map(|text| {
                hex_to_bytes(text)
                    .try_into()
                    .expect("32-byte encryption key")
            }),
            key_pair,
        };
        let key = recorded["key"].as_str().map(|_| key_of(&recorded["key"]));

        if scenario.get("error").is_some() {
            let error = match boot::boot(&store, key, options).await {
                Err(error) => error,
                Ok(_) => panic!("{name}: boot succeeded"),
            };
            // our decision point refuses legacy storage without decoding it
            assert!(
                matches!(
                    error,
                    boot::Error::Migration(migrations::Error::Legacy(
                        migrations::LegacyAutobaseStorage
                    ))
                ),
                "{name}: {error:?}"
            );
            continue;
        }

        let expect = &scenario["expect"];
        let booted = boot::boot(&store, key, options)
            .await
            .unwrap_or_else(|error| panic!("{name}: {error:?}"));

        assert_eq!(
            hex(&booted.key),
            expect["key"].as_str().unwrap(),
            "{name}: key"
        );
        assert_eq!(
            hex(booted.local.core().key()),
            expect["localKey"].as_str().unwrap(),
            "{name}: local"
        );
        assert_eq!(
            hex(booted.bootstrap.core().key()),
            expect["bootstrapKey"].as_str().unwrap(),
            "{name}: bootstrap"
        );
        assert_eq!(
            booted.encryption_key.as_ref().map(|key| hex(key)),
            expect["encryptionKey"].as_str().map(str::to_string),
            "{name}: encryption key"
        );
        assert_eq!(
            booted.previous_drain,
            expect["previousDrain"].as_u64().unwrap(),
            "{name}: previous drain"
        );

        let system = &expect["system"];
        match (&booted.system, system.as_object()) {
            (None, None) => {}
            (Some(link), Some(expected)) => {
                assert_eq!(
                    hex(&link.key),
                    expected["key"].as_str().unwrap(),
                    "{name}: system"
                );
                assert_eq!(
                    link.length,
                    expected["length"].as_u64().unwrap(),
                    "{name}: system length"
                );
                // a stored boot record decodes and re-encodes byte for byte
                let stored = booted
                    .local
                    .user_data("autobee/head")
                    .unwrap()
                    .expect("stored boot record");
                assert_eq!(
                    encoding::encode_boot_record(link),
                    stored,
                    "{name}: boot record"
                );
            }
            other => panic!("{name}: system mismatch {other:?}"),
        }

        check_user_data(name, &booted.local, &expect["localUserData"]).await;
        check_user_data(name, &booted.bootstrap, &expect["bootstrapUserData"]).await;
    }
}

// --- triggers ------------------------------------------------------------

struct TestWriter {
    name: String,
    waiting: Mutex<Option<triggers::Entry>>,
    bumps: Arc<Mutex<Vec<String>>>,
}

impl TestWriter {
    fn new(name: &str, bumps: Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            name: name.to_string(),
            waiting: Mutex::new(None),
            bumps,
        }
    }
}

impl triggers::Writer for TestWriter {
    fn waiting(&self) -> Option<triggers::Entry> {
        self.waiting.lock().unwrap().clone()
    }

    fn bump(&self) {
        self.bumps.lock().unwrap().push(self.name.clone());
    }
}

#[test]
fn triggers_fixtures() {
    let fixtures = fixture(UNITS);
    let steps = fixtures["triggers"]["steps"].as_array().unwrap();

    let mut triggers = triggers::Triggers::new();
    let bumps = Arc::new(Mutex::new(Vec::new()));
    let mut writers: std::collections::HashMap<String, Arc<TestWriter>> =
        std::collections::HashMap::new();
    let mut entry: Option<triggers::Entry> = None;

    let writer = |writers: &mut std::collections::HashMap<String, Arc<TestWriter>>, name: &str| {
        writers
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(TestWriter::new(name, bumps.clone())))
            .clone()
    };

    for step in steps {
        let op = &step["op"];
        if let Some(add) = op.get("add") {
            let name = add["writer"].as_str().unwrap();
            let w = writer(&mut writers, name);
            let writer_ref: Arc<dyn triggers::Writer> = w.clone();
            let added = triggers.add(
                key_of(&add["id"]),
                add["length"].as_u64().unwrap(),
                &writer_ref,
            );
            *w.waiting.lock().unwrap() = Some(added.clone());
            entry = Some(added);
        } else if let Some(remove) = op.get("remove") {
            let w = writer(&mut writers, remove["writer"].as_str().unwrap());
            if let Some(waiting) = w.waiting() {
                triggers.remove(&waiting);
            }
        } else if let Some(trigger) = op.get("trigger") {
            triggers.trigger(&key_of(&trigger["id"]), trigger["length"].as_u64().unwrap());
        } else {
            panic!("unknown trigger op {op}");
        }

        let expected_entry = &step["entry"];
        match (&entry, expected_entry.as_object()) {
            (None, None) => {}
            (Some(entry), Some(expected)) => {
                assert_eq!(hex(&entry.id()), expected["id"].as_str().unwrap());
                assert_eq!(entry.index(), expected["index"].as_u64().unwrap() as usize);
                assert_eq!(entry.length(), expected["length"].as_u64().unwrap());
            }
            other => panic!("entry mismatch {other:?}"),
        }

        let bumped: Vec<String> = bumps.lock().unwrap().drain(..).collect();
        let expected_bumps: Vec<String> = step["bumps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap().to_string())
            .collect();
        assert_eq!(bumped, expected_bumps);

        let waiting = step["waiting"].as_object().unwrap();
        assert_eq!(
            waiting.len(),
            writers
                .values()
                .filter(|writer| writer.waiting().is_some())
                .count(),
            "waiting writer count"
        );
        for (name, expected) in waiting {
            let entry = writers[name].waiting().expect("waiting entry");
            assert_eq!(hex(&entry.id()), expected["id"].as_str().unwrap());
            assert_eq!(entry.index(), expected["index"].as_u64().unwrap() as usize);
            assert_eq!(entry.length(), expected["length"].as_u64().unwrap());
        }
    }
}

// --- trusted -------------------------------------------------------------

struct TrustHandler {
    calls: AtomicUsize,
    answers: Vec<(Key, bool)>,
    most_recent: Option<Head>,
}

impl TrustHandler {
    fn new(answers: Vec<(Key, bool)>, most_recent: Option<Head>) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            answers,
            most_recent,
        }
    }

    fn answer(&self, key: &Key) -> bool {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.answers
            .iter()
            .find(|(candidate, _)| candidate == key)
            .map(|(_, answer)| *answer)
            .unwrap_or(false)
    }
}

impl Handlers for TrustHandler {
    fn apply<'a, 'b>(
        &'a self,
        _nodes: &'a [Node],
        _view: &'a Bee,
        _host: &'a ApplyCalls<'a, 'b>,
    ) -> BoxFuture<'a, HandlerResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn update<'a>(&'a self, _view: &'a Bee, _changes: &'a updates::Changes) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }

    fn is_trusted<'a>(&'a self, key: &'a Key, _reference: Option<&'a Bee>) -> BoxFuture<'a, bool> {
        Box::pin(async move { self.answer(key) })
    }

    fn has_most_recent_trusted(&self) -> bool {
        self.most_recent.is_some()
    }

    fn most_recent_trusted<'a>(
        &'a self,
        _target: &'a Bee,
        _reference: Option<&'a Bee>,
    ) -> BoxFuture<'a, Option<Head>> {
        Box::pin(async move { self.most_recent })
    }
}

/// The reference's hooks `{}`: `isTrusted` defaults to `!mostRecentTrusted`.
struct BareHandler;

impl Handlers for BareHandler {
    fn apply<'a, 'b>(
        &'a self,
        _nodes: &'a [Node],
        _view: &'a Bee,
        _host: &'a ApplyCalls<'a, 'b>,
    ) -> BoxFuture<'a, HandlerResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn update<'a>(&'a self, _view: &'a Bee, _changes: &'a updates::Changes) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }
}

#[tokio::test]
async fn trusted_fixtures() {
    let fixtures = fixture(UNITS);
    let fixture = &fixtures["trusted"];

    let genesis_value = &fixture["genesis"];
    let genesis_key = key_of(&genesis_value["key"]);
    let indexer = key_of(&genesis_value["indexers"][0]);
    let genesis = trusted::Genesis {
        is_genesis: genesis_value["isGenesis"].as_bool().unwrap(),
        key: &genesis_key,
        indexers: &[indexer],
    };

    // a bee only to satisfy the target/reference parameters
    let dir = tempfile::tempdir().unwrap();
    let store = Corestore::open(dir.path()).unwrap();
    let bee = Bee::open(&store, None).await.unwrap();

    let answers: Vec<(Key, bool)> = fixture["steps"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|step| {
            step.get("key").and_then(|key| key.as_str()).map(|_| {
                (
                    key_of(&step["key"]),
                    step["result"].as_bool().unwrap_or(true),
                )
            })
        })
        .collect();
    let mut unique = Vec::new();
    for (key, answer) in answers {
        if !unique
            .iter()
            .any(|(candidate, _): &(Key, bool)| *candidate == key)
        {
            unique.push((key, answer));
        }
    }

    let recent_step = fixture["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|step| step["kind"] == "mostRecentTrustedHook")
        .expect("mostRecentTrustedHook fixture step");
    let recent_head = Head {
        key: Some(key_of(&recent_step["result"]["key"])),
        length: recent_step["result"]["length"].as_u64().unwrap(),
    };

    let handler = Arc::new(TrustHandler::new(unique, None));
    let peers = trusted::TrustedPeers::new(handler.clone());

    for step in fixture["steps"].as_array().unwrap() {
        match step["kind"].as_str().unwrap() {
            "isTrusted" => {
                let key = key_of(&step["key"]);
                let before = handler.calls.load(Ordering::SeqCst);
                let result = peers.is_trusted(&key, None, genesis).await;
                assert_eq!(
                    result,
                    step["result"].as_bool().unwrap(),
                    "{}",
                    step["label"]
                );
                assert_eq!(
                    handler.calls.load(Ordering::SeqCst) - before,
                    step["hookCalls"].as_u64().unwrap() as usize,
                    "{} hook calls",
                    step["label"]
                );
            }
            "concurrent" => {
                let key = key_of(&step["key"]);
                let before = handler.calls.load(Ordering::SeqCst);
                let (first, second) = tokio::join!(
                    peers.is_trusted(&key, None, genesis),
                    peers.is_trusted(&key, None, genesis)
                );
                let expected = &step["results"];
                assert_eq!(first, expected[0].as_bool().unwrap());
                assert_eq!(second, expected[1].as_bool().unwrap());
                assert_eq!(
                    handler.calls.load(Ordering::SeqCst) - before,
                    step["hookCalls"].as_u64().unwrap() as usize,
                    "concurrent hook calls"
                );
            }
            "clear" => peers.clear(),
            "mostRecentTrusted" => {
                assert_eq!(peers.most_recent_trusted(&bee, None).await, None);
            }
            "read" => {
                let input = step["input"].as_bool();
                let expected = step["result"].as_bool();
                assert_eq!(peers.read(input), expected);
            }
            "bare" => {
                let bare = trusted::TrustedPeers::new(Arc::new(BareHandler));
                let key = key_of(&step["key"]);
                assert_eq!(
                    bare.is_trusted(&key, None, genesis).await,
                    step["result"].as_bool().unwrap()
                );
            }
            "mostRecentTrustedHook" => {
                let recent = trusted::TrustedPeers::new(Arc::new(TrustHandler::new(
                    Vec::new(),
                    Some(recent_head),
                )));
                assert_eq!(
                    recent.most_recent_trusted(&bee, None).await,
                    Some(recent_head)
                );
                assert_eq!(recent_head.key, Some(key_of(&step["result"]["key"])));
            }
            other => panic!("unknown trusted step {other}"),
        }
    }
}

// --- updates -------------------------------------------------------------

fn head_of(value: &Value) -> Option<Head> {
    opt_head(value)
}

#[test]
fn updates_fixtures() {
    let fixtures = fixture(UNITS);
    for scenario in fixtures["updates"]["scenarios"].as_array().unwrap() {
        let mut changes = updates::UpdateChanges::new();
        if let Some(track) = scenario["track"].as_object() {
            changes.track(head_of(&track["view"]), head_of(&track["system"]));
        }
        let finalise = &scenario["finalise"];
        let result = changes.finalise(
            head_of(&finalise["view"]),
            head_of(&finalise["system"]),
            finalise["flushes"].as_u64().unwrap(),
            finalise["fastForward"].as_bool().unwrap(),
        );

        let name = scenario["name"].as_str().unwrap();
        for (field, actual) in [("view", result.view), ("system", result.system)] {
            let expected = &scenario["expect"][field];
            match (&actual, expected.as_object()) {
                (None, None) => {}
                (Some(change), Some(expected)) => {
                    assert_eq!(
                        change.flushes,
                        expected["flushes"].as_u64().unwrap(),
                        "{name} {field}"
                    );
                    assert_eq!(
                        change.from,
                        head_of(&expected["from"]),
                        "{name} {field} from"
                    );
                    assert_eq!(change.to, head_of(&expected["to"]), "{name} {field} to");
                    assert_eq!(
                        change.incremental,
                        expected["incremental"].as_bool().unwrap(),
                        "{name} {field} incremental"
                    );
                }
                other => panic!("{name} {field}: {other:?}"),
            }
        }
    }
}

// --- apply calls ---------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Recorded {
    AddWriter(Key, SystemAddWriter),
    RemoveWriter(Key),
    Interrupt(Option<String>),
    CreateAnchor(Key, u64),
}

struct TestAuto {
    recorded: Mutex<Vec<Recorded>>,
    key: Key,
    discovery_key: Key,
}

impl Auto for TestAuto {
    fn id(&self) -> Option<&str> {
        Some("id-1")
    }

    fn key(&self) -> Key {
        self.key
    }

    fn discovery_key(&self) -> Option<Key> {
        Some(self.discovery_key)
    }

    fn name(&self) -> Option<&str> {
        Some("test-name")
    }

    fn flushes(&self) -> u64 {
        7
    }

    fn is_genesis(&self) -> bool {
        true
    }

    fn add_writer<'a>(
        &'a self,
        key: Key,
        opts: SystemAddWriter,
    ) -> BoxFuture<'a, std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>> {
        self.recorded
            .lock()
            .unwrap()
            .push(Recorded::AddWriter(key, opts));
        Box::pin(async { Ok(()) })
    }

    fn remove_writer<'a>(
        &'a self,
        key: Key,
    ) -> BoxFuture<'a, std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>> {
        self.recorded
            .lock()
            .unwrap()
            .push(Recorded::RemoveWriter(key));
        Box::pin(async { Ok(()) })
    }

    fn interrupt(&self, reason: Option<&str>) {
        self.recorded
            .lock()
            .unwrap()
            .push(Recorded::Interrupt(reason.map(str::to_string)));
    }

    fn create_anchor<'a>(
        &'a self,
        key: Key,
        length: u64,
    ) -> BoxFuture<'a, std::result::Result<Link, Box<dyn std::error::Error + Send + Sync>>> {
        self.recorded
            .lock()
            .unwrap()
            .push(Recorded::CreateAnchor(key, length));
        Box::pin(async move { Ok(Link { key, length }) })
    }
}

fn recorded_add(key: &Value, opts: &Value) -> Recorded {
    let opts = &opts["opts"];
    Recorded::AddWriter(
        key_of(key),
        SystemAddWriter {
            weight: opts["weight"].as_u64().unwrap(),
            is_genesis: opts["isGenesis"].as_bool().unwrap(),
            coord: opts["coord"].as_object().map(|coord| Link {
                key: key_of(&coord["key"]),
                length: coord["length"].as_u64().unwrap(),
            }),
            carrier: opts["carrier"].as_i64().unwrap(),
        },
    )
}

#[tokio::test]
async fn apply_calls_fixtures() {
    let fixtures = fixture(UNITS);
    let steps = fixtures["applyCalls"]["steps"].as_array().unwrap();

    let auto = TestAuto {
        recorded: Mutex::new(Vec::new()),
        key: [0x11; 32],
        discovery_key: [0x22; 32],
    };

    for step in steps {
        let op = step["op"].as_str().unwrap();
        let host = ApplyCalls::new(&auto, None);
        match op {
            "addWriter-default" | "addWriter-writer" | "addWriter-weight" | "addWriter-id"
            | "addWriter-applying" => {
                let options = if op == "addWriter-writer" {
                    AddWriterOptions::writer()
                } else {
                    AddWriterOptions::default()
                };
                match op {
                    "addWriter-id" => {
                        // the valid id the fixture used, as text
                        let key = [0x34; 32];
                        host.add_writer_id(&hex(&key), options).await.unwrap();
                    }
                    "addWriter-applying" => {
                        let batch = [Node {
                            core: None,
                            key: [0x35; 32],
                            length: 3,
                            weight: 4,
                            version: 4,
                            timestamp: 0,
                            links: Vec::new(),
                            batch: autobee::encoding::Batch { start: 0, end: 0 },
                            views: None,
                            trusted: Vec::new(),
                            witness: None,
                            approvals: None,
                            optimistic: false,
                            value: None,
                            hash: None,
                            weight_hint: 0,
                            drift: 0,
                        }];
                        let host = ApplyCalls::new(&auto, Some(&batch));
                        host.add_writer([0x35; 32], options).await.unwrap();
                    }
                    _ => {
                        let key = match op {
                            "addWriter-default" => [0x31; 32],
                            "addWriter-writer" => [0x32; 32],
                            _ => [0x33; 32],
                        };
                        let options = match op {
                            "addWriter-writer" => AddWriterOptions::writer(),
                            "addWriter-weight" => AddWriterOptions {
                                is_indexer: true,
                                weight: Some(5),
                            },
                            _ => options,
                        };
                        host.add_writer(key, options).await.unwrap();
                    }
                }
                let recorded = auto.recorded.lock().unwrap().drain(..).collect::<Vec<_>>();
                let expected = &step["calls"];
                assert_eq!(recorded.len(), expected.as_array().unwrap().len(), "{op}");
                for (actual, expected) in recorded.iter().zip(expected.as_array().unwrap()) {
                    assert_eq!(actual, &recorded_add(&expected["key"], expected), "{op}");
                }
            }
            "removeWriter" => {
                let key = key_of(&step["calls"][0]["key"]);
                host.remove_writer(key).await.unwrap();
                let recorded = auto.recorded.lock().unwrap().drain(..).collect::<Vec<_>>();
                assert_eq!(recorded, vec![Recorded::RemoveWriter(key)]);
            }
            "addWriter-invalid" => {
                let error = host
                    .add_writer_id("not-a-key", AddWriterOptions::default())
                    .await
                    .unwrap_err();
                assert_eq!(error.to_string(), step["error"].as_str().unwrap());
            }
            "removeWriter-invalid" => {
                let error = host.remove_writer_id("zzz").await.unwrap_err();
                assert_eq!(error.to_string(), step["error"].as_str().unwrap());
            }
            "interrupt" => {
                host.interrupt(Some("stop"));
                let recorded = auto.recorded.lock().unwrap().drain(..).collect::<Vec<_>>();
                assert_eq!(
                    recorded,
                    vec![Recorded::Interrupt(Some("stop".to_string()))]
                );
            }
            "accessors" => {
                assert_eq!(host.id(), Some("id-1"));
                assert_eq!(hex(&host.key()), step["key"].as_str().unwrap());
                assert_eq!(
                    host.discovery_key().map(|key| hex(&key)),
                    step["discoveryKey"].as_str().map(str::to_string)
                );
                assert_eq!(host.name(), Some("test-name"));
                assert_eq!(host.clock(), step["clock"].as_u64().unwrap());
                assert_eq!(host.genesis(), step["genesis"].as_bool().unwrap());
                assert_eq!(
                    host.removeable(&[0u8; 32]),
                    step["removeable"].as_bool().unwrap()
                );
            }
            other => panic!("unknown apply-calls op {other}"),
        }
    }
}
