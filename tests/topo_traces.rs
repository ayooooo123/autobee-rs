use autobee::{
    encoding::{Approval, Batch as Coords, ExternalBatch, GrantWitness, Link, Views},
    topo::{self, HistoryEntry},
    types::{Batch, Change, Head, Key, Node, WeightedLink},
};
use bytes::Bytes;
use serde_json::Value;
use std::{
    collections::HashMap,
    fs,
    future::Future,
    path::Path,
    task::{Context, Poll, Waker},
};

fn hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}
fn key(v: &Value) -> Key {
    hex(v.as_str().unwrap()).try_into().unwrap()
}
fn link(v: &Value) -> Link {
    Link {
        key: key(&v["key"]),
        length: v["length"].as_u64().unwrap(),
    }
}
fn head(v: &Value) -> Head {
    Head {
        key: v["key"].as_str().map(|_| key(&v["key"])),
        length: v["length"].as_u64().unwrap(),
    }
}
fn external(v: &Value) -> ExternalBatch {
    ExternalBatch {
        key: key(&v["key"]),
        start: v["start"].as_u64().unwrap(),
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
        batch: Coords {
            start: v["batch"]["start"].as_u64().unwrap(),
            end: v["batch"]["end"].as_u64().unwrap(),
        },
        views: v["views"].as_object().map(|_| Views {
            flushes: v["views"]["flushes"].as_u64().unwrap(),
            system: external(&v["views"]["system"]),
            view: v["views"]["view"]
                .as_object()
                .map(|_| external(&v["views"]["view"])),
        }),
        trusted: vec![],
        witness: v["witness"].as_object().map(|w| GrantWitness {
            weight: w["weight"].as_u64().unwrap(),
            link: link(&w["link"]),
        }),
        approvals: v["approvals"].as_array().map(|a| {
            a.iter()
                .map(|a| Approval {
                    key: key(&a["key"]),
                    weight: a["weight"].as_u64().unwrap(),
                })
                .collect()
        }),
        optimistic: v["optimistic"].as_bool().unwrap(),
        value: v["value"].as_str().map(|s| Bytes::from(hex(s))),
        hash: v["hash"].as_str().map(|s| hex(s).try_into().unwrap()),
        weight_hint: 0,
        drift: 0,
    }
}
fn batch(v: &Value) -> Batch {
    v.as_array()
        .unwrap()
        .iter()
        .map(|n| if n.is_null() { None } else { Some(node(n)) })
        .collect()
}
fn batches(v: &Value) -> Vec<Batch> {
    v.as_array().unwrap().iter().map(batch).collect()
}
fn id(b: &Batch) -> (Key, u64) {
    let n = b[0].as_ref().unwrap();
    (n.key, n.length)
}
fn run<F: Future>(f: F) -> F::Output {
    let w = Waker::noop();
    let mut cx = Context::from_waker(w);
    let mut f = std::pin::pin!(f);
    match f.as_mut().poll(&mut cx) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("fixture reader must not suspend"),
    }
}
#[derive(Default)]
struct Reader {
    history: Vec<(Change, Batch)>,
    position: usize,
    blocks: HashMap<(Key, u64), Bytes>,
}
impl topo::Reader for Reader {
    type Error = ();
    async fn history(&mut self, _: bool) -> Result<(), ()> {
        self.position = self.history.len();
        Ok(())
    }
    async fn next_change(&mut self) -> Result<Option<Option<HistoryEntry>>, ()> {
        if self.position == 0 {
            return Ok(None);
        }
        self.position -= 1;
        Ok(Some(Some(HistoryEntry {
            legacy: false,
            refs: vec![self.history[self.position].0.clone()],
        })))
    }
    async fn node(
        &mut self,
        key: &Key,
        length: u64,
        weight: i64,
        _: bool,
        _: u64,
    ) -> Result<Option<Node>, ()> {
        if let Some(block) = self.blocks.get(&(*key, length)) {
            let oplog = autobee::encoding::decode_oplog(block.clone()).unwrap();
            let mut node = Node::from_oplog(*key, length, weight, &oplog, None);
            // The recorder omits these fields from every node.
            node.trusted.clear();
            node.weight_hint = 0;
            node.drift = 0;
            return Ok(Some(node));
        }
        Ok(self
            .history
            .iter()
            .rev()
            .flat_map(|(_, b)| b.iter().flatten())
            .find(|n| &n.key == key && n.length == length)
            .cloned()
            .map(|mut n| {
                n.weight = weight;
                n
            }))
    }
}
#[derive(Default)]
struct Peer {
    reader: Reader,
    cursor: usize,
    current: Option<Head>,
}

#[test]
fn topology_traces() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/traces");
    let mut calls = 0;
    for file in fs::read_dir(dir).unwrap() {
        let path = file.unwrap().path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let trace: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        let mut blocks = HashMap::new();
        for p in trace["peers"].as_object().unwrap().values() {
            let k = key(&p["oplog"]["key"]);
            for (i, block) in p["oplog"]["blocks"].as_array().unwrap().iter().enumerate() {
                blocks.insert((k, i as u64 + 1), Bytes::from(hex(block.as_str().unwrap())));
            }
        }
        let mut peers: HashMap<String, Peer> = HashMap::new();
        for event in trace["topology"].as_array().unwrap() {
            calls += 1;
            let input = batches(&event["batches"]);
            let expected = batches(&event["list"]);
            if event["method"] == "linearize" {
                assert_eq!(topo::linearize(input), expected, "{}", path.display());
                continue;
            }
            let peer_name = event["peer"].as_str().unwrap();
            let peer = peers.entry(peer_name.to_owned()).or_insert_with(|| Peer {
                reader: Reader {
                    blocks: blocks.clone(),
                    ..Reader::default()
                },
                ..Peer::default()
            });
            let flushes: Vec<_> = trace["flushes"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|f| f["peer"] == peer_name)
                .collect();
            while peer.cursor < flushes.len() {
                let f = flushes[peer.cursor];
                let b = batch(&f["batch"]);
                if id(&b) == id(&input[0]) {
                    break;
                }
                let n = b[0].as_ref().unwrap();
                peer.reader.history.push((
                    Change {
                        oplog: WeightedLink::from(n),
                        undo: peer.current.or(Some(autobee::types::EMPTY_HEAD)),
                    },
                    b,
                ));
                peer.current = Some(head(&f["systemHead"]));
                peer.cursor += 1;
            }
            let result = run(topo::sort(&mut peer.reader, input[0].clone()))
                .unwrap()
                .expect("complete history");
            assert_eq!(
                result.tip,
                expected,
                "{} peer {}",
                path.display(),
                peer_name
            );
            assert_eq!(
                result.undo,
                if event["undo"].is_null() {
                    None
                } else {
                    Some(head(&event["undo"]))
                },
                "{} peer {}",
                path.display(),
                peer_name
            );
            assert_eq!(result.view, None);
            if let Some(undo) = result.undo {
                while let Some((change, _)) = peer.reader.history.pop() {
                    if change.undo == Some(undo) {
                        break;
                    }
                }
                peer.current = Some(undo);
            }
        }
    }
    assert!(calls > 0);
}

struct WitnessReader {
    bootstrap: Key,
    weight: i64,
    records: HashMap<Key, autobee::witness::Standing>,
    nodes: HashMap<(Key, u64), Node>,
}
impl autobee::witness::Reader for WitnessReader {
    type Error = ();
    fn bootstrap_key(&self) -> &Key {
        &self.bootstrap
    }
    fn bootstrap_weight(&self) -> i64 {
        self.weight
    }
    async fn standing(&mut self, key: &Key) -> Result<Option<autobee::witness::Standing>, ()> {
        Ok(self.records.get(key).copied())
    }
    async fn oplog_node(&mut self, link: &Link) -> Result<Node, ()> {
        Ok(self.nodes[&(link.key, link.length)].clone())
    }
}

#[test]
fn witness_claims_from_traces() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/traces");
    let mut claims = 0;
    for file in fs::read_dir(dir).unwrap() {
        let path = file.unwrap().path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let trace: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        let mut nodes = HashMap::new();
        for event in trace["topology"].as_array().unwrap() {
            for b in batches(&event["batches"])
                .into_iter()
                .chain(batches(&event["list"]))
            {
                for n in b.into_iter().flatten() {
                    nodes.insert((n.key, n.length), n);
                }
            }
        }
        for f in trace["flushes"].as_array().unwrap() {
            for n in batch(&f["batch"]).into_iter().flatten() {
                nodes.insert((n.key, n.length), n);
            }
        }
        let bootstrap = key(&trace["peers"]["a"]["writer"]);
        let weight = nodes.get(&(bootstrap, 1)).map_or(0, |n| n.weight);
        let mut reader = WitnessReader {
            bootstrap,
            weight,
            records: HashMap::new(),
            nodes: nodes.clone(),
        };
        for n in nodes.values().filter(|n| n.witness.is_some()) {
            assert_eq!(
                run(autobee::witness::resolve_weight(&mut reader, n)).unwrap(),
                n.weight
            );
            claims += 1;
        }
    }
    assert!(claims > 0);
}

#[test]
fn witness_rejects_unqualified_and_insufficient_approvals() {
    use autobee::witness::{Standing, current_weight, resolve_weight};
    let standing = Standing {
        weight: 1,
        max_weight: 3,
        is_genesis: true,
    };
    assert_eq!(current_weight(Some(&standing)), 3);
    assert_eq!(current_weight(None), 0);
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/traces/concurrent-writers.json");
    let trace: Value = serde_json::from_slice(&fs::read(dir).unwrap()).unwrap();
    let nodes: Vec<_> = trace["topology"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|e| batches(&e["batches"]))
        .flatten()
        .flatten()
        .collect();
    let target = nodes.iter().find(|n| n.witness.is_some()).unwrap();
    let anchor_link = &target.witness.as_ref().unwrap().link;
    let anchor = nodes
        .iter()
        .find(|n| n.key == anchor_link.key && n.length == anchor_link.length)
        .unwrap()
        .clone();
    let mut reader = WitnessReader {
        bootstrap: [0; 32],
        weight: 0,
        records: HashMap::new(),
        nodes: HashMap::from([((anchor.key, anchor.length), anchor)]),
    };
    assert_eq!(run(resolve_weight(&mut reader, target)).unwrap(), 0);
    reader.records.insert(
        anchor_link.key,
        Standing {
            weight: 1,
            max_weight: 1,
            is_genesis: false,
        },
    );
    assert_eq!(run(resolve_weight(&mut reader, target)).unwrap(), 1);
    reader
        .nodes
        .get_mut(&(anchor_link.key, anchor_link.length))
        .unwrap()
        .approvals = None;
    assert_eq!(run(resolve_weight(&mut reader, target)).unwrap(), 0);
    let mut without_claim = target.clone();
    without_claim.witness = None;
    reader.records.insert(
        target.key,
        Standing {
            weight: 1,
            max_weight: 3,
            is_genesis: true,
        },
    );
    assert_eq!(run(resolve_weight(&mut reader, &without_claim)).unwrap(), 3);
    reader.records.remove(&target.key);
    without_claim.key = reader.bootstrap;
    without_claim.length = 1;
    reader.weight = 2;
    assert_eq!(run(resolve_weight(&mut reader, &without_claim)).unwrap(), 2);
}

#[test]
fn replay_and_links() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/traces/single-writer.json");
    let trace: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    let mut reader = Reader::default();
    for f in trace["flushes"].as_array().unwrap() {
        let b = batch(&f["batch"]);
        let n = b[0].as_ref().unwrap();
        reader.history.push((
            Change {
                oplog: WeightedLink::from(n),
                undo: None,
            },
            b,
        ));
    }
    let expected: Vec<_> = reader.history.iter().flat_map(|(_, b)| b.clone()).collect();
    assert_eq!(run(topo::replay(&mut reader)).unwrap(), expected);
    let first = expected[0].as_ref().unwrap();
    assert!(topo::is_linking(
        first,
        &Link {
            key: first.key,
            length: first.length
        }
    ));
    assert!(!topo::is_linking(
        first,
        &Link {
            key: first.key,
            length: first.length + 1
        }
    ));
    assert!(topo::is_linking_all(first, &[]));
}

#[test]
fn linearize_respects_priority_and_links() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/traces/single-writer.json");
    let trace: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    let base = node(&trace["topology"][0]["batches"][0][0]);
    let mut low = base.clone();
    low.key = [1; 32];
    low.weight = 1;
    let mut high = base;
    high.key = [2; 32];
    high.weight = 2;
    let low_batch = vec![Some(low.clone())];
    let high_batch = vec![Some(high.clone())];
    assert_eq!(
        topo::linearize(vec![low_batch.clone(), high_batch.clone()]),
        vec![high_batch, low_batch.clone()]
    );
    high.links.push(Link {
        key: low.key,
        length: low.length,
    });
    let linked_high = vec![Some(high)];
    assert_eq!(
        topo::linearize(vec![low_batch.clone(), linked_high.clone()]),
        vec![low_batch, linked_high]
    );
}

struct MissingHistory;
impl topo::Reader for MissingHistory {
    type Error = ();
    async fn history(&mut self, _: bool) -> Result<(), ()> {
        Ok(())
    }
    async fn next_change(&mut self) -> Result<Option<Option<HistoryEntry>>, ()> {
        Err(())
    }
    async fn node(&mut self, _: &Key, _: u64, _: i64, _: bool, _: u64) -> Result<Option<Node>, ()> {
        unreachable!()
    }
}

#[test]
fn replay_preserves_missing_history_marker() {
    assert_eq!(run(topo::replay(&mut MissingHistory)).unwrap(), vec![None]);
}
