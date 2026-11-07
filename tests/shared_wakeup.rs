//! Two Autobees on one node over one connection.
//!
//! A Protomux connection has a single pairing owner per protocol: a second
//! wakeup swarm's `pair("wakeup", None)` takes the entry from the first, whose
//! pairing loop then ends. JS keeps one `ProtomuxWakeup` per node for exactly
//! that reason, so every bee in a node shares the node's swarm and each of its
//! topics gets its channel on every connection.

use std::sync::Arc;
use std::time::Duration;

use autobee::Autobee;
use autobee::apply_calls::{ApplyCalls, BoxFuture, Result as HandlerResult};
use autobee::apply_view::Handlers;
use autobee::types::{Key, Node};
use autobee::updates::Changes;
use corestore::Corestore;
use hyperbee::Bee;
use protomux::{Mux, StreamInfo, memory};
use protomux_wakeup::WakeupSwarm;
use tokio::time::timeout;

struct Bare;

impl Handlers for Bare {
    fn apply<'a, 'b>(
        &'a self,
        _nodes: &'a [Node],
        _view: &'a Bee,
        _host: &'a ApplyCalls<'a, 'b>,
    ) -> BoxFuture<'a, HandlerResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn update<'a>(&'a self, _view: &'a Bee, _changes: &'a Changes) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }
}

fn mux_pair() -> (Mux, Mux) {
    let ((ar, aw), (br, bw)) = memory::duplex(64);
    let info = |is_initiator| StreamInfo {
        is_initiator,
        handshake_hash: Some([9u8; 64]),
        remote_public_key: None,
    };
    (Mux::new(ar, aw, info(true)), Mux::new(br, bw, info(false)))
}

fn store(root: &std::path::Path, name: &str, primary: u8) -> Corestore {
    let dir = root.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    Corestore::open_with_primary_key(&dir, [primary; 32]).unwrap()
}

async fn open(store: &Corestore, key: Option<Key>, swarm: &Arc<WakeupSwarm>) -> Arc<Autobee> {
    Autobee::open_with_wakeup(store, key, Arc::new(Bare), swarm.clone())
        .await
        .unwrap()
}

/// Both of a node's bees pair on one mux: every bee's topic has the peer.
#[tokio::test(flavor = "multi_thread")]
async fn two_bees_pair_on_one_mux() {
    let dir = tempfile::tempdir().unwrap();
    let store_a = store(dir.path(), "a", 1);
    let store_b = store(dir.path(), "b", 2);
    // One namespace per bee, as the node does (`channel:<key>`): a store holds
    // each bee's `local` core under its own namespace.
    let (ns_a1, ns_a2) = (store_a.namespace("one"), store_a.namespace("two"));
    let (ns_b1, ns_b2) = (store_b.namespace("one"), store_b.namespace("two"));
    let swarm_a = Arc::new(WakeupSwarm::new(|_, _| {}));
    let swarm_b = Arc::new(WakeupSwarm::new(|_, _| {}));

    let a1 = open(&ns_a1, None, &swarm_a).await;
    let a2 = open(&ns_a2, None, &swarm_a).await;
    let b1 = open(&ns_b1, Some(a1.key), &swarm_b).await;
    let b2 = open(&ns_b2, Some(a2.key), &swarm_b).await;
    assert_ne!(a1.key, a2.key);

    let (mux_a, mux_b) = mux_pair();
    store_a.replicate(&mux_a);
    store_b.replicate(&mux_b);
    for bee in [&a1, &a2] {
        bee.wakeup.add_stream(&mux_a);
    }
    for bee in [&b1, &b2] {
        bee.wakeup.add_stream(&mux_b);
    }

    let paired = timeout(Duration::from_secs(5), async {
        loop {
            let counts: Vec<usize> = [
                (&a1, &swarm_a),
                (&a2, &swarm_a),
                (&b1, &swarm_b),
                (&b2, &swarm_b),
            ]
            .iter()
            .map(|(bee, swarm)| {
                swarm
                    .get_sessions(&bee.key)
                    .iter()
                    .map(|session| session.peers().len())
                    .sum()
            })
            .collect();
            if counts.iter().all(|count| *count == 1) {
                return counts;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        paired.is_ok(),
        "every bee must pair over the one connection"
    );
}
