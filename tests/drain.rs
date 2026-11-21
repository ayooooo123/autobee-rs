//! Drain ownership between callers and the driver task, checked across two
//! peers that replicate over in-process streams.

use std::sync::Arc;
use std::time::Duration;

use autobee::apply_calls::{ApplyCalls, BoxFuture, Result as HandlerResult};
use autobee::apply_view::Handlers;
use autobee::base::Autobee;
use autobee::types::{Key, Node};
use bytes::Bytes;
use corestore::Corestore;
use hyperbee::Bee;
use protomux::{Mux, StreamInfo, memory};

/// Writes every node's value as a view key, after `delay`.
struct Rows {
    delay: Duration,
}

impl Handlers for Rows {
    fn apply<'a, 'b>(
        &'a self,
        nodes: &'a [Node],
        view: &'a Bee,
        _host: &'a ApplyCalls<'a, 'b>,
    ) -> BoxFuture<'a, HandlerResult<()>> {
        Box::pin(async move {
            tokio::time::sleep(self.delay).await;
            let mut batch = view.batch();
            for node in nodes {
                if let Some(value) = &node.value {
                    batch.put(value.clone(), value.clone());
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
    ) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }
}

async fn open(store: &Corestore, key: Option<Key>, delay: Duration) -> Arc<Autobee> {
    Autobee::open(store, key, Arc::new(Rows { delay }) as Arc<dyn Handlers>)
        .await
        .expect("open")
}

/// Replicates both stores over one in-process stream; no wakeup channel.
fn connect(a: &Corestore, b: &Corestore) -> (Mux, Mux) {
    let ((ar, aw), (br, bw)) = memory::duplex(1024);
    let info = |is_initiator| StreamInfo {
        is_initiator,
        handshake_hash: Some([7u8; 64]),
        remote_public_key: None,
    };
    let (mux_a, mux_b) = (Mux::new(ar, aw, info(true)), Mux::new(br, bw, info(false)));
    a.replicate(&mux_a);
    b.replicate(&mux_b);
    (mux_a, mux_b)
}

async fn heads(db: &Autobee) -> (Vec<(Key, u64)>, Option<[u8; 8]>) {
    let system = db.system.lock().await;
    let heads = system
        .heads
        .iter()
        .map(|head| (head.key, head.length))
        .collect();
    (heads, system.hash)
}

/// Updates `follower` until its system heads and hash equal `leader`'s.
async fn converge(leader: &Autobee, follower: &Autobee) {
    let target = heads(leader).await;
    for _ in 0..400 {
        follower.update().await.expect("update");
        if heads(follower).await == target {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("follower never reached the leader's heads");
}

async fn row(db: &Autobee, key: &[u8]) -> bool {
    db.view.bee().get(key).await.expect("view get").is_some()
}

/// Dropping an `append` future while its drain sits in the apply handler
/// must not abort the drain halfway: the next append would flush a system
/// that recorded a node whose apply never finished.
#[tokio::test]
async fn a_cancelled_append_still_converges_with_a_second_peer() {
    let dir = tempfile::tempdir().unwrap();
    let store_a = Corestore::open_with_primary_key(&dir.path().join("a"), [51; 32]).unwrap();
    let store_b = Corestore::open_with_primary_key(&dir.path().join("b"), [52; 32]).unwrap();
    let a = open(&store_a, None, Duration::from_millis(50)).await;

    let cancelled = tokio::time::timeout(
        Duration::from_millis(1),
        a.append(vec![Bytes::from_static(b"one")], false),
    )
    .await;
    assert!(cancelled.is_err(), "the append future is dropped mid-drain");
    a.append(vec![Bytes::from_static(b"two")], false)
        .await
        .expect("second append");
    assert_eq!(a.local.length(), 2);

    let b = open(&store_b, Some(a.key), Duration::ZERO).await;
    let _streams = connect(&store_a, &store_b);
    b.wakeup(a.local_key, a.local.length())
        .await
        .expect("wakeup");
    converge(&a, &b).await;

    assert_eq!(heads(&a).await, heads(&b).await);
    for db in [&a, &b] {
        assert!(row(db, b"one").await, "the cancelled append was applied");
        assert!(row(db, b"two").await);
    }
    a.close().await.unwrap();
    b.close().await.unwrap();
}

/// `_bootOnline`: a fresh follower tracks the bootstrap writer from the
/// start, so a peer that only seeds cores (no wakeup) still gets applied.
#[tokio::test]
async fn a_fresh_follower_applies_the_bootstrap_writer_without_a_wakeup() {
    let dir = tempfile::tempdir().unwrap();
    let store_a = Corestore::open_with_primary_key(&dir.path().join("a"), [53; 32]).unwrap();
    let store_b = Corestore::open_with_primary_key(&dir.path().join("b"), [54; 32]).unwrap();
    let a = open(&store_a, None, Duration::ZERO).await;
    a.append(vec![Bytes::from_static(b"x")], false)
        .await
        .expect("append x");
    a.append(vec![Bytes::from_static(b"y")], false)
        .await
        .expect("append y");

    let b = open(&store_b, Some(a.key), Duration::ZERO).await;
    assert!(
        b.writers.lock().await.get(&a.key).is_some(),
        "the bootstrap writer is tracked at open"
    );
    let _streams = connect(&store_a, &store_b);
    converge(&a, &b).await;

    assert!(row(&b, b"x").await);
    assert!(row(&b, b"y").await);
    a.close().await.unwrap();
    b.close().await.unwrap();
}
