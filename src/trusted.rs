//! `autobee/lib/trusted.js`: the fast-forward trust cache. A truthy answer is
//! cached until the key is forgotten (a writer removed by apply) or `clear`
//! runs (a system undo); concurrent probes of the same key share one handler
//! call, and an answer still in flight when its key is forgotten resolves
//! untrusted for every caller and is not cached.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use hyperbee::Bee;
use tokio::sync::OnceCell;

use crate::apply_view::Handlers;
use crate::types::{Head, Key};

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// The bootstrap facts `trusted.js` reads off `auto` (`system.isGenesis()`,
/// `auto.key`, the legacy `system.indexers`). Passed per call because a Rust
/// `Autobee` cannot hand a `&self` to a struct it owns.
#[derive(Clone, Copy)]
pub struct Genesis<'a> {
    pub is_genesis: bool,
    pub key: &'a Key,
    pub indexers: &'a [Key],
}

pub struct TrustedPeers {
    trusted: Mutex<HashMap<Key, bool>>,
    pending: Mutex<HashMap<Key, Arc<OnceCell<bool>>>>,
    hooks: Arc<dyn Handlers>,
}

impl TrustedPeers {
    pub fn new(hooks: Arc<dyn Handlers>) -> Self {
        Self {
            trusted: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            hooks,
        }
    }

    /// `trusted.isTrusted(key, reference)`.
    pub async fn is_trusted(
        &self,
        key: &Key,
        reference: Option<&Bee>,
        genesis: Genesis<'_>,
    ) -> bool {
        if lock(&self.trusted).contains_key(key) {
            return true;
        }

        // bootstrap
        if genesis.is_genesis && key == genesis.key {
            return true;
        }

        // legacy handling
        if genesis.indexers.iter().any(|indexer| indexer == key) {
            return true;
        }

        let cell = {
            let mut pending = lock(&self.pending);
            pending.entry(*key).or_default().clone()
        };

        // The reference shares the in-flight promise; the cell does the same. The verdict is settled once, inside
        // the cell, so every caller sharing it gets the same answer: it holds only if nothing forgot this key while
        // the hook ran (a writer removed, or an undo), and only then is it cached.
        let settle = cell.clone();
        *cell
            .get_or_init(|| async move {
                let trusted = self.hooks.is_trusted(key, reference).await;
                let mut pending = lock(&self.pending);
                if !pending
                    .get(key)
                    .is_some_and(|entry| Arc::ptr_eq(entry, &settle))
                {
                    return false;
                }
                pending.remove(key);
                if trusted {
                    // under the pending lock, so a `forget` racing this verdict cannot cache it after clearing.
                    lock(&self.trusted).insert(*key, true);
                }
                trusted
            })
            .await
    }

    /// `trusted.read(trusted)`: `trusted || null`.
    pub fn read(&self, trusted: Option<bool>) -> Option<bool> {
        trusted.filter(|trusted| *trusted)
    }

    /// `trusted.mostRecentTrusted(target, reference)`.
    pub async fn most_recent_trusted(&self, target: &Bee, reference: Option<&Bee>) -> Option<Head> {
        if !self.hooks.has_most_recent_trusted() {
            return None;
        }
        self.hooks.most_recent_trusted(target, reference).await
    }

    /// A system undo invalidates every answer: views and grants moved.
    pub fn clear(&self) {
        lock(&self.pending).clear();
        lock(&self.trusted).clear();
    }

    /// `trusted.forget(key)`: a writer removed by apply must be judged again, and any answer still in
    /// flight for it no longer counts.
    pub fn forget(&self, key: &Key) {
        let mut pending = lock(&self.pending);
        pending.remove(key);
        lock(&self.trusted).remove(key);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;
    use crate::apply_calls::{ApplyCalls, BoxFuture, Result};
    use crate::updates::Changes;

    /// A scripted `isTrusted` hook: its verdict, how often it was consulted, and an optional window
    /// that holds every answer until it closes.
    struct Hook {
        verdict: AtomicBool,
        calls: AtomicUsize,
        gate: Mutex<Option<tokio::sync::watch::Receiver<bool>>>,
        release: tokio::sync::watch::Sender<bool>,
    }

    impl Hook {
        fn new(verdict: bool, deferred: bool) -> Arc<Self> {
            let (tx, rx) = tokio::sync::watch::channel(false);
            Arc::new(Self {
                verdict: AtomicBool::new(verdict),
                calls: AtomicUsize::new(0),
                gate: Mutex::new(deferred.then_some(rx)),
                release: tx,
            })
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::Acquire)
        }

        fn set(&self, verdict: bool) {
            self.verdict.store(verdict, Ordering::Release);
        }
    }

    impl Handlers for Hook {
        fn apply<'a, 'b>(
            &'a self,
            _nodes: &'a [crate::types::Node],
            _view: &'a Bee,
            _host: &'a ApplyCalls<'a, 'b>,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }

        fn update<'a>(&'a self, _view: &'a Bee, _changes: &'a Changes) -> BoxFuture<'a, ()> {
            Box::pin(async {})
        }

        fn is_trusted<'a>(
            &'a self,
            _key: &'a Key,
            _reference: Option<&'a Bee>,
        ) -> BoxFuture<'a, bool> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            Box::pin(async move {
                let gate = lock(&self.gate).clone();
                if let Some(mut gate) = gate {
                    while !*gate.borrow_and_update() {
                        if gate.changed().await.is_err() {
                            break;
                        }
                    }
                }
                self.verdict.load(Ordering::Acquire)
            })
        }
    }

    const KEY: Key = [7u8; 32];
    const OTHER: Key = [9u8; 32];

    fn genesis(key: &Key) -> Genesis<'_> {
        Genesis {
            is_genesis: false,
            key,
            indexers: &[],
        }
    }

    /// A writer trusted once and then removed by apply is asked again: the hook decides, not the cache.
    #[tokio::test]
    async fn a_removed_writer_is_asked_again() {
        let hook = Hook::new(true, false);
        let peers = TrustedPeers::new(hook.clone());
        let bootstrap = OTHER;

        assert!(peers.is_trusted(&KEY, None, genesis(&bootstrap)).await);
        assert_eq!(hook.calls(), 1);
        // a truthy answer is cached
        assert!(peers.is_trusted(&KEY, None, genesis(&bootstrap)).await);
        assert_eq!(hook.calls(), 1);

        hook.set(false);
        peers.forget(&KEY);
        assert!(
            !peers.is_trusted(&KEY, None, genesis(&bootstrap)).await,
            "a removed writer is not trusted by its stale answer"
        );
        assert_eq!(hook.calls(), 2, "the hook was consulted again");
    }

    /// An answer still in flight when its key is forgotten resolves untrusted for every caller sharing
    /// it, is not cached, and the next call asks the hook again.
    #[tokio::test]
    async fn an_in_flight_answer_forgotten_mid_hook_is_untrusted() {
        let hook = Hook::new(true, true);
        let peers = Arc::new(TrustedPeers::new(hook.clone()));
        let bootstrap = OTHER;

        let first = {
            let peers = peers.clone();
            tokio::spawn(async move { peers.is_trusted(&KEY, None, genesis(&bootstrap)).await })
        };
        // the hook is running and holds the only cell
        while hook.calls() == 0 {
            tokio::task::yield_now().await;
        }
        let second = {
            let peers = peers.clone();
            tokio::spawn(async move { peers.is_trusted(&KEY, None, genesis(&bootstrap)).await })
        };
        // both callers share the one pending cell: the map, this clone, the first caller's and the clone its
        // running hook settles against, then the second caller's
        let cell = {
            let pending = lock(&peers.pending);
            pending.get(&KEY).expect("pending entry").clone()
        };
        while Arc::strong_count(&cell) < 5 {
            tokio::task::yield_now().await;
        }

        peers.forget(&KEY);
        hook.release.send(true).expect("release");
        assert!(!first.await.expect("first"), "the forgotten caller");
        assert!(!second.await.expect("second"), "the caller sharing it");
        assert_eq!(hook.calls(), 1, "one shared hook call");

        // not cached either: the next call consults the hook again
        assert!(peers.is_trusted(&KEY, None, genesis(&bootstrap)).await);
        assert_eq!(hook.calls(), 2);
        assert!(peers.is_trusted(&KEY, None, genesis(&bootstrap)).await);
        assert_eq!(hook.calls(), 2, "a fresh truthy answer is cached");
    }

    /// Two callers sharing one answer that nothing forgot both get it, from one hook call.
    #[tokio::test]
    async fn callers_sharing_an_answer_get_the_same_verdict() {
        let hook = Hook::new(true, true);
        let peers = Arc::new(TrustedPeers::new(hook.clone()));
        let bootstrap = OTHER;

        let first = {
            let peers = peers.clone();
            tokio::spawn(async move { peers.is_trusted(&KEY, None, genesis(&bootstrap)).await })
        };
        while hook.calls() == 0 {
            tokio::task::yield_now().await;
        }
        let second = {
            let peers = peers.clone();
            tokio::spawn(async move { peers.is_trusted(&KEY, None, genesis(&bootstrap)).await })
        };
        // let the second caller reach the shared answer (this test runs on one thread)
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        hook.release.send(true).expect("release");
        assert!(first.await.expect("first"), "the caller that ran the hook");
        assert!(
            second.await.expect("second"),
            "the caller sharing its answer"
        );
        assert_eq!(hook.calls(), 1, "one shared hook call");
    }
}
