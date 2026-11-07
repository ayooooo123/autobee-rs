//! `autobee/lib/trusted.js`: the fast-forward trust cache. A truthy answer is
//! cached forever (`clear` drops it on an undo); concurrent probes of the same
//! key share one handler call.

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

        // the reference shares the in-flight promise; the cell does the same
        let trusted = *cell
            .get_or_init(|| self.hooks.is_trusted(key, reference))
            .await;

        if trusted {
            lock(&self.trusted).insert(*key, true);
        }

        let mut pending = lock(&self.pending);
        if pending
            .get(key)
            .is_some_and(|entry| Arc::ptr_eq(entry, &cell))
        {
            pending.remove(key);
        }

        trusted
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
}
