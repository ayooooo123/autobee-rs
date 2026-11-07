//! `autobee/lib/writers.js`: the active-writer map, the pending queue and the
//! per-writer batch reader.
//!
//! The reference keys writers by id string and keeps a swap-removal array of
//! pending writers; this port keys them by the core key, which is the same
//! thing without the hex conversion. `Triggers` is folded into
//! [`Writer::waiting`] plus [`ActiveWriters::notify_batch`]: a writer waits on
//! the first missing link and any batch that advances that link wakes it.

use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use hypercore::core::{Core, CoreEvent, Session};
use tokio::task::JoinHandle;

/// `auto.bumpSoon()`: marks the bee for a drain. Called when a writer's queue
/// advances, from core events and from the wakeup hint sink.
pub type Wake = Arc<dyn Fn() + Send + Sync>;

use crate::types::{Batch, Key, Link, Node, WriterRef};

/// `DEFAULT_MAX_UNTRUSTED_WRITERS`.
pub const DEFAULT_MAX_UNTRUSTED_WRITERS: usize = 128;
const WRITER_PREFETCH: u64 = 10;
const BATCH_WRITER_SANITY: usize = 1024;
const BATCH_OPTIMISTIC_SANITY: u64 = 32;
const MAX_FORWARD_TIME_DRIFT: u64 = 2 * 3600 * 1000;

/// An approval weight is a writer's standing, and promotions keeps one digest
/// slot per weight unit. The protocol caps untrusted writers at
/// [`DEFAULT_MAX_UNTRUSTED_WRITERS`], so no honest grant can approach this:
/// freeze a block above it instead of allocating the digest (JS OOMs here).
const MAX_APPROVAL_WEIGHT: u64 = 1 << 20;

/// The core-coupler side of a writer: announces the core over peers that lack
/// it (JS `writer.couple()` / `decouple()`).
pub trait Coupler: Send + Sync {
    fn add(&self, core: Core) -> bool;
    fn remove(&self, core: &Core) -> bool;
}

/// The pending writer array uses swap-removal, as in `Writer.removePending`.
#[derive(Default)]
pub struct PendingWriters {
    keys: Vec<Key>,
    indexes: HashMap<Key, usize>,
}

impl PendingWriters {
    pub fn insert(&mut self, key: Key) {
        if self.indexes.contains_key(&key) {
            return;
        }
        self.indexes.insert(key, self.keys.len());
        self.keys.push(key);
    }

    pub fn remove(&mut self, key: &Key) {
        let Some(index) = self.indexes.remove(key) else {
            return;
        };
        self.keys.swap_remove(index);
        if let Some(moved) = self.keys.get(index) {
            self.indexes.insert(*moved, index);
        }
    }

    pub fn contains(&self, key: &Key) -> bool {
        self.indexes.contains_key(key)
    }

    pub fn as_slice(&self) -> &[Key] {
        &self.keys
    }
}

/// Active writer sessions and their pending queue.
pub struct ActiveWriters {
    pub local: Key,
    /// `writers.writable`: the local writer is granted and not removed.
    pub writable: bool,
    /// `writable`, mirrored for synchronous readers (JS reads `base.writable` without awaiting).
    writable_flag: Arc<AtomicBool>,
    pub pending: PendingWriters,
    writers: HashMap<Key, Writer>,
    buffered: HashMap<Key, Vec<Node>>,
    wake: Wake,
    coupler: Option<Arc<dyn Coupler>>,
}

impl ActiveWriters {
    pub fn new(local: Session, wake: Wake, coupler: Option<Arc<dyn Coupler>>) -> Self {
        let key = *local.key();
        let mut writers = HashMap::new();
        writers.insert(
            key,
            Writer::new(local, true, Arc::clone(&wake), coupler.clone()),
        );
        let mut pending = PendingWriters::default();
        pending.insert(key);
        Self {
            local: key,
            writable: false,
            writable_flag: Arc::new(AtomicBool::new(false)),
            pending,
            writers,
            buffered: HashMap::new(),
            wake,
            coupler,
        }
    }

    /// Closes every writer, local last, as `close()` collects the promises.
    pub fn close(&mut self) {
        self.pending = PendingWriters::default();
        self.buffered.clear();
        let local = self.writers.remove(&self.local);
        for (_, writer) in self.writers.drain() {
            writer.core.close();
        }
        if let Some(writer) = local {
            writer.core.close();
        }
    }

    pub fn get(&self, key: &Key) -> Option<&Writer> {
        self.writers.get(key)
    }

    pub fn get_mut(&mut self, key: &Key) -> Option<&mut Writer> {
        self.writers.get_mut(key)
    }

    pub fn keys(&self) -> Vec<Key> {
        self.writers.keys().copied().collect()
    }

    pub fn pending_nodes(&self) -> Vec<Node> {
        self.writers
            .get(&self.local)
            .and_then(|writer| writer.pending.as_ref())
            .cloned()
            .unwrap_or_default()
    }

    /// `_getWakeupWriters`: the attached writers that are not pending, as key + head length.
    /// A pending writer still has work queued, so its head is not ours to announce yet; a
    /// drained one (the publisher's own writer after a publish) is what a peer's lookup needs.
    pub fn wakeup_writers(&self) -> Vec<Link> {
        self.writers
            .iter()
            .filter(|(key, writer)| {
                writer.attached
                    && !self.pending.contains(key)
                    && !writer.is_frozen
                    && !writer.core.is_closed()
            })
            .map(|(_, writer)| Link {
                key: *writer.core.key(),
                length: writer.core.length(),
            })
            .collect()
    }

    /// `hasReferrals`: some writer is waiting on a link this key supplies.
    pub fn has_referrals(&self, key: &Key) -> bool {
        self.writers
            .values()
            .any(|writer| writer.waiting.as_ref().is_some_and(|link| &link.key == key))
    }

    /// `writers.writable = localWriter.isAdded && !localWriter.isRemoved`.
    fn sync_writable(&mut self) {
        self.writable = self
            .writers
            .get(&self.local)
            .is_some_and(|writer| writer.is_added && !writer.is_removed);
        self.writable_flag.store(self.writable, Ordering::Release);
    }

    /// The flag a synchronous `Autobee::writable` reads.
    pub fn writable_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.writable_flag)
    }

    pub async fn update_local_state(
        &mut self,
        system: &crate::system::SystemBee,
        bootstrap: &Key,
    ) -> hyperbee::Result<()> {
        if let Some(writer) = self.writers.get_mut(&self.local) {
            writer.update(system, bootstrap).await?;
        }
        self.sync_writable();
        Ok(())
    }

    /// Stamp the sort weight back onto the local pending queue, where the
    /// flushed blocks read the weight hint from (JS mutates the same objects).
    pub fn set_batch_weight(&mut self, batch: &[Node], weight: i64) {
        if let Some(writer) = self.writers.get_mut(&self.local) {
            writer.set_batch_weight(batch, weight);
        }
    }

    /// `None` once `close()` dropped the local writer.
    pub fn append_local(
        &mut self,
        oplog: crate::encoding::OplogMessageV4,
        writable: bool,
    ) -> Option<Node> {
        let node = self.writers.get_mut(&self.local)?.append(oplog, writable);
        self.pending.insert(self.local);
        Some(node)
    }

    pub async fn flush_local(
        &mut self,
        views: crate::types::Views,
        trusted: Option<Link>,
    ) -> hypercore::core::Result<bool> {
        let Some(writer) = self.writers.get_mut(&self.local) else {
            return Err(hypercore::core::CoreError::Closed);
        };
        writer.flush(views, trusted).await
    }

    pub async fn latest_local_oplog(
        &self,
    ) -> hyperbee::Result<Option<crate::encoding::DecodedOplog>> {
        let Some(writer) = self.writers.get(&self.local) else {
            return Err(hyperbee::Error::Message("Autobee closed"));
        };
        let core = &writer.core;
        if core.length() == 0 {
            return Ok(None);
        }
        core.get(core.length() - 1, Default::default())
            .await?
            .map(crate::encoding::decode_oplog)
            .transpose()
            .map_err(Into::into)
    }

    pub fn attach(&mut self, core: Session) -> bool {
        let key = *core.key();
        if let Some(existing) = self.writers.get_mut(&key) {
            if existing.attached {
                return false;
            }
            existing.attached = true;
            self.pending.insert(key);
            return true;
        }
        self.writers.insert(
            key,
            Writer::new(core, false, Arc::clone(&self.wake), self.coupler.clone()),
        );
        self.pending.insert(key);
        true
    }

    pub async fn add(
        &mut self,
        store: &corestore::Corestore,
        bootstrap: &Key,
        key: Key,
        trusted: bool,
        max_untrusted: usize,
    ) -> corestore::Result<bool> {
        // a session that the store's gc collected must be reopened, or the
        // writer would wait on a dead core forever
        if self
            .writers
            .get(&key)
            .is_some_and(|writer| writer.core.is_closed())
        {
            if let Some(writer) = self.writers.remove(&key) {
                writer.core.close();
            }
            self.pending.remove(&key);
        }
        if let Some(writer) = self.writers.get_mut(&key) {
            if !writer.attached {
                writer.attached = true;
                self.pending.insert(key);
            }
            return Ok(true);
        }
        // the local writer is never tracked twice; reattach instead
        if key == self.local {
            if let Some(writer) = self.writers.get_mut(&key) {
                writer.attached = true;
                self.pending.insert(key);
            }
            return Ok(true);
        }
        if !trusted
            && max_untrusted != 0
            && self
                .writers
                .values()
                .filter(|writer| writer.attached)
                .count()
                >= max_untrusted
        {
            return Ok(false);
        }
        let core = store
            .get(corestore::GetOptions {
                key: Some(key),
                inactive: true,
                ..Default::default()
            })
            .await?;
        core.set_user_data("referrer", Some(bootstrap)).await?;
        self.attach(core);
        Ok(true)
    }

    pub async fn wakeup(
        &mut self,
        store: &corestore::Corestore,
        bootstrap: &Key,
        key: Key,
        length: u64,
        trusted: bool,
        max_untrusted: usize,
    ) -> corestore::Result<()> {
        if key == self.local {
            return Ok(());
        }
        let added = !self.writers.contains_key(&key);
        if !self
            .add(store, bootstrap, key, trusted, max_untrusted)
            .await?
        {
            return Ok(());
        }
        let writer = self.writers.get_mut(&key).expect("added writer");
        let advanced = length > writer.hinted_length;
        let attached = writer.attached;
        writer.hint(length);
        if length > 0 {
            // prefetch the oplog head for the fast-forward that runs right after
            let core = writer.core.core().clone();
            tokio::spawn(async move {
                let _ = core.get(length - 1, Default::default()).await;
            });
        }
        if (added || advanced) && attached {
            // A hint past what the system indexed is the only notice that this writer appended again:
            // the indexer only visits queued writers, so an already-attached one that is not queued
            // here is never re-read and its new blocks are never applied (JS re-scans every writer).
            self.pending.insert(key);
        }
        if added || advanced {
            (self.wake)();
        }
        Ok(())
    }

    /// `rotateLocalWriter(core)`: only one writer owns the local key.
    pub async fn rotate_local_writer(
        &mut self,
        core: Session,
        system: &crate::system::SystemBee,
        bootstrap: &Key,
    ) -> hyperbee::Result<()> {
        self.buffered.clear();
        self.pending = PendingWriters::default();
        let key = *core.key();
        if let Some(old) = self.writers.remove(&self.local) {
            old.core.close();
        }
        if let Some(existing) = self.writers.remove(&key) {
            existing.core.close();
        }
        self.local = key;
        self.writers.insert(
            key,
            Writer::new(core, true, Arc::clone(&self.wake), self.coupler.clone()),
        );
        self.pending.insert(key);
        self.update_local_state(system, bootstrap).await
    }

    /// `clearLocal`: drops the local pending queue.
    pub fn clear_local(&mut self) {
        if let Some(writer) = self.writers.get_mut(&self.local) {
            writer.clear();
        }
    }

    /// `removePending` for one writer, including any peeked batch.
    pub fn remove_pending(&mut self, key: &Key) {
        self.pending.remove(key);
        self.buffered.remove(key);
    }

    pub fn detach(&mut self, key: &Key) -> Option<Writer> {
        self.pending.remove(key);
        self.buffered.remove(key);
        if *key == self.local {
            if let Some(local) = self.writers.get_mut(key) {
                local.attached = false;
                local.waiting = None;
            }
            return None;
        }
        self.writers.remove(key)
    }

    pub async fn remove(
        &mut self,
        key: &Key,
        system: &crate::system::SystemBee,
        bootstrap: &Key,
    ) -> hyperbee::Result<()> {
        if *key == self.local {
            self.update_local_state(system, bootstrap).await?;
        }
        let Some(writer) = self.writers.get_mut(key) else {
            return Ok(());
        };
        let length = writer.update(system, bootstrap).await?;
        if *key != self.local && (length < writer.core.length() || length < writer.hinted_length) {
            let key = *key;
            self.pending.insert(key);
            return Ok(());
        }
        if let Some(writer) = self.detach(key) {
            writer.core.close();
        }
        self.sync_writable();
        Ok(())
    }

    pub async fn refresh(
        &mut self,
        system: &crate::system::SystemBee,
        bootstrap: &Key,
    ) -> hyperbee::Result<()> {
        self.buffered.clear();
        let keys: Vec<_> = self.writers.keys().copied().collect();
        for key in keys {
            self.pending.remove(&key);
            let writer = self.writers.get_mut(&key).expect("active writer");
            if writer.core.is_closed() && key != self.local {
                if let Some(writer) = self.detach(&key) {
                    writer.core.close();
                }
                continue;
            }
            if writer.is_frozen && key != self.local {
                if let Some(writer) = self.detach(&key) {
                    writer.core.close();
                }
                continue;
            }
            writer.waiting = None;
            if !writer.attached {
                // A granted writer we are caught up on keeps its session but stops being
                // announced and re-read: it is not the system head any more, and the next block it
                // appends is only noticed through this writer's append/download events
                // (`requeue_changed`). Closing it here parked the bee on a view the peer had
                // already moved past. A hint re-attaches it through `add`.
                continue;
            }
            let length = writer.update(system, bootstrap).await?;
            if length < writer.core.length() || length < writer.hinted_length || writer.appending()
            {
                self.pending.insert(key);
            } else if key != self.local && !writer.is_head {
                if writer.is_added && !writer.is_removed {
                    writer.attached = false;
                } else if let Some(writer) = self.detach(&key) {
                    // a writer the system never added (a stranger's optimistic write) or has
                    // revoked keeps nothing: its blocks can never reach the view
                    writer.core.close();
                }
            }
        }
        self.sync_writable();
        Ok(())
    }

    pub fn bump(&mut self, key: &Key) {
        if let Some(writer) = self.writers.get_mut(key) {
            writer.waiting = None;
            if writer.attached {
                self.pending.insert(*key);
            }
            (self.wake)();
        }
    }

    /// Requeue external writers after an append or download notification. A block landing on a
    /// hibernated writer is news: it re-attaches so the indexer reads it and peers hear its head.
    pub fn requeue_changed(&mut self) {
        for (key, writer) in &mut self.writers {
            if writer.take_changed() && writer.waiting.is_none() {
                writer.attached = true;
                self.pending.insert(*key);
            }
        }
    }

    /// `triggers.trigger(id, length)`: wake every writer waiting on that link.
    pub fn notify_batch(&mut self, key: &Key, length: u64) {
        let ready: Vec<_> = self
            .writers
            .iter()
            .filter_map(|(id, writer)| {
                writer
                    .waiting
                    .as_ref()
                    .filter(|link| &link.key == key && link.length <= length)
                    .map(|_| *id)
            })
            .collect();
        for id in ready {
            self.bump(&id);
        }
    }

    pub async fn next_pending<R: crate::witness::Reader>(
        &mut self,
        system: &crate::system::SystemBee,
        reader: &mut R,
        writable: bool,
        local_only: bool,
        now: u64,
    ) -> Result<PendingNext, SelectionError<R::Error>> {
        self.requeue_changed();
        let keys = self.pending.as_slice().to_vec();
        let bootstrap = *reader.bootstrap_key();
        for key in keys {
            if (local_only && key != self.local) || self.buffered.contains_key(&key) {
                continue;
            }
            let Some(writer) = self.writers.get_mut(&key) else {
                continue;
            };
            if writer.is_frozen {
                continue;
            }
            let writable_local = writable && key == self.local;
            match writer
                .next(system, &bootstrap, writable_local, now)
                .await
                .map_err(SelectionError::System)?
            {
                Next::Batch(batch) => {
                    if key == self.local
                        && writer
                            .pending
                            .as_ref()
                            .is_some_and(|nodes| writer.processed == nodes.len())
                    {
                        self.pending.remove(&key);
                    }
                    self.buffered.insert(key, batch);
                    let links = std::mem::take(&mut writer.referrals);
                    if !links.is_empty() {
                        return Ok(PendingNext::Missing { writer: key, links });
                    }
                }
                Next::Missing(links) => {
                    if key != self.local {
                        self.pending.remove(&key);
                    }
                    return Ok(PendingNext::Missing { writer: key, links });
                }
                Next::Unavailable => self.pending.remove(&key),
                Next::Idle => {}
            }
        }
        self.sync_writable();
        let mut best = None;
        for (key, batch) in &mut self.buffered {
            if local_only && *key != self.local {
                continue;
            }
            let head = &mut batch[0];
            if head.weight < 0 {
                head.weight = crate::witness::resolve_weight(reader, head)
                    .await
                    .map_err(SelectionError::Witness)?;
            }
            let order = crate::types::WeightedLink::from(&*head);
            if best
                .as_ref()
                .is_none_or(|(_, previous)| crate::types::cmp_node(&order, previous).is_lt())
            {
                best = Some((*key, order));
            }
        }
        Ok(match best {
            Some((writer, _)) => PendingNext::Batch {
                writer,
                batch: self.buffered.remove(&writer).expect("selected batch"),
            },
            None => PendingNext::Idle,
        })
    }
}

pub enum PendingNext {
    Batch { writer: Key, batch: Vec<Node> },
    Missing { writer: Key, links: Vec<Link> },
    Idle,
}

#[derive(Debug)]
pub enum SelectionError<E> {
    System(hyperbee::Error),
    Witness(E),
}

/// Result of reading a writer: a complete batch, missing causal links, or no work.
pub enum Next {
    Batch(Vec<Node>),
    Missing(Vec<Link>),
    Unavailable,
    Idle,
}

/// One writer's session and download wakeup subscription.
pub struct Writer {
    pub core: Session,
    pub hinted_length: u64,
    pub is_frozen: bool,
    pub is_added: bool,
    pub is_removed: bool,
    pub is_head: bool,
    pub weight: u64,
    pub is_coupled: bool,
    pending: Option<Vec<Node>>,
    processed: usize,
    append_length: u64,
    pub attached: bool,
    referrals: Vec<Link>,
    pub waiting: Option<Link>,
    changed: Arc<AtomicBool>,
    events: Option<JoinHandle<()>>,
    download: Option<JoinHandle<()>>,
    coupler: Option<Arc<dyn Coupler>>,
}

impl Writer {
    pub fn new(core: Session, local: bool, wake: Wake, coupler: Option<Arc<dyn Coupler>>) -> Self {
        let changed = Arc::new(AtomicBool::new(false));
        let events = if local {
            None
        } else {
            let mut events = core.events();
            let changed = Arc::clone(&changed);
            let wake = Arc::clone(&wake);
            Some(tokio::spawn(async move {
                loop {
                    match events.recv().await {
                        Ok(CoreEvent::Append { .. } | CoreEvent::Download { .. })
                        | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            changed.store(true, Ordering::Release);
                            wake();
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        Ok(_) => {}
                    }
                }
            }))
        };
        let append_length = core.length() + 1;
        Self {
            core,
            hinted_length: 0,
            is_frozen: false,
            waiting: None,
            attached: true,
            is_added: false,
            is_removed: false,
            is_head: false,
            weight: 0,
            is_coupled: false,
            pending: None,
            processed: 0,
            append_length,
            referrals: Vec::new(),
            changed,
            events,
            download: None,
            coupler,
        }
    }

    /// `writer.isIndexer`.
    pub fn is_indexer(&self) -> bool {
        self.weight >= 2
    }

    pub fn hint(&mut self, length: u64) {
        if length > self.hinted_length {
            self.hinted_length = length;
            self.core.set_active(true);
        }
    }

    /// Consume an append/download hint; lagged event streams also recheck the core.
    pub fn take_changed(&mut self) -> bool {
        if !self.changed.swap(false, Ordering::AcqRel) {
            return false;
        }
        self.hint(self.core.length());
        true
    }

    /// `writer.couple()`.
    fn couple(&mut self) {
        if self.is_coupled {
            return;
        }
        if let Some(coupler) = &self.coupler
            && coupler.add(self.core.core().clone())
        {
            self.is_coupled = true;
        }
    }

    /// `writer.decouple()`.
    fn decouple(&mut self) {
        if !self.is_coupled {
            return;
        }
        if let Some(coupler) = &self.coupler
            && coupler.remove(self.core.core())
        {
            self.is_coupled = false;
        }
    }

    /// Read an available oplog block; malformed or failed reads freeze the writer.
    pub fn read_node(&mut self, index: u64, weight: i64) -> Option<Node> {
        if self.is_frozen || !self.core.has(index) {
            return None;
        }
        let oplog = match self.core.get_local(index) {
            Ok(Some(block)) => crate::encoding::decode_oplog(block),
            Ok(None) => return None,
            Err(_) => {
                self.is_frozen = true;
                return None;
            }
        };
        match oplog {
            Ok(oplog) => {
                // a weight above the bound is malformed: freezing skips the
                // block without touching system bytes or the promotions digest
                if approvals_over_bound(&oplog) {
                    self.is_frozen = true;
                    return None;
                }
                Some(Node::from_oplog(
                    *self.core.key(),
                    index + 1,
                    weight,
                    &oplog,
                    Some(WriterRef(self.core.core().clone())),
                ))
            }
            Err(_) => {
                self.is_frozen = true;
                None
            }
        }
    }

    /// Queue a local oplog record without writing it until its view is flushed.
    pub fn append(&mut self, mut oplog: crate::encoding::OplogMessageV4, writable: bool) -> Node {
        oplog.views = None;
        oplog.trusted = None;
        oplog.optimistic &= !writable;
        oplog.weight_hint = None;
        let node = Node::from_oplog(
            *self.core.key(),
            self.append_length,
            -1,
            &crate::encoding::DecodedOplog::V4(oplog),
            Some(WriterRef(self.core.core().clone())),
        );
        self.append_length += 1;
        self.pending.get_or_insert_with(Vec::new).push(node.clone());
        node
    }

    pub fn appending(&self) -> bool {
        self.pending.is_some()
    }

    pub fn pending_node(&self, length: u64) -> Option<Node> {
        self.pending
            .as_ref()?
            .iter()
            .find(|node| node.length == length)
            .cloned()
    }

    pub fn set_batch_weight(&mut self, batch: &[Node], weight: i64) {
        if let Some(pending) = &mut self.pending {
            for node in pending {
                if batch
                    .iter()
                    .any(|applied| applied.key == node.key && applied.length == node.length)
                {
                    node.weight = weight;
                }
            }
        }
    }

    pub fn clear(&mut self) {
        if let Some(pending) = &mut self.pending {
            pending.drain(..self.processed);
            if pending.is_empty() {
                self.pending = None;
            }
        }
        self.processed = 0;
    }

    /// Persist processed local nodes, recording the resulting view on the last one.
    pub async fn flush(
        &mut self,
        views: crate::types::Views,
        trusted: Option<Link>,
    ) -> hypercore::core::Result<bool> {
        if self.processed == 0 {
            return Ok(false);
        }
        let pending = self
            .pending
            .as_mut()
            .expect("processed nodes belong to the pending queue");
        let head = &mut pending[self.processed - 1];
        head.views = Some(views);
        head.trusted = trusted
            .into_iter()
            .map(|link| crate::types::TrustedHead {
                key: link.key,
                length: link.length,
                flushes: 0,
            })
            .collect();
        let head_index = self.processed - 1;
        let blocks: Vec<_> = pending[..self.processed]
            .iter()
            .enumerate()
            .map(|(index, node)| {
                let mut oplog = crate::encoding::Oplog::V4(crate::encoding::OplogMessageV4 {
                    timestamp: node.timestamp,
                    links: node.links.clone(),
                    batch: Some(node.batch.clone()),
                    views: node.views.clone(),
                    // `createNode` defaults `trusted` to `[]`, so only the
                    // flushed head (which `flush` sets) encodes null when empty
                    trusted: if node.trusted.is_empty() {
                        (index != head_index).then(Vec::new)
                    } else {
                        Some(
                            node.trusted
                                .iter()
                                .map(|head| Link {
                                    key: head.key,
                                    length: head.length,
                                })
                                .collect(),
                        )
                    },
                    witness: node.witness.clone(),
                    approvals: node.approvals.clone(),
                    optimistic: node.optimistic,
                    value: node.value.clone(),
                    hash: node.hash,
                    weight_hint: (node.weight > 0).then_some(node.weight as u64),
                    drift: (node.drift > 0).then_some(node.drift),
                });
                crate::encoding::encode_oplog(&mut oplog)
            })
            .collect();
        let result = self.core.append(&blocks).await;
        self.clear();
        result.map(|_| true)
    }

    /// Refresh membership and session activity from the current system tree.
    pub async fn update(
        &mut self,
        system: &crate::system::SystemBee,
        bootstrap: &Key,
    ) -> hyperbee::Result<u64> {
        let info = system.get(self.core.key(), false).await?;
        let length = info.as_ref().map_or(0, |info| info.length);
        self.is_head = self.core.length() > 0
            && if system.heads.is_empty() {
                self.core.key() == bootstrap
            } else {
                system.is_head(self.core.key())
            };
        if self.is_head {
            self.couple();
        } else {
            self.decouple();
        }
        self.core
            .set_active(self.is_head || length < self.core.length() || length < self.hinted_length);
        if let Some(info) = info {
            self.is_added = true;
            self.is_removed = info.is_removed;
            self.weight = info.max_weight;
        } else {
            self.is_added = self.core.key() == bootstrap;
        }
        Ok(length)
    }

    /// Read the next complete on-disk batch and report missing causal links.
    pub async fn next(
        &mut self,
        system: &crate::system::SystemBee,
        bootstrap: &Key,
        writable_local: bool,
        now: u64,
    ) -> hyperbee::Result<Next> {
        if self.is_frozen || self.waiting.is_some() {
            return Ok(Next::Idle);
        }
        let mut length = self.update(system, bootstrap).await?;
        if std::env::var_os("AUTOBEE_DEBUG").is_some() {
            eprintln!(
                "  [w {:?}] sys={length} core={} hint={} added={} head={}",
                &self.core.key()[..3],
                self.core.length(),
                self.hinted_length,
                self.is_added,
                self.is_head
            );
        }
        if let Some(download) = self.download.take() {
            download.abort();
        }
        let download =
            self.core
                .download(length, Some(length.saturating_add(WRITER_PREFETCH)), false);
        self.download = Some(tokio::spawn(async move {
            let _ = download.await;
        }));
        let mut batch = Vec::new();
        let mut timestamp = 0;
        let mut optimistic = false;
        while length < self.core.length() {
            let Some(mut node) = self.read_node(length, -1) else {
                return Ok(Next::Unavailable);
            };
            length += 1;
            if writable_local {
                node.optimistic = false;
            }
            timestamp = timestamp.max(node.timestamp);
            node.timestamp = timestamp;
            optimistic |= node.optimistic;
            node.optimistic = optimistic;
            if (optimistic && !self.is_added && node.batch.end >= BATCH_OPTIMISTIC_SANITY)
                || node.timestamp.saturating_sub(now) > MAX_FORWARD_TIME_DRIFT
            {
                return Ok(Next::Idle);
            }
            if !optimistic
                && node.batch.start == 0
                && node.batch.end > 1
                && node.batch.end < BATCH_WRITER_SANITY as u64
            {
                let download =
                    self.core
                        .download(length, Some(length.saturating_add(node.batch.end)), false);
                tokio::spawn(async move {
                    let _ = download.await;
                });
            }
            let mut links = node.links.clone();
            if let Some(witness) = &node.witness {
                links.push(witness.link.clone());
            }
            let mut missing = Vec::new();
            for link in links {
                if link.key == node.key && link.length < node.length {
                    continue;
                }
                let has = system.has(&link).await?;
                if !has {
                    missing.push(link);
                }
            }
            if !missing.is_empty() {
                self.waiting = missing.first().cloned();
                return Ok(Next::Missing(missing));
            }
            if (optimistic && system.is_genesis())
                || (!self.is_added && !optimistic && !system.is_genesis())
            {
                return Ok(Next::Idle);
            }
            let end = node.batch.end;
            batch.push(node);
            if end == 0 {
                let mut batch: Batch = batch.into_iter().map(Some).collect();
                finalize_node_batch(&mut batch);
                return Ok(Next::Batch(batch.into_iter().flatten().collect()));
            }
            if batch.len() >= BATCH_WRITER_SANITY {
                return Ok(Next::Idle);
            }
        }
        if !self.core.writable() || self.pending.is_none() {
            return Ok(Next::Idle);
        }
        let pending = self.pending.as_ref().expect("pending queue exists");
        let mut start = self.processed;
        while start < pending.len() && pending[start].length <= length {
            start += 1;
        }
        self.processed = start;
        if start < pending.len() && !writable_local && !pending[start].optimistic {
            self.clear();
            return Err(hyperbee::Error::Message("Not writable"));
        }
        let end = pending.len();
        let mut missing = Vec::new();
        for i in start..end {
            let node = &self.pending.as_ref().expect("pending queue exists")[i];
            if (node.optimistic && system.is_genesis())
                || (node.optimistic && !self.is_added && end as u64 >= BATCH_OPTIMISTIC_SANITY)
                || node.timestamp.saturating_sub(now) > MAX_FORWARD_TIME_DRIFT
            {
                break;
            }
            let mut links = node.links.clone();
            if let Some(witness) = &node.witness {
                links.push(witness.link.clone());
            }
            for link in links {
                if link.key == node.key && link.length < node.length {
                    continue;
                }
                if !system.has(&link).await? {
                    missing.push(link);
                }
            }
            if !missing.is_empty() {
                self.waiting = missing.first().cloned();
                self.referrals = missing.clone();
                break;
            }
            self.processed = i + 1;
            if node.batch.end == 0 {
                break;
            }
        }
        if start < self.processed {
            return Ok(Next::Batch(
                self.pending.as_ref().expect("pending queue exists")[start..self.processed]
                    .to_vec(),
            ));
        }
        if !missing.is_empty() {
            return Ok(Next::Missing(std::mem::take(&mut self.referrals)));
        }
        Ok(Next::Idle)
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        // a detached writer must stop announcing its dead core
        self.decouple();
        if let Some(events) = self.events.take() {
            events.abort();
        }
        if let Some(download) = self.download.take() {
            download.abort();
        }
    }
}

fn approvals_over_bound(oplog: &crate::encoding::DecodedOplog) -> bool {
    let approvals = match oplog {
        crate::encoding::DecodedOplog::Mapped(m) => m.approvals.as_ref(),
        crate::encoding::DecodedOplog::V4(m) => m.approvals.as_ref(),
    };
    approvals.is_some_and(|approvals| {
        approvals
            .iter()
            .any(|approval| approval.weight > MAX_APPROVAL_WEIGHT)
    })
}

/// Normalize legacy batch coordinates while preserving unavailable blocks.
pub fn finalize_node_batch(batch: &mut Batch) {
    if batch
        .iter()
        .flatten()
        .next()
        .is_none_or(|node| node.version > crate::encoding::LEGACY_OPLOG_VERSION)
    {
        return;
    }
    for (index, node) in batch.iter_mut().enumerate() {
        if let Some(node) = node {
            node.batch.start = index as u64;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BatchCoords, Node};

    fn node(version: u64, start: u64) -> Node {
        Node {
            core: None,
            key: [0; 32],
            length: 1,
            weight: 0,
            version,
            timestamp: 0,
            links: Vec::new(),
            batch: BatchCoords { start, end: 0 },
            views: None,
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

    /// A hint past the system's length must re-queue the writer: the indexer only visits queued
    /// writers, so a second append from an already-attached peer would otherwise never be read.
    #[tokio::test]
    async fn an_advanced_hint_requeues_an_attached_writer() {
        let dir = tempfile::tempdir().unwrap();
        let store = corestore::Corestore::open_with_primary_key(dir.path(), [6; 32]).unwrap();
        let local = store
            .get(corestore::GetOptions::name("local"))
            .await
            .unwrap();
        let remote = store
            .get(corestore::GetOptions::name("remote"))
            .await
            .unwrap();
        let remote_key = *remote.key();
        let mut writers = ActiveWriters::new(local, Arc::new(|| {}), None);
        let bootstrap = [6; 32];
        assert!(
            writers
                .add(&store, &bootstrap, remote_key, true, 0)
                .await
                .unwrap()
        );
        // The queue drains once the writer is read and its block is indexed.
        writers.pending.remove(&remote_key);
        assert!(!writers.pending.as_slice().contains(&remote_key));

        writers
            .wakeup(&store, &bootstrap, remote_key, 2, true, 0)
            .await
            .unwrap();
        assert!(
            writers.pending.as_slice().contains(&remote_key),
            "an advanced hint re-queues the writer"
        );
        assert_eq!(writers.get(&remote_key).unwrap().hinted_length, 2);
    }

    /// JS `_getWakeupWriters` announces the attached writers that are *not* pending: a drained
    /// writer's head is what a peer's lookup needs. Announcing the pending ones instead left a
    /// Rust publisher answering a lookup with nothing, and a Rust follower never started.
    #[tokio::test]
    async fn wakeup_writers_are_the_drained_attached_ones() {
        let dir = tempfile::tempdir().unwrap();
        let store = corestore::Corestore::open_with_primary_key(dir.path(), [7; 32]).unwrap();
        let local = store
            .get(corestore::GetOptions::name("local"))
            .await
            .unwrap();
        let remote = store
            .get(corestore::GetOptions::name("remote"))
            .await
            .unwrap();
        let remote_key = *remote.key();
        let mut writers = ActiveWriters::new(local, Arc::new(|| {}), None);
        let bootstrap = [7; 32];
        assert!(
            writers
                .add(&store, &bootstrap, remote_key, true, 0)
                .await
                .unwrap()
        );
        assert!(
            writers
                .wakeup_writers()
                .iter()
                .all(|link| link.key != remote_key),
            "a queued writer is not announced yet"
        );
        writers.pending.remove(&remote_key);
        let announced = writers.wakeup_writers();
        assert!(
            announced.iter().any(|link| link.key == remote_key),
            "a drained writer is announced: {announced:?}"
        );
    }

    /// A granted writer that stops being the system head keeps its session: the next block it
    /// appends is only noticed through this writer's append/download events, so a `refresh` that
    /// closes it leaves the bee parked on a view the peer has already moved past. A writer the
    /// system never added (a stranger's optimistic write) still drops out.
    #[tokio::test]
    async fn refresh_keeps_a_granted_writer_that_stopped_being_head() {
        let dir = tempfile::tempdir().unwrap();
        let store = corestore::Corestore::open_with_primary_key(dir.path(), [12; 32]).unwrap();
        let local = store
            .get(corestore::GetOptions::name("local"))
            .await
            .unwrap();
        let remote = store
            .get(corestore::GetOptions::name("remote"))
            .await
            .unwrap();
        let stranger = store
            .get(corestore::GetOptions::name("stranger"))
            .await
            .unwrap();
        let local_key = *local.key();
        let remote_key = *remote.key();
        let stranger_key = *stranger.key();
        remote.append(&[b"first"]).await.unwrap();

        // The system knows the remote as a granted writer at its head length, while another
        // writer owns the head: `refresh` used to detach a writer exactly in this state.
        let mut system =
            crate::system::SystemBee::new(hyperbee::Bee::open(&store, None).await.unwrap());
        let view = system.bee.head().unwrap();
        system
            .add_writer(
                remote_key,
                crate::system::AddWriterOptions {
                    length: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let mut head = node(4, 0);
        head.key = local_key;
        system.add_node(&head).unwrap();
        system
            .flush(std::slice::from_ref(&head), view)
            .await
            .unwrap();

        let mut writers = ActiveWriters::new(local, Arc::new(|| {}), None);
        assert!(
            writers
                .add(&store, &local_key, remote_key, true, 0)
                .await
                .unwrap()
        );
        assert!(
            writers
                .add(&store, &local_key, stranger_key, true, 0)
                .await
                .unwrap()
        );
        writers.pending.remove(&remote_key);
        writers.pending.remove(&stranger_key);
        writers.refresh(&system, &local_key).await.unwrap();

        let kept = writers
            .get(&remote_key)
            .expect("a granted writer stays tracked");
        assert!(!kept.core.is_closed(), "its session stays open");
        assert!(!kept.attached, "a caught-up non-head writer hibernates");
        assert!(
            writers
                .wakeup_writers()
                .iter()
                .all(|link| link.key != remote_key),
            "a hibernated writer is not announced"
        );
        assert!(
            writers.get(&stranger_key).is_none(),
            "a writer the system never added is dropped"
        );

        let writer = writers.get_mut(&remote_key).unwrap();
        writer.core.append(&[b"second"]).await.unwrap();
        let mut requeued = false;
        for _ in 0..200 {
            writers.requeue_changed();
            if writers.pending.as_slice().contains(&remote_key) {
                requeued = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(
            requeued,
            "a later block on the granted writer re-queues it for the indexer"
        );
        assert!(
            writers.get(&remote_key).unwrap().attached,
            "news re-attaches the writer so peers hear its head"
        );
    }

    #[tokio::test]
    async fn writer_rechecks_on_append_and_freezes_on_bad_oplog() {
        let dir = tempfile::tempdir().unwrap();
        let store = corestore::Corestore::open_with_primary_key(dir.path(), [8; 32]).unwrap();
        let core = store
            .get(corestore::GetOptions {
                name: Some(b"writer".to_vec()),
                ..Default::default()
            })
            .await
            .unwrap();
        let bumped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let wake: Wake = {
            let bumped = Arc::clone(&bumped);
            Arc::new(move || {
                bumped.fetch_add(1, Ordering::AcqRel);
            })
        };
        let mut writer = Writer::new(core, false, wake, None);
        writer.core.append(&[b"invalid"]).await.unwrap();
        for _ in 0..200 {
            if bumped.load(Ordering::Acquire) > 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(bumped.load(Ordering::Acquire) > 0, "append wakes the bee");
        assert!(writer.take_changed());
        assert_eq!(writer.hinted_length, 1);
        assert!(writer.read_node(0, 0).is_none());
        assert!(writer.is_frozen);
    }

    #[tokio::test]
    async fn local_batch_is_buffered_until_view_flush() {
        let dir = tempfile::tempdir().unwrap();
        let store = corestore::Corestore::open_with_primary_key(dir.path(), [9; 32]).unwrap();
        let core = store
            .get(corestore::GetOptions::name("writer"))
            .await
            .unwrap();
        let key = *core.key();
        let system =
            crate::system::SystemBee::new(hyperbee::Bee::open(&store, None).await.unwrap());
        let mut writer = Writer::new(core, true, Arc::new(|| {}), None);
        let oplog = crate::encoding::OplogMessageV4 {
            timestamp: 1,
            links: vec![],
            batch: None,
            views: None,
            trusted: None,
            witness: None,
            approvals: None,
            optimistic: false,
            value: Some(bytes::Bytes::from_static(b"value")),
            hash: None,
            weight_hint: None,
            drift: None,
        };
        writer.append(oplog, true);
        assert_eq!(writer.core.length(), 0);
        let Next::Batch(batch) = writer.next(&system, &key, true, 1).await.unwrap() else {
            panic!("expected local batch");
        };
        writer.set_batch_weight(&batch, 2);
        let views = crate::types::Views {
            flushes: 1,
            system: crate::encoding::ExternalBatch {
                key,
                start: 0,
                length: 1,
            },
            view: None,
        };
        assert!(writer.flush(views, None).await.unwrap());
        let node = writer.read_node(0, 0).unwrap();
        assert_eq!(node.value.unwrap(), &b"value"[..]);
        assert_eq!(node.weight_hint, 2);
        assert!(!writer.appending());
    }

    #[tokio::test]
    async fn linked_writer_wakes_only_after_requested_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let store = corestore::Corestore::open_with_primary_key(dir.path(), [10; 32]).unwrap();
        let core = store
            .get(corestore::GetOptions::name("writer"))
            .await
            .unwrap();
        let local = *core.key();
        let mut writers = ActiveWriters::new(core, Arc::new(|| {}), None);
        writers.pending.remove(&local);
        writers.get_mut(&local).unwrap().waiting = Some(Link {
            key: [1; 32],
            length: 3,
        });
        writers.notify_batch(&[1; 32], 2);
        assert!(writers.pending.as_slice().is_empty());
        writers.notify_batch(&[1; 32], 3);
        assert_eq!(writers.pending.as_slice(), &[local]);
        assert!(writers.get(&local).unwrap().waiting.is_none());
    }

    #[tokio::test]
    async fn pending_selection_resolves_weight_before_timestamp() {
        struct Reader(Key, Key);
        impl crate::witness::Reader for Reader {
            type Error = ();
            fn bootstrap_key(&self) -> &Key {
                &self.0
            }
            fn bootstrap_weight(&self) -> i64 {
                2
            }
            async fn standing(
                &mut self,
                key: &Key,
            ) -> Result<Option<crate::witness::Standing>, ()> {
                Ok((*key == self.1).then_some(crate::witness::Standing {
                    weight: 3,
                    max_weight: 3,
                    is_genesis: true,
                }))
            }
            async fn oplog_node(&mut self, _: &Link) -> Result<Node, ()> {
                unreachable!()
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let store = corestore::Corestore::open_with_primary_key(dir.path(), [11; 32]).unwrap();
        let local = store
            .get(corestore::GetOptions::name("local"))
            .await
            .unwrap();
        let other = store
            .get(corestore::GetOptions::name("other"))
            .await
            .unwrap();
        let local_key = *local.key();
        let other_key = *other.key();
        let system =
            crate::system::SystemBee::new(hyperbee::Bee::open(&store, None).await.unwrap());
        let make = |timestamp| crate::encoding::OplogMessageV4 {
            timestamp,
            links: vec![],
            batch: None,
            views: None,
            trusted: None,
            witness: None,
            approvals: None,
            optimistic: false,
            value: None,
            hash: None,
            weight_hint: None,
            drift: None,
        };
        let mut encoded = crate::encoding::Oplog::V4(make(2));
        other
            .append(&[crate::encoding::encode_oplog(&mut encoded)])
            .await
            .unwrap();
        let mut writers = ActiveWriters::new(local, Arc::new(|| {}), None);
        writers.get_mut(&local_key).unwrap().append(make(1), true);
        writers.attach(other);
        let mut reader = Reader(local_key, other_key);
        let result = writers
            .next_pending(&system, &mut reader, true, false, 2)
            .await
            .unwrap();
        assert!(matches!(result, PendingNext::Batch { writer, .. } if writer == other_key));
    }

    #[test]
    fn pending_queue_deduplicates_and_swap_removes() {
        let mut pending = PendingWriters::default();
        let (a, b, c) = ([1; 32], [2; 32], [3; 32]);
        pending.insert(a);
        pending.insert(b);
        pending.insert(c);
        pending.insert(a);
        pending.remove(&b);
        assert_eq!(pending.as_slice(), &[a, c]);
        pending.remove(&c);
        pending.remove(&c);
        assert_eq!(pending.as_slice(), &[a]);
        pending.insert(b);
        assert_eq!(pending.as_slice(), &[a, b]);
    }

    #[test]
    fn legacy_batches_keep_holes_and_normalize_coordinates() {
        let mut batch = vec![None, Some(node(2, 7)), None, Some(node(2, 8))];
        finalize_node_batch(&mut batch);
        assert_eq!(batch[1].as_ref().unwrap().batch.start, 1);
        assert_eq!(batch[3].as_ref().unwrap().batch.start, 3);
        let mut modern = vec![None, Some(node(4, 7))];
        finalize_node_batch(&mut modern);
        assert_eq!(modern[1].as_ref().unwrap().batch.start, 7);
        finalize_node_batch(&mut vec![None]);
    }

    async fn test_core(byte: u8, name: &str) -> (tempfile::TempDir, corestore::Corestore, Session) {
        let dir = tempfile::tempdir().unwrap();
        let store = corestore::Corestore::open_with_primary_key(dir.path(), [byte; 32]).unwrap();
        let core = store.get(corestore::GetOptions::name(name)).await.unwrap();
        (dir, store, core)
    }

    fn oplog(approvals: Option<Vec<crate::encoding::Approval>>) -> crate::encoding::OplogMessageV4 {
        crate::encoding::OplogMessageV4 {
            timestamp: 1,
            links: vec![],
            batch: None,
            views: None,
            trusted: None,
            witness: None,
            approvals,
            optimistic: false,
            value: Some(bytes::Bytes::from_static(b"value")),
            hash: None,
            weight_hint: None,
            drift: None,
        }
    }

    #[tokio::test]
    async fn oversized_approval_weight_freezes_the_writer() {
        let (_dir, _store, core) = test_core(12, "remote").await;
        let mut encoded =
            crate::encoding::Oplog::V4(oplog(Some(vec![crate::encoding::Approval {
                key: [1; 32],
                // one byte over the bound: the promotions digest would resize by
                // this many slots before any honest grant could reach it
                weight: (1 << 20) + 1,
            }])));
        core.append(&[crate::encoding::encode_oplog(&mut encoded)])
            .await
            .unwrap();
        let mut writer = Writer::new(core, false, Arc::new(|| {}), None);
        assert!(writer.read_node(0, 0).is_none());
        assert!(writer.is_frozen, "the malformed block freezes the writer");

        // the same block within the bound still reads
        let (_dir, _store, core) = test_core(13, "remote").await;
        let mut encoded =
            crate::encoding::Oplog::V4(oplog(Some(vec![crate::encoding::Approval {
                key: [1; 32],
                weight: 1 << 20,
            }])));
        core.append(&[crate::encoding::encode_oplog(&mut encoded)])
            .await
            .unwrap();
        let mut writer = Writer::new(core, false, Arc::new(|| {}), None);
        assert!(writer.read_node(0, 0).is_some());
        assert!(!writer.is_frozen);
    }

    #[tokio::test]
    async fn closed_writers_error_instead_of_panicking() {
        let (_dir, _store, core) = test_core(14, "local").await;
        let mut writers = ActiveWriters::new(core, Arc::new(|| {}), None);
        writers.close();
        assert!(writers.append_local(oplog(None), true).is_none());
        let views = crate::types::Views {
            flushes: 0,
            system: crate::encoding::ExternalBatch {
                key: [0; 32],
                start: 0,
                length: 0,
            },
            view: None,
        };
        assert!(writers.flush_local(views, None).await.is_err());
        assert!(writers.latest_local_oplog().await.is_err());
    }

    struct CountingCoupler(std::sync::atomic::AtomicUsize);

    impl Coupler for CountingCoupler {
        fn add(&self, _core: Core) -> bool {
            true
        }
        fn remove(&self, _core: &Core) -> bool {
            self.0.fetch_add(1, Ordering::AcqRel);
            true
        }
    }

    #[tokio::test]
    async fn dropping_a_writer_decouples_its_core() {
        let (_dir, _store, core) = test_core(15, "head").await;
        let coupler = Arc::new(CountingCoupler(std::sync::atomic::AtomicUsize::new(0)));
        let mut writer = Writer::new(core, false, Arc::new(|| {}), Some(coupler.clone()));
        writer.couple();
        assert!(writer.is_coupled);
        drop(writer);
        assert_eq!(coupler.0.load(Ordering::Acquire), 1, "drop decouples");
    }
}
