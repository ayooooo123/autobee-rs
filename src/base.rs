//! `autobee/index.js`: the session that owns the local system, the view, the
//! active writers and the drain loop.
//!
//! One `Autobee` is one writer's materialised state: its own `system` and
//! `view` cores under `autobee/<local>/view/...`, its local oplog, and the
//! remote writers it follows. Every public mutation (`append`, `wakeup`,
//! `update`) bumps a single counter and waits for the drain that consumes it;
//! a background driver runs the same drain for hints that arrive on their own.
//!
//! Cuts, all unreachable for PearTube and refused rather than half-ported:
//! legacy autobase migration (`SystemBee::reset` fails on old storage), the
//! reindex/compact path (`_isReindexing` is false because every core this crate
//! opens uses manifest version 3), and `setLocal` rotation. `moveTo`/`replay`
//! are not part of the writable path.

use std::sync::{
    Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard, Weak,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use corestore::{Corestore, GetOptions};
use hyperbee::{Bee, Head as BeeHead};
use hypercore::core::{Core, Session};
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;

use crate::apply_calls::{AddWriterOptions, ApplyCalls};
use crate::apply_view::{ApplyView, Handlers, WakeupCapability};
use crate::fast_forward::{FastForward, FromOptions, Host, Landing};
use crate::system::SystemBee;
use crate::trusted::{Genesis, TrustedPeers};
use crate::types::{Key, Link, Node, Views};
use crate::updates::UpdateChanges;
use crate::wakeup::{AutobeeWakeup, UNKNOWN_LENGTH, WakeupBase};
use crate::writers::{ActiveWriters, Coupler, DEFAULT_MAX_UNTRUSTED_WRITERS, PendingNext};

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// `DEFAULT_ACK_THRESHOLD`.
const DEFAULT_ACK_THRESHOLD: u64 = 32;

/// `stats`: the counters the reference exposes for tests and logs.
#[derive(Debug, Default)]
pub struct Stats {
    pub undos: AtomicU64,
    pub fast_forwards: AtomicU64,
    pub drains: AtomicU64,
    pub applies: AtomicU64,
    pub appends: AtomicU64,
}

/// A hook called after every system flush, for trace tests that compare each
/// flush's system and view core contents. Cheap and inert when unset.
pub trait FlushObserver: Send + Sync {
    /// `system_length`/`view_length` are the local system and view core
    /// lengths: `Bee::length()` is the moved-to root, not the local core.
    fn flushed(
        &self,
        flushes: u64,
        batch: &[Node],
        system: &SystemBee,
        system_length: u64,
        view: &Bee,
        view_length: u64,
    );
}

/// The drain's request counter plus the wake the driver waits on
/// (`_bumpSignal` and `bumping`).
#[derive(Default)]
struct Bump {
    pending: AtomicU64,
    wake: Notify,
}

impl Bump {
    fn bump(&self) {
        self.pending.fetch_add(1, Ordering::AcqRel);
        self.wake.notify_one();
    }

    fn take(&self) -> u64 {
        self.pending.load(Ordering::Acquire)
    }
}

/// The mutable odds and ends the drain carries between steps. One std mutex,
/// never held across an await.
#[derive(Default)]
struct Seeds {
    previous_drain: u64,
    boot_from: Option<Link>,
    fast_forward_to: Option<Landing>,
    fast_forwarding: bool,
    ff_searching: bool,
    /// `_ffCandidates`, in first-seen order, longest length per key.
    candidates: Vec<(Key, u64)>,
    local_system_start: u64,
    local_system_length: u64,
    local_view_start: u64,
    local_view_length: u64,
    local_flushes: u64,
    /// `_ackFlushes`, with -1 as "not resolved yet".
    ack_flushes: i64,
    acking: bool,
    ff_result: Option<Landing>,
}

struct WakeupHost {
    bootstrap: Core,
    links: StdMutex<Vec<Link>>,
    bump: Arc<Bump>,
    wakeup: StdMutex<Option<Weak<AutobeeWakeup>>>,
}

impl WakeupBase for WakeupHost {
    fn bootstrap(&self) -> Core {
        self.bootstrap.clone()
    }

    fn wakeup_writers(&self) -> Vec<Link> {
        self.links.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn bump_soon(&self) {
        self.bump.bump();
    }
}

impl Coupler for WakeupHost {
    fn add(&self, core: Core) -> bool {
        let wakeup = self
            .wakeup
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .and_then(Weak::upgrade);
        wakeup.is_some_and(|wakeup| wakeup.add_core(core))
    }

    fn remove(&self, core: &Core) -> bool {
        let wakeup = self
            .wakeup
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .and_then(Weak::upgrade);
        wakeup.is_some_and(|wakeup| wakeup.remove_core(core))
    }
}

pub struct Autobee {
    pub key: Key,
    id: String,
    pub local_key: Key,
    pub bootstrap: Session,
    /// The local writer's core handle; the writer session closes it.
    pub local: Core,
    pub system: Mutex<SystemBee>,
    pub writers: Arc<Mutex<ActiveWriters>>,
    /// Read-only snapshot moved to the working view's head before `update`.
    pub view: ApplyView,
    pub working_view: ApplyView,
    pub store: Corestore,
    pub trusted: TrustedPeers,
    pub wakeup: Arc<AutobeeWakeup>,
    pub stats: Stats,
    pub wakeup_capability: Option<WakeupCapability>,
    bump: Arc<Bump>,
    handlers: Arc<dyn Handlers>,
    system_store: Corestore,
    view_store: Corestore,
    /// The local root cores of the system and view trees. `Bee::key()` and
    /// `Bee::length()` follow the moved-to root, so the local ones are kept.
    system_core: Core,
    view_core: Core,
    seeds: StdMutex<Seeds>,
    history: Mutex<Option<hyperbee::ChangesStream>>,
    applying: StdMutex<Option<Vec<Node>>>,
    interrupted: StdMutex<Option<String>>,
    interrupting: AtomicBool,
    closed: AtomicBool,
    flushes: AtomicU64,
    genesis: AtomicBool,
    ack_threshold: u64,
    needs_update: AtomicBool,
    approval_check: AtomicBool,
    reindexed: AtomicBool,
    pre_applied: AtomicBool,
    ff_enabled: bool,
    conservative: bool,
    has_update: bool,
    wake_host: Arc<WakeupHost>,
    drain_lock: Mutex<()>,
    driver: StdMutex<Option<JoinHandle<()>>>,
    weak_self: StdMutex<Weak<Autobee>>,
    last_error: StdMutex<Option<String>>,
    flush_observer: StdMutex<Option<Arc<dyn FlushObserver>>>,
}

impl Autobee {
    /// Boots the local writer, opens the system and view under its stable
    /// names, resumes the stored system head and starts the drain driver.
    pub async fn open(
        store: &Corestore,
        key: Option<Key>,
        handlers: Arc<dyn Handlers>,
    ) -> Result<Arc<Self>> {
        handlers.wait().await;
        if handlers.encryption_key().is_some() {
            return Err(std::io::Error::other(
                "encrypted Autobee sessions require the session encryption provider",
            )
            .into());
        }
        let boot = crate::boot::boot(
            store,
            key,
            crate::boot::Options {
                encryption_key: None,
                key_pair: handlers.key_pair(),
                ..Default::default()
            },
        )
        .await?;
        if boot.encryption_key.is_some() {
            return Err(std::io::Error::other(
                "encrypted Autobee sessions require the session encryption provider",
            )
            .into());
        }

        let local_key = *boot.local.key();
        let local = boot.local.core().clone();
        let local_id = hypercore_id_encoding::encode(&local_key);
        let bootstrap_key = boot.key;
        let discovery_key = *boot.bootstrap.discovery_key();
        let previous_drain = boot.previous_drain;

        let system_store = store.session();
        let view_store = store.session();
        let options = |name: &str| GetOptions {
            name: Some(format!("autobee/{local_id}/view/{name}").into_bytes()),
            manifest_version: Some(crate::constants::DEFAULT_MANIFEST_VERSION),
            inflight_range: Some((256, 512)),
            ..Default::default()
        };
        let system_core = Arc::new(system_store.get(options("system")).await?);
        let view_core = Arc::new(view_store.get(options("view")).await?);
        let system_key = *system_core.key();
        let view_key = *view_core.key();
        let system_local = system_core.core().clone();
        let view_local = view_core.core().clone();
        let mut system = SystemBee::new(Bee::from_session(&system_store, system_core).await?);
        let system_head = boot.system.map_or(
            BeeHead {
                key: system_key,
                length: 0,
            },
            |head| BeeHead {
                key: head.key,
                length: head.length,
            },
        );
        system.boot(system_head).await?;

        let bee = Bee::from_session(&view_store, view_core).await?;
        let view_head = system.view.unwrap_or(BeeHead {
            key: view_key,
            length: 0,
        });
        bee.move_to(view_head).await?;
        let snapshot = bee.snapshot().await?;
        let working_view = ApplyView::new(bee.clone(), handlers.as_ref());
        let view = ApplyView::new(snapshot, handlers.as_ref());

        let bump = Arc::new(Bump::default());
        let host = Arc::new(WakeupHost {
            bootstrap: boot.bootstrap.core().clone(),
            links: StdMutex::new(Vec::new()),
            bump: Arc::clone(&bump),
            wakeup: StdMutex::new(None),
        });
        let wakeup = Arc::new(AutobeeWakeup::new(host.clone()));
        let wake: crate::writers::Wake = {
            let bump = Arc::clone(&bump);
            Arc::new(move || bump.bump())
        };
        let mut writers =
            ActiveWriters::new(boot.local, wake, Some(host.clone() as Arc<dyn Coupler>));
        writers.update_local_state(&system, &bootstrap_key).await?;

        let flushes = system.flushes;
        let genesis = system.is_genesis();
        let fast_forward = handlers.fast_forward();
        let seeds = Seeds {
            previous_drain,
            boot_from: fast_forward
                .as_ref()
                .and_then(|options| options.boot.clone()),
            local_system_start: system_local.length(),
            local_view_start: view_local.length(),
            local_flushes: flushes,
            ack_flushes: -1,
            ..Default::default()
        };
        let capability = handlers.wakeup_capability();

        let auto = Arc::new(Self {
            key: bootstrap_key,
            id: hypercore_id_encoding::encode(&bootstrap_key),
            local_key,
            bootstrap: boot.bootstrap,
            local,
            system: Mutex::new(system),
            writers: Arc::new(Mutex::new(writers)),
            view,
            working_view,
            store: store.clone(),
            trusted: TrustedPeers::new(Arc::clone(&handlers)),
            wakeup: Arc::clone(&wakeup),
            stats: Stats::default(),
            wakeup_capability: capability,
            bump,
            ack_threshold: DEFAULT_ACK_THRESHOLD,
            has_update: handlers.has_update(),
            ff_enabled: fast_forward.is_some(),
            conservative: fast_forward
                .as_ref()
                .is_some_and(|options| options.conservative),
            handlers,
            system_store,
            view_store,
            system_core: system_local.clone(),
            view_core: view_local,
            seeds: StdMutex::new(seeds),
            history: Mutex::new(None),
            applying: StdMutex::new(None),
            interrupted: StdMutex::new(None),
            interrupting: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            flushes: AtomicU64::new(flushes),
            genesis: AtomicBool::new(genesis),
            needs_update: AtomicBool::new(false),
            approval_check: AtomicBool::new(true),
            reindexed: AtomicBool::new(false),
            pre_applied: AtomicBool::new(false),
            wake_host: host,
            drain_lock: Mutex::new(()),
            driver: StdMutex::new(None),
            weak_self: StdMutex::new(Weak::new()),
            last_error: StdMutex::new(None),
            flush_observer: StdMutex::new(None),
        });

        *auto.weak_self.lock().unwrap_or_else(|e| e.into_inner()) = Arc::downgrade(&auto);
        *auto
            .wake_host
            .wakeup
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(Arc::downgrade(&wakeup));

        // `_registerWakeup`
        let capability = capability.unwrap_or(WakeupCapability {
            key: bootstrap_key,
            discovery_key,
        });
        auto.wakeup.recouple();
        auto.wakeup
            .set_capability(capability.key, Some(capability.discovery_key));
        auto.refresh_wakeup_writers().await;

        spawn_driver(&auto);
        auto.update_acking().await;
        auto.bump_soon();

        Ok(auto)
    }

    fn lock_seeds(&self) -> StdMutexGuard<'_, Seeds> {
        self.seeds.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    /// The landing a fast-forward has scheduled but not applied yet.
    pub fn fast_forward_to(&self) -> Option<Landing> {
        self.lock_seeds().fast_forward_to
    }

    /// Records every system flush; used by the trace replay tests.
    pub fn set_flush_observer(&self, observer: Arc<dyn FlushObserver>) {
        *self
            .flush_observer
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(observer);
    }

    pub fn is_genesis(&self) -> bool {
        self.genesis.load(Ordering::Acquire)
    }

    pub fn flushes(&self) -> u64 {
        self.flushes.load(Ordering::Acquire)
    }

    pub fn is_interrupting(&self) -> bool {
        self.interrupting.load(Ordering::Acquire)
    }

    pub fn last_error(&self) -> Option<String> {
        self.last_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// `bumpSoon()`: mark work and let the driver pick it up.
    pub fn bump_soon(&self) {
        self.bump.bump();
    }

    /// `update()`: bump and wait for the drain that consumes it.
    pub async fn update(&self) -> Result<()> {
        self.bump.bump();
        self.run_drain().await
    }

    /// `updated()`: wait for any drain already running.
    pub async fn updated(&self) -> Result<()> {
        self.run_drain().await
    }

    /// `flush()`: the reference waits for boot here; boot is part of `open`.
    pub async fn flush(&self) -> Result<()> {
        self.run_drain().await
    }

    /// Serialises drains: callers and the driver share one drain at a time and
    /// a caller that finds its bump already consumed returns immediately.
    async fn run_drain(&self) -> Result<()> {
        loop {
            let _guard = self.drain_lock.lock().await;
            if self.bump.take() == 0 || self.closed.load(Ordering::Acquire) {
                return Ok(());
            }
            self.drain_inner().await?;
        }
    }

    /// `_drain()`.
    async fn drain_inner(&self) -> Result<()> {
        self.run_pre_apply().await;
        self.stats.drains.fetch_add(1, Ordering::Relaxed);

        let boot_from = self.lock_seeds().boot_from.take();
        if let Some(head) = boot_from {
            self.wakeup.hint(std::slice::from_ref(&head));
            self.boot_from_head(head).await;
        }

        let mut changes = self.has_update.then(UpdateChanges::new);
        if let Some(changes) = changes.as_mut() {
            changes.track(
                head_of(self.working_view.bee().head()),
                head_of(self.system.lock().await.bee.head()),
            );
        }

        while !self.is_interrupting() && self.bump.take() > 0 {
            let result = self.drain_pass().await;
            // the reference's `finally`: one bump survives so an append that
            // landed mid-pass gets its own pass
            let pending = self.bump.take();
            self.bump
                .pending
                .store(if pending == 1 { 0 } else { 1 }, Ordering::Release);
            result?;
        }

        if self.is_interrupting() {
            return Ok(());
        }

        if self.needs_update.swap(false, Ordering::AcqRel) {
            self.update_view(changes.as_mut(), false).await?;
        }
        self.store_boot().await
    }

    /// One pass of the inner drain loop.
    async fn drain_pass(&self) -> Result<()> {
        loop {
            if self.is_interrupting() {
                break;
            }
            self.flush_wakeup().await?;
            if self.is_interrupting() {
                break;
            }

            if self.lock_seeds().fast_forward_to.is_some() {
                self.apply_fast_forward().await?;
                self.needs_update.store(false, Ordering::Release);
                break;
            }

            if self.bump_pending_writers(false).await? {
                continue;
            }

            if !self.append_ack().await? {
                break;
            }
            self.needs_update.store(true, Ordering::Release);
        }

        self.flush_local().await?;
        if !self.is_interrupting() {
            let system = {
                let system = self.system.lock().await;
                fork_system(&system)
            };
            self.writers
                .lock()
                .await
                .refresh(&system, &self.key)
                .await?;
            self.refresh_wakeup_writers().await;
        }
        Ok(())
    }

    async fn run_pre_apply(&self) {
        if self.pre_applied.swap(true, Ordering::AcqRel) {
            return;
        }
        self.handlers.pre_apply(self.view.view()).await;
    }

    /// `_applyWakeupHints`.
    async fn apply_wakeup_hints(&self) -> Result<Vec<Link>> {
        let hints = self.wakeup.flush();
        if hints.is_empty() {
            return Ok(Vec::new());
        }
        self.lock_seeds().previous_drain = self.handlers.now();

        let mut results = Vec::new();
        for link in hints {
            let known = {
                let system = self.system.lock().await;
                system.get(&link.key, false).await?
            };
            match known {
                // an unknown length is `-1`: always behind a known record
                Some(info) if link.length == UNKNOWN_LENGTH || info.length >= link.length => {
                    continue;
                }
                _ => results.push(Link {
                    key: link.key,
                    length: if link.length == UNKNOWN_LENGTH {
                        0
                    } else {
                        link.length
                    },
                }),
            }
        }

        for link in &results {
            let trusted = self.is_local_trusted().await;
            self.writers
                .lock()
                .await
                .wakeup(
                    &self.store,
                    &self.key,
                    link.key,
                    link.length,
                    trusted,
                    DEFAULT_MAX_UNTRUSTED_WRITERS,
                )
                .await?;
        }
        Ok(results)
    }

    /// `_flushWakeup`.
    async fn flush_wakeup(&self) -> Result<()> {
        let hints = self.apply_wakeup_hints().await?;
        if hints.is_empty() {
            return Ok(());
        }
        self.queue_fast_forward(&hints);
        Ok(())
    }

    /// `_queueFastForward(hints)`.
    pub fn queue_fast_forward(&self, hints: &[Link]) {
        if !self.ff_enabled {
            return;
        }
        {
            let mut seeds = self.lock_seeds();
            for link in hints {
                if link.length == 0 {
                    continue;
                }
                match seeds
                    .candidates
                    .iter_mut()
                    .find(|(key, _)| *key == link.key)
                {
                    Some((_, length)) => *length = (*length).max(link.length),
                    None => seeds.candidates.push((link.key, link.length)),
                }
            }
            if seeds.candidates.is_empty() || seeds.ff_searching {
                return;
            }
            if seeds.fast_forward_to.is_some() || seeds.fast_forwarding {
                return;
            }
            if self.is_interrupting()
                || self.closed.load(Ordering::Acquire)
                || seeds.boot_from.is_some()
            {
                return;
            }
            seeds.ff_searching = true;
        }

        let Some(auto) = self
            .weak_self
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .upgrade()
        else {
            self.lock_seeds().ff_searching = false;
            return;
        };
        tokio::spawn(async move {
            auto.search_fast_forward().await;
            auto.lock_seeds().ff_searching = false;
        });
    }

    /// `_searchFastForward`.
    async fn search_fast_forward(&self) {
        loop {
            let hints = {
                let mut seeds = self.lock_seeds();
                if seeds.candidates.is_empty()
                    || seeds.fast_forward_to.is_some()
                    || seeds.fast_forwarding
                    || self.is_interrupting()
                    || self.closed.load(Ordering::Acquire)
                    || seeds.boot_from.is_some()
                {
                    return;
                }
                std::mem::take(&mut seeds.candidates)
            };
            let heads: Vec<Link> = hints
                .into_iter()
                .map(|(key, length)| Link { key, length })
                .collect();
            let heads = self.read_candidate_heads(&heads).await;
            if heads.is_empty() {
                continue;
            }
            let ff = FastForward::from_heads(
                self,
                &heads,
                FromOptions {
                    skip_gap: false,
                    timeout: FastForward::DEFAULT_TIMEOUT,
                    ..Default::default()
                },
            )
            .await;
            let Some(mut ff) = ff else {
                continue;
            };
            if self.is_interrupting() || self.closed.load(Ordering::Acquire) {
                ff.close(self).await;
                return;
            }
            if let Ok(true) = self.run_fast_forward(&mut ff, false, false).await {
                return;
            }
        }
    }

    /// `_readCandidateHeads`: resolve each announced head to a readable oplog.
    async fn read_candidate_heads(&self, hints: &[Link]) -> Vec<crate::types::Head> {
        let mut heads = Vec::new();
        for hint in hints {
            if hint.length == 0 {
                continue;
            }
            if let Some(node) = self.resolve_oplog_hint(hint.clone()).await {
                heads.push(crate::types::Head {
                    key: Some(node.key),
                    length: node.length,
                });
            }
        }
        heads
    }

    /// `_resolveOplogHint`.
    async fn resolve_oplog_hint(&self, hint: Link) -> Option<Node> {
        let core = self.open_core(hint.key).await?;
        let max = hint.length.max(core.length());
        let block =
            Host::read_oplog(self, Arc::clone(&core), max, FastForward::DEFAULT_TIMEOUT).await;
        let result = match block {
            Some(node) if max >= core.length() => Some(node),
            _ => {
                Host::read_oplog(
                    self,
                    Arc::clone(&core),
                    core.length(),
                    FastForward::DEFAULT_TIMEOUT,
                )
                .await
            }
        };
        core.close();
        result
    }

    /// `_runFastForward(ff, { force, rewind })`.
    async fn run_fast_forward(
        &self,
        ff: &mut FastForward,
        force: bool,
        rewind: bool,
    ) -> Result<bool> {
        let busy = {
            let mut seeds = self.lock_seeds();
            if seeds.fast_forward_to.is_some() || seeds.fast_forwarding {
                true
            } else {
                seeds.fast_forwarding = true;
                false
            }
        };
        if busy {
            ff.close(self).await;
            return Ok(false);
        }

        let result = ff.run(self).await;
        let flushes = ff.system.flushes;
        self.lock_seeds().fast_forwarding = false;

        let Some(landing) = result else {
            return Ok(false);
        };
        // never move backwards, a boot head can be older than what we already have
        if !rewind && flushes < self.flushes() {
            return Ok(false);
        }
        // apply ran while we searched; only drop it if it no longer moves forward
        if !force && flushes <= self.flushes() {
            return Ok(false);
        }

        self.lock_seeds().fast_forward_to = Some(landing);
        self.bump_soon();
        Ok(true)
    }

    /// `_bootFromHead`: gated boot straight onto an oplog head.
    async fn boot_from_head(&self, head: Link) -> bool {
        let timeout = FastForward::DEFAULT_TIMEOUT;
        let Some(node) = self.resolve_oplog_hint(head).await else {
            return false;
        };
        let oplog_head = crate::types::Head {
            key: Some(node.key),
            length: node.length,
        };
        let Some(mut ff) = FastForward::from_head(
            self,
            oplog_head,
            None,
            FromOptions {
                force: true,
                timeout,
                ..Default::default()
            },
        )
        .await
        else {
            return false;
        };
        self.run_fast_forward(&mut ff, true, false)
            .await
            .unwrap_or(false)
    }

    /// `_applyFastForward`.
    async fn apply_fast_forward(&self) -> Result<()> {
        let mut changes = self.has_update.then(UpdateChanges::new);
        if let Some(changes) = changes.as_mut() {
            changes.track(
                head_of(self.working_view.bee().head()),
                head_of(self.system.lock().await.bee.head()),
            );
        }

        let Some(landing) = self.lock_seeds().fast_forward_to else {
            return Ok(());
        };
        let to = landing.head;
        let from = self.system.lock().await.bee.head();

        if let Some(head) = to.key {
            let mut system = self.system.lock().await;
            system
                .bee
                .move_to(BeeHead {
                    key: head,
                    length: to.length,
                })
                .await?;
            system.reset().await?;
            self.flushes.store(system.flushes, Ordering::Release);
            self.genesis.store(system.is_genesis(), Ordering::Release);
        }
        let view = self.system.lock().await.view;
        let view_head = view.unwrap_or(BeeHead {
            key: self.working_view.bee().key(),
            length: 0,
        });
        self.working_view.bee().move_to(view_head).await?;

        self.approval_check.store(true, Ordering::Release);
        self.lock_seeds().fast_forward_to = None;

        let hints = self.apply_wakeup_hints().await?;
        self.queue_fast_forward(&hints);
        let system = {
            let system = self.system.lock().await;
            fork_system(&system)
        };
        self.writers
            .lock()
            .await
            .refresh(&system, &self.key)
            .await?;
        self.refresh_wakeup_writers().await;

        // we moved, so ask our peers to tell us their heads again
        self.wakeup.request_lookup();

        self.update_view(changes.as_mut(), true).await?;
        self.store_boot().await?;
        self.update_acking().await;

        self.stats.fast_forwards.fetch_add(1, Ordering::Relaxed);
        self.lock_seeds().ff_result = Some(Landing { head: to });
        let _ = from;
        Ok(())
    }

    /// `_bumpPendingWriters({ local })`.
    async fn bump_pending_writers(&self, local_only: bool) -> Result<bool> {
        // lock order is always system before writers; read writers first and
        // take a system copy so no await holds both
        let writable = self.writers.lock().await.writable;
        let system = {
            let system = self.system.lock().await;
            fork_system(&system)
        };
        let next = {
            let mut writers = self.writers.lock().await;
            let mut reader = Refs(self);
            writers
                .next_pending(
                    &system,
                    &mut reader,
                    writable,
                    local_only,
                    self.handlers.now(),
                )
                .await
                .map_err(|err| match err {
                    crate::writers::SelectionError::System(err) => Error::from(err),
                    crate::writers::SelectionError::Witness(err) => err,
                })?
        };

        if std::env::var_os("AUTOBEE_DEBUG").is_some() {
            eprintln!(
                "[next {:?}] {:?}",
                &self.id[..6],
                match &next {
                    PendingNext::Idle => "idle".to_string(),
                    PendingNext::Missing { writer, links } => format!(
                        "missing {:?} {:?}",
                        &writer[..3],
                        links
                            .iter()
                            .map(|l| (&l.key[..3], l.length))
                            .collect::<Vec<_>>()
                    ),
                    PendingNext::Batch { writer, batch } =>
                        format!("batch {:?} len{}", &writer[..3], batch.len()),
                }
            );
        }
        match next {
            PendingNext::Idle => Ok(false),
            PendingNext::Missing { links, .. } => {
                let trusted = self.is_local_trusted().await;
                for link in links {
                    self.writers
                        .lock()
                        .await
                        .wakeup(
                            &self.store,
                            &self.key,
                            link.key,
                            link.length,
                            trusted,
                            DEFAULT_MAX_UNTRUSTED_WRITERS,
                        )
                        .await?;
                }
                Ok(true)
            }
            PendingNext::Batch { writer, batch } => {
                let optimistic = self.handlers.optimistic() && batch[0].optimistic;
                let (is_added, is_removed, referrals) = {
                    let writers = self.writers.lock().await;
                    let info = writers.get(&writer);
                    (
                        info.is_some_and(|writer| writer.is_added),
                        info.is_some_and(|writer| writer.is_removed),
                        writers.has_referrals(&writer),
                    )
                };
                if optimistic || is_added || (is_removed && referrals) {
                    self.process_batch(batch.clone()).await?;
                } else {
                    self.writers.lock().await.remove_pending(&writer);
                    return Ok(true);
                }
                let length = batch.last().map_or(0, |node| node.length);
                self.writers.lock().await.notify_batch(&writer, length);
                self.needs_update.store(true, Ordering::Release);
                Ok(true)
            }
        }
    }

    /// `prepareBatch`.
    async fn prepare_batch(&self, batch: &[Node]) -> Result<crate::types::SortResult> {
        let mut batch = batch.to_vec();
        let weight = {
            let mut reader = Refs(self);
            crate::witness::resolve_weight(&mut reader, &batch[0]).await?
        };
        batch[0].weight = weight;
        for node in &mut batch[1..] {
            node.weight = weight;
        }
        self.writers.lock().await.set_batch_weight(&batch, weight);

        let system_hash = self.system.lock().await.hash;
        if batch[0].hash.is_some() && system_hash == batch[0].hash {
            let linking = {
                let system = self.system.lock().await;
                crate::topo::is_linking_all(&batch[0], &system.heads)
            };
            if linking {
                return Ok(crate::types::SortResult {
                    undo: None,
                    view: None,
                    tip: vec![batch.into_iter().map(Some).collect()],
                });
            }
        }

        let sort = {
            let mut reader = Refs(self);
            crate::topo::sort(&mut reader, batch.into_iter().map(Some).collect()).await?
        };
        let Some(mut sort) = sort else {
            return Ok(crate::types::SortResult {
                undo: None,
                view: None,
                tip: Vec::new(),
            });
        };

        if let Some(undo) = sort.undo {
            self.stats.undos.fetch_add(1, Ordering::Relaxed);
            self.trusted.clear();
            let mut system = self.system.lock().await;
            let view = match undo.key {
                Some(key) => {
                    system
                        .undo(BeeHead {
                            key,
                            length: undo.length,
                        })
                        .await?
                }
                None => None,
            };
            self.flushes.store(system.flushes, Ordering::Release);
            self.genesis.store(system.is_genesis(), Ordering::Release);
            let view = view.unwrap_or(BeeHead {
                key: self.working_view.bee().key(),
                length: 0,
            });
            sort.view = Some(crate::types::Head {
                key: Some(view.key),
                length: view.length,
            });
        }

        Ok(sort)
    }

    /// `_processBatch`.
    async fn process_batch(&self, batch: Vec<Node>) -> Result<()> {
        let mut stack = vec![batch];
        while let Some(batch) = stack.pop() {
            let sorted = self.prepare_batch(&batch).await?;
            if let Some(view) = sorted.view {
                let Some(key) = view.key else { continue };
                self.working_view
                    .bee()
                    .move_to(BeeHead {
                        key,
                        length: view.length,
                    })
                    .await?;
                for item in sorted.tip.into_iter().rev() {
                    stack.push(item.into_iter().flatten().collect());
                }
                continue;
            }

            if batch[0].key == self.key && batch[0].length == 1 {
                let host = ApplyCalls::new(self, None);
                host.add_writer(
                    batch[0].key,
                    AddWriterOptions {
                        is_indexer: true,
                        weight: Some(self.handlers.bootstrap_weight()),
                    },
                )
                .await?;
            }

            self.apply_batch(&batch, batch[0].optimistic).await?;
        }
        Ok(())
    }

    /// `_applyBatch`.
    async fn apply_batch(&self, batch: &[Node], optimistic: bool) -> Result<()> {
        let local = batch[0]
            .core
            .as_ref()
            .is_some_and(|core| core.key() == &self.local_key);
        let mut user_batch = Vec::new();

        {
            let mut system = self.system.lock().await;
            for node in batch {
                system.add_node(node)?;
                if let Some(approvals) = &node.approvals {
                    for approval in approvals {
                        system
                            .add_writer(
                                approval.key,
                                crate::system::AddWriterOptions {
                                    weight: approval.weight,
                                    coord: Some(Link {
                                        key: node.key,
                                        length: node.length,
                                    }),
                                    carrier: node.weight,
                                    anchor: true,
                                    ..Default::default()
                                },
                            )
                            .await?;
                    }
                }
                if node.value.as_ref().is_some_and(|value| !value.is_empty()) {
                    user_batch.push(node.clone());
                }
            }
        }

        let can_apply = {
            let system = self.system.lock().await;
            system.can_apply(&batch[0].key, optimistic).await?
        };
        if can_apply {
            self.stats.applies.fetch_add(1, Ordering::Relaxed);
            *self.applying.lock().unwrap_or_else(|e| e.into_inner()) = Some(batch.to_vec());
            let result = {
                let host = ApplyCalls::new(self, Some(batch));
                self.working_view
                    .apply(&user_batch, self.handlers.as_ref(), &host)
                    .await
            };
            *self.applying.lock().unwrap_or_else(|e| e.into_inner()) = None;
            result?;
            if self.is_interrupting() {
                return Err(std::io::Error::other("Apply interrupted").into());
            }
        }

        if optimistic {
            self.system.lock().await.ack_writer(batch[0].key, 0).await?;
        }

        let changed = {
            let view = self.working_view.bee().head().unwrap_or(BeeHead {
                key: self.working_view.bee().key(),
                length: 0,
            });
            let mut system = self.system.lock().await;
            let (changed, _ops) = system.flush(batch, view).await?;
            self.flushes.store(system.flushes, Ordering::Release);
            self.genesis.store(system.is_genesis(), Ordering::Release);
            if local {
                let mut seeds = self.lock_seeds();
                seeds.local_system_length = self.system_core.length() - seeds.local_system_start;
                seeds.local_view_length = self.view_core.length() - seeds.local_view_start;
                seeds.local_flushes = system.flushes;
            }
            if let Some(observer) = self
                .flush_observer
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
            {
                observer.flushed(
                    system.flushes,
                    batch,
                    &system,
                    self.system_core.length(),
                    self.working_view.bee(),
                    self.view_core.length(),
                );
            }
            changed
        };

        let system = {
            let system = self.system.lock().await;
            fork_system(&system)
        };
        for (key, added) in changed {
            let trusted = self.is_local_trusted().await;
            let mut writers = self.writers.lock().await;
            if added {
                writers
                    .add(
                        &self.store,
                        &self.key,
                        key,
                        trusted,
                        DEFAULT_MAX_UNTRUSTED_WRITERS,
                    )
                    .await?;
            } else {
                writers.remove(&key, &system, &self.key).await?;
            }
        }
        self.refresh_wakeup_writers().await;
        Ok(())
    }

    /// `_appendAck`.
    async fn append_ack(&self) -> Result<bool> {
        let acking = self.lock_seeds().acking;
        if !acking && !self.reindexed.load(Ordering::Acquire) {
            return Ok(false);
        }
        if !self.writers.lock().await.writable {
            return Ok(false);
        }
        if self
            .writers
            .lock()
            .await
            .get(&self.local_key)
            .is_some_and(|writer| writer.appending())
        {
            return Ok(false);
        }

        if !self.reindexed.load(Ordering::Acquire) {
            if self.flushes_behind().await? < self.ack_threshold {
                return Ok(false);
            }
            if self.all_heads_trusted().await {
                return Ok(false);
            }
        }

        let (links, timestamp) = {
            let system = self.system.lock().await;
            (system.get_links(Some(&self.local_key)), system.timestamp)
        };
        let now = self.handlers.now();
        let timestamp = timestamp.max(now);
        let writable = self.writers.lock().await.writable;
        self.writers.lock().await.append_local(
            crate::encoding::OplogMessageV4 {
                timestamp,
                links,
                batch: Some(crate::types::BatchCoords { start: 0, end: 0 }),
                views: None,
                trusted: None,
                witness: None,
                approvals: None,
                optimistic: false,
                value: None,
                hash: None,
                weight_hint: None,
                drift: Some(timestamp.saturating_sub(now)),
            },
            writable,
        );
        Ok(true)
    }

    async fn all_heads_trusted(&self) -> bool {
        let heads = self.system.lock().await.heads.clone();
        if heads.is_empty() {
            return false;
        }
        for head in heads {
            if !self.is_trusted(&head.key).await {
                return false;
            }
        }
        true
    }

    async fn flushes_behind(&self) -> Result<u64> {
        let cached = self.lock_seeds().ack_flushes;
        let ack_flushes = if cached == -1 {
            let latest = self.writers.lock().await.latest_local_oplog().await?;
            let value = latest
                .as_ref()
                .and_then(|oplog| match oplog {
                    crate::encoding::DecodedOplog::Mapped(_) => None,
                    crate::encoding::DecodedOplog::V4(message) => {
                        message.views.as_ref().map(|views| views.flushes)
                    }
                })
                .unwrap_or_else(|| self.flushes());
            self.lock_seeds().ack_flushes = value as i64;
            value as i64
        } else {
            cached
        };
        Ok(self.flushes().saturating_sub(ack_flushes.max(0) as u64))
    }

    /// `_flushLocal`.
    async fn flush_local(&self) -> Result<()> {
        while !self.is_interrupting() && self.bump_pending_writers(true).await? {}

        let views = {
            let seeds = self.lock_seeds();
            Views {
                flushes: seeds.local_flushes,
                system: crate::encoding::ExternalBatch {
                    key: *self.system_core.key(),
                    start: seeds.local_system_start,
                    length: seeds.local_system_length,
                },
                view: Some(crate::encoding::ExternalBatch {
                    key: *self.view_core.key(),
                    start: seeds.local_view_start,
                    length: seeds.local_view_length,
                }),
            }
        };

        let trusted = self
            .trusted
            .most_recent_trusted(self.working_view.view(), None)
            .await
            .and_then(|head| head.link());
        let flushed = self
            .writers
            .lock()
            .await
            .flush_local(views, trusted)
            .await?;

        let (flushes, system_length_now, view_length_now) = {
            let system = self.system.lock().await;
            (
                system.flushes,
                self.system_core.length(),
                self.view_core.length(),
            )
        };
        let mut seeds = self.lock_seeds();
        if flushed {
            seeds.ack_flushes = seeds.local_flushes as i64;
        }
        seeds.local_system_start = system_length_now;
        seeds.local_system_length = 0;
        seeds.local_flushes = flushes;
        seeds.local_view_start = view_length_now;
        seeds.local_view_length = 0;
        Ok(())
    }

    /// `_update(changes, fastForward)`.
    async fn update_view(
        &self,
        changes: Option<&mut UpdateChanges>,
        fast_forward: bool,
    ) -> Result<()> {
        self.needs_update.store(false, Ordering::Release);
        let head = self.working_view.bee().head().unwrap_or(BeeHead {
            key: self.working_view.bee().key(),
            length: 0,
        });
        self.view.bee().move_to(head).await?;

        let Some(changes) = changes else {
            return Ok(());
        };
        let system_head = self.system.lock().await.bee.head();
        let finalised = changes.finalise(
            head_of(self.view.bee().head()),
            head_of(system_head),
            self.flushes(),
            fast_forward,
        );
        self.handlers.update(self.view.view(), &finalised).await;
        Ok(())
    }

    /// `_storeBoot`.
    async fn store_boot(&self) -> Result<()> {
        let previous_drain = self.lock_seeds().previous_drain;
        self.local
            .set_user_data(
                "autobee/previous-drain",
                Some(&crate::encoding::encode_previous_drain(previous_drain)),
            )
            .await?;

        let boot = {
            let system = self.system.lock().await;
            system.boot_record()
        };
        if let Some(boot) = boot {
            let link = Link {
                key: boot.key,
                length: boot.length,
            };
            self.local
                .set_user_data(
                    "autobee/head",
                    Some(&crate::encoding::encode_boot_record(&link)),
                )
                .await?;
        }
        Ok(())
    }

    /// `setAcking`.
    pub fn set_acking(&self, acking: bool) {
        self.lock_seeds().acking = acking;
        if acking {
            self.bump_soon();
        }
    }

    async fn update_acking(&self) {
        if self.is_interrupting() {
            return;
        }
        let trusted = self.is_local_trusted().await;
        self.set_acking(trusted);
    }

    pub async fn is_local_trusted(&self) -> bool {
        self.is_trusted(&self.local_key).await
    }

    async fn is_trusted(&self, key: &Key) -> bool {
        let genesis = {
            let system = self.system.lock().await;
            Genesis {
                is_genesis: system.is_genesis(),
                key: &self.key,
                indexers: &[],
            }
        };
        self.trusted
            .is_trusted(key, Some(self.working_view.view()), genesis)
            .await
    }

    pub async fn refresh_wakeup_writers(&self) {
        let links = self.writers.lock().await.wakeup_writers();
        *self
            .wake_host
            .links
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = links;
    }

    /// `append(values, { optimistic })`.
    pub async fn append(&self, values: Vec<bytes::Bytes>, optimistic: bool) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(std::io::Error::other("Autobee closed").into());
        }
        self.stats.appends.fetch_add(1, Ordering::Relaxed);

        if !optimistic
            && self
                .writers
                .lock()
                .await
                .get(&self.local_key)
                .is_some_and(|writer| writer.is_removed)
        {
            return Err(std::io::Error::other("Not writable").into());
        }

        let (links, hash, timestamp) = {
            let system = self.system.lock().await;
            (
                system.get_links(Some(&self.local_key)),
                system.hash,
                system.timestamp,
            )
        };
        let now = self.handlers.now();
        let timestamp = timestamp.max(now);
        let drift = timestamp - now;

        let witness = {
            let system = self.system.lock().await;
            let record = system.get(&self.local_key, false).await?;
            match record {
                Some(record) => {
                    let current = crate::witness::current_weight(Some(&crate::witness::Standing {
                        weight: record.weight as i64,
                        max_weight: record.max_weight as i64,
                        is_genesis: record.is_genesis,
                    }));
                    match system.grant_hint(&self.local_key).await? {
                        Some(hint) if hint.weight as i64 > current => {
                            Some(crate::encoding::GrantWitness {
                                weight: hint.weight,
                                link: Link {
                                    key: hint.key,
                                    length: hint.length,
                                },
                            })
                        }
                        _ => None,
                    }
                }
                None => None,
            }
        };

        let approvals = if optimistic {
            None
        } else {
            self.collect_approvals().await?
        };

        let writable = self.writers.lock().await.writable;
        for (index, value) in values.into_iter().enumerate() {
            let links = if index == 0 {
                links.clone()
            } else {
                Vec::new()
            };
            self.writers.lock().await.append_local(
                crate::encoding::OplogMessageV4 {
                    timestamp,
                    links,
                    batch: Some(crate::types::BatchCoords {
                        start: index as u64,
                        end: 0,
                    }),
                    views: None,
                    trusted: None,
                    witness: if index == 0 { witness.clone() } else { None },
                    approvals: if index == 0 { approvals.clone() } else { None },
                    optimistic,
                    value: Some(value),
                    hash: if index == 0 { hash } else { None },
                    weight_hint: None,
                    drift: (drift > 0).then_some(drift),
                },
                writable,
            );
        }

        self.update().await
    }

    /// `_collectApprovals`.
    async fn collect_approvals(&self) -> Result<Option<Vec<crate::encoding::Approval>>> {
        let digest = {
            let system = self.system.lock().await;
            if !system.promotions.changed && !self.approval_check.load(Ordering::Acquire) {
                return Ok(None);
            }
            system.promotions.digest.clone()
        };
        if !self.writers.lock().await.writable {
            return Ok(None);
        }

        let standing = {
            let system = self.system.lock().await;
            let record = system.get(&self.local_key, false).await?;
            record.map_or(0, |record| {
                crate::witness::current_weight(Some(&crate::witness::Standing {
                    weight: record.weight as i64,
                    max_weight: record.max_weight as i64,
                    is_genesis: record.is_genesis,
                }))
            })
        };
        if standing <= 0 {
            return Ok(None);
        }

        {
            let mut system = self.system.lock().await;
            system.promotions.changed = false;
        }
        self.approval_check.store(false, Ordering::Release);

        let end = (standing.max(0) as usize).min(digest.len());
        if !digest[..end].iter().any(|pending| *pending) {
            return Ok(None);
        }

        let list = {
            let mut system = self.system.lock().await;
            system.list_pending_promotions().await?
        };
        let mut approvals = Vec::new();
        for (key, weight) in list.into_iter().take(32) {
            let amount = (standing.max(0) as u64).min(weight);
            if amount == 0 {
                continue;
            }
            let system = self.system.lock().await;
            let Some(target) = system.get(&key, false).await? else {
                continue;
            };
            if target.is_removed {
                continue;
            }
            if let Some(hint) = system.grant_hint(&key).await?
                && hint.weight >= amount
            {
                continue;
            }
            approvals.push(crate::encoding::Approval {
                key,
                weight: amount,
            });
        }
        Ok((!approvals.is_empty()).then_some(approvals))
    }

    /// `wakeup({ key, length })`.
    pub async fn wakeup(&self, key: Key, length: u64) -> Result<()> {
        if std::env::var_os("AUTOBEE_DEBUG").is_some() {
            eprintln!("[wakeup {:?}] {:?}:{length}", &self.id[..6], &key[..3]);
        }
        let trusted = self.is_local_trusted().await;
        self.writers
            .lock()
            .await
            .wakeup(
                &self.store,
                &self.key,
                key,
                length,
                trusted,
                DEFAULT_MAX_UNTRUSTED_WRITERS,
            )
            .await?;
        self.update().await
    }

    /// Releases the view, then the writers, then the system and the wakeup.
    pub async fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.interrupting.store(true, Ordering::Release);
        self.bump.wake.notify_waiters();

        // let an in-flight drain notice `closed` before its sessions go away
        {
            let _guard = self.drain_lock.lock().await;
        }
        if let Some(driver) = self.driver.lock().unwrap_or_else(|e| e.into_inner()).take() {
            driver.abort();
        }

        let view_result = self.view.close(self.handlers.as_ref()).await;
        self.writers.lock().await.close();
        self.system.lock().await.bee.close().await;
        self.wakeup.close();
        self.system_store.close().await;
        self.bootstrap.close();
        let working_result = self.working_view.close(self.handlers.as_ref()).await;
        self.view_store.close().await;
        view_result?;
        working_result?;
        Ok(())
    }
}

fn spawn_driver(auto: &Arc<Autobee>) {
    let weak = Arc::downgrade(auto);
    let bump = Arc::clone(&auto.bump);
    let handle = tokio::spawn(async move {
        loop {
            bump.wake.notified().await;
            let Some(auto) = weak.upgrade() else {
                return;
            };
            if auto.closed.load(Ordering::Acquire) {
                return;
            }
            if let Err(err) = auto.run_drain().await {
                auto.on_error(err);
            }
        }
    });
    *auto.driver.lock().unwrap_or_else(|e| e.into_inner()) = Some(handle);
}

impl Autobee {
    fn on_error(&self, err: Error) {
        let message = err.to_string();
        *self.last_error.lock().unwrap_or_else(|e| e.into_inner()) = Some(message);
        self.interrupting.store(false, Ordering::Release);
    }
}

fn head_of(head: Option<BeeHead>) -> Option<crate::types::Head> {
    head.map(|head| crate::types::Head {
        key: Some(head.key),
        length: head.length,
    })
}

/// A light copy of the system state for the writers' reads, so the system lock
/// is not held across a batch walk that may also read it.
fn fork_system(system: &SystemBee) -> SystemBee {
    let mut copy = SystemBee::new(system.bee.clone());
    copy.view = system.view;
    copy.heads = system.heads.clone();
    copy.hash = system.hash;
    copy.version = system.version;
    copy.timestamp = system.timestamp;
    copy.flushes = system.flushes;
    copy.writers = system.writers.clone();
    copy
}

// ---------------------------------------------------------------------------
// readers

/// The oplog reader the topo and witness modules run over this session.
struct Refs<'a>(&'a Autobee);

impl crate::topo::Reader for Refs<'_> {
    type Error = Error;

    async fn history(&mut self, wait: bool) -> Result<()> {
        self.0.start_history(wait).await
    }

    async fn next_change(&mut self) -> Result<Option<Option<crate::topo::HistoryEntry>>> {
        self.0.history_next().await
    }

    async fn node(
        &mut self,
        key: &Key,
        length: u64,
        weight: i64,
        wait: bool,
        timeout: u64,
    ) -> Result<Option<Node>> {
        self.0.read_node(key, length, weight, wait, timeout).await
    }
}

impl crate::witness::Reader for Refs<'_> {
    type Error = Error;

    fn bootstrap_key(&self) -> &Key {
        &self.0.key
    }

    fn bootstrap_weight(&self) -> i64 {
        self.0.handlers.bootstrap_weight() as i64
    }

    async fn standing(&mut self, key: &Key) -> Result<Option<crate::witness::Standing>> {
        Ok(self
            .0
            .system
            .lock()
            .await
            .get(key, false)
            .await?
            .map(|writer| crate::witness::Standing {
                weight: writer.weight as i64,
                max_weight: writer.max_weight as i64,
                is_genesis: writer.is_genesis,
            }))
    }

    async fn oplog_node(&mut self, link: &Link) -> Result<Node> {
        // a witness anchor is a committed block; read it without touching the
        // writers lock, which `next_pending` already holds on this path
        self.0
            .read_committed_node(&link.key, link.length, 0, true, 0)
            .await?
            .ok_or_else(|| hyperbee::Error::BlockNotAvailable.into())
    }
}

impl Autobee {
    async fn start_history(&self, wait: bool) -> Result<()> {
        let system = self.system.lock().await;
        let options = hypercore::GetOptions {
            wait,
            ..Default::default()
        };
        let changes = system.bee.changes_with_options(None, 0, options).await?;
        *self.history.lock().await = Some(changes);
        Ok(())
    }

    async fn history_next(&self) -> Result<Option<Option<crate::topo::HistoryEntry>>> {
        let mut history = self.history.lock().await;
        let Some(stream) = history.as_mut() else {
            return Ok(None);
        };
        let Some(change) = stream.next().await? else {
            return Ok(None);
        };
        let undo = change
            .tail
            .map_or(crate::types::EMPTY_HEAD, |head| crate::types::Head {
                key: Some(head.key),
                length: head.length,
            });
        let mut legacy = false;
        let mut oplogs = Vec::new();
        let mut candidates = Vec::new();
        for block in change.batch {
            for entry in block.keys {
                if entry.key.as_ref() == b"\0\0info" {
                    legacy = true;
                }
                if entry.key.first() != Some(&1) {
                    continue;
                }
                let Some(value) = entry.value else {
                    return Err(hyperbee::Error::Message("system writer is not inline").into());
                };
                let writer = crate::system::decode_writer(&entry.key, value)?;
                if writer.length == 0 {
                    continue;
                }
                let item = crate::types::Change {
                    undo: Some(undo),
                    oplog: crate::types::WeightedLink {
                        key: writer.key,
                        length: writer.length,
                        weight: writer.weight as i64,
                        timestamp: writer.timestamp,
                    },
                };
                if writer.is_oplog {
                    oplogs.push(item);
                } else {
                    candidates.push(item);
                }
            }
        }
        let refs = if oplogs.is_empty() && legacy {
            candidates
        } else {
            oplogs
        };
        if refs.is_empty() && !legacy {
            return Err(hyperbee::Error::Message("system change has no oplog").into());
        }
        Ok(Some(Some(crate::topo::HistoryEntry { legacy, refs })))
    }

    async fn read_node(
        &self,
        key: &Key,
        length: u64,
        weight: i64,
        wait: bool,
        timeout: u64,
    ) -> Result<Option<Node>> {
        if length == 0 {
            return Ok(None);
        }
        if *key == self.local_key {
            let writers = self.writers.lock().await;
            if let Some(writer) = writers.get(key)
                && length > writer.core.length()
                && let Some(mut node) = writer.pending_node(length)
            {
                node.weight = weight;
                return Ok(Some(node));
            }
        }
        self.read_committed_node(key, length, weight, wait, timeout)
            .await
    }

    /// Reads a flushed oplog block. Never takes the writers lock, so the
    /// witness reader can use it while `next_pending` holds that lock.
    async fn read_committed_node(
        &self,
        key: &Key,
        length: u64,
        weight: i64,
        wait: bool,
        timeout: u64,
    ) -> Result<Option<Node>> {
        if length == 0 {
            return Ok(None);
        }
        let core = self.store.get(GetOptions::key(*key)).await?;
        let options = hypercore::GetOptions {
            wait,
            timeout: (timeout > 0).then(|| std::time::Duration::from_millis(timeout)),
            ..Default::default()
        };
        let block = core.get(length - 1, options).await;
        let result = match block {
            Ok(Some(block)) => {
                let oplog = crate::encoding::decode_oplog(block)?;
                let reader = std::sync::Arc::new(core.core().clone());
                Some(Node::from_oplog(
                    *key,
                    length,
                    weight,
                    &oplog,
                    Some(crate::types::WriterRef((*reader).clone())),
                ))
            }
            Ok(None) => None,
            Err(err) => {
                core.close();
                return Err(err.into());
            }
        };
        core.close();
        Ok(result)
    }
}

impl crate::apply_calls::Auto for Autobee {
    fn id(&self) -> Option<&str> {
        Some(&self.id)
    }

    fn key(&self) -> Key {
        self.key
    }

    fn discovery_key(&self) -> Option<Key> {
        Some(*self.bootstrap.discovery_key())
    }

    fn name(&self) -> Option<&str> {
        None
    }

    fn flushes(&self) -> u64 {
        self.flushes.load(Ordering::Acquire)
    }

    fn is_genesis(&self) -> bool {
        self.genesis.load(Ordering::Acquire)
    }

    fn add_writer<'a>(
        &'a self,
        key: Key,
        opts: crate::apply_calls::SystemAddWriter,
    ) -> crate::apply_calls::BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.system
                .lock()
                .await
                .add_writer(
                    key,
                    crate::system::AddWriterOptions {
                        weight: opts.weight,
                        is_genesis: opts.is_genesis,
                        coord: opts.coord,
                        carrier: opts.carrier,
                        ..Default::default()
                    },
                )
                .await?;
            Ok(())
        })
    }

    fn remove_writer<'a>(&'a self, key: Key) -> crate::apply_calls::BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.system.lock().await.remove_writer(key, 0).await?;
            Ok(())
        })
    }

    fn interrupt(&self, reason: Option<&str>) {
        *self.interrupted.lock().unwrap_or_else(|e| e.into_inner()) = reason.map(str::to_owned);
        self.interrupting.store(true, Ordering::Release);
    }

    fn create_anchor<'a>(
        &'a self,
        key: Key,
        length: u64,
    ) -> crate::apply_calls::BoxFuture<'a, Result<Link>> {
        Box::pin(async move { self.create_anchor(key, length).await })
    }
}

impl Host for Autobee {
    fn open_core(&self, key: Key) -> crate::fast_forward::BoxFuture<'_, Option<Arc<Session>>> {
        Box::pin(async move {
            self.store
                .get(GetOptions::key(key))
                .await
                .ok()
                .map(Arc::new)
        })
    }

    fn open_legacy_core(
        &self,
        key: Key,
    ) -> crate::fast_forward::BoxFuture<'_, Option<Arc<Session>>> {
        Box::pin(async move {
            self.store
                .get(GetOptions::key(key))
                .await
                .ok()
                .map(Arc::new)
        })
    }

    fn read_oplog(
        &self,
        core: Arc<Session>,
        length: u64,
        timeout: u64,
    ) -> crate::fast_forward::BoxFuture<'_, Option<Node>> {
        Box::pin(async move {
            if length == 0 {
                return None;
            }
            let options = hypercore::GetOptions {
                wait: true,
                timeout: (timeout > 0).then(|| std::time::Duration::from_millis(timeout)),
                ..Default::default()
            };
            let block = core.get(length - 1, options).await.ok()??;
            let oplog = crate::encoding::decode_oplog(block).ok()?;
            Some(Node::from_oplog(
                *core.key(),
                length,
                0,
                &oplog,
                Some(crate::types::WriterRef(core.core().clone())),
            ))
        })
    }

    fn open_view(
        &self,
        head: crate::types::Head,
        _timeout: u64,
    ) -> crate::fast_forward::BoxFuture<'_, Option<Bee>> {
        Box::pin(async move {
            let key = head.key?;
            let bee = self
                .view
                .bee()
                .checkout(BeeHead {
                    key,
                    length: head.length,
                })
                .await
                .ok()?;
            Some(self.handlers.open(bee))
        })
    }

    fn close_view(&self, view: Bee) -> crate::fast_forward::BoxFuture<'_, ()> {
        Box::pin(async move {
            let _ = self.handlers.close(&view).await;
        })
    }

    fn is_trusted(&self, key: Key) -> crate::fast_forward::BoxFuture<'_, bool> {
        Box::pin(async move { Autobee::is_trusted(self, &key).await })
    }

    fn most_recent_trusted(
        &self,
        view: Bee,
    ) -> crate::fast_forward::BoxFuture<'_, Option<crate::types::Head>> {
        Box::pin(async move {
            self.trusted
                .most_recent_trusted(&view, Some(self.working_view.view()))
                .await
        })
    }

    fn should_reindex(
        &self,
        key: Key,
        length: u64,
        timeout: u64,
    ) -> crate::fast_forward::BoxFuture<'_, bool> {
        Box::pin(async move {
            // every autobee core this crate opens uses manifest version 3, so a
            // core that needs reindexing is legacy storage, which is refused
            let Ok(core) = self.store.get(GetOptions::key(key)).await else {
                return false;
            };
            let mut manifest = core.manifest();
            if manifest.is_none() && length > 0 {
                let options = hypercore::GetOptions {
                    wait: true,
                    timeout: (timeout > 0).then(|| std::time::Duration::from_millis(timeout)),
                    ..Default::default()
                };
                let _ = core.get_raw_local(length - 1).ok();
                let _ = core.get(length - 1, options).await;
                manifest = core.manifest();
            }
            core.close();
            let Some(manifest) = manifest else {
                return false;
            };
            manifest.version < crate::constants::DEFAULT_MANIFEST_VERSION && manifest.version > 1
        })
    }

    fn system_flushes(&self) -> u64 {
        self.flushes.load(Ordering::Acquire)
    }

    fn local_key(&self) -> Key {
        self.local_key
    }

    fn conservative(&self) -> bool {
        self.conservative
    }

    fn forwarding(&self) -> bool {
        let seeds = self.lock_seeds();
        seeds.fast_forwarding || seeds.fast_forward_to.is_some()
    }

    fn open_system(&self) -> crate::fast_forward::BoxFuture<'_, Option<SystemBee>> {
        Box::pin(async move {
            let store = self.store.namespace("reboot");
            let session = store
                .get(GetOptions {
                    name: Some(format!("autobee/{}/view/system", self.id).into_bytes()),
                    manifest_version: Some(crate::constants::DEFAULT_MANIFEST_VERSION),
                    inflight_range: Some((256, 512)),
                    ..Default::default()
                })
                .await
                .ok()?;
            let bee = Bee::from_session(&store, Arc::new(session)).await.ok()?;
            Some(SystemBee::new(bee))
        })
    }

    fn warmup(
        &self,
        view: Bee,
        requests: hypercore::RequestBag,
    ) -> Option<crate::fast_forward::BoxFuture<'static, Result<(), ()>>> {
        self.handlers
            .warmup(view, requests)
            .map(|future| -> crate::fast_forward::BoxFuture<'static, Result<(), ()>> { future })
    }

    fn core_length(
        &self,
        core: Arc<Session>,
        _timeout: u64,
    ) -> crate::fast_forward::BoxFuture<'_, u64> {
        Box::pin(async move { core.length() })
    }

    fn read_legacy_system_info(
        &self,
        _core: Arc<Session>,
        _length: u64,
        _timeout: u64,
    ) -> crate::fast_forward::BoxFuture<'_, Option<crate::types::Info>> {
        // legacy autobase storage is refused at boot; nothing to decode here
        Box::pin(async { None })
    }
}

impl Autobee {
    async fn create_anchor(&self, key: Key, length: u64) -> Result<Link> {
        let node = self
            .applying
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .and_then(|nodes| {
                nodes
                    .iter()
                    .rev()
                    .find(|node| node.key == key && node.length == length)
            })
            .cloned()
            .ok_or_else(|| std::io::Error::other("Anchor node is not in system"))?;
        {
            let system = self.system.lock().await;
            if system
                .get(&key, true)
                .await?
                .is_none_or(|info| info.length < length)
            {
                return Err(std::io::Error::other("Anchor node is not in system").into());
            }
        }

        let mut data = [0u8; 40];
        data[..32].copy_from_slice(&key);
        data[32..].copy_from_slice(&length.to_le_bytes());
        let namespace = hypercore::crypto::hash(&[&data]);
        let manifest_data = compact_encoding::encode(&crate::encoding::ManifestData {
            version: 0,
            legacy_blocks: None,
            namespace: Some(namespace),
        });
        let link = Link { key, length };
        let mut oplog = if node.version <= crate::constants::LEGACY_OPLOG_VERSION {
            crate::encoding::Oplog::V2(crate::encoding::OplogMessageV2 {
                node: crate::encoding::schema::autobase_compat::Node {
                    heads: vec![crate::encoding::Checkout { key, length }],
                    batch: 1,
                    value: None,
                },
                checkpoint: None,
                digest: None,
                optimistic: false,
                trace: None,
            })
        } else {
            crate::encoding::Oplog::V4(crate::encoding::OplogMessageV4 {
                timestamp: 0,
                links: vec![link],
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
            })
        };
        let block = crate::encoding::encode_oplog(&mut oplog);
        let root = compact_encoding::hypercore::Node {
            index: 0,
            size: block.len() as u64,
            hash: hypercore::crypto::data_hash(&block),
        };
        let manifest = compact_encoding::hypercore::Manifest {
            version: 2,
            hash: compact_encoding::hypercore::HashAlgo::Blake2b,
            allow_patch: false,
            quorum: 0,
            signers: vec![],
            prologue: Some(compact_encoding::hypercore::Prologue {
                hash: hypercore::crypto::tree_hash(&[root]),
                length: 1,
            }),
            linked: None,
            user_data: Some(manifest_data),
        };
        let core = self
            .store
            .get(GetOptions {
                manifest: Some(manifest),
                inactive: true,
                ..Default::default()
            })
            .await?;
        let append = if core.length() == 0 {
            core.append_with(
                &[block],
                hypercore::core::AppendOptions {
                    writable: true,
                    max_length: Some(1),
                },
            )
            .await
            .map(|_| ())
        } else {
            Ok(())
        };
        let result = Link {
            key: *core.key(),
            length: core.length(),
        };
        core.close();
        append?;
        self.system.lock().await.add_anchor(result.key);
        Ok(result)
    }
}
