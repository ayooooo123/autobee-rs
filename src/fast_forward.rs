//! `autobee/lib/fast-forward.js`: the boot-time landing search.
//!
//! A fast-forward boots a scratch [`SystemBee`] at a candidate head and returns
//! the head an integrator may land on; it never moves the shared system or any
//! head. The search itself is pure over the [`Host`] surface below.
//!
//! The reference reaches into a concrete `Autobee`; a Rust port cannot, so the
//! slice it touches lives behind [`Host`]:
//!
//! * oplog reads and core sessions (`openCore`, `readOplog`)
//! * view checkouts and their handler close (`openView`, `openViewAt`)
//! * the trust cache (`isTrusted`, `mostRecentTrusted`)
//! * the reindex probe and the two counters the landing rules compare
//! * the scratch (`reboot` namespace) system bee
//! * the warmup hook and the two `migrations.js` primitives used by the legacy
//!   crawl, which `crate::migrations` does not expose yet
//!
//! Cancellation is one [`RequestBag`] shared by every read the fast-forward
//! issues. The bag is handed to the warmup hook as well: hyperbee-rs takes read
//! options per call rather than an `activeRequests` list, so a bag is the only
//! way to cancel a handler's warmup together with the landing's reads. The
//! writer-record reads of [`SystemBee::get`] take no options, so they are the
//! one place the reference's `{ timeout, activeRequests }` cannot reach.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use hyperbee::{Bee, Head as BeeHead};
use hypercore::core::Session;
use hypercore::{GetOptions, RequestBag};
use tokio::task::JoinHandle;

use crate::constants::{DEFAULT_OP_TIMEOUT, LEGACY_AUTOBASE_VERSION, LEGACY_OPLOG_VERSION};
use crate::system::SystemBee;
use crate::types::{Head, Key, Node};

/// How far behind the local system a candidate may be before the ordinary
/// boot carries it; below this a landing is not worth booting.
pub const MIN_FF_GAP: u64 = 16;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The `auto` surface `fast-forward.js` reads. Every method maps a call site of
/// the reference; the doc comment names it.
pub trait Host: Send + Sync {
    /// `auto.openCore(key)` and `auto.store.get({ key, ... })`.
    fn open_core(&self, key: Key) -> BoxFuture<'_, Option<Arc<Session>>>;

    /// `auto.store.get({ key, encryption: null })` after `setEncryption`.
    fn open_legacy_core(&self, key: Key) -> BoxFuture<'_, Option<Arc<Session>>>;

    /// `auto.readOplog(core, length, { timeout })`, decoded.
    fn read_oplog(
        &self,
        core: Arc<Session>,
        length: u64,
        timeout: u64,
    ) -> BoxFuture<'_, Option<Node>>;

    /// `auto.openView(head, { timeout })`: the checkout the handler opened.
    fn open_view(&self, head: Head, timeout: u64) -> BoxFuture<'_, Option<Bee>>;

    /// `opened.close()`: the handler's close for a view opened here.
    fn close_view(&self, view: Bee) -> BoxFuture<'_, ()>;

    /// `auto.trusted.isTrusted(key, auto._workingView.view)`.
    fn is_trusted(&self, key: Key) -> BoxFuture<'_, bool>;

    /// `auto.trusted.mostRecentTrusted(view, auto._workingView.view)`.
    fn most_recent_trusted(&self, view: Bee) -> BoxFuture<'_, Option<Head>>;

    /// `auto._shouldReindex(key, { unknown: true, length, timeout })`.
    ///
    /// The reference catches a throwing probe and treats it as `false`; the
    /// host does that mapping, so this never fails.
    fn should_reindex(&self, key: Key, length: u64, timeout: u64) -> BoxFuture<'_, bool>;

    /// `auto.system.flushes`.
    fn system_flushes(&self) -> u64;

    /// `auto.local.key`.
    fn local_key(&self) -> Key;

    /// `auto._conservativeFF`.
    fn conservative(&self) -> bool;

    /// `auto.fastForwarding !== null || auto.fastForwardTo !== null`.
    fn forwarding(&self) -> bool;

    /// The system bee over the store's `reboot` namespace.
    fn open_system(&self) -> BoxFuture<'_, Option<SystemBee>>;

    /// `auto._warmup(view)`: `None` when the handler supplied no hook. The
    /// `Err` arm is a rejected warmup promise: it fails the landing.
    fn warmup(&self, view: Bee, requests: RequestBag)
    -> Option<BoxFuture<'static, Result<(), ()>>>;

    /// `migrations.coreLength(core, timeout)`.
    fn core_length(&self, core: Arc<Session>, timeout: u64) -> BoxFuture<'_, u64>;

    /// `migrations.readLegacySystemInfo(core, length, { timeout })`.
    fn read_legacy_system_info(
        &self,
        core: Arc<Session>,
        length: u64,
        timeout: u64,
    ) -> BoxFuture<'_, Option<crate::types::Info>>;
}

/// What `run()` hands back: the head the integrator may boot onto. The
/// reference's `migrate` field is always `null` and nobody reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Landing {
    pub head: Head,
}

/// `constructor(auto, head, { timeout, conservative })`.
#[derive(Debug, Clone, Copy)]
pub struct Options {
    pub timeout: u64,
    pub conservative: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_OP_TIMEOUT,
            conservative: false,
        }
    }
}

/// The `{ force, skipGap, rewind, timeout }` bag the `from*` helpers take.
/// `timeout: 0` reaches the oplog reads as "no timeout", exactly like `if
/// (timeout)` in hypercore; only the fast-forward it builds falls back to
/// [`DEFAULT_OP_TIMEOUT`].
#[derive(Debug, Clone, Copy, Default)]
pub struct FromOptions {
    pub force: bool,
    pub skip_gap: bool,
    pub rewind: bool,
    pub timeout: u64,
}

pub struct FastForward {
    pub conservative: bool,
    pub active_requests: RequestBag,
    pub system: SystemBee,
    /// `{ key, length }`; the key is known even when the original length was.
    pub head: Head,
    pub timeout: u64,
    pub destroyed: bool,
    pub failed: bool,
    pub cores: Vec<Arc<Session>>,
    warmup: Option<JoinHandle<Result<(), ()>>>,
    warmup_view: Option<Bee>,
}

impl FastForward {
    pub const DEFAULT_TIMEOUT: u64 = DEFAULT_OP_TIMEOUT;

    pub fn new(system: SystemBee, head: Head, options: Options) -> Self {
        Self {
            conservative: options.conservative,
            active_requests: RequestBag::new(),
            system,
            head,
            timeout: if options.timeout == 0 {
                DEFAULT_OP_TIMEOUT
            } else {
                options.timeout
            },
            destroyed: false,
            failed: false,
            cores: Vec::new(),
            warmup: None,
            warmup_view: None,
        }
    }

    pub async fn from_head(
        host: &dyn Host,
        head: Head,
        trusted: Option<Head>,
        opts: FromOptions,
    ) -> Option<FastForward> {
        let oplog = Self::flush_head(host, head, opts.timeout).await?;

        let verified = match trusted {
            Some(trusted) => Self::flush_head(host, trusted, opts.timeout).await?,
            None => oplog,
        };

        Self::from_verified(host, &verified, opts).await
    }

    pub async fn from_verified(
        host: &dyn Host,
        verified: &Node,
        opts: FromOptions,
    ) -> Option<FastForward> {
        let conservative = !opts.force && host.conservative();

        // legacy nodes from non-indexers have no system info to fast-forward from
        let views = verified.views.as_ref()?;

        // never move backwards, even when forced or the gap is skipped
        if !opts.rewind && views.flushes < host.system_flushes() {
            return None;
        }

        // a negative gap (only reachable with `rewind`) is 0 and fails the gap,
        // as `x - y < MIN_FF_GAP` does on a negative number
        if !opts.force
            && !opts.skip_gap
            && views.flushes.saturating_sub(host.system_flushes()) < MIN_FF_GAP
        {
            return None;
        }

        let system = host.open_system().await?;
        let head = batch_to_head(views.system.key, views.system.start, views.system.length);

        Some(Self::new(
            system,
            head,
            Options {
                timeout: opts.timeout,
                conservative,
            },
        ))
    }

    pub async fn from_heads(
        host: &dyn Host,
        heads: &[Head],
        opts: FromOptions,
    ) -> Option<FastForward> {
        let hints = Self::flush_heads(host, heads, opts.timeout).await;
        if host.forwarding() {
            return None;
        }

        let mut trust = Vec::with_capacity(hints.len());
        for hint in &hints {
            trust.push(host.is_trusted(hint.key).await);
        }
        if host.forwarding() {
            return None;
        }

        let mut landings = Oplogs::default();
        let mut unverified = Oplogs::default();
        let mut unresolved = Vec::new();

        for (hint, trusted) in hints.iter().zip(trust) {
            if trusted {
                landings.insert(hint);
            } else {
                unresolved.push(hint);
            }
        }

        // the reference resolves these concurrently, so their insertion order is
        // a race; probing in hint order keeps the ranking deterministic
        let mut resolved = Vec::new();
        for hint in unresolved {
            resolved.extend(Self::signalled(host, hint, opts.timeout).await);
        }
        for (oplog, trusted) in &resolved {
            if *trusted {
                landings.insert(oplog);
            } else {
                unverified.insert(oplog);
            }
        }
        if host.forwarding() {
            return None;
        }

        let ranked = landings.ranked();
        let mut reindexed = Vec::with_capacity(ranked.len());
        for res in &ranked {
            reindexed.push(Self::is_reindexed(host, res, opts.timeout).await);
        }

        for (res, reindexed) in ranked.into_iter().zip(reindexed) {
            if !reindexed {
                continue;
            }
            if host.forwarding() {
                return None;
            }

            if let Some(ff) = Self::from_verified(host, res, opts).await {
                return Some(ff);
            }
        }

        for sig in unverified.ranked() {
            let Some(views) = sig.views.as_ref() else {
                continue;
            };
            if views.flushes < host.system_flushes() {
                continue;
            }
            if !opts.force
                && !opts.skip_gap
                && views.flushes.saturating_sub(host.system_flushes()) < MIN_FF_GAP
            {
                continue;
            }

            let Some(head) = Self::most_recent_trusted(host, sig, opts.timeout).await else {
                continue;
            };
            if host.forwarding() {
                return None;
            }

            let Some(verified) = Self::flush_head(host, head, opts.timeout).await else {
                continue;
            };
            if !Self::is_reindexed(host, &verified, opts.timeout).await {
                continue;
            }
            if host.forwarding() {
                return None;
            }

            if let Some(ff) = Self::from_verified(host, &verified, opts).await {
                return Some(ff);
            }
        }

        None
    }

    pub async fn flush_heads(host: &dyn Host, heads: &[Head], timeout: u64) -> Vec<Node> {
        let mut oplogs = Vec::new();
        for head in heads {
            if head.length == 0 {
                continue;
            }
            if let Some(node) = Self::flush_head(host, *head, timeout).await {
                oplogs.push(node);
            }
        }
        oplogs
    }

    pub async fn signalled(host: &dyn Host, res: &Node, timeout: u64) -> Vec<(Node, bool)> {
        let mut heads = Vec::new();
        for entry in &res.trusted {
            // `trusted.read(entry)` is the identity on a truthy entry
            if entry.length > 0 {
                heads.push(Head {
                    key: Some(entry.key),
                    length: entry.length,
                });
            }
        }

        let oplogs = Self::flush_heads(host, &heads, timeout).await;
        let mut trust = Vec::with_capacity(oplogs.len());
        for oplog in &oplogs {
            trust.push(host.is_trusted(oplog.key).await);
        }

        oplogs.into_iter().zip(trust).collect()
    }

    /// Never rejects in the reference: a throwing probe counts as "reindexed",
    /// which fails the search; [`Host::should_reindex`] folds errors to `false`.
    pub async fn is_reindexed(host: &dyn Host, res: &Node, timeout: u64) -> bool {
        // the reference throws here and its caller folds that into "no landing"
        let Some(views) = res.views.as_ref() else {
            return false;
        };

        let mut batches = vec![(views.system.key, views.system.start, views.system.length)];
        if let Some(view) = &views.view {
            batches.push((view.key, view.start, view.length));
        }

        let mut checks = Vec::with_capacity(batches.len());
        for (key, start, length) in batches {
            let head = batch_to_head(key, start, length);
            match head.key {
                Some(key) if head.length != 0 => {
                    checks.push(host.should_reindex(key, head.length, timeout).await);
                }
                _ => checks.push(false),
            }
        }

        !checks.into_iter().any(|should| should)
    }

    pub async fn flush_head(host: &dyn Host, head: Head, timeout: u64) -> Option<Node> {
        let key = head.key?;
        let core = host.open_core(key).await?;

        let oplog = host.read_oplog(core.clone(), head.length, timeout).await;
        let result = match oplog {
            Some(oplog) if oplog.views.is_some() => Some(oplog),
            _ => None,
        };

        core.close();
        result
    }

    pub async fn most_recent_trusted(host: &dyn Host, head: &Node, timeout: u64) -> Option<Head> {
        let views = head.views.as_ref()?;
        let view = views.view.as_ref()?;
        let opened = host
            .open_view(batch_to_head(view.key, view.start, view.length), timeout)
            .await?;

        let result = host.most_recent_trusted(opened.clone()).await;
        host.close_view(opened).await;
        result
    }

    /// `run()`: search, then close. The reference memoises `_run` on the
    /// instance; a Rust caller holds `&mut self`, so a second `run` searches
    /// again instead of joining the first.
    pub async fn run(&mut self, host: &dyn Host) -> Option<Landing> {
        let result = match self.run_inner(host).await {
            Ok(result) => result,
            Err(()) => {
                self.failed = true;
                None
            }
        };

        self.close(host).await;
        result
    }

    async fn run_inner(&mut self, host: &dyn Host) -> Result<Option<Landing>, ()> {
        // an uninitialised head has an unknown length: wait for the core to catch up
        if self.head.length == 0 {
            self.head.length = self.resolve_length(host).await;
        }

        // zero-length boot is unsupported
        if self.head.length == 0 {
            return Ok(None);
        }

        let Some(head) = bee_head(self.head) else {
            return Ok(None);
        };
        // the reference passes `{ timeout }` to every system read, bounding each
        // block read: a peer that serves the oplog head but stalls its system
        // core must not wedge the search (and, before, the drain behind it). A
        // whole boot walks several blocks, so one budget for all of it failed
        // every fast-forward whose boot took longer than a single read may.
        let timeout = Some(Duration::from_millis(self.timeout));
        self.system.boot(head, timeout).await.map_err(|_| ())?;

        if self.system.version <= LEGACY_AUTOBASE_VERSION && !self.run_legacy(host).await {
            return Ok(None);
        }

        self.start_warmup(host).await;

        // ensure local key is locally available always
        let local = host.local_key();
        self.system
            .get(&local, false, timeout)
            .await
            .map_err(|_| ())?;

        // a legacy system without the designated view migrates onto an empty view
        let mut view = None;
        if let Some(view_head) = self.system.view.filter(|head| head.length != 0)
            && let Some(core) = host.open_core(view_head.key).await
        {
            self.cores.push(core.clone());
            let opts = GetOptions {
                timeout,
                bag: Some(self.active_requests.clone()),
                ..Default::default()
            };
            // a timeout or cancellation rejects `Promise.all` in the reference
            core.get(view_head.length - 1, opts).await.map_err(|_| ())?;
            view = Some(core);
        }

        for head in self.system.heads.clone() {
            self.system
                .get(&head.key, false, timeout)
                .await
                .map_err(|_| ())?;
        }

        if let Some(warmup) = self.warmup.take() {
            match warmup.await {
                Ok(Ok(())) => {}
                _ => return Err(()),
            }
        }

        if self.destroyed {
            return Ok(None);
        }

        // conservative: the lengths we land on must be held whole, either by us
        // or by a connected peer, so the view does not stall on a block nobody has
        if self.conservative {
            let Some(head) = self.head.key else {
                return Ok(None);
            };
            if !self.is_contiguous(host, head).await {
                return Ok(None);
            }
            if let (Some(view), Some(view_head)) = (view, self.system.view)
                && !(view.contiguous_length() >= view_head.length
                    || view.remote_contiguous_length() >= view_head.length)
            {
                return Ok(None);
            }
        }

        Ok(Some(Landing { head: self.head }))
    }

    async fn is_contiguous(&self, host: &dyn Host, system_key: Key) -> bool {
        let Some(core) = host.open_core(system_key).await else {
            return false;
        };
        let contiguous = core.contiguous_length() >= self.head.length
            || core.remote_contiguous_length() >= self.head.length;
        core.close();
        contiguous
    }

    async fn start_warmup(&mut self, host: &dyn Host) -> bool {
        let Some(view_head) = self.system.view.filter(|head| head.length != 0) else {
            return false;
        };
        let Some(view) = host.open_view(bee_head_view(view_head), self.timeout).await else {
            return false;
        };

        let Some(warmup) = host.warmup(view.clone(), self.active_requests.clone()) else {
            host.close_view(view).await;
            return false;
        };

        self.warmup_view = Some(view);
        self.warmup = Some(tokio::spawn(warmup));
        true
    }

    async fn resolve_length(&mut self, host: &dyn Host) -> u64 {
        let Some(key) = self.head.key else {
            return 0;
        };
        let Some(core) = host.open_core(key).await else {
            return 0;
        };
        let length = host.core_length(core.clone(), self.timeout).await;
        self.cores.push(core);
        length
    }

    async fn run_legacy(&mut self, host: &dyn Host) -> bool {
        let Some((head, autobee)) = self.resolve_legacy(host).await else {
            return false;
        };
        let timeout = Some(Duration::from_millis(self.timeout));

        if let Some(autobee) = autobee
            && let Some(views) = autobee.views.as_ref()
        {
            self.head = batch_to_head(views.system.key, views.system.start, views.system.length);
            if let Some(head) = bee_head(self.head)
                && self.system.boot(head, timeout).await.is_ok()
                && self.system.version > LEGACY_AUTOBASE_VERSION
            {
                return true;
            }
        }

        self.head = head;
        match bee_head(self.head) {
            Some(head) => self.system.boot(head, timeout).await.is_ok(),
            None => false,
        }
    }

    /// The legacy crawl of `_resolveLegacy`: public because it is a self
    /// contained phase the reference's `_run` calls, and the only place the
    /// hop and cycle bounds live.
    pub async fn resolve_legacy(&mut self, host: &dyn Host) -> Option<(Head, Option<Node>)> {
        let mut head = self.head;
        let mut seen = HashSet::new();

        // bound at 5 hops for safety
        let mut hops = 5i32;
        loop {
            seen.insert(head.key?);

            let core = host.open_legacy_core(head.key?).await?;
            self.cores.push(core.clone());

            // successors are resolved by key only - wait for the replicated length
            if head.length == 0 {
                head.length = host.core_length(core.clone(), self.timeout).await;
            }
            if head.length == 0 || self.destroyed {
                return None;
            }

            let info = host
                .read_legacy_system_info(core, head.length, self.timeout)
                .await?;
            let (legacy, autobee) = self.find_newest_system(host, &info).await;
            if self.destroyed {
                return None;
            }

            // an autobee candidate ends the crawl: it may be behind, but once we
            // are on autobee the ordinary fast-forward carries us to the newest
            if autobee.is_some() {
                return Some((head, autobee));
            }

            if hops > 0 {
                hops -= 1;
                if let Some(legacy) = legacy
                    && !seen.contains(&legacy)
                {
                    head = Head {
                        key: Some(legacy),
                        length: 0,
                    };
                    continue;
                }
            }

            return Some((head, None));
        }
    }

    async fn find_newest_system(
        &mut self,
        host: &dyn Host,
        info: &crate::types::Info,
    ) -> (Option<Key>, Option<Node>) {
        let mut autobee = None;
        let mut legacy = None;
        let mut flushes = -1i64;

        for idx in info.indexers.iter().flatten() {
            let Some(key) = idx.key else {
                continue;
            };
            let Some(core) = host.open_core(key).await else {
                continue;
            };
            self.cores.push(core.clone());

            let length = host.core_length(core, self.timeout).await;
            if length == 0 || length <= idx.length || self.destroyed {
                continue;
            }

            let oplog = Self::flush_head(
                host,
                Head {
                    key: Some(key),
                    length,
                },
                self.timeout,
            )
            .await;
            let Some(oplog) = oplog else {
                continue;
            };
            let Some(views) = oplog.views.as_ref() else {
                continue;
            };

            // this indexer has already moved to autobee: follow it rather than
            // digging a legacy stamp out of its history
            if oplog.version > LEGACY_OPLOG_VERSION {
                if autobee
                    .as_ref()
                    .is_none_or(|current: &Node| views.flushes > flushes_of(current))
                {
                    autobee = Some(oplog);
                }
                continue;
            }

            let sys = &views.system;

            // follow whichever indexer has checkpointed furthest
            if views.flushes as i64 > flushes {
                flushes = views.flushes as i64;
                legacy = Some(sys.key);
            }
        }

        (legacy, autobee)
    }

    /// `clearRequests(err)`: fails every read in flight and any that come after.
    pub fn clear_requests(&self, reason: impl Into<String>) {
        self.active_requests.destroy(reason);
    }

    pub async fn close(&mut self, host: &dyn Host) {
        self.destroyed = true;
        self.clear_requests("fast forward closed");

        // close view to cancel inflight reads
        if let Some(view) = self.warmup_view.take() {
            host.close_view(view).await;
        }

        if let Some(warmup) = self.warmup.take() {
            let _ = warmup.await;
        }

        // hyperbee-rs has no bee close; the system's sessions are the store's
        for core in std::mem::take(&mut self.cores) {
            core.close();
        }
    }
}

fn flushes_of(node: &Node) -> u64 {
    node.views.as_ref().map_or(0, |views| views.flushes)
}

fn batch_to_head(key: Key, start: u64, length: u64) -> Head {
    Head {
        key: Some(key),
        length: start + length,
    }
}

fn bee_head(head: Head) -> Option<BeeHead> {
    head.key.map(|key| BeeHead {
        key,
        length: head.length,
    })
}

fn bee_head_view(head: BeeHead) -> Head {
    Head {
        key: Some(head.key),
        length: head.length,
    }
}

/// The reference's `Map` keyed by `hex(key):length`, keeping first-seen order.
#[derive(Default)]
struct Oplogs<'a> {
    entries: Vec<(Key, u64, &'a Node)>,
}

impl<'a> Oplogs<'a> {
    fn insert(&mut self, node: &'a Node) {
        if let Some(entry) = self
            .entries
            .iter_mut()
            .find(|(key, length, _)| *key == node.key && *length == node.length)
        {
            entry.2 = node;
        } else {
            self.entries.push((node.key, node.length, node));
        }
    }

    /// `byFlushes`: a stable sort, newest flushes first.
    fn ranked(&self) -> Vec<&'a Node> {
        let mut ranked: Vec<&Node> = self.entries.iter().map(|(_, _, node)| *node).collect();
        ranked.sort_by_key(|node| std::cmp::Reverse(flushes_of(node)));
        ranked
    }
}
