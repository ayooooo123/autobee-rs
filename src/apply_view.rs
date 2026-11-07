//! `autobee/lib/apply-view.js`, and the `Handlers` object it dispatches to.
//!
//! `Handlers` mirrors the JS `handlers` constructor argument as PearTube uses
//! it (`src/node/channel.js`, `src/apply.js`): `apply`, `update`, `open`,
//! `close`, `isTrusted`, `keyPair`, `wakeupCapability`, `optimistic`,
//! `bootstrapWeight`, plus the boot options `wait`, `encryptionKey` and
//! `fastForward`. Every method has the reference's default, so a handler only
//! overrides what it needs.
//!
//! `ApplyView::close` does not close the bee: hyperbee-rs has no `close` yet;
//! closing the view core is the store session's job.

use hyperbee::Bee;
use hypercore::crypto::KeyPair;

use crate::apply_calls::{ApplyCalls, BoxFuture, Result};
use crate::types::{Head, Key, Link, Node};
use crate::updates::Changes;

/// `wakeupCapability`: `{ key, discoveryKey }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WakeupCapability {
    pub key: Key,
    pub discovery_key: Key,
}

/// `handlers.fastForward` (the `{}` the reference defaults to).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FastForwardOptions {
    /// Oplog head to boot from.
    pub boot: Option<Link>,
    /// Only land on a system and view held whole.
    pub conservative: bool,
}

pub trait Handlers: Send + Sync {
    /// `handlers.open(bee, auto)`: the writable view the handler writes to.
    /// The reference also passes the Autobee; the Rust handler gets it from
    /// the closure that built it instead.
    fn open(&self, bee: Bee) -> Bee {
        bee
    }

    /// `handlers.close(view)`.
    fn close<'a>(&'a self, view: &'a Bee) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let _ = view;
            Ok(())
        })
    }

    /// `handlers.apply(userBatch, view, host)`.
    fn apply<'a, 'b>(
        &'a self,
        nodes: &'a [Node],
        view: &'a Bee,
        host: &'a ApplyCalls<'a, 'b>,
    ) -> BoxFuture<'a, Result<()>>;

    /// `handlers.update(view, changes)`.
    fn update<'a>(&'a self, view: &'a Bee, changes: &'a Changes) -> BoxFuture<'a, ()>;

    /// Whether the handler supplied `update`; the reference only builds the
    /// change records when it did.
    fn has_update(&self) -> bool {
        false
    }

    /// `handlers.now || Date.now`: the clock oplog timestamps are stamped with.
    fn now(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_millis() as u64)
    }

    /// `handlers.preapply(view)`: a one-shot gate the first drain awaits once.
    fn pre_apply<'a>(&'a self, view: &'a Bee) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let _ = view;
        })
    }

    /// `handlers.warmup(view, activeRequests)`. `None` means no hook.
    fn warmup(
        &self,
        view: Bee,
        requests: hypercore::RequestBag,
    ) -> Option<BoxFuture<'static, Result<(), ()>>> {
        let _ = (view, requests);
        None
    }

    /// Whether the handler supplied `mostRecentTrusted`; the reference's
    /// `isTrusted` default is `!mostRecentTrusted`.
    fn has_most_recent_trusted(&self) -> bool {
        false
    }

    /// `handlers.isTrusted(key, reference)`. `reference` is the working view,
    /// sparse after a fast-forward landing.
    fn is_trusted<'a>(&'a self, key: &'a Key, reference: Option<&'a Bee>) -> BoxFuture<'a, bool> {
        Box::pin(async move {
            let _ = (key, reference);
            !self.has_most_recent_trusted()
        })
    }

    /// `handlers.mostRecentTrusted(target, reference)`.
    fn most_recent_trusted<'a>(
        &'a self,
        target: &'a Bee,
        reference: Option<&'a Bee>,
    ) -> BoxFuture<'a, Option<Head>> {
        Box::pin(async move {
            let _ = (target, reference);
            None
        })
    }

    /// `handlers.wait()`, awaited before the store opens.
    fn wait<'a>(&'a self) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }

    /// `handlers.keyPair`; `None` means the store's own key pair.
    fn key_pair(&self) -> Option<KeyPair> {
        None
    }

    /// `handlers.encryptionKey`.
    fn encryption_key(&self) -> Option<Key> {
        None
    }

    /// `handlers.wakeupCapability`.
    fn wakeup_capability(&self) -> Option<WakeupCapability> {
        None
    }

    /// `handlers.fastForward`: `None` disables both wakeup and boot
    /// fast-forwards; `Some(default)` is the reference's `{}`.
    fn fast_forward(&self) -> Option<FastForwardOptions> {
        Some(FastForwardOptions::default())
    }

    /// `handlers.optimistic !== false`.
    fn optimistic(&self) -> bool {
        true
    }

    /// `handlers.bootstrapWeight`: the weight the bootstrap writer is granted.
    fn bootstrap_weight(&self) -> u64 {
        2
    }
}

pub struct ApplyView {
    bee: Bee,
    view: Bee,
}

impl ApplyView {
    /// `ApplyView.open(bee, auto)`.
    pub fn new(bee: Bee, handlers: &dyn Handlers) -> Self {
        let view = handlers.open(bee.clone());
        Self { bee, view }
    }

    pub fn from_parts(bee: Bee, view: Bee) -> Self {
        Self { bee, view }
    }

    /// The underlying tree, before `open` wrapped it.
    pub fn bee(&self) -> &Bee {
        &self.bee
    }

    pub fn view(&self) -> &Bee {
        &self.view
    }

    /// `view.apply(userBatch)`.
    pub fn apply<'a, 'b>(
        &'a self,
        nodes: &'a [Node],
        handlers: &'a dyn Handlers,
        host: &'a ApplyCalls<'a, 'b>,
    ) -> BoxFuture<'a, Result<()>> {
        handlers.apply(nodes, &self.view, host)
    }

    /// `ApplyView.close(view, auto)`.
    pub async fn close_view(view: &Bee, handlers: &dyn Handlers) -> Result<()> {
        handlers.close(view).await
    }

    /// `view.close()`: the handler's close, then the bee's. hyperbee-rs has no
    /// bee close yet, so only the handler runs.
    pub async fn close(&self, handlers: &dyn Handlers) -> Result<()> {
        Self::close_view(&self.view, handlers).await
    }
}
