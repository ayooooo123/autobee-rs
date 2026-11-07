//! `core-coupler`: announces a bee's coupled cores over the streams of the
//! bootstrap core's peers that do not replicate them yet.
//!
//! JS reads `target.peers` and `core.peer-add` to find the other side's
//! protomux instance. hypercore-rs reports `PeerAdd`/`PeerRemove` without the
//! mux, so the consumer hands the coupler the streams it replicates on; every
//! peer event then sweeps those streams.

use std::sync::{Arc, Mutex, MutexGuard, Weak};

use hypercore::replicator::PROTOCOL;
use hypercore::{Core, CoreEvent};
use protomux::Mux;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;

use crate::types::Link;

/// Called with the stream and the writers that stream is missing.
pub type OnWakeup = Box<dyn Fn(&Mux, &[Link]) + Send + Sync>;

/// Couples cores to a target core's peers, as `core-coupler` does.
pub struct CoreCoupler {
    inner: Arc<Inner>,
    watcher: JoinHandle<()>,
}

struct Inner {
    target: Core,
    wakeup: OnWakeup,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// The coupled set, in add order; a core is identified by its key.
    cores: Vec<Core>,
    /// The target core's streams, as far as the consumer reports them.
    streams: Vec<Mux>,
}

impl CoreCoupler {
    /// Follows `target`'s peer events until the coupler is dropped.
    pub fn new(target: Core, wakeup: OnWakeup) -> Self {
        let inner = Arc::new(Inner {
            target,
            wakeup,
            state: Mutex::new(State::default()),
        });
        let watcher = tokio::spawn(watch(Arc::downgrade(&inner)));
        Self { inner, watcher }
    }

    /// Adds `core` (JS `add`): announced over every stream that lacks it.
    pub fn add(&self, core: Core) {
        let streams = {
            let mut st = self.inner.lock();
            if st.cores.iter().any(|c| c.key() == core.key()) {
                return;
            }
            st.cores.push(core.clone());
            st.streams.clone()
        };
        for mux in &streams {
            self.inner.couple(mux, &core);
        }
    }

    pub fn remove(&self, core: &Core) {
        self.inner.lock().cores.retain(|c| c.key() != core.key());
    }

    /// Registers a stream the target replicates on (one per connection).
    pub fn add_stream(&self, mux: &Mux) {
        let coupled = {
            let mut st = self.inner.lock();
            if st.streams.iter().any(|m| same_mux(m, mux)) {
                return;
            }
            st.streams.push(mux.clone());
            st.cores.clone()
        };
        for core in &coupled {
            self.inner.couple(mux, core);
        }
    }

    /// JS `update(stream)`: announces the coupled cores `mux` is missing.
    pub fn update(&self, mux: &Mux) {
        let links = self.inner.missing(mux, None);
        if !links.is_empty() {
            (self.inner.wakeup)(mux, &links);
        }
    }
}

impl Drop for CoreCoupler {
    fn drop(&mut self) {
        self.watcher.abort();
    }
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Whether we replicate `core` over `mux` already (JS `_hasMuxer`).
    fn opened(&self, mux: &Mux, core: &Core) -> bool {
        mux.is_opened(PROTOCOL, Some(core.discovery_key()))
    }

    /// The coupled cores `mux` does not have yet, as writers.
    fn missing(&self, mux: &Mux, only: Option<&Core>) -> Vec<Link> {
        if !self.opened(mux, &self.target) {
            return Vec::new();
        }
        let st = self.lock();
        st.cores
            .iter()
            .filter(|core| only.is_none_or(|want| want.key() == core.key()))
            .filter(|core| !self.opened(mux, core))
            .map(|core| Link {
                key: *core.key(),
                length: core.length(),
            })
            .collect()
    }

    /// Announces `core` if `mux` lacks it (JS `_couple` for one peer).
    fn couple(&self, mux: &Mux, core: &Core) {
        if !self.opened(mux, &self.target) || self.opened(mux, core) {
            return;
        }
        let link = Link {
            key: *core.key(),
            length: core.length(),
        };
        (self.wakeup)(mux, std::slice::from_ref(&link));
    }

    /// JS `_onpeeradd`: a peer appeared somewhere, so announce what it lacks.
    fn sweep(&self) {
        let streams = self.lock().streams.clone();
        for mux in &streams {
            let links = self.missing(mux, None);
            if !links.is_empty() {
                (self.wakeup)(mux, &links);
            }
        }
    }

    /// Drops streams whose connection is gone (nothing to detach on the wire).
    fn prune(&self) {
        self.lock().streams.retain(|mux| !mux.is_destroyed());
    }
}

async fn watch(inner: Weak<Inner>) {
    let Some(up) = inner.upgrade() else {
        return;
    };
    let mut events = up.target.events();
    drop(up);
    loop {
        match events.recv().await {
            Ok(CoreEvent::PeerAdd) => {
                if let Some(up) = inner.upgrade() {
                    up.sweep();
                } else {
                    return;
                }
            }
            Ok(CoreEvent::PeerRemove) => {
                let Some(up) = inner.upgrade() else {
                    return;
                };
                up.prune();
            }
            Ok(_) => {}
            Err(RecvError::Lagged(_)) => {}
            Err(RecvError::Closed) => return,
        }
    }
}

/// `Mux` exposes no id; its shared stream info's address identifies it.
pub(crate) fn same_mux(a: &Mux, b: &Mux) -> bool {
    std::ptr::eq(a.stream_info(), b.stream_info())
}
