//! `autobee-wakeup`: one wakeup session per capability, announcing the bee's
//! active writers, collecting the hints peers announce, and coupling writer
//! cores so peers that miss them are told about them.

use std::sync::{Arc, Mutex, MutexGuard};

use hypercore::Core;
use protomux::Mux;
use protomux_wakeup::wakeup::{Announce, Writer_};
use protomux_wakeup::{Lookup, SessionHandlers, WakeupSession, WakeupSwarm};

use crate::core_coupler::{CoreCoupler, same_mux};
use crate::types::Link;

/// JS `-1`: a hint whose length the sender does not know. It always wins, so
/// the writer is asked for its head length.
pub const UNKNOWN_LENGTH: u64 = u64::MAX;

/// What `autobee-wakeup` needs from the bee.
pub trait WakeupBase: Send + Sync {
    /// `this.bootstrap`: the core whose peers the coupler follows.
    fn bootstrap(&self) -> Core;
    /// Active, non-pending writers as `key` + head length (JS `_getWakeupWriters`).
    fn wakeup_writers(&self) -> Vec<Link>;
    /// Marks the bee for a drain after hints land (JS `bumpSoon`).
    fn bump_soon(&self);
}

/// The wakeup side of a bee: protocol, session, hints and coupling.
pub struct AutobeeWakeup {
    shared: Arc<Shared>,
    swarm: Arc<WakeupSwarm>,
    owner: bool,
}

struct Shared {
    base: Arc<dyn WakeupBase>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    session: Option<WakeupSession>,
    coupler: Option<Arc<CoreCoupler>>,
    /// Pending hints, in first-seen order, longest length per key.
    hints: Vec<Link>,
    streams: Vec<Mux>,
}

impl AutobeeWakeup {
    /// Owns a fresh wakeup swarm, as JS does without `opts.wakeup`.
    pub fn new(base: Arc<dyn WakeupBase>) -> Self {
        Self::with_swarm(base, Arc::new(WakeupSwarm::new(|_, _| {})), true)
    }

    /// Shares `swarm` with the rest of the node; `close` leaves it alone.
    pub fn with_swarm(base: Arc<dyn WakeupBase>, swarm: Arc<WakeupSwarm>, owner: bool) -> Self {
        Self {
            shared: Arc::new(Shared {
                base,
                state: Mutex::new(State::default()),
            }),
            swarm,
            owner,
        }
    }

    /// Merges hints; a longer length or an unknown one wins (JS `hint`).
    pub fn hint(&self, hints: &[Link]) {
        self.shared.hint(hints);
    }

    /// Takes the hints accumulated since the last flush.
    pub fn flush(&self) -> Vec<Link> {
        std::mem::take(&mut self.shared.lock().hints)
    }

    /// Registers a stream for wakeup and for core coupling.
    pub fn add_stream(&self, mux: &Mux) {
        self.swarm.add_stream(mux);
        let coupler = {
            let mut st = self.shared.lock();
            if !st.streams.iter().any(|m| same_mux(m, mux)) {
                st.streams.push(mux.clone());
            }
            st.coupler.clone()
        };
        if let Some(coupler) = coupler {
            coupler.add_stream(mux);
        }
    }

    /// Replaces the session with one for `capability` (JS `setCapability`).
    pub fn set_capability(&self, capability: [u8; 32], discovery_key: Option<[u8; 32]>) {
        let old = self.shared.lock().session.take();
        if let Some(old) = old {
            old.destroy();
        }

        let lookup = Arc::downgrade(&self.shared);
        let hint_sink = Arc::downgrade(&self.shared);
        let active = Arc::downgrade(&self.shared);
        let session = self.swarm.session(
            capability,
            SessionHandlers {
                discovery_key,
                on_lookup: Some(Box::new(move |_req, peer, session| {
                    let Some(shared) = lookup.upgrade() else {
                        return;
                    };
                    let writers = announce(&shared.base.wakeup_writers());
                    if writers.is_empty() {
                        return;
                    }
                    session.announce(peer, &writers);
                })),
                on_announce: Some(Box::new(move |writers, _, _| {
                    if let Some(shared) = hint_sink.upgrade() {
                        shared.hint(&links(writers));
                    }
                })),
                on_peeractive: Some(Box::new(move |peer, _| {
                    if let Some(shared) = active.upgrade()
                        && let Some(coupler) = shared.coupler()
                    {
                        coupler.update(peer.stream());
                    }
                })),
                ..Default::default()
            },
        );

        let coupler = {
            let mut st = self.shared.lock();
            st.session = Some(session.clone());
            st.coupler.clone()
        };

        // A session can open onto streams that already hold an active peer.
        if let Some(coupler) = coupler {
            for peer in session.peers() {
                if peer.is_active() {
                    coupler.update(peer.stream());
                }
            }
        }
    }

    /// Broadcasts a lookup, asking every peer for the writers it has
    /// (JS `_requestWakeup`).
    pub fn request_lookup(&self) {
        if let Some(session) = self.shared.lock().session.clone() {
            session.broadcast_lookup(&Lookup { hash: None });
        }
    }

    /// Couples the bootstrap core's peers to the wakeup streams (JS `recouple`).
    pub fn recouple(&self) {
        let shared = Arc::downgrade(&self.shared);
        let coupler = Arc::new(CoreCoupler::new(
            self.shared.base.bootstrap(),
            Box::new(move |mux, links| {
                if let Some(shared) = shared.upgrade() {
                    shared.wakeup_peer(mux, links);
                }
            }),
        ));
        let streams = {
            let mut st = self.shared.lock();
            st.coupler = Some(coupler.clone());
            st.streams.clone()
        };
        for mux in &streams {
            coupler.add_stream(mux);
        }
    }

    /// Couples a writer core; false before `recouple`.
    pub fn add_core(&self, core: Core) -> bool {
        match self.shared.coupler() {
            Some(coupler) => {
                coupler.add(core);
                true
            }
            None => false,
        }
    }

    /// Decouples a writer core; false before `recouple`.
    pub fn remove_core(&self, core: &Core) -> bool {
        match self.shared.coupler() {
            Some(coupler) => {
                coupler.remove(core);
                true
            }
            None => false,
        }
    }

    pub fn close(&self) {
        let (session, coupler) = {
            let mut st = self.shared.lock();
            st.streams.clear();
            (st.session.take(), st.coupler.take())
        };
        if let Some(session) = session {
            session.destroy();
        }
        drop(coupler);
        if self.owner {
            self.swarm.destroy();
        }
    }
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn coupler(&self) -> Option<Arc<CoreCoupler>> {
        self.lock().coupler.clone()
    }

    fn hint(&self, hints: &[Link]) {
        {
            let mut st = self.lock();
            for link in hints {
                match st.hints.iter_mut().find(|h| h.key == link.key) {
                    Some(prev) => {
                        if link.length == UNKNOWN_LENGTH || prev.length < link.length {
                            *prev = link.clone();
                        }
                    }
                    None => st.hints.push(link.clone()),
                }
            }
        }
        self.base.bump_soon();
    }

    /// JS `_wakeupPeer`: announce `links` over `mux`, falling back to the
    /// bee's writers when the coupler found no core to name.
    fn wakeup_peer(&self, mux: &Mux, links: &[Link]) {
        let Some(session) = self.lock().session.clone() else {
            return;
        };
        let writers = if links.is_empty() {
            announce(&self.base.wakeup_writers())
        } else {
            announce(links)
        };
        if writers.is_empty() {
            return;
        }
        session.announce_by_stream(mux, &writers);
    }
}

fn announce(links: &[Link]) -> Announce {
    links
        .iter()
        .map(|l| Writer_ {
            key: l.key,
            length: l.length,
        })
        .collect()
}

fn links(writers: &[Writer_]) -> Vec<Link> {
    writers
        .iter()
        .map(|w| Link {
            key: w.key,
            length: w.length,
        })
        .collect()
}
