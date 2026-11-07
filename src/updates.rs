//! `autobee/lib/updates.js`: the per-core change records handed to the update
//! handler, `{ flushes, from, to, incremental }` for the view and system trees.
//!
//! The reference reads `auto.system.flushes` and the two live `bee.head()`
//! values; this port takes those at `track` and `finalise` time instead, so it
//! holds no reference back into the Autobee.

use crate::types::Head;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Change {
    /// System flushes at the moment `track` or `finalise` was called.
    pub flushes: u64,
    pub from: Option<Head>,
    pub to: Option<Head>,
    /// False only when a fast-forward landed the tree.
    pub incremental: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Changes {
    pub view: Option<Change>,
    pub system: Option<Change>,
}

impl Changes {
    /// `changes.get(name)`.
    pub fn get(&self, name: &str) -> Option<&Change> {
        match name {
            "view" => self.view.as_ref(),
            "system" => self.system.as_ref(),
            _ => None,
        }
    }
}

#[derive(Debug, Default)]
pub struct UpdateChanges {
    tracked: bool,
    from_view: Option<Head>,
    from_system: Option<Head>,
}

impl UpdateChanges {
    pub fn new() -> Self {
        Self::default()
    }

    /// `changes.track()`: remember both heads before the update, so a
    /// fast-forward landing is visible as a jump. The reference also snapshots
    /// the flush count here and overwrites it in `finalise`, so it is not kept.
    pub fn track(&mut self, view: Option<Head>, system: Option<Head>) {
        self.tracked = true;
        self.from_view = view;
        self.from_system = system;
    }

    /// `changes.finalise(fastForward)`: close the records against the current
    /// heads. `flushes` is read live by the reference, so it is passed in.
    pub fn finalise(
        &mut self,
        view: Option<Head>,
        system: Option<Head>,
        flushes: u64,
        fast_forward: bool,
    ) -> Changes {
        if !self.tracked {
            return Changes::default();
        }
        let incremental = !fast_forward;
        let change = |from: Option<Head>, to: Option<Head>| Change {
            flushes,
            from,
            to,
            incremental,
        };
        Changes {
            view: Some(change(self.from_view, view)),
            system: Some(change(self.from_system, system)),
        }
    }
}
