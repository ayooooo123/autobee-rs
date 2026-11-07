//! `autobee/lib/triggers.js`: writers waiting on a node of another writer,
//! keyed by that writer's id. A writer waits on at most one entry at a time
//! (`writer.waiting`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::types::Key;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// The writer side of a trigger: what `Triggers` reads and calls back into.
pub trait Writer: Send + Sync {
    /// `writer.waiting`: the entry this writer currently waits on.
    fn waiting(&self) -> Option<Entry>;
    fn bump(&self);
}

/// A bucket slot. Kept shared between the bucket and the waiter's `waiting`,
/// as the reference's entry object is: removing it nulls it for both.
#[derive(Clone)]
pub struct Entry(Arc<Mutex<State>>);

impl std::fmt::Debug for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = lock(&self.0);
        f.debug_struct("Entry")
            .field("id", &state.id)
            .field("index", &state.index)
            .field("length", &state.length)
            .field("writer", &state.writer.is_some())
            .finish()
    }
}

struct State {
    index: usize,
    id: Key,
    writer: Option<Arc<dyn Writer>>,
    length: u64,
}

impl Entry {
    fn new(index: usize, id: Key, writer: Option<Arc<dyn Writer>>, length: u64) -> Self {
        Entry(Arc::new(Mutex::new(State {
            index,
            id,
            writer,
            length,
        })))
    }

    pub fn id(&self) -> Key {
        lock(&self.0).id
    }

    pub fn index(&self) -> usize {
        lock(&self.0).index
    }

    pub fn length(&self) -> u64 {
        lock(&self.0).length
    }

    pub fn is_removed(&self) -> bool {
        lock(&self.0).writer.is_none()
    }

    fn writer(&self) -> Option<Arc<dyn Writer>> {
        lock(&self.0).writer.clone()
    }

    fn is_waiting_on(&self, writer: &Arc<dyn Writer>) -> bool {
        let state = lock(&self.0);
        state
            .writer
            .as_ref()
            .is_some_and(|w| Arc::ptr_eq(w, writer))
    }

    fn set_length(&self, length: u64) {
        lock(&self.0).length = length;
    }

    fn set_index(&self, index: usize) {
        lock(&self.0).index = index;
    }
}

#[derive(Default)]
pub struct Triggers {
    waiting: HashMap<Key, Vec<Entry>>,
}

impl Triggers {
    pub fn new() -> Self {
        Self::default()
    }

    /// `triggers.add(id, length, writer)`.
    pub fn add(&mut self, id: Key, length: u64, writer: &Arc<dyn Writer>) -> Entry {
        // a writer only ever waits on one link: drop whatever it waited on before
        if let Some(waiting) = writer.waiting()
            && waiting.id() != id
        {
            self.remove(&waiting);
        }

        let entries = self.waiting.entry(id).or_default();
        for entry in entries.iter() {
            if entry.is_waiting_on(writer) {
                entry.set_length(length);
                return entry.clone();
            }
        }

        let entry = Entry::new(entries.len(), id, Some(writer.clone()), length);
        entries.push(entry.clone());
        entry
    }

    /// `triggers.remove(entry)`. A removed entry is a no-op.
    pub fn remove(&mut self, entry: &Entry) {
        if entry.writer().is_none() {
            return;
        }
        let id = entry.id();
        let Some(entries) = self.waiting.get_mut(&id) else {
            return;
        };
        remove_in(entries, entry);
        if entries.is_empty() {
            self.waiting.remove(&id);
        }
    }

    pub fn has(&self, id: &Key) -> bool {
        self.waiting.contains_key(id)
    }

    pub fn get(&self, id: &Key) -> Option<&[Entry]> {
        self.waiting.get(id).map(Vec::as_slice)
    }

    /// `triggers.trigger(id, length)`: bump every writer waiting on a prefix
    /// that this batch now covers.
    pub fn trigger(&mut self, id: &Key, length: u64) {
        let mut bump = Vec::new();
        let mut empty = false;

        if let Some(entries) = self.waiting.get_mut(id) {
            // the walk swap-removes, so collect the writers and bump after
            let mut i = entries.len();
            while i > 0 {
                i -= 1;
                let entry = entries[i].clone();
                if entry.length() > length {
                    continue;
                }
                if let Some(writer) = entry.writer() {
                    bump.push(writer);
                }
                remove_in(entries, &entry);
            }
            empty = entries.is_empty();
        }

        if empty {
            self.waiting.remove(id);
        }

        for writer in bump {
            writer.bump();
        }
    }
}

/// `triggers._remove(w, entry)`.
fn remove_in(entries: &mut Vec<Entry>, entry: &Entry) {
    let head = entries.pop().expect("bucket is non-empty");
    if !Arc::ptr_eq(&head.0, &entry.0) {
        let index = entry.index();
        head.set_index(index);
        entries[index] = head;
    }
    lock(&entry.0).writer = None;
}
