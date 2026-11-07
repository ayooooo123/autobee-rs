//! Deterministic ordering and replay of oplog batches.
use std::cmp::Ordering;
use std::collections::HashMap;

use crate::types::{Batch, Change, Head, Key, Link, Node, SortResult, WeightedLink};

#[derive(Default)]
pub struct Clock(HashMap<Key, u64>);

impl Clock {
    pub fn add(&mut self, link: &Link) {
        let length = self.0.entry(link.key).or_default();
        *length = (*length).max(link.length);
    }

    pub fn has(&self, link: &Link) -> bool {
        self.0
            .get(&link.key)
            .is_some_and(|length| *length >= link.length)
    }
}

pub fn cmp(a: &Node, b: &WeightedLink) -> Ordering {
    b.weight
        .cmp(&a.weight)
        .then_with(|| a.timestamp.cmp(&b.timestamp))
        .then_with(|| a.key.cmp(&b.key))
        .then_with(|| {
            if a.length < b.length {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        })
}

pub fn is_ordered_before(node: &Node, oplog: &WeightedLink) -> bool {
    cmp(node, oplog) == Ordering::Less
}

fn weighted(node: &Node) -> WeightedLink {
    WeightedLink {
        key: node.key,
        length: node.length,
        weight: node.weight,
        timestamp: node.timestamp,
    }
}

struct Entry {
    batch: Batch,
    index: Option<usize>,
}

fn first(batch: &Batch) -> &Node {
    batch
        .first()
        .and_then(Option::as_ref)
        .expect("ordering requires a complete first node")
}

fn add_sorted(list: &mut Vec<Entry>, entry: Entry) -> usize {
    let node = first(&entry.batch);
    let mut clock = Clock::default();
    clock.add(&Link {
        key: node.key,
        length: node.length,
    });
    for link in &node.links {
        clock.add(link);
    }
    if let Some(witness) = &node.witness {
        clock.add(&witness.link);
    }
    let mut tip = Vec::new();
    let mut linked = Vec::new();
    while let Some(last) = list.last() {
        if cmp(node, &weighted(first(&last.batch))) == Ordering::Greater {
            break;
        }
        let item = list.pop().unwrap();
        let target = first(&item.batch);
        if !clock.has(&Link {
            key: target.key,
            length: target.length,
        }) {
            tip.push(item);
            continue;
        }
        for link in &target.links {
            clock.add(link);
        }
        linked.push(item);
    }
    while let Some(item) = linked.pop() {
        add_sorted(list, item);
    }
    list.push(entry);
    while let Some(item) = tip.pop() {
        list.push(item);
    }
    list.iter()
        .enumerate()
        .take_while(|(i, item)| item.index == Some(*i))
        .count()
}

pub fn linearize(batches: Vec<Batch>) -> Vec<Batch> {
    let mut list = Vec::new();
    for batch in batches {
        add_sorted(&mut list, Entry { batch, index: None });
    }
    list.into_iter().map(|entry| entry.batch).collect()
}

pub fn is_linking(node: &Node, link: &Link) -> bool {
    (node.key == link.key && node.length >= link.length)
        || node
            .links
            .iter()
            .any(|l| l.key == link.key && l.length >= link.length)
}

pub fn is_linking_all(node: &Node, heads: &[Link]) -> bool {
    heads.iter().all(|head| is_linking(node, head))
}

/// A system change in reverse chronological order. `None` marks unavailable history.
/// `refs` must contain the oplog entries selected by the system change decoder,
/// including all legacy candidates when none is explicitly marked as an oplog.
pub struct HistoryEntry {
    pub legacy: bool,
    pub refs: Vec<Change>,
}

/// The integration owns core sessions and decodes oplog blocks into nodes.
/// `history` starts a changes stream and `next_change` advances it once;
/// outer `None` ends the stream, inner `None` marks unavailable history.
/// A missing block is `Ok(None)`; `wait = false` must never wait for peers.
#[allow(async_fn_in_trait)]
pub trait Reader {
    type Error;
    async fn history(&mut self, wait: bool) -> Result<(), Self::Error>;
    async fn next_change(&mut self) -> Result<Option<Option<HistoryEntry>>, Self::Error>;
    async fn node(
        &mut self,
        key: &Key,
        length: u64,
        weight: i64,
        wait: bool,
        timeout: u64,
    ) -> Result<Option<Node>, Self::Error>;
}

pub struct InflatedBatch {
    pub undo: Option<Head>,
    pub batch: Batch,
    pub index: usize,
}

pub async fn get_oplog_batch<R: Reader>(
    reader: &mut R,
    change: &Change,
    index: usize,
    wait: bool,
    timeout: u64,
) -> Result<Option<InflatedBatch>, R::Error> {
    let oplog = &change.oplog;
    let Some(mut head) = reader
        .node(&oplog.key, oplog.length, oplog.weight, wait, timeout)
        .await?
    else {
        return Ok(None);
    };
    let seq = oplog.length - 1;
    if head.version <= crate::encoding::LEGACY_OPLOG_VERSION {
        for i in (1..oplog.length).rev() {
            let Some(previous) = reader.node(&oplog.key, i, oplog.weight, false, 0).await? else {
                break;
            };
            if previous.batch.end == 0 {
                break;
            }
            head.batch.start += 1;
        }
    }
    let start = seq - head.batch.start;
    let end = seq + head.batch.end;
    let mut batch = Vec::new();
    for i in start..end {
        batch.push(
            reader
                .node(&oplog.key, i + 1, oplog.weight, wait, timeout)
                .await?,
        );
    }
    batch.push(Some(head));
    crate::writers::finalize_node_batch(&mut batch);
    Ok(Some(InflatedBatch {
        undo: change.undo,
        batch,
        index,
    }))
}

pub async fn inflate<R: Reader>(
    reader: &mut R,
    changes: Vec<Option<Change>>,
    wait: bool,
    timeout: u64,
) -> Result<Vec<Option<InflatedBatch>>, R::Error> {
    let mut result = Vec::with_capacity(changes.len());
    for (i, change) in changes.into_iter().enumerate() {
        result.push(match change {
            Some(change) => get_oplog_batch(reader, &change, i, wait, timeout).await?,
            None => None,
        });
    }
    Ok(result)
}

pub async fn sort<R: Reader>(reader: &mut R, batch: Batch) -> Result<Option<SortResult>, R::Error> {
    let node = first(&batch);
    let mut reorder = Vec::new();
    reader.history(true).await?;
    while let Some(data) = reader.next_change().await? {
        let data = data.expect("incomplete system history during sort");
        if data.legacy {
            break;
        }
        let change = data
            .refs
            .into_iter()
            .next()
            .expect("system change has no oplog");
        if !is_ordered_before(node, &change.oplog) {
            break;
        }
        reorder.push(Some(change));
    }
    reorder.reverse();
    let inflated = inflate(reader, reorder, true, 0).await?;
    let Some(inflated) = inflated.into_iter().collect::<Option<Vec<_>>>() else {
        return Ok(None);
    };
    let undos: Vec<_> = inflated.iter().map(|entry| entry.undo).collect();
    let mut list: Vec<_> = inflated
        .into_iter()
        .map(|entry| Entry {
            batch: entry.batch,
            index: Some(entry.index),
        })
        .collect();
    let shared = add_sorted(&mut list, Entry { batch, index: None });
    Ok(Some(SortResult {
        undo: undos.get(shared).copied().flatten(),
        view: None,
        tip: list
            .into_iter()
            .skip(shared)
            .map(|entry| entry.batch)
            .collect(),
    }))
}

pub async fn replay<R: Reader>(reader: &mut R) -> Result<Vec<Option<Node>>, R::Error> {
    let mut refs = Vec::new();
    if reader.history(false).await.is_err() {
        refs.push(None);
    } else {
        loop {
            match reader.next_change().await {
                Ok(Some(Some(entry))) => refs.extend(entry.refs.into_iter().map(Some)),
                Ok(None) => break,
                Ok(Some(None)) | Err(_) => {
                    refs.push(None);
                    break;
                }
            }
        }
    }
    refs.reverse();
    let mut nodes = Vec::new();
    for batch in inflate(reader, refs, false, 0).await? {
        match batch {
            Some(batch) => nodes.extend(batch.batch),
            None => nodes.push(None),
        }
    }
    Ok(nodes)
}
