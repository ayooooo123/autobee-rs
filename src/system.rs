//! System tree and writer membership state.
use crate::encoding::{
    self, GrantHint, Link, SystemInfo, SystemInfoV3, SystemInfoValue, SystemWriter,
    SystemWriterInfo, SystemWriterV5,
};
use crate::order_hash::advance_hash;
use crate::promotions::{Operation, Promotions};
use crate::types::Node;
use bytes::Bytes;
use hyperbee::{Bee, Error, Head, RangeOptions, Result};

const INFO_KEY: &[u8] = &[0];
const LEGACY_INFO_KEY: &[u8] = b"\0\0info";

#[derive(Clone, Debug, PartialEq)]
pub struct WriterInfo {
    pub key: [u8; 32],
    pub is_removed: bool,
    pub is_oplog: bool,
    pub is_genesis: bool,
    pub is_added: bool,
    pub weight: u64,
    pub max_weight: u64,
    pub length: u64,
    pub timestamp: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriterUpdate {
    pub key: [u8; 32],
    pub length: u64,
    pub weight: Option<u64>,
    pub max_weight: Option<u64>,
    pub is_genesis: bool,
    pub is_added: bool,
    pub is_removed: bool,
    pub timestamp: Option<u64>,
    pub grant: Option<GrantHint>,
    pub pending: Option<u64>,
}

impl WriterUpdate {
    pub fn new(key: [u8; 32]) -> Self {
        Self {
            key,
            length: 0,
            weight: None,
            max_weight: None,
            is_genesis: false,
            is_added: false,
            is_removed: false,
            timestamp: None,
            grant: None,
            pending: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct AddWriterOptions {
    pub length: u64,
    pub weight: u64,
    pub is_genesis: bool,
    pub coord: Option<Link>,
    pub carrier: i64,
    pub anchor: bool,
}

impl Default for AddWriterOptions {
    fn default() -> Self {
        Self {
            length: 0,
            weight: 1,
            is_genesis: false,
            coord: None,
            carrier: -1,
            anchor: false,
        }
    }
}

pub struct SystemSnapshot {
    bee: Bee,
}
impl SystemSnapshot {
    pub async fn get(&self, key: &[u8; 32]) -> Result<Option<WriterInfo>> {
        read_writer(&self.bee, key).await
    }
    pub async fn has(&self, link: &Link) -> Result<bool> {
        Ok(self
            .get(&link.key)
            .await?
            .is_some_and(|w| w.length >= link.length))
    }
}

pub struct SystemBee {
    pub bee: Bee,
    pub view: Option<Head>,
    pub heads: Vec<Link>,
    pub hash: Option<[u8; 8]>,
    pub version: u64,
    pub timestamp: u64,
    pub flushes: u64,
    pub updates: Vec<WriterUpdate>,
    pub writers: Vec<([u8; 32], bool)>,
    pub promotions: Promotions,
}

impl SystemBee {
    pub fn new(bee: Bee) -> Self {
        Self {
            promotions: Promotions::new(bee.clone()),
            bee,
            view: None,
            heads: Vec::new(),
            hash: None,
            version: 0,
            timestamp: 0,
            flushes: 0,
            updates: Vec::new(),
            writers: Vec::new(),
        }
    }

    pub fn is_genesis(&self) -> bool {
        self.bee.head().is_none_or(|h| h.length == 0)
    }
    pub fn is_head(&self, key: &[u8; 32]) -> bool {
        self.heads.iter().any(|h| &h.key == key)
    }
    pub fn boot_record(&self) -> Option<Head> {
        self.bee.head().filter(|h| h.length != 0)
    }
    pub fn get_links(&self, key: Option<&[u8; 32]>) -> Vec<Link> {
        self.heads
            .iter()
            .filter(|h| key != Some(&h.key))
            .cloned()
            .collect()
    }

    pub async fn get_info(&self) -> Result<Option<SystemInfoValue>> {
        let mut node = self.bee.get(INFO_KEY).await?;
        if node.is_none() {
            node = self.bee.get(LEGACY_INFO_KEY).await?;
        }
        node.map(|n| encoding::decode_system_info(n.value).map_err(Into::into))
            .transpose()
    }

    pub async fn reset(&mut self) -> Result<()> {
        let info = self.get_info().await?;
        match info {
            None => {
                self.version = 0;
                self.view = None;
                self.heads.clear();
                self.hash = None;
                self.timestamp = 0;
                self.flushes = self.bee.length() / 2;
                self.promotions.reset(self.bee.clone(), Vec::new());
            }
            Some(SystemInfoValue::V3(info)) => {
                self.version = 3;
                self.view = Some(Head {
                    key: info.view.key,
                    length: info.view.length,
                });
                self.heads = info.heads;
                self.hash = info.hash;
                self.timestamp = info.timestamp;
                self.flushes = info.flushes;
                self.promotions
                    .reset(self.bee.clone(), info.pending.unwrap_or_default());
            }
            Some(_) => {
                return Err(Error::Message(
                    "LEGACY_AUTOBASE_STORAGE: legacy autobase storage is not migrated",
                ));
            }
        }
        self.updates.clear();
        self.writers.clear();
        Ok(())
    }

    pub async fn boot(&mut self, head: Head) -> Result<()> {
        self.bee.move_to(head).await?;
        self.reset().await
    }
    pub async fn undo(&mut self, head: Head) -> Result<Option<Head>> {
        self.bee.move_to(head).await?;
        self.reset().await?;
        Ok(self.view)
    }
    pub async fn snapshot(&self, head: Option<Head>) -> Result<SystemSnapshot> {
        Ok(SystemSnapshot {
            bee: match head {
                Some(head) => self.bee.checkout(head).await?,
                None => self.bee.snapshot().await?,
            },
        })
    }

    pub fn add_node(&mut self, node: &Node) -> Result<()> {
        let weight =
            u64::try_from(node.weight).map_err(|_| Error::Message("negative node weight"))?;
        self.heads.retain(|h| {
            !((h.key == node.key && node.length >= h.length)
                || node
                    .links
                    .iter()
                    .any(|l| l.key == h.key && l.length >= h.length))
        });
        self.heads.push(Link {
            key: node.key,
            length: node.length,
        });
        self.timestamp = self.timestamp.max(node.timestamp);
        self.hash = Some(advance_hash(
            self.hash.as_ref(),
            &node.key,
            node.length,
            weight,
        ));
        let mut update = WriterUpdate::new(node.key);
        update.length = node.length;
        update.weight = Some(weight);
        update.timestamp = Some(node.timestamp);
        self.update(update);
        Ok(())
    }

    pub async fn get(&self, key: &[u8; 32], unflushed: bool) -> Result<Option<WriterInfo>> {
        let mut info = read_writer(&self.bee, key).await?;
        if !unflushed {
            return Ok(info);
        }
        let Some(upd) = self.updates.iter().find(|u| &u.key == key) else {
            return Ok(info);
        };
        let item = info.get_or_insert(WriterInfo {
            key: *key,
            is_removed: upd.is_removed,
            is_oplog: false,
            is_genesis: upd.max_weight.is_some() && upd.is_genesis,
            is_added: upd.is_added,
            weight: upd.weight.unwrap_or(0),
            max_weight: upd.max_weight.unwrap_or(0),
            length: upd.length,
            timestamp: 0,
        });
        if upd.is_added {
            item.is_added = true;
        }
        if upd.is_removed {
            item.is_removed = true;
        }
        if let Some(w) = upd.max_weight {
            item.is_genesis = upd.is_genesis;
            item.max_weight = w;
        }
        if let Some(w) = upd.weight {
            item.weight = w;
        }
        item.length = item.length.max(upd.length);
        Ok(info)
    }

    pub async fn has(&self, link: &Link) -> Result<bool> {
        if let Some(h) = self.heads.iter().find(|h| h.key == link.key) {
            return Ok(h.length >= link.length);
        }
        Ok(self
            .get(&link.key, false)
            .await?
            .is_some_and(|w| w.length >= link.length))
    }
    pub async fn list(&self) -> Result<Vec<WriterInfo>> {
        let mut stream = self.bee.range(RangeOptions {
            gte: Some(Bytes::from_static(&[1])),
            lt: Some(Bytes::from_static(&[2])),
            ..Default::default()
        });
        let mut list = Vec::new();
        while let Some(e) = stream.next().await? {
            list.push(decode_writer(&e.key, e.value)?);
        }
        Ok(list)
    }
    pub async fn can_apply(&self, key: &[u8; 32], optimistic: bool) -> Result<bool> {
        if optimistic {
            return Ok(true);
        }
        if let Some((_, added)) = self.writers.iter().find(|(k, _)| k == key) {
            return Ok(*added);
        }
        Ok(self.get(key, false).await?.is_some_and(|v| !v.is_removed))
    }

    pub async fn add_writer(&mut self, key: [u8; 32], opts: AddWriterOptions) -> Result<()> {
        let info = self.get(&key, true).await?;
        let length = if opts.length == 0 {
            info.as_ref().map_or(0, |w| w.length)
        } else {
            opts.length
        };
        let effective = if opts.coord.is_some() && opts.carrier < opts.weight as i64 {
            opts.carrier.max(0) as u64
        } else {
            opts.weight
        };
        let hint = if opts.anchor {
            opts.coord.as_ref().map(|c| GrantHint {
                key: c.key,
                length: c.length,
                weight: effective,
            })
        } else {
            None
        };
        let pending = if opts.coord.is_some() && !opts.is_genesis {
            Some(opts.weight)
        } else {
            None
        };
        let current = info.as_ref().map_or(0, |w| w.max_weight);
        let mut update = WriterUpdate::new(key);
        update.length = length;
        update.is_added = true;
        update.grant = hint;
        update.pending = pending;
        if effective <= current {
            if info
                .as_ref()
                .is_some_and(|w| w.is_added && !w.is_removed && length <= w.length)
                && update.grant.is_none()
                && pending.is_none()
            {
                return Ok(());
            }
        } else {
            update.weight = opts.is_genesis.then_some(effective);
            update.max_weight = Some(effective);
            update.is_genesis = opts.is_genesis;
        }
        self.update(update);
        Ok(())
    }
    pub async fn ack_writer(&mut self, key: [u8; 32], length: u64) -> Result<()> {
        let info = self.get(&key, true).await?;
        let granted = self
            .writers
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, a)| *a)
            .unwrap_or_else(|| info.as_ref().is_some_and(|w| w.is_added && !w.is_removed));
        let mut upd = WriterUpdate::new(key);
        upd.length = if length == 0 {
            info.map_or(0, |w| w.length)
        } else {
            length
        };
        upd.is_removed = !granted;
        self.update(upd);
        Ok(())
    }
    pub async fn remove_writer(&mut self, key: [u8; 32], length: u64) -> Result<()> {
        let mut upd = WriterUpdate::new(key);
        upd.length = if length == 0 {
            self.get(&key, true).await?.map_or(0, |w| w.length)
        } else {
            length
        };
        upd.max_weight = Some(0);
        upd.is_removed = true;
        self.update(upd);
        Ok(())
    }
    pub fn add_anchor(&mut self, key: [u8; 32]) {
        let mut u = WriterUpdate::new(key);
        u.is_added = true;
        self.update(u);
    }

    pub fn update(&mut self, update: WriterUpdate) {
        if update.is_added || update.is_removed {
            let added = update.is_added;
            if let Some((_, value)) = self.writers.iter_mut().find(|(k, _)| *k == update.key) {
                *value = added;
            } else {
                self.writers.push((update.key, added));
            }
        }
        let index = match self.updates.iter().position(|u| u.key == update.key) {
            Some(i) => i,
            None => {
                self.updates.push(WriterUpdate::new(update.key));
                self.updates.len() - 1
            }
        };
        let upd = &mut self.updates[index];
        if let Some(w) = update.weight {
            upd.weight = Some(upd.weight.map_or(w, |prev| prev.max(w)));
        }
        if let Some(w) = update.max_weight {
            upd.max_weight = Some(w);
            upd.is_genesis = update.is_genesis;
        }
        if update.is_added {
            upd.is_added = true;
            upd.is_removed = false;
        }
        if update.is_removed {
            upd.is_removed = true;
            upd.is_added = false;
        }
        upd.length = upd.length.max(update.length);
        if update.timestamp.is_some() {
            upd.timestamp = update.timestamp;
        }
        if let Some(p) = update.pending {
            upd.pending = Some(upd.pending.map_or(p, |old| old.max(p)));
        }
        if let Some(grant) = update.grant
            && upd.grant.as_ref().is_none_or(|g| grant.weight > g.weight)
        {
            upd.grant = Some(grant);
        }
    }

    /// Flush the recorded node batch and return membership changes and ordered bee operations.
    pub async fn flush(
        &mut self,
        batch: &[Node],
        view: Head,
    ) -> Result<(Vec<([u8; 32], bool)>, Vec<Operation>)> {
        let Some(last) = batch.last() else {
            return Ok((Vec::new(), Vec::new()));
        };
        let mut ops = Vec::new();
        let mut grants = Vec::new();
        for upd in &self.updates {
            let previous = read_writer(&self.bee, &upd.key).await?;
            let is_oplog = upd.key == last.key;
            let writer = SystemWriterV5 {
                is_removed: if upd.is_removed {
                    true
                } else if upd.is_added {
                    false
                } else {
                    previous.as_ref().is_some_and(|v| v.is_removed)
                },
                is_oplog,
                is_genesis: if upd.max_weight.is_some() {
                    upd.is_genesis
                } else {
                    previous.as_ref().is_some_and(|v| v.is_genesis)
                },
                weight: upd
                    .weight
                    .unwrap_or_else(|| previous.as_ref().map_or(0, |v| v.weight)),
                max_weight: upd
                    .max_weight
                    .unwrap_or_else(|| previous.as_ref().map_or(0, |v| v.max_weight)),
                length: upd.length.max(previous.as_ref().map_or(0, |v| v.length)),
                timestamp: if is_oplog {
                    upd.timestamp.unwrap_or(0)
                } else {
                    0
                },
            };
            ops.push(Operation {
                key: Bytes::copy_from_slice(&encoding::encode_system_writer_key(&upd.key)),
                value: Some(encoding::encode_system_writer(&SystemWriter::V5(writer))),
            });
            if let Some(op) = self.promotions.update_hint(upd).await? {
                grants.push(op);
            }
        }
        let pending = self.promotions.flush_pending(&self.updates).await?;
        ops.extend(grants);
        ops.extend(pending);
        let flushes = self.flushes + 1;
        let info = SystemInfoV3 {
            timestamp: self.timestamp,
            flushes,
            view: Link {
                key: view.key,
                length: view.length,
            },
            heads: self.heads.clone(),
            indexers: None,
            pending: Some(self.promotions.digest.clone()),
            hash: self.hash,
        };
        ops.push(Operation {
            key: Bytes::from_static(INFO_KEY),
            value: Some(encoding::encode_system_info(&SystemInfo::V3(info))),
        });
        let mut write = self.bee.batch();
        for op in &ops {
            match &op.value {
                Some(value) => write.put(op.key.clone(), value.clone()),
                None => write.del(op.key.clone()),
            }
        }
        write.flush().await?;
        self.version = 3;
        self.view = Some(view);
        self.flushes = flushes;
        self.updates.clear();
        Ok((std::mem::take(&mut self.writers), ops))
    }

    pub async fn pending_promotion(&self, key: &[u8; 32]) -> Result<u64> {
        self.promotions.pending(key).await
    }
    pub async fn list_pending_promotions(&mut self) -> Result<Vec<([u8; 32], u64)>> {
        self.promotions.list().await
    }
    pub async fn grant_hint(&self, key: &[u8; 32]) -> Result<Option<GrantHint>> {
        self.promotions.hint(key).await
    }
}

async fn read_writer(bee: &Bee, key: &[u8; 32]) -> Result<Option<WriterInfo>> {
    bee.get(&encoding::encode_system_writer_key(key))
        .await?
        .map(|e| decode_writer(&e.key, e.value))
        .transpose()
}
pub(crate) fn decode_writer(key: &[u8], value: Bytes) -> Result<WriterInfo> {
    let decoded = encoding::decode_system_writer(key, value)?;
    let key: [u8; 32] = decoded
        .key
        .as_ref()
        .try_into()
        .map_err(|_| Error::Message("invalid system writer key"))?;
    let (is_removed, is_oplog, is_genesis, weight, max_weight, length, timestamp) =
        match decoded.value {
            SystemWriterInfo::Legacy(v) => (
                v.is_removed,
                v.is_oplog,
                v.is_genesis,
                v.weight,
                v.max_weight,
                v.length,
                v.timestamp.max(0) as u64,
            ),
            SystemWriterInfo::V4(v) => (
                v.is_removed,
                v.is_oplog,
                v.is_genesis,
                v.weight,
                v.max_weight,
                v.length,
                v.timestamp.unwrap_or(0).max(0) as u64,
            ),
            SystemWriterInfo::V5(v) => (
                v.is_removed,
                v.is_oplog,
                v.is_genesis,
                v.weight,
                v.max_weight,
                v.length,
                v.timestamp,
            ),
        };
    Ok(WriterInfo {
        key,
        is_removed,
        is_oplog,
        is_genesis,
        is_added: true,
        weight,
        max_weight,
        length,
        timestamp,
    })
}
