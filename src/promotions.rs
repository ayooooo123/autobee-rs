//! Writer grant hints and outstanding promotion requests.
use crate::encoding::{self, GrantHint};
use crate::system::WriterUpdate;
use bytes::Bytes;
use hyperbee::{Bee, RangeOptions, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Operation {
    pub key: Bytes,
    pub value: Option<Bytes>,
}

pub struct Promotions {
    bee: Bee,
    pub digest: Vec<bool>,
    pub changed: bool,
    pending: Option<Vec<([u8; 32], u64)>>,
}

impl Promotions {
    pub fn new(bee: Bee) -> Self {
        Self {
            bee,
            digest: Vec::new(),
            changed: false,
            pending: None,
        }
    }

    pub fn reset(&mut self, bee: Bee, digest: Vec<bool>) {
        self.bee = bee;
        self.digest = digest;
        self.pending = None;
    }

    pub async fn hint(&self, key: &[u8; 32]) -> Result<Option<GrantHint>> {
        self.bee
            .get(&encoding::encode_system_grant_key(key))
            .await?
            .map(|entry| encoding::decode_system_grant_hint(entry.value).map_err(Into::into))
            .transpose()
    }

    pub async fn pending(&self, key: &[u8; 32]) -> Result<u64> {
        if let Some(pending) = &self.pending {
            return Ok(pending
                .iter()
                .find(|(k, _)| k == key)
                .map_or(0, |(_, w)| *w));
        }
        match self
            .bee
            .get(&encoding::encode_system_pending_key(key))
            .await?
        {
            Some(entry) => Ok(encoding::decode_system_pending(entry.value)?),
            None => Ok(0),
        }
    }

    pub async fn anchored(&self, key: &[u8; 32]) -> Result<u64> {
        Ok(self.hint(key).await?.map_or(0, |hint| hint.weight))
    }

    pub async fn list(&mut self) -> Result<Vec<([u8; 32], u64)>> {
        self.load().await?;
        Ok(self.pending.as_ref().unwrap().clone())
    }

    async fn load(&mut self) -> Result<()> {
        if self.pending.is_some() {
            return Ok(());
        }
        let (gte, lt) = encoding::encode_system_pending_range();
        let mut stream = self.bee.range(RangeOptions {
            gte: Some(Bytes::copy_from_slice(&gte)),
            lt: Some(Bytes::copy_from_slice(&lt)),
            ..Default::default()
        });
        let mut entries = Vec::new();
        while let Some(entry) = stream.next().await? {
            entries.push((
                *encoding::decode_system_pending_key(&entry.key)?,
                encoding::decode_system_pending(entry.value)?,
            ));
        }
        self.pending = Some(entries);
        Ok(())
    }

    pub async fn update_hint(&self, upd: &WriterUpdate) -> Result<Option<Operation>> {
        if upd.grant.is_none() && !upd.is_removed {
            return Ok(None);
        }
        let key = Bytes::copy_from_slice(&encoding::encode_system_grant_key(&upd.key));
        let existing = self.bee.get(&key).await?;
        if upd.is_removed {
            return Ok(existing.map(|_| Operation { key, value: None }));
        }
        let grant = upd.grant.as_ref().expect("grant checked above");
        if let Some(entry) = existing
            && encoding::decode_system_grant_hint(entry.value)?.weight >= grant.weight
        {
            return Ok(None);
        }
        Ok(Some(Operation {
            key,
            value: Some(encoding::encode_system_grant_hint(grant)),
        }))
    }

    pub async fn flush_pending(&mut self, updates: &[WriterUpdate]) -> Result<Vec<Operation>> {
        if !updates
            .iter()
            .any(|u| u.pending.is_some() || u.grant.is_some() || u.is_removed)
        {
            return Ok(Vec::new());
        }
        self.load().await?;
        if self.pending.as_ref().unwrap().is_empty() && !updates.iter().any(|u| u.pending.is_some())
        {
            return Ok(Vec::new());
        }
        let mut ops = Vec::new();
        let mut anchored_by = Vec::new();
        for upd in updates {
            let index = self
                .pending
                .as_ref()
                .unwrap()
                .iter()
                .position(|(k, _)| *k == upd.key);
            let key = Bytes::copy_from_slice(&encoding::encode_system_pending_key(&upd.key));
            if upd.is_removed {
                if let Some(i) = index {
                    self.pending.as_mut().unwrap().remove(i);
                    ops.push(Operation { key, value: None });
                }
                continue;
            }
            if upd.pending.is_none() && upd.grant.is_none() {
                continue;
            }
            let anchored = self
                .anchored(&upd.key)
                .await?
                .max(upd.grant.as_ref().map_or(0, |g| g.weight));
            anchored_by.push((upd.key, anchored));
            let existing = index
                .map(|i| self.pending.as_ref().unwrap()[i].1)
                .unwrap_or(0);
            let want = existing.max(upd.pending.unwrap_or(0));
            if want > anchored {
                if index.is_some() && want == existing {
                    continue;
                }
                if let Some(i) = index {
                    self.pending.as_mut().unwrap()[i].1 = want;
                } else {
                    self.pending.as_mut().unwrap().push((upd.key, want));
                }
                ops.push(Operation {
                    key,
                    value: Some(encoding::encode_system_pending(want)),
                });
                self.changed = true;
            } else if let Some(i) = index {
                self.pending.as_mut().unwrap().remove(i);
                ops.push(Operation { key, value: None });
            }
        }
        let mut digest = Vec::new();
        for (key, weight) in self.pending.as_ref().unwrap() {
            let anchored = match anchored_by.iter().find(|(k, _)| k == key) {
                Some((_, weight)) => *weight,
                None => self.anchored(key).await?,
            };
            if anchored < *weight {
                digest.resize(digest.len().max(*weight as usize), false);
                for w in anchored + 1..=*weight {
                    digest[(w - 1) as usize] = true;
                }
            }
        }
        self.digest = digest;
        Ok(ops)
    }
}
