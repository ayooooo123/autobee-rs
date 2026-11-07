//! Witness weight resolution.
use crate::types::{Key, Link, Node};

#[derive(Debug, Clone, Copy)]
pub struct Standing {
    pub weight: i64,
    pub max_weight: i64,
    pub is_genesis: bool,
}

pub fn current_weight(record: Option<&Standing>) -> i64 {
    match record {
        None => 0,
        Some(record) if record.max_weight > 0 && record.is_genesis => record.max_weight,
        Some(record) => record.weight,
    }
}

#[allow(async_fn_in_trait)]
pub trait Reader {
    type Error;
    fn bootstrap_key(&self) -> &Key;
    fn bootstrap_weight(&self) -> i64;
    async fn standing(&mut self, key: &Key) -> Result<Option<Standing>, Self::Error>;
    async fn oplog_node(&mut self, link: &Link) -> Result<Node, Self::Error>;
}

pub async fn resolve_weight<R: Reader>(reader: &mut R, node: &Node) -> Result<i64, R::Error> {
    let record = reader.standing(&node.key).await?;
    let prev = match record.as_ref() {
        Some(record) => current_weight(Some(record)),
        None if node.length == 1 && &node.key == reader.bootstrap_key() => {
            reader.bootstrap_weight()
        }
        None => 0,
    };
    let Some(witness) = &node.witness else {
        return Ok(prev);
    };
    let weight = i64::try_from(witness.weight).unwrap_or(i64::MAX);
    if weight <= prev || !verify_claim(reader, &node.key, weight, &witness.link).await? {
        return Ok(prev);
    }
    Ok(weight)
}

pub async fn verify_claim<R: Reader>(
    reader: &mut R,
    key: &Key,
    weight: i64,
    link: &Link,
) -> Result<bool, R::Error> {
    if link.length == 0 {
        return Ok(false);
    }
    let anchor = reader.oplog_node(link).await?;
    let Some(approval) = anchor
        .approvals
        .as_ref()
        .and_then(|approvals| approvals.iter().find(|approval| &approval.key == key))
    else {
        return Ok(false);
    };
    if i64::try_from(approval.weight).unwrap_or(i64::MAX) < weight {
        return Ok(false);
    }
    if &link.key == reader.bootstrap_key() {
        return Ok(reader.bootstrap_weight() >= i64::try_from(approval.weight).unwrap_or(i64::MAX));
    }
    let record = reader.standing(&link.key).await?;
    Ok(current_weight(record.as_ref()) >= i64::try_from(approval.weight).unwrap_or(i64::MAX))
}
