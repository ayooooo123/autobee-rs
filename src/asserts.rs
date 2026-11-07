//! `autobee/lib/asserts.js`: the assertion failures the reference throws, with
//! the same messages and the `ERR_ASSERTION` code.

use std::fmt;

use crate::types::{Key, Link};

/// `err.code` of every failure this module raises.
pub const CODE: &str = "ERR_ASSERTION";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssertionError {
    pub message: String,
}

impl AssertionError {
    pub fn code(&self) -> &'static str {
        CODE
    }
}

impl fmt::Display for AssertionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{CODE}: {}", self.message)
    }
}

impl std::error::Error for AssertionError {}

/// `asserts.bail(msg)`.
pub fn bail(message: &str) -> AssertionError {
    AssertionError {
        message: message.to_string(),
    }
}

/// `asserts.assert(cond, msg)`.
pub fn assert(cond: bool, message: &str) -> Result<(), AssertionError> {
    if cond { Ok(()) } else { Err(bail(message)) }
}

/// `asserts.heads(heads, node)`: no head may name the node's core twice. The
/// reference takes the node and only reads `node.key`.
pub fn heads(heads: &[Link], key: &Key) -> Result<(), AssertionError> {
    for head in heads {
        if head.key == *key {
            return Err(bail("Same node included twice"));
        }
    }
    Ok(())
}

/// `asserts.systemFlush(w)`: a system batch must contain an applied system op.
/// `ops` yields `(applied, key)` for each op of the writer batch.
pub fn system_flush<'a>(
    ops: impl IntoIterator<Item = (bool, &'a [u8])>,
) -> Result<(), AssertionError> {
    for (applied, key) in ops {
        if applied && key.first() == Some(&1) {
            return Ok(());
        }
    }
    Err(bail("Invalid system batch"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn link(key: u8) -> Link {
        Link {
            key: [key; 32],
            length: 1,
        }
    }

    #[test]
    fn messages_match_the_reference() {
        assert_eq!(
            bail("Invalid system batch").to_string(),
            "ERR_ASSERTION: Invalid system batch"
        );
        assert!(assert(false, "nope").is_err());
        assert!(assert(true, "nope").is_ok());
    }

    #[test]
    fn heads_rejects_a_repeated_core() {
        assert!(heads(&[link(1), link(2)], &[2; 32]).is_err());
        assert!(heads(&[link(1)], &[2; 32]).is_ok());
    }

    #[test]
    fn system_flush_needs_an_applied_system_op() {
        assert!(system_flush([(true, &[1u8][..])]).is_ok());
        assert!(system_flush([(false, &[1u8][..])]).is_err());
        assert!(system_flush([(true, &[2u8][..])]).is_err());
        assert!(system_flush(Vec::new()).is_err());
    }
}
