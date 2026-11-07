//! The system order hash, a port of `advance_hash` in `autobee/lib/system.js`.
//!
//! Each system node commits to its predecessor, key, length and weight with a
//! 64-bit SipHash-2-4, which is what libsodium's `crypto_shorthash` computes.

use std::hash::Hasher;

use compact_encoding::Writer;
use siphasher::sip::SipHasher24;

/// Bytes in an order hash, `crypto_shorthash_BYTES`.
pub const HASHBYTES: usize = 8;

/// `crypto_shorthash_KEYBYTES` bytes; the JS module uses the ASCII name as the key.
const ORDER_KEY: &[u8; 16] = b"autobee-orderkey";

/// `advanceHash(prev, node)`: SipHash-2-4 over
/// `prev || node.key || node.length || node.weight`, where a missing `prev` is
/// eight zero bytes.
pub fn advance_hash(
    prev: Option<&[u8; HASHBYTES]>,
    key: &[u8; 32],
    length: u64,
    weight: u64,
) -> [u8; HASHBYTES] {
    let prev = prev.copied().unwrap_or([0; HASHBYTES]);

    let mut w = Writer::with_capacity(HASHBYTES + 32 + 10 + 10);
    w.raw(&prev);
    w.raw(key);
    w.uint(length);
    w.uint(weight);

    let mut hasher = SipHasher24::new_with_key(ORDER_KEY);
    hasher.write(&w.into_vec());
    // libsodium stores the 64-bit shorthash little-endian.
    hasher.finish().to_le_bytes()
}
