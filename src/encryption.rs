//! Autobee 2.12.1 block encryption, a port of `autobee/lib/encryption.js`.
//!
//! Block layout, all in place: [`PADDING`] bytes of plaintext header followed by
//! the payload. The header is `[type, hash, hash, hash, id; 4]` for encrypted
//! blocks, where `hash` is the first eight bytes of a keyed BLAKE2b-128 of the
//! payload and `id` a little-endian `u32` block id. Payloads are XORed with
//! XSalsa20 under a nonce of the block index followed by the header.
//!
//! Pre-v3 manifests ("compat" cores) use hypercore's own default encryption
//! instead, via [`BlockEncryption`].

use std::fmt;
use std::sync::LazyLock;

use blake2b_simd::Params;
use bytes::Bytes;
use compact_encoding::Reader;
use hypercore::crypto;
use hypercore::encryption::{BlockEncryption, EncryptionOptions};
use salsa20::XSalsa20;
use salsa20::cipher::{KeyIvInit, StreamCipher};

/// Bytes of padding in front of every autobee block.
pub const PADDING: usize = 8;

/// Manifest version from which views use autobee's own block encryption.
pub const DEFAULT_MANIFEST_VERSION: u64 = 3;

/// Last manifest version stored with hypercore's default encryption.
const COMPAT_MANIFEST_VERSION: u64 = 1;

/// `crypto.namespace('autobase', 4)`; the port only needs the view block key and
/// hash key namespaces.
static NAMESPACES: LazyLock<[[u8; 32]; 4]> = LazyLock::new(|| {
    crypto::namespace(b"autobase", 4)
        .try_into()
        .expect("four autobase namespaces")
});

/// Block key namespace, `NS_VIEW_BLOCK_KEY`.
const NS_VIEW_BLOCK_KEY: usize = 1;
/// Hash key namespace, `NS_HASH_KEY`.
const NS_HASH_KEY: usize = 2;

/// `crypto.namespace('autobase/entropy', 2)[0]`; block keys are always keyed by
/// the genesis entropy in this version.
static GENESIS_ENTROPY: LazyLock<[u8; 32]> =
    LazyLock::new(|| crypto::namespace(b"autobase/entropy", 2)[0]);

/// Something the JS implementation throws on.
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// The block is shorter than the padding prefix.
    BlockTooShort,
    /// A view refuses to encrypt below [`DEFAULT_MANIFEST_VERSION`], as `ViewEncryption.encrypt`.
    UnsupportedManifestVersion,
    /// The plaintext type byte is neither 0 (unencrypted) nor 1 (encrypted).
    UnrecognisedEncryptionType(u8),
    /// `manifest.user_data` is not an `@autobee/manifest-data`.
    ManifestData,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BlockTooShort => f.write_str("block shorter than the padding prefix"),
            Self::UnsupportedManifestVersion => f.write_str("unexpected manifest version"),
            Self::UnrecognisedEncryptionType(t) => write!(f, "unrecognised encryption type {t}"),
            Self::ManifestData => f.write_str("invalid manifest user data"),
        }
    }
}

impl std::error::Error for Error {}

/// The manifest fields key derivation reads.
#[derive(Clone)]
pub struct ManifestContext {
    /// `manifest.version`.
    pub version: u64,
    /// The core key this provider is attached to, `ctx.key`.
    pub key: [u8; 32],
    /// `manifest.user_data`, decoded for a namespace override.
    pub user_data: Option<Vec<u8>>,
}

#[derive(Clone)]
enum Kind {
    /// A view core, identified by its name when pre-v3 compat keys are needed.
    View { name: Option<String> },
    /// A writer core.
    Writer,
}

/// Encrypts and decrypts the blocks of one core.
#[derive(Clone)]
pub struct AutobeeEncryption {
    bootstrap: [u8; 32],
    encryption_key: [u8; 32],
    kind: Kind,
    manifest: ManifestContext,
}

impl AutobeeEncryption {
    /// A view core (`getViewEncryption`).
    pub fn view(
        bootstrap: [u8; 32],
        encryption_key: [u8; 32],
        name: Option<&str>,
        manifest: ManifestContext,
    ) -> Self {
        Self {
            bootstrap,
            encryption_key,
            kind: Kind::View {
                name: name.map(str::to_owned),
            },
            manifest,
        }
    }

    /// The system core, `getSystemEncryption`.
    pub fn system(
        bootstrap: [u8; 32],
        encryption_key: [u8; 32],
        manifest: ManifestContext,
    ) -> Self {
        Self::view(bootstrap, encryption_key, Some("_system"), manifest)
    }

    /// A writer core (`getWriterEncryption`).
    pub fn writer(
        bootstrap: [u8; 32],
        encryption_key: [u8; 32],
        manifest: ManifestContext,
    ) -> Self {
        Self {
            bootstrap,
            encryption_key,
            kind: Kind::Writer,
            manifest,
        }
    }

    /// Bytes of padding in front of every block.
    pub fn padding(&self) -> usize {
        PADDING
    }

    /// Whether hypercore's default encryption is in use, `isCompat`.
    pub fn is_compat(&self) -> bool {
        self.manifest.version <= COMPAT_MANIFEST_VERSION
    }

    /// Encrypts a [`PADDING`] byte header followed by the payload, in place.
    pub fn encrypt(&self, index: u64, block: &mut [u8], fork: u64) -> Result<(), Error> {
        if block.len() < PADDING {
            return Err(Error::BlockTooShort);
        }

        // ViewEncryption refuses pre-v3 manifests before it ever reaches the compat path.
        if matches!(&self.kind, Kind::View { .. })
            && self.manifest.version < DEFAULT_MANIFEST_VERSION
        {
            return Err(Error::UnsupportedManifestVersion);
        }

        if self.is_compat() {
            self.compat_cipher().encrypt(index, block, fork);
            return Ok(());
        }

        let block_key = self.block_key(&GENESIS_ENTROPY)?;
        encrypt_block(index, block, 0, &block_key, &hash_key(&block_key))
    }

    /// Decrypts the payload in place from a [`PADDING`] byte header; a type 0
    /// header leaves the payload untouched.
    pub fn decrypt(&self, index: u64, block: &mut [u8]) -> Result<(), Error> {
        if block.len() < PADDING {
            return Err(Error::BlockTooShort);
        }

        if self.is_compat() {
            self.compat_cipher().decrypt(index, block);
            return Ok(());
        }

        match block[0] {
            // Unencrypted: the payload is stored as-is.
            0 => Ok(()),
            1 => {
                let block_key = self.block_key(&GENESIS_ENTROPY)?;
                let mut cipher = XSalsa20::new(
                    (&block_key).into(),
                    (&nonce(index, &block[..PADDING])).into(),
                );
                cipher.apply_keystream(&mut block[PADDING..]);
                Ok(())
            }
            other => Err(Error::UnrecognisedEncryptionType(other)),
        }
    }

    /// Key derivation for the block id `0`, `getKeys`.
    fn block_key(&self, entropy: &[u8; 32]) -> Result<[u8; 32], Error> {
        match &self.kind {
            Kind::View { name } => {
                // Pre-v3 views key by the entropy in place of the encryption key,
                // exactly as ViewEncryption.blockKey does.
                if self.manifest.version < DEFAULT_MANIFEST_VERSION {
                    return Ok(compat_block_key(
                        &self.bootstrap,
                        entropy,
                        name.as_deref().unwrap_or_default().as_bytes(),
                    ));
                }
                Ok(view_block_key(
                    &self.bootstrap,
                    &self.encryption_key,
                    entropy,
                    &self.manifest.key,
                ))
            }
            Kind::Writer => {
                if let Some(user_data) = &self.manifest.user_data
                    && let Some(namespace) = manifest_namespace(user_data)?
                {
                    return Ok(view_block_key(
                        &self.bootstrap,
                        &self.encryption_key,
                        entropy,
                        &namespace,
                    ));
                }
                Ok(view_block_key(
                    &self.bootstrap,
                    &self.encryption_key,
                    entropy,
                    &self.manifest.key,
                ))
            }
        }
    }

    /// The compat core's hypercore encryption keys, `compatKeys`.
    fn compat_cipher(&self) -> BlockEncryption {
        match &self.kind {
            Kind::View { name } => {
                let block = compat_block_key(
                    &self.bootstrap,
                    &self.encryption_key,
                    name.as_deref().unwrap_or_default().as_bytes(),
                );
                BlockEncryption::new(
                    &block,
                    &self.manifest.key,
                    EncryptionOptions {
                        block: true,
                        compat: false,
                    },
                )
            }
            Kind::Writer => BlockEncryption::new(
                &self.encryption_key,
                &self.manifest.key,
                EncryptionOptions::default(),
            ),
        }
    }

    /// `encryptAnchor`: encrypts a block with index 0 and block id 0, keyed by
    /// `namespace` and with a hash key derived from the plaintext.
    pub fn encrypt_anchor(
        block: &mut [u8],
        bootstrap: &[u8; 32],
        encryption_key: &[u8; 32],
        namespace: &[u8; 32],
    ) -> Result<(), Error> {
        let block_key = view_block_key(bootstrap, encryption_key, &GENESIS_ENTROPY, namespace);
        // Unlike a regular block, the anchor's hash key commits to the plaintext.
        let hash_key = crypto::hash(&[&NAMESPACES[NS_HASH_KEY], block]);
        encrypt_block(0, block, 0, &block_key, &hash_key)
    }
}

/// `crypto.hash([NS_HASH_KEY, block])`.
fn hash_key(block_key: &[u8; 32]) -> [u8; 32] {
    crypto::hash(&[&NAMESPACES[NS_HASH_KEY], block_key])
}

/// `getBlockKey`.
fn view_block_key(
    bootstrap: &[u8; 32],
    encryption_key: &[u8; 32],
    entropy: &[u8; 32],
    hypercore_key: &[u8; 32],
) -> [u8; 32] {
    crypto::hash(&[
        &NAMESPACES[NS_VIEW_BLOCK_KEY],
        bootstrap,
        encryption_key,
        entropy,
        hypercore_key,
    ])
}

/// `getCompatBlockKey`.
fn compat_block_key(bootstrap: &[u8; 32], encryption_key: &[u8; 32], name: &[u8]) -> [u8; 32] {
    crypto::hash(&[
        &NAMESPACES[NS_VIEW_BLOCK_KEY],
        bootstrap,
        encryption_key,
        name,
    ])
}

/// `nonce = index || padding`, zero padded to the XSalsa20 nonce size.
fn nonce(index: u64, padding: &[u8]) -> [u8; 24] {
    let mut nonce = [0u8; 24];
    nonce[..8].copy_from_slice(&index.to_le_bytes());
    nonce[8..16].copy_from_slice(padding);
    nonce
}

/// `encryptBlock`: writes the header and encrypts the payload in place.
fn encrypt_block(
    index: u64,
    block: &mut [u8],
    id: u32,
    block_key: &[u8; 32],
    hash_key: &[u8; 32],
) -> Result<(), Error> {
    if block.len() < PADDING {
        return Err(Error::BlockTooShort);
    }

    // Keyed BLAKE2b-128 of the payload; only its first eight bytes are stored.
    let digest = Params::new()
        .hash_length(16)
        .key(hash_key)
        .hash(&block[PADDING..]);

    block[..8].copy_from_slice(&digest.as_bytes()[..8]);
    block[4..8].copy_from_slice(&id.to_le_bytes());
    block[0] = 1; // Plaintext type marker, and part of the nonce tail.

    let mut cipher = XSalsa20::new(block_key.into(), (&nonce(index, &block[..PADDING])).into());
    cipher.apply_keystream(&mut block[PADDING..]);
    Ok(())
}

/// Decodes `@autobee/manifest-data` down to its namespace.
fn manifest_namespace(user_data: &[u8]) -> Result<Option<[u8; 32]>, Error> {
    let mut r = Reader::new(Bytes::copy_from_slice(user_data));
    r.uint().map_err(|_| Error::ManifestData)?; // version
    let flags = r.uint().map_err(|_| Error::ManifestData)?;
    if flags & 1 != 0 {
        r.uint().map_err(|_| Error::ManifestData)?; // legacy blocks
    }
    if flags & 2 != 0 {
        Ok(Some(r.fixed::<32>().map_err(|_| Error::ManifestData)?))
    } else {
        Ok(None)
    }
}
