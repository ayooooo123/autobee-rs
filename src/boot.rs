//! `autobee/lib/boot.js`: resolve the local writer, the bootstrap core and the
//! stored boot record a store starts from.
//!
//! The corestore opens cores ready, so the reference's `await core.ready()`
//! calls have nothing to await here.
//!
use compact_encoding::Error as EncodingError;
use corestore::{Corestore, Error as StoreError, GetOptions};
use hypercore::core::{CoreError, Session};
use hypercore::crypto::KeyPair;

use crate::encoding;
use crate::migrations::{self, LegacyAutobaseStorage};
use crate::types::{Key, Link};

/// `boot(store, key, { encrypt, encryptionKey, keyPair })`.
#[derive(Debug, Default, Clone)]
pub struct Options {
    /// Generate and store an encryption key when none exists.
    pub encrypt: bool,
    pub encryption_key: Option<Key>,
    pub key_pair: Option<KeyPair>,
}

/// The reference's `result` object.
pub struct Booted {
    pub key: Key,
    pub local: Session,
    pub bootstrap: Session,
    pub encryption_key: Option<Key>,
    /// The decoded `autobee/head` boot record.
    pub system: Option<Link>,
    /// `autobee/previous-drain`.
    pub previous_drain: u64,
}

#[derive(Debug)]
pub enum Error {
    Store(StoreError),
    Core(CoreError),
    Encoding(EncodingError),
    Migration(migrations::Error),
    /// A stored key user data value was not a 32-byte key.
    BadKey(&'static str),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Store(e) => e.fmt(f),
            Error::Core(e) => e.fmt(f),
            Error::Encoding(e) => e.fmt(f),
            Error::Migration(e) => e.fmt(f),
            Error::BadKey(field) => write!(f, "bad key in user data {field}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<StoreError> for Error {
    fn from(e: StoreError) -> Self {
        Error::Store(e)
    }
}

impl From<CoreError> for Error {
    fn from(e: CoreError) -> Self {
        Error::Core(e)
    }
}

impl From<EncodingError> for Error {
    fn from(e: EncodingError) -> Self {
        Error::Encoding(e)
    }
}

impl From<migrations::Error> for Error {
    fn from(e: migrations::Error) -> Self {
        Error::Migration(e)
    }
}

impl From<LegacyAutobaseStorage> for Error {
    fn from(e: LegacyAutobaseStorage) -> Self {
        Error::Migration(e.into())
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

fn key_from(value: impl AsRef<[u8]>, field: &'static str) -> Result<Key, Error> {
    value.as_ref().try_into().map_err(|_| Error::BadKey(field))
}

/// `boot.js`.
pub async fn boot(store: &Corestore, key: Option<Key>, opts: Options) -> Result<Booted> {
    // only what is stored (or generated) becomes `result.encryptionKey`; the
    // caller's key is separate, as in the reference
    let mut encryption_key: Option<Key> = None;
    // `if (key || keyPair)` guards the referrer writes; the parameter form
    // matters because a key found on the local core does not trigger them
    let writes_referrer = key.is_some() || opts.key_pair.is_some();

    let (key, local, bootstrap) = match key {
        Some(key) => {
            let bootstrap = store.get(GetOptions::key(key)).await?;

            let local = match opts.key_pair.clone() {
                Some(key_pair) => {
                    store
                        .get(GetOptions {
                            key_pair: Some(key_pair),
                            inactive: true,
                            exclusive: true,
                            ..Default::default()
                        })
                        .await?
                }
                None => {
                    let local_key = get_local_key(&bootstrap)?;
                    if bootstrap.core().writable() && local_key.is_none() {
                        let local = bootstrap.core().session(false);
                        local.acquire_exclusive().await?;
                        local
                    } else {
                        match local_key {
                            Some(key) => {
                                store
                                    .get(GetOptions {
                                        key: Some(key),
                                        inactive: true,
                                        exclusive: true,
                                        ..Default::default()
                                    })
                                    .await?
                            }
                            None => {
                                store
                                    .get(GetOptions {
                                        name: Some(b"local".to_vec()),
                                        inactive: true,
                                        exclusive: true,
                                        ..Default::default()
                                    })
                                    .await?
                            }
                        }
                    }
                }
            };

            (key, local, bootstrap)
        }
        None => {
            let local = match opts.key_pair.clone() {
                Some(key_pair) => {
                    store
                        .get(GetOptions {
                            key_pair: Some(key_pair),
                            inactive: true,
                            exclusive: true,
                            ..Default::default()
                        })
                        .await?
                }
                None => {
                    store
                        .get(GetOptions {
                            name: Some(b"local".to_vec()),
                            inactive: true,
                            exclusive: true,
                            ..Default::default()
                        })
                        .await?
                }
            };

            let (key, bootstrap) = match local.user_data("referrer")? {
                Some(referrer) => {
                    let key = key_from(referrer, "referrer")?;
                    let bootstrap = store
                        .get(GetOptions {
                            key: Some(key),
                            inactive: true,
                            ..Default::default()
                        })
                        .await?;
                    (key, bootstrap)
                }
                None => {
                    let key = *local.core().key();
                    let bootstrap = local.core().session(false);
                    bootstrap.set_user_data("autobee/local", Some(&key)).await?;
                    (key, bootstrap)
                }
            };

            (key, local, bootstrap)
        }
    };
    if writes_referrer {
        bootstrap.set_user_data("referrer", Some(&key)).await?;
        bootstrap
            .set_user_data("autobee/local", Some(local.core().key()))
            .await?;
        local.set_user_data("referrer", Some(&key)).await?;
    }

    check_migration(&local).await?;

    let system_head = local.user_data("autobee/head")?;
    let encryption_key_user_data = local.user_data("autobee/encryption")?;
    let previous_drain_user_data = local.user_data("autobee/previous-drain")?;

    let system = match system_head {
        Some(head) => Some(encoding::decode_boot_record(head)?),
        None => None,
    };

    if let Some(value) = encryption_key_user_data {
        encryption_key = Some(key_from(value, "autobee/encryption")?);
    }

    let previous_drain = match previous_drain_user_data {
        Some(value) => encoding::decode_previous_drain(value)?,
        None => 0,
    };

    if encryption_key.is_none() && (opts.encryption_key.is_some() || opts.encrypt) {
        let key = match opts.encryption_key {
            Some(key) => key,
            None => {
                let key_pair = store.create_key_pair("autobee/encryption");
                key_pair.secret_key[..32]
                    .try_into()
                    .expect("a 64-byte secret key has 32-byte halves")
            }
        };
        local
            .set_user_data("autobee/encryption", Some(&key))
            .await?;
        encryption_key = Some(key);
    }

    Ok(Booted {
        key,
        local,
        bootstrap,
        encryption_key,
        system,
        previous_drain,
    })
}

async fn check_migration(local: &Session) -> Result<()> {
    Ok(migrations::check_autobase_migration(local).await?)
}

/// `getLocalKey`: `autobee/local` wins over the legacy `autobase/local`.
fn get_local_key(bootstrap: &Session) -> Result<Option<Key>> {
    if let Some(key) = bootstrap.user_data("autobee/local")? {
        return key_from(key, "autobee/local").map(Some);
    }
    match bootstrap.user_data("autobase/local")? {
        Some(key) => key_from(key, "autobase/local").map(Some),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn local_writer_is_held_exclusively_until_closed() {
        let dir = tempfile::tempdir().unwrap();
        let store = Corestore::open_with_primary_key(dir.path(), [7; 32]).unwrap();
        let first = boot(&store, None, Options::default()).await.unwrap();
        let mut second = Box::pin(boot(&store, None, Options::default()));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut second)
                .await
                .is_err()
        );
        first.local.close();
        tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .unwrap()
            .unwrap();
    }
}
