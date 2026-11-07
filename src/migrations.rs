//! `autobee/lib/migrations.js`, reduced to its boot-time decision.
//!
//! Legacy autobase storage is not migrated: PearTube never shipped autobase, so
//! a store carrying an autobase boot record starts fresh rather than half
//! migrated. `checkAutobaseMigration` therefore only probes the recorded boot
//! record and fails. `boot.js`'s `migrateWithFallback` retries the probe on the
//! namespace `local` core; both of its branches rethrow the same error, so one
//! probe is the whole decision.

use std::fmt;

use hypercore::core::{CoreError, Session};

/// The user data key a legacy autobase boot record lives under.
pub const LEGACY_BOOT_KEY: &str = "autobase/boot";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LegacyAutobaseStorage;

impl fmt::Display for LegacyAutobaseStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LEGACY_AUTOBASE_STORAGE: legacy autobase storage is not migrated")
    }
}

impl std::error::Error for LegacyAutobaseStorage {}

#[derive(Debug)]
pub enum Error {
    Legacy(LegacyAutobaseStorage),
    Core(CoreError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Legacy(e) => e.fmt(f),
            Error::Core(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for Error {}

impl From<LegacyAutobaseStorage> for Error {
    fn from(e: LegacyAutobaseStorage) -> Self {
        Error::Legacy(e)
    }
}

impl From<CoreError> for Error {
    fn from(e: CoreError) -> Self {
        Error::Core(e)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// `checkAutobaseMigration(store, local, bootstrap)`: `Ok(())` means nothing to
/// migrate; legacy storage is refused.
pub async fn check_autobase_migration(local: &Session) -> Result<()> {
    if local.user_data(LEGACY_BOOT_KEY)?.is_some() {
        return Err(LegacyAutobaseStorage.into());
    }
    Ok(())
}
