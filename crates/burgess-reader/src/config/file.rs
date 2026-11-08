//! The settings as written in the file, before they are checked.

use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use burgess_store::StoreConfig;
use serde::Deserialize;

use super::{ConfigError, Settings};
use crate::manifests::ManifestCacheConfig;
use crate::pages::PageLimits;
use crate::states_file::StatesFileConfig;
use crate::tree_lookup::TreeLookupConfig;

/// The whole file. Unknown keys are errors, so a misspelled setting is
/// reported instead of silently left at its default.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FileConfig {
    listen: SocketAddr,
    store: StoreConfig,
    #[serde(default)]
    pages: PagesSection,
    #[serde(default)]
    manifests: ManifestsSection,
    #[serde(default)]
    files: FilesSection,
    #[serde(default)]
    lookup: LookupSection,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct PagesSection {
    default_limit: NonZeroUsize,
    max_limit: NonZeroUsize,
}

/// Durations are written like `"5s"` or `"1m"`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct ManifestsSection {
    #[serde(with = "humantime_serde")]
    unsealed_ttl: Duration,
    retention_days: NonZeroU32,
}

/// Sizes in bytes.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct FilesSection {
    tail_bytes: NonZeroUsize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct LookupSection {
    stop_slack_days: u32,
    days_before: u32,
    concurrent_files: NonZeroUsize,
}

/// Section defaults come from the typed settings, so each default is defined
/// once, next to what it configures.
impl Default for PagesSection {
    fn default() -> Self {
        let defaults = PageLimits::default();
        PagesSection {
            default_limit: defaults.default_limit,
            max_limit: defaults.max_limit,
        }
    }
}

impl Default for ManifestsSection {
    fn default() -> Self {
        let defaults = ManifestCacheConfig::default();
        ManifestsSection {
            unsealed_ttl: defaults.unsealed_ttl,
            retention_days: defaults.retention_days,
        }
    }
}

impl Default for FilesSection {
    fn default() -> Self {
        FilesSection {
            tail_bytes: StatesFileConfig::default().tail_bytes,
        }
    }
}

impl Default for LookupSection {
    fn default() -> Self {
        let defaults = TreeLookupConfig::default();
        LookupSection {
            stop_slack_days: defaults.stop_slack_days,
            days_before: defaults.days_before,
            concurrent_files: defaults.concurrent_files,
        }
    }
}

impl TryFrom<FileConfig> for Settings {
    type Error = ConfigError;

    fn try_from(file: FileConfig) -> Result<Self, Self::Error> {
        let pages = PageLimits {
            default_limit: file.pages.default_limit,
            max_limit: file.pages.max_limit,
        };
        pages.check()?;
        Ok(Settings {
            listen: file.listen,
            store: file.store,
            pages,
            manifests: ManifestCacheConfig {
                unsealed_ttl: file.manifests.unsealed_ttl,
                retention_days: file.manifests.retention_days,
            },
            files: StatesFileConfig {
                tail_bytes: file.files.tail_bytes,
            },
            lookup: TreeLookupConfig {
                stop_slack_days: file.lookup.stop_slack_days,
                days_before: file.lookup.days_before,
                concurrent_files: file.lookup.concurrent_files,
            },
        })
    }
}
