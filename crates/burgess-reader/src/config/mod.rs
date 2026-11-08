//! The query service's settings: a TOML file, with any setting overridable
//! from the environment.
//!
//! As with the writer, stable settings usually live in the file and the rest
//! is injected through the environment, and every setting is checked at
//! start, so a typo stops the service before it answers anything.
//!
//! Environment variables are named `BURGESS_READER__` followed by the
//! setting's path, sections separated by `__`, for example
//! `BURGESS_READER__LISTEN` or `BURGESS_READER__PAGES__MAX_LIMIT`. They take
//! precedence over the file. The double underscore after the prefix keeps
//! out variables other tools define for a service of the same name, such as
//! Kubernetes' `BURGESS_READER_PORT`.

mod file;

use std::net::SocketAddr;
use std::path::Path;

use burgess_store::StoreConfig;
use figment::Figment;
use figment::providers::{Env, Format, Toml};

use self::file::FileConfig;
use crate::manifests::ManifestCacheConfig;
use crate::pages::{PageLimits, PageLimitsError};
use crate::parquet_file::FileReadConfig;
use crate::refs_lookup::RefsLookupConfig;
use crate::states_lookup::StatesLookupConfig;
use crate::tree_lookup::TreeLookupConfig;

/// Prefix of the environment variables that override settings.
const ENV_PREFIX: &str = "BURGESS_READER__";

/// Every setting of the query service, checked.
#[derive(Debug, Clone, PartialEq)]
pub struct Settings {
    /// The address the API listens on.
    pub listen: SocketAddr,
    /// Where the files are read from.
    pub store: StoreConfig,
    /// How many rows a page may hold.
    pub pages: PageLimits,
    /// How manifests are cached, and which days are kept.
    pub manifests: ManifestCacheConfig,
    /// How states files and refs files are read.
    pub files: FileReadConfig,
    /// How far tree lookups walk, and how many files they read at once.
    pub lookup: TreeLookupConfig,
    /// How refs lookups walk. `lookup.concurrent_files` and
    /// `lookup.days_before` apply to the two lookups.
    pub refs: RefsLookupConfig,
    /// How states lookups walk. `lookup.concurrent_files` and
    /// `lookup.days_before` apply to them too.
    pub states: StatesLookupConfig,
}

/// Why the settings cannot be used.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// A setting is missing, unknown, or of the wrong type.
    #[error("invalid configuration")]
    Invalid(#[from] Box<figment::Error>),
    /// The page limits do not fit together.
    #[error("invalid page limits")]
    Pages(#[from] PageLimitsError),
}

impl Settings {
    /// Reads the settings from `file`, if given, and the environment.
    pub fn load(file: Option<&Path>) -> Result<Self, ConfigError> {
        let mut figment = Figment::new();
        if let Some(file) = file {
            figment = figment.merge(Toml::file_exact(file));
        }
        Self::from_figment(figment.merge(Env::prefixed(ENV_PREFIX).split("__")))
    }

    fn from_figment(figment: Figment) -> Result<Self, ConfigError> {
        let file: FileConfig = figment.extract().map_err(Box::new)?;
        file.try_into()
    }
}

#[cfg(test)]
mod tests;
