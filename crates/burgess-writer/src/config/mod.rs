//! The writer's settings: a TOML file, with any setting overridable from the
//! environment.
//!
//! A deployment usually keeps stable settings in the file and injects the
//! rest, such as broker addresses, through the environment. Every setting is
//! checked when the writer starts, so a typo or an invalid value stops it
//! before it reads anything, rather than at the first flush.
//!
//! Environment variables are named `BURGESS_WRITER__` followed by the
//! setting's path, sections separated by `__`, for example
//! `BURGESS_WRITER__WRITER_ID` or `BURGESS_WRITER__KAFKA__TOPIC`. They take
//! precedence over the file. The double underscore after the prefix keeps
//! out variables other tools define for a service of the same name, such as
//! Kubernetes' `BURGESS_WRITER_PORT`, which would otherwise be rejected as
//! unknown settings.

mod file;
mod store;
mod text;

pub use store::StoreConfig;

use std::path::Path;

use burgess_core::storage::{WriterId, WriterIdError};
use burgess_core::validation::Limits;
use figment::Figment;
use figment::providers::{Env, Format, Toml};

use self::file::FileConfig;
use crate::batch::{BatchConfig, BatchConfigError};
use crate::parquet_files::ParquetConfig;
use crate::source::{KafkaConfig, KafkaLogError};
use parquet::errors::ParquetError;

/// Prefix of the environment variables that override settings.
const ENV_PREFIX: &str = "BURGESS_WRITER__";

/// Every setting of the writer, checked.
#[derive(Debug, Clone, PartialEq)]
pub struct Settings {
    /// The writer's name in its checkpoint and manifests.
    pub writer_id: WriterId,
    /// Where records are read from.
    pub kafka: KafkaConfig,
    /// Where files are stored.
    pub store: StoreConfig,
    /// When batches end.
    pub batch: BatchConfig,
    /// Size limits records must fit in.
    pub limits: Limits,
    /// How the Parquet files are encoded.
    pub parquet: ParquetConfig,
}

/// Why the settings cannot be used.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// A setting is missing, unknown, or of the wrong type.
    #[error("invalid configuration")]
    Invalid(#[from] Box<figment::Error>),
    /// The writer id is not a valid one.
    #[error(transparent)]
    WriterId(#[from] WriterIdError),
    /// The batch settings cannot be used.
    #[error(transparent)]
    Batch(#[from] BatchConfigError),
    /// The Kafka settings cannot be used.
    #[error("invalid kafka settings")]
    Kafka(#[from] KafkaLogError),
    /// The Parquet settings cannot be used.
    #[error("invalid parquet settings")]
    Parquet(#[source] ParquetError),
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
