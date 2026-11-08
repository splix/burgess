//! The settings as written in the file, before they are checked.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::time::Duration;

use burgess_core::validation::Limits;
use burgess_store::StoreConfig;
use burgess_store::text::{text, text_map};
use serde::Deserialize;

use super::{ConfigError, Settings};
use crate::batch::BatchConfig;
use crate::parquet_files::ParquetConfig;
use crate::source::KafkaConfig;

/// The whole file. Unknown keys are errors, so a misspelled setting is
/// reported instead of silently left at its default.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FileConfig {
    #[serde(deserialize_with = "text")]
    writer_id: String,
    kafka: KafkaSection,
    store: StoreConfig,
    #[serde(default)]
    batch: BatchSection,
    #[serde(default)]
    limits: LimitsSection,
    #[serde(default)]
    parquet: ParquetSection,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct KafkaSection {
    #[serde(deserialize_with = "text")]
    bootstrap_servers: String,
    #[serde(deserialize_with = "text")]
    topic: String,
    #[serde(default = "default_request_timeout", with = "humantime_serde")]
    request_timeout: Duration,
    /// librdkafka settings. Its names contain only dots, which environment
    /// variable names cannot, so `_` in a key is read as `.`:
    /// `BURGESS_WRITER__KAFKA__PROPERTIES__SECURITY_PROTOCOL` sets
    /// `security.protocol`.
    #[serde(default, deserialize_with = "text_map")]
    properties: BTreeMap<String, String>,
}

/// Durations are written like `"10m"` or `"90s"`; sizes in bytes.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct BatchSection {
    #[serde(with = "humantime_serde")]
    max_interval: Duration,
    max_bytes: NonZeroUsize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct LimitsSection {
    max_payload_bytes: NonZeroUsize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct ParquetSection {
    zstd_level: i32,
    max_row_group_rows: NonZeroUsize,
    data_page_bytes: NonZeroUsize,
}

fn default_request_timeout() -> Duration {
    KafkaConfig::new("", "").request_timeout
}

/// Section defaults come from the typed settings, so each default is defined
/// once, next to what it configures.
impl Default for BatchSection {
    fn default() -> Self {
        let defaults = BatchConfig::default();
        BatchSection {
            max_interval: defaults.max_interval,
            max_bytes: defaults.max_bytes,
        }
    }
}

impl Default for LimitsSection {
    fn default() -> Self {
        LimitsSection {
            max_payload_bytes: Limits::default().max_payload_bytes,
        }
    }
}

impl Default for ParquetSection {
    fn default() -> Self {
        let defaults = ParquetConfig::default();
        ParquetSection {
            zstd_level: defaults.zstd_level,
            max_row_group_rows: defaults.max_row_group_rows,
            data_page_bytes: defaults.data_page_bytes,
        }
    }
}

impl TryFrom<FileConfig> for Settings {
    type Error = ConfigError;

    fn try_from(file: FileConfig) -> Result<Self, Self::Error> {
        let batch = BatchConfig {
            max_interval: file.batch.max_interval,
            max_bytes: file.batch.max_bytes,
        };
        batch.check()?;
        let kafka = KafkaConfig {
            bootstrap_servers: file.kafka.bootstrap_servers,
            topic: file.kafka.topic,
            request_timeout: file.kafka.request_timeout,
            properties: file
                .kafka
                .properties
                .into_iter()
                .map(|(key, value)| (key.replace('_', "."), value))
                .collect(),
        };
        kafka.check()?;
        let parquet = ParquetConfig {
            zstd_level: file.parquet.zstd_level,
            max_row_group_rows: file.parquet.max_row_group_rows,
            data_page_bytes: file.parquet.data_page_bytes,
        };
        parquet.compression().map_err(ConfigError::Parquet)?;
        Ok(Settings {
            writer_id: file.writer_id.parse()?,
            kafka,
            store: file.store,
            batch,
            limits: Limits {
                max_payload_bytes: file.limits.max_payload_bytes,
            },
            parquet,
        })
    }
}
