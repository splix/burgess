//! Which object store the files are in.

use std::path::PathBuf;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use serde::Deserialize;

use crate::text::{optional_text, text};

/// Where the files are stored.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
pub enum StoreConfig {
    /// An S3 bucket, or any store with the S3 API such as MinIO. Credentials
    /// come from the standard `AWS_*` environment variables, so they never
    /// sit in the config file.
    S3 {
        /// The bucket.
        #[serde(deserialize_with = "text")]
        bucket: String,
        /// A key prefix, for sharing a bucket with other data.
        #[serde(default, deserialize_with = "optional_text")]
        prefix: Option<String>,
        /// The region, when not given by `AWS_REGION`.
        #[serde(default, deserialize_with = "optional_text")]
        region: Option<String>,
        /// A custom endpoint, for S3-compatible stores, when not given by
        /// `AWS_ENDPOINT`.
        #[serde(default, deserialize_with = "optional_text")]
        endpoint: Option<String>,
        /// Whether the endpoint may use plain HTTP, as local test stores do,
        /// when not given by `AWS_ALLOW_HTTP`.
        #[serde(default)]
        allow_http: Option<bool>,
    },
    /// A local directory, for trying Burgess out or single-host setups.
    Local {
        /// The directory; created if missing.
        path: PathBuf,
    },
}

impl StoreConfig {
    /// Connects to the store, creating a local directory that is missing,
    /// as a writer's first start finds it.
    pub fn open(&self) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
        if let StoreConfig::Local { path } = self {
            std::fs::create_dir_all(path).map_err(|source| object_store::Error::Generic {
                store: "LocalFileSystem",
                source: source.into(),
            })?;
        }
        self.open_existing()
    }

    /// Connects to the store, failing on a local directory that is missing.
    ///
    /// For readers: one that created the directory would serve empty results
    /// from a mistyped path, and the mistake would never show.
    pub fn open_existing(&self) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
        match self {
            StoreConfig::S3 {
                bucket,
                prefix,
                region,
                endpoint,
                allow_http,
            } => {
                let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
                if let Some(allow_http) = allow_http {
                    builder = builder.with_allow_http(*allow_http);
                }
                if let Some(region) = region {
                    builder = builder.with_region(region);
                }
                if let Some(endpoint) = endpoint {
                    builder = builder.with_endpoint(endpoint);
                }
                let store = builder.build()?;
                Ok(match prefix {
                    Some(prefix) => Arc::new(PrefixStore::new(store, Path::from(prefix.as_str()))),
                    None => Arc::new(store),
                })
            }
            StoreConfig::Local { path } => Ok(Arc::new(LocalFileSystem::new_with_prefix(path)?)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opens_a_local_store_creating_its_directory() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("nested/store");
        StoreConfig::Local { path: path.clone() }.open().unwrap();
        assert!(path.is_dir());
    }

    #[test]
    fn opening_an_existing_store_does_not_create_its_directory() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("missing");
        let store = StoreConfig::Local { path: path.clone() };

        assert!(store.open_existing().is_err());
        assert!(!path.exists());
        assert!(
            StoreConfig::Local {
                path: root.path().into()
            }
            .open_existing()
            .is_ok()
        );
    }
}
