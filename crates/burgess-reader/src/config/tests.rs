//! Loading settings from TOML and environment variables.

// figment's `Jail` requires closures returning its own error type, which is
// large; that is figment's signature, not a choice these tests can change.
#![allow(clippy::result_large_err)]

use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use figment::Jail;

use super::*;

const MINIMAL: &str = r#"
listen = "0.0.0.0:8080"

[store]
kind = "local"
path = "/var/lib/burgess"
"#;

/// Loads `toml` as the config file inside a jail, where environment variables
/// can be set without leaking into other tests.
fn load_in_jail(
    toml: &str,
    env: &[(&str, &str)],
    check: impl FnOnce(Result<Settings, ConfigError>),
) {
    Jail::expect_with(|jail| {
        jail.create_file("reader.toml", toml)?;
        for (name, value) in env {
            jail.set_env(name, value);
        }
        check(Settings::load(Some(Path::new("reader.toml"))));
        Ok(())
    });
}

#[test]
fn minimal_file_gets_every_default() {
    load_in_jail(MINIMAL, &[], |settings| {
        let settings = settings.unwrap();
        assert_eq!(settings.listen, "0.0.0.0:8080".parse().unwrap());
        assert_eq!(
            settings.store,
            StoreConfig::Local {
                path: "/var/lib/burgess".into()
            }
        );
        assert_eq!(settings.pages, PageLimits::default());
        assert_eq!(settings.manifests, ManifestCacheConfig::default());
        assert_eq!(settings.files, FileReadConfig::default());
        assert_eq!(settings.lookup, TreeLookupConfig::default());
        assert_eq!(settings.refs, RefsLookupConfig::default());
    });
}

#[test]
fn every_setting_can_be_set_in_the_file() {
    let toml = r#"
listen = "127.0.0.1:9000"

[store]
kind = "s3"
bucket = "burgess"

[pages]
default_limit = 50
max_limit = 500

[manifests]
unsealed_ttl = "1m"
retention_days = 7

[files]
tail_bytes = 4096
refs_tail_bytes = 2048

[lookup]
stop_slack_days = 2
days_before = 1
concurrent_files = 8

[refs]
concurrent_days = 6
"#;
    load_in_jail(toml, &[], |settings| {
        let settings = settings.unwrap();
        assert_eq!(settings.listen, "127.0.0.1:9000".parse().unwrap());
        assert!(matches!(settings.store, StoreConfig::S3 { .. }));
        assert_eq!(
            settings.pages,
            PageLimits {
                default_limit: NonZeroUsize::new(50).unwrap(),
                max_limit: NonZeroUsize::new(500).unwrap(),
            }
        );
        assert_eq!(
            settings.manifests,
            ManifestCacheConfig {
                unsealed_ttl: Duration::from_secs(60),
                retention_days: NonZeroU32::new(7).unwrap(),
            }
        );
        assert_eq!(settings.files.tail_bytes.get(), 4096);
        assert_eq!(settings.files.refs_tail_bytes.get(), 2048);
        assert_eq!(
            settings.lookup,
            TreeLookupConfig {
                stop_slack_days: 2,
                days_before: 1,
                concurrent_files: NonZeroUsize::new(8).unwrap(),
            }
        );
        assert_eq!(
            settings.refs,
            RefsLookupConfig {
                concurrent_days: NonZeroUsize::new(6).unwrap(),
                concurrent_files: NonZeroUsize::new(8).unwrap(),
                days_before: 1,
            }
        );
    });
}

#[test]
fn the_environment_overrides_the_file() {
    let env = [
        ("BURGESS_READER__LISTEN", "127.0.0.1:7000"),
        ("BURGESS_READER__PAGES__MAX_LIMIT", "200"),
        ("BURGESS_READER__STORE__PATH", "/srv/burgess"),
    ];
    load_in_jail(MINIMAL, &env, |settings| {
        let settings = settings.unwrap();
        assert_eq!(settings.listen, "127.0.0.1:7000".parse().unwrap());
        assert_eq!(settings.pages.max_limit.get(), 200);
        assert_eq!(
            settings.store,
            StoreConfig::Local {
                path: "/srv/burgess".into()
            }
        );
    });
}

#[test]
fn variables_of_a_service_with_the_same_name_are_ignored() {
    let env = [
        ("BURGESS_READER_PORT", "tcp://10.0.0.1:80"),
        ("BURGESS_READER_SERVICE_HOST", "10.0.0.1"),
    ];
    load_in_jail(MINIMAL, &env, |settings| {
        assert!(settings.is_ok(), "{settings:?}");
    });
}

#[test]
fn rejects_missing_unknown_and_invalid_settings() {
    let cases = [
        (
            MINIMAL.replace("listen = \"0.0.0.0:8080\"", ""),
            "no listen",
        ),
        (
            MINIMAL.replace("0.0.0.0:8080", "localhost"),
            "listen without a port",
        ),
        (
            format!("{MINIMAL}\n[pages]\nmax_limt = 5\n"),
            "misspelled key",
        ),
        (format!("{MINIMAL}\n[pages]\nmax_limit = 0\n"), "zero max"),
        (
            format!("{MINIMAL}\n[lookup]\nconcurrent_files = 0\n"),
            "zero files",
        ),
        (
            format!("{MINIMAL}\n[manifests]\nunsealed_ttl = \"soon\"\n"),
            "bad duration",
        ),
    ];
    for (toml, what) in cases {
        load_in_jail(&toml, &[], |settings| {
            assert!(
                matches!(settings, Err(ConfigError::Invalid(_))),
                "{what}: {settings:?}"
            );
        });
    }
}

#[test]
fn rejects_a_default_limit_over_the_maximum() {
    let toml = format!("{MINIMAL}\n[pages]\ndefault_limit = 200\nmax_limit = 100\n");
    load_in_jail(&toml, &[], |settings| {
        assert!(
            matches!(settings, Err(ConfigError::Pages(_))),
            "{settings:?}"
        );
    });
}
