//! Loading settings from TOML and environment variables.

// figment's `Jail` requires closures returning its own error type, which is
// large; that is figment's signature, not a choice these tests can change.
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::time::Duration;

use figment::Jail;

use super::*;

const MINIMAL: &str = r#"
writer_id = "writer-1"

[kafka]
bootstrap_servers = "broker:9092"
topic = "records"

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
        jail.create_file("writer.toml", toml)?;
        for (name, value) in env {
            jail.set_env(name, value);
        }
        check(Settings::load(Some(Path::new("writer.toml"))));
        Ok(())
    });
}

#[test]
fn minimal_file_gets_every_default() {
    load_in_jail(MINIMAL, &[], |settings| {
        let settings = settings.unwrap();
        assert_eq!(settings.writer_id.to_string(), "writer-1");
        assert_eq!(settings.kafka, KafkaConfig::new("broker:9092", "records"));
        assert_eq!(
            settings.store,
            StoreConfig::Local {
                path: "/var/lib/burgess".into()
            }
        );
        assert_eq!(settings.batch, BatchConfig::default());
        assert_eq!(settings.limits, Limits::default());
        assert_eq!(settings.parquet, ParquetConfig::default());
    });
}

#[test]
fn every_setting_can_be_set_in_the_file() {
    let toml = r#"
writer_id = "writer-2"

[kafka]
bootstrap_servers = "a:9092,b:9092"
topic = "states"
request_timeout = "5s"
properties = { "client.id" = "burgess", "security.protocol" = "PLAINTEXT" }

[store]
kind = "s3"
bucket = "burgess"
prefix = "prod"
region = "eu-west-1"
endpoint = "http://minio:9000"
allow_http = true

[batch]
max_interval = "15m"
max_bytes = 1000

[limits]
max_payload_bytes = 2048

[parquet]
zstd_level = 9
max_row_group_rows = 100
data_page_bytes = 4096
"#;
    load_in_jail(toml, &[], |settings| {
        let settings = settings.unwrap();
        assert_eq!(settings.kafka.request_timeout, Duration::from_secs(5));
        assert_eq!(
            settings.kafka.properties,
            BTreeMap::from([
                ("client.id".to_string(), "burgess".to_string()),
                ("security.protocol".to_string(), "PLAINTEXT".to_string()),
            ])
        );
        assert_eq!(
            settings.store,
            StoreConfig::S3 {
                bucket: "burgess".into(),
                prefix: Some("prod".into()),
                region: Some("eu-west-1".into()),
                endpoint: Some("http://minio:9000".into()),
                allow_http: Some(true),
            }
        );
        assert_eq!(
            settings.batch,
            BatchConfig {
                max_interval: Duration::from_secs(15 * 60),
                max_bytes: NonZeroUsize::new(1000).unwrap(),
            }
        );
        assert_eq!(settings.limits.max_payload_bytes.get(), 2048);
        assert_eq!(settings.parquet.zstd_level, 9);
        assert_eq!(settings.parquet.max_row_group_rows.get(), 100);
        assert_eq!(settings.parquet.data_page_bytes.get(), 4096);
    });
}

#[test]
fn environment_overrides_the_file() {
    let env = [
        ("BURGESS_WRITER__WRITER_ID", "from-env"),
        ("BURGESS_WRITER__KAFKA__TOPIC", "other-topic"),
        ("BURGESS_WRITER__BATCH__MAX_INTERVAL", "90s"),
        ("BURGESS_WRITER__PARQUET__ZSTD_LEVEL", "5"),
    ];
    load_in_jail(MINIMAL, &env, |settings| {
        let settings = settings.unwrap();
        assert_eq!(settings.writer_id.to_string(), "from-env");
        assert_eq!(settings.kafka.topic, "other-topic");
        assert_eq!(settings.batch.max_interval, Duration::from_secs(90));
        assert_eq!(settings.parquet.zstd_level, 5);
    });
}

#[test]
fn environment_alone_is_enough() {
    Jail::expect_with(|jail| {
        for (name, value) in [
            ("BURGESS_WRITER__WRITER_ID", "writer-1"),
            ("BURGESS_WRITER__KAFKA__BOOTSTRAP_SERVERS", "broker:9092"),
            ("BURGESS_WRITER__KAFKA__TOPIC", "records"),
            ("BURGESS_WRITER__STORE__KIND", "local"),
            ("BURGESS_WRITER__STORE__PATH", "/tmp/burgess"),
        ] {
            jail.set_env(name, value);
        }
        let settings = Settings::load(None).unwrap();
        assert_eq!(settings.kafka.topic, "records");
        Ok(())
    });
}

#[test]
fn rejects_a_misspelled_setting() {
    let toml = format!("{MINIMAL}\n[batch]\nmax_intreval = \"5m\"\n");
    load_in_jail(&toml, &[], |settings| {
        assert!(matches!(settings, Err(ConfigError::Invalid(_))));
    });
}

#[test]
fn rejects_a_missing_required_setting() {
    let toml = MINIMAL.replace("topic = \"records\"\n", "");
    load_in_jail(&toml, &[], |settings| {
        assert!(matches!(settings, Err(ConfigError::Invalid(_))));
    });
}

#[test]
fn rejects_invalid_values() {
    let cases = [
        (MINIMAL.replace("writer-1", "a/b"), "writer id"),
        (format!("{MINIMAL}\n[batch]\nmax_bytes = 0\n"), "zero size"),
        (
            format!("{MINIMAL}\n[batch]\nmax_interval = \"0s\"\n"),
            "zero interval",
        ),
        (
            format!("{MINIMAL}\n[batch]\nmax_interval = \"soon\"\n"),
            "duration",
        ),
        (
            MINIMAL.replace("kind = \"local\"", "kind = \"ftp\""),
            "store kind",
        ),
        (
            format!("{MINIMAL}\n[limits]\nmax_payload_bytes = 0\n"),
            "zero payload limit",
        ),
    ];
    for (toml, what) in cases {
        load_in_jail(&toml, &[], |settings| {
            assert!(settings.is_err(), "{what} accepted");
        });
    }
}

#[test]
fn rejects_an_invalid_compression_level_at_load() {
    let toml = format!("{MINIMAL}\n[parquet]\nzstd_level = 30\n");
    load_in_jail(&toml, &[], |settings| {
        assert!(matches!(settings, Err(ConfigError::Parquet(_))));
    });
}

#[test]
fn rejects_a_reserved_kafka_property_at_load() {
    let toml = MINIMAL.replace(
        "topic = \"records\"",
        "topic = \"records\"\nproperties = { \"group.id\" = \"mine\" }",
    );
    load_in_jail(&toml, &[], |settings| {
        assert!(matches!(settings, Err(ConfigError::Kafka(_))));
    });
}

#[test]
fn numeric_environment_values_are_read_as_text() {
    let env = [
        ("BURGESS_WRITER__WRITER_ID", "42"),
        ("BURGESS_WRITER__KAFKA__TOPIC", "2026"),
        ("BURGESS_WRITER__KAFKA__PROPERTIES__SASL_PASSWORD", "123456"),
    ];
    load_in_jail(MINIMAL, &env, |settings| {
        let settings = settings.unwrap();
        assert_eq!(settings.writer_id.to_string(), "42");
        assert_eq!(settings.kafka.topic, "2026");
        assert_eq!(settings.kafka.properties["sasl.password"], "123456");
    });
}

#[test]
fn kafka_properties_come_from_the_environment_with_dots_for_underscores() {
    let env = [(
        "BURGESS_WRITER__KAFKA__PROPERTIES__SECURITY_PROTOCOL",
        "SSL",
    )];
    load_in_jail(MINIMAL, &env, |settings| {
        let settings = settings.unwrap();
        assert_eq!(
            settings.kafka.properties,
            BTreeMap::from([("security.protocol".to_string(), "SSL".to_string())])
        );
    });
}

#[test]
fn variables_of_a_service_with_the_same_name_are_ignored() {
    let env = [
        ("BURGESS_WRITER_PORT", "tcp://10.0.0.1:80"),
        ("BURGESS_WRITER_SERVICE_HOST", "10.0.0.1"),
    ];
    load_in_jail(MINIMAL, &env, |settings| {
        assert!(settings.is_ok(), "{settings:?}");
    });
}

#[test]
fn allow_http_unset_leaves_it_to_the_environment() {
    let toml = MINIMAL.replace(
        "kind = \"local\"\npath = \"/var/lib/burgess\"",
        "kind = \"s3\"\nbucket = \"burgess\"",
    );
    load_in_jail(&toml, &[], |settings| {
        let StoreConfig::S3 { allow_http, .. } = settings.unwrap().store else {
            panic!("expected an s3 store");
        };
        assert_eq!(allow_http, None);
    });
}
