//! The service as it runs: settings, a store in a local directory, and
//! requests over a real socket.

use std::path::Path;

use burgess_api::{ErrorBody, Page};
use burgess_store::StoreConfig;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::*;
use crate::manifests::ManifestCacheConfig;
use crate::pages::PageLimits;
use crate::states_file::StatesFileConfig;
use crate::test_support::{at, days_ago, flush, minted_on, put_day, state_row};
use crate::tree_lookup::TreeLookupConfig;

fn settings(store: &Path) -> Settings {
    Settings {
        listen: "127.0.0.1:0".parse().unwrap(),
        store: StoreConfig::Local {
            path: store.to_path_buf(),
        },
        pages: PageLimits::default(),
        manifests: ManifestCacheConfig::default(),
        files: StatesFileConfig::default(),
        lookup: TreeLookupConfig::default(),
    }
}

/// Sends a GET for `path` and returns the status code and the body, read
/// as JSON.
async fn get<T: DeserializeOwned>(addr: SocketAddr, path: &str) -> (u16, T) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8(response).unwrap();
    let (head, body) = response.split_once("\r\n\r\n").expect("a head and a body");
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    (status, serde_json::from_str(body).unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn answers_lookups_over_files_in_a_local_directory() {
    let directory = tempfile::tempdir().unwrap();
    let day = days_ago(2);
    let tree = minted_on(day, 5);
    let rows = flush(vec![vec![
        state_row(tree, 0, at(day, 0), None),
        state_row(tree, 0, at(day, 1), Some(1)),
        state_row(tree, 1, at(day, 2), None),
    ]]);
    let config = settings(directory.path());
    put_day(config.store.open().unwrap().as_ref(), day, &[&rows]).await;

    let server = Server::bind(config).await.unwrap();
    let addr = server.local_addr().unwrap();
    let (stop, stopped) = watch::channel(false);
    let running = tokio::spawn(server.run(stopped));

    let (status, page) = get::<Page>(addr, &format!("/v1/trees/{tree}")).await;
    assert_eq!((status, page.rows.len()), (200, 3));
    let (status, error) = get::<ErrorBody>(addr, &format!("/v1/flows/{tree}")).await;
    assert_eq!(status, 400, "{error:?}");

    stop.send(true).unwrap();
    running.await.unwrap().expect("the service stops cleanly");
}

#[tokio::test]
async fn a_missing_store_directory_is_a_setting_to_fix() {
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("mistyped");

    let error = Server::bind(settings(&missing))
        .await
        .err()
        .expect("binding fails");

    assert!(matches!(error, AppError::Store(_)), "{error:?}");
    assert!(!error.restart_may_fix());
    assert!(!missing.exists());
}

#[tokio::test]
async fn an_address_this_host_does_not_have_is_a_setting_to_fix() {
    let directory = tempfile::tempdir().unwrap();
    let config = Settings {
        // TEST-NET-1, reserved for documentation, so no host has it.
        listen: "192.0.2.1:8080".parse().unwrap(),
        ..settings(directory.path())
    };

    let error = Server::bind(config).await.err().expect("binding fails");

    assert!(matches!(error, AppError::Listen { .. }), "{error:?}");
    assert!(!error.restart_may_fix());
}

#[tokio::test]
async fn an_address_already_taken_is_reported() {
    let directory = tempfile::tempdir().unwrap();
    let taken = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = Settings {
        listen: taken.local_addr().unwrap(),
        ..settings(directory.path())
    };

    let error = Server::bind(config).await.err().expect("binding fails");

    assert!(matches!(error, AppError::Listen { .. }), "{error:?}");
    assert!(error.restart_may_fix());
}
