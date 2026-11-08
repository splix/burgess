//! Starting the query service from its settings.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::config::Settings;
use crate::manifests::ManifestCache;
use crate::rest::{self, Queries};
use crate::states_file::StatesFileReader;
use crate::tree_lookup::TreeLookup;

/// Why the query service could not start or stopped with an error.
#[derive(Debug, thiserror::Error)]
pub enum AppError {
    /// The object store could not be set up.
    #[error("opening the object store failed")]
    Store(#[from] object_store::Error),
    /// The listen address could not be taken.
    #[error("listening on {addr} failed")]
    Listen {
        /// The configured address.
        addr: SocketAddr,
        /// What the system said.
        #[source]
        source: io::Error,
    },
    /// Serving stopped with an error.
    #[error("serving the API failed")]
    Serve(#[source] io::Error),
}

impl AppError {
    /// Whether starting again may succeed. A store that cannot be set up,
    /// or an address this host does not have or this process may not take,
    /// is a setting to fix. An address in use may be freed once the process
    /// holding it is gone.
    pub fn restart_may_fix(&self) -> bool {
        match self {
            AppError::Store(_) => false,
            AppError::Listen { source, .. } => !matches!(
                source.kind(),
                io::ErrorKind::PermissionDenied | io::ErrorKind::AddrNotAvailable
            ),
            AppError::Serve(_) => true,
        }
    }
}

/// The query service, listening but not yet answering.
///
/// Listening before serving lets a caller learn the address taken, which
/// differs from the configured one when that names port 0.
pub struct Server {
    listener: TcpListener,
    router: Router,
}

impl Server {
    /// Opens the store and takes the listen address.
    pub async fn bind(settings: Settings) -> Result<Self, AppError> {
        tracing::info!(
            listen = %settings.listen,
            default_limit = settings.pages.default_limit,
            max_limit = settings.pages.max_limit,
            retention_days = settings.manifests.retention_days,
            "starting"
        );
        let store = settings.store.open_existing()?;
        let manifests = ManifestCache::new(store.clone(), settings.manifests);
        let files = StatesFileReader::new(store, settings.files);
        let lookup = TreeLookup::new(Arc::new(manifests), files, settings.lookup);
        let router = rest::router(Queries {
            lookup,
            limits: settings.pages,
        });
        let listener =
            TcpListener::bind(settings.listen)
                .await
                .map_err(|source| AppError::Listen {
                    addr: settings.listen,
                    source,
                })?;
        Ok(Server { listener, router })
    }

    /// The address the service listens on.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Answers requests until `stop` turns true, then finishes the requests
    /// already being answered.
    pub async fn run(self, mut stop: watch::Receiver<bool>) -> Result<(), AppError> {
        match self.local_addr() {
            Ok(addr) => tracing::info!(%addr, "listening"),
            Err(error) => tracing::warn!(%error, "listening on an unknown address"),
        }
        axum::serve(self.listener, self.router)
            .with_graceful_shutdown(async move {
                let _ = stop.wait_for(|stop| *stop).await;
            })
            .await
            .map_err(AppError::Serve)?;
        tracing::info!("stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests;
