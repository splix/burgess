//! The address of a query service.

use std::fmt;
use std::str::FromStr;

use tonic::transport::Uri;

/// The address of a query service, in a form that gRPC can use: http or
/// https, a host, an optional port, and nothing more.
///
/// gRPC uses only the scheme and the authority of the address. A path,
/// a query or a user would be lost, thus they are refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerAddress(Uri);

/// Why a text is not an address that gRPC can use.
#[derive(Debug, thiserror::Error)]
pub enum ServerAddressError {
    /// The text is not an address. Holds the words of the parser.
    #[error("{0}")]
    NotAnAddress(String),
    /// gRPC runs on HTTP/2, over TCP or over TLS.
    #[error("expected an http:// or https:// address")]
    Scheme,
    /// gRPC does not send a user or a password from the address.
    #[error("expected no user or password in the address; the CLI cannot send them")]
    User,
    /// gRPC sends each method to its own fixed path.
    #[error("expected no path in the address; gRPC uses only the scheme, host and port")]
    Path,
    /// gRPC sends no query string.
    #[error("expected no query in the address; gRPC uses only the scheme, host and port")]
    Query,
}

impl ServerAddress {
    /// The address as tonic takes it.
    pub fn uri(&self) -> &Uri {
        &self.0
    }

    /// Whether the connection uses TLS.
    pub fn is_https(&self) -> bool {
        self.0.scheme_str() == Some("https")
    }

    /// The name that the TLS certificate must have. The host of an IPv6
    /// address is in brackets, which are not part of the name.
    pub fn tls_name(&self) -> &str {
        let host = self.0.host().unwrap_or_default();
        host.strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(host)
    }
}

impl FromStr for ServerAddress {
    type Err = ServerAddressError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        // The parser refuses an http address without a host, and makes an
        // empty path "/".
        let uri: Uri = text
            .parse()
            .map_err(|error| ServerAddressError::NotAnAddress(format!("{error}")))?;
        if !matches!(uri.scheme_str(), Some("http" | "https")) {
            return Err(ServerAddressError::Scheme);
        }
        if uri
            .authority()
            .is_some_and(|authority| authority.as_str().contains('@'))
        {
            return Err(ServerAddressError::User);
        }
        if uri.path() != "/" {
            return Err(ServerAddressError::Path);
        }
        if uri.query().is_some() {
            return Err(ServerAddressError::Query);
        }
        Ok(ServerAddress(uri))
    }
}

impl fmt::Display for ServerAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_http_or_https_address_with_or_without_a_port_is_accepted() {
        for text in [
            "http://burgess:9000",
            "http://burgess",
            "https://burgess.example.com/",
            "https://[::1]:8443",
        ] {
            assert!(text.parse::<ServerAddress>().is_ok(), "{text}");
        }
    }

    #[test]
    fn an_address_that_grpc_cannot_use_is_refused_with_the_cause() {
        let cases = [
            ("burgess 9000", "invalid"),
            ("burgess:9000", "expected an http:// or https:// address"),
            (
                "ftp://burgess:9000",
                "expected an http:// or https:// address",
            ),
            ("https://user:pw@burgess", "expected no user or password"),
            ("http://burgess:9000/burgess", "expected no path"),
            ("http://burgess:9000/?a=1", "expected no query"),
        ];
        for (text, expected) in cases {
            let error = text.parse::<ServerAddress>().unwrap_err().to_string();
            assert!(error.contains(expected), "{text}: {error}");
        }
    }

    #[test]
    fn the_tls_name_has_no_brackets() {
        let names = [
            ("https://burgess.example.com", "burgess.example.com"),
            ("https://[::1]:8443", "::1"),
            ("https://10.0.0.5", "10.0.0.5"),
        ];
        for (text, name) in names {
            assert_eq!(text.parse::<ServerAddress>().unwrap().tls_name(), name);
        }
    }
}
