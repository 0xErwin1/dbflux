//! TLS connector construction for the PostgreSQL driver: the per-mode
//! validation policy plus the root CA and client identity files a profile or
//! a libpq-style URI names.

use dbflux_core::DbError;
use native_tls::{Certificate, Identity, TlsConnector};

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TlsVerification {
    pub(crate) chain: bool,
    pub(crate) hostname: bool,
}

/// Certificate files for one connection. Blank paths count as unset.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct PgTlsFiles {
    pub(crate) root_cert_path: Option<String>,
    pub(crate) client_cert_path: Option<String>,
    pub(crate) client_key_path: Option<String>,
}

impl PgTlsFiles {
    pub(crate) fn new(
        root_cert_path: Option<&str>,
        client_cert_path: Option<&str>,
        client_key_path: Option<&str>,
    ) -> Self {
        Self {
            root_cert_path: non_blank(root_cert_path),
            client_cert_path: non_blank(client_cert_path),
            client_key_path: non_blank(client_key_path),
        }
    }
}

fn non_blank(path: Option<&str>) -> Option<String> {
    path.map(str::trim)
        .filter(|path| !path.is_empty())
        .map(str::to_string)
}

/// The root CA is added to the trust store, never substituted for
/// validation, so naming one does not weaken the mode's policy. Modes that do
/// not validate the chain skip it: it would change nothing, and the form keeps
/// a stale path hidden in those modes that must not break the connection.
pub(crate) fn build_tls_connector(
    verification: TlsVerification,
    files: &PgTlsFiles,
) -> Result<TlsConnector, DbError> {
    let mut builder = TlsConnector::builder();
    builder
        .danger_accept_invalid_certs(!verification.chain)
        .danger_accept_invalid_hostnames(!verification.hostname);

    if verification.chain
        && let Some(path) = files.root_cert_path.as_deref()
    {
        builder.add_root_certificate(load_root_certificate(path)?);
    }

    if let Some(identity) = load_client_identity(files)? {
        builder.identity(identity);
    }

    builder
        .build()
        .map_err(|e| DbError::ConnectionFailed(format!("TLS setup failed: {e}").into()))
}

fn load_root_certificate(path: &str) -> Result<Certificate, DbError> {
    let pem = std::fs::read(path).map_err(|e| {
        DbError::ConnectionFailed(
            format!("Failed to read TLS root certificate '{path}': {e}").into(),
        )
    })?;

    Certificate::from_pem(&pem).map_err(|e| {
        DbError::ConnectionFailed(format!("Invalid TLS root certificate '{path}': {e}").into())
    })
}

/// Error messages name the files only; key contents never appear in them.
fn load_client_identity(files: &PgTlsFiles) -> Result<Option<Identity>, DbError> {
    match (
        files.client_cert_path.as_deref(),
        files.client_key_path.as_deref(),
    ) {
        (None, None) => Ok(None),

        (Some(cert_path), Some(key_path)) => {
            let cert_pem = std::fs::read(cert_path).map_err(|e| {
                DbError::ConnectionFailed(
                    format!("Failed to read TLS client certificate '{cert_path}': {e}").into(),
                )
            })?;

            let key_pem = std::fs::read(key_path).map_err(|e| {
                DbError::ConnectionFailed(
                    format!("Failed to read TLS client key '{key_path}': {e}").into(),
                )
            })?;

            Identity::from_pkcs8(&cert_pem, &key_pem)
                .map(Some)
                .map_err(|e| {
                    DbError::ConnectionFailed(
                        format!(
                            "Invalid TLS client certificate '{cert_path}' or its private key: {e}"
                        )
                        .into(),
                    )
                })
        }

        (Some(cert_path), None) => Err(DbError::ConnectionFailed(
            format!("TLS client certificate '{cert_path}' provided without a matching private key")
                .into(),
        )),

        (None, Some(key_path)) => Err(DbError::ConnectionFailed(
            format!("TLS client private key '{key_path}' provided without a matching certificate")
                .into(),
        )),
    }
}

/// Removes the libpq `sslrootcert`, `sslcert` and `sslkey` query parameters,
/// which the wire parser rejects as unknown options, and returns them as
/// files. The last occurrence of a key wins, as in libpq.
pub(crate) fn take_uri_tls_files(uri: &str) -> (String, PgTlsFiles) {
    let Some((base, query)) = uri.split_once('?') else {
        return (uri.to_string(), PgTlsFiles::default());
    };

    let mut root = None;
    let mut cert = None;
    let mut key = None;
    let mut retained = Vec::new();

    for pair in query.split('&') {
        let slot = match pair.split_once('=') {
            Some(("sslrootcert", value)) => Some((&mut root, value)),
            Some(("sslcert", value)) => Some((&mut cert, value)),
            Some(("sslkey", value)) => Some((&mut key, value)),
            _ => None,
        };

        match slot {
            Some((slot, value)) => {
                *slot = Some(
                    urlencoding::decode(value)
                        .map(|decoded| decoded.into_owned())
                        .unwrap_or_else(|_| value.to_string()),
                );
            }
            None => retained.push(pair),
        }
    }

    let files = PgTlsFiles::new(root.as_deref(), cert.as_deref(), key.as_deref());

    if retained.is_empty() {
        (base.to_string(), files)
    } else {
        (format!("{base}?{}", retained.join("&")), files)
    }
}
