/// External connection-profile importers.
///
/// Each importer turns a foreign client's connection-storage format (DBeaver's
/// `data-sources.json`, Beekeeper Studio's SQLite database, ...) into DBflux
/// `DbConfig` candidates. This module is pure logic: parsing and mapping only.
/// I/O is the caller's job, with one deliberate exception — Beekeeper Studio
/// stores connections in a SQLite file, so its importer opens that file
/// directly through `rusqlite` from a filesystem path, mirroring how the
/// SQLite driver itself must read a file to do anything useful.
///
/// A single bad entry never fails the whole batch: unsupported drivers,
/// missing fields, or undecryptable secrets are reported as per-entry skips
/// in `ExternalImportOutcome`, while every other entry still imports.
pub mod beekeeper;
pub mod dbeaver;

use std::path::PathBuf;

use dbflux_core::{DbConfig, DbKind};
use secrecy::SecretString;
use thiserror::Error;

/// Input handed to a `ConnectionImporter`.
///
/// `Bytes` covers JSON-based formats the caller already read into memory.
/// `Path` covers formats that require their own file handle, such as
/// Beekeeper Studio's SQLite database, which cannot be parsed from an
/// in-memory byte buffer without vendoring a SQLite implementation twice.
pub enum ImportSource {
    Bytes(Vec<u8>),
    Path(PathBuf),
}

impl ImportSource {
    /// Returns the source bytes, or an error when this source is a `Path`.
    pub fn as_bytes(&self) -> Result<&[u8], ExternalImportError> {
        match self {
            ImportSource::Bytes(bytes) => Ok(bytes),
            ImportSource::Path(_) => Err(ExternalImportError::ExpectedBytes),
        }
    }

    /// Returns the source path, or an error when this source is `Bytes`.
    pub fn as_path(&self) -> Result<&std::path::Path, ExternalImportError> {
        match self {
            ImportSource::Path(path) => Ok(path.as_path()),
            ImportSource::Bytes(_) => Err(ExternalImportError::ExpectedPath),
        }
    }
}

/// Input bundle passed to `ConnectionImporter::parse`.
///
/// `primary` is the format's main file (DBeaver's `data-sources.json`,
/// Beekeeper's `app.db`). `secondary` is an optional companion file used only
/// by formats that split credentials from connection metadata, such as
/// DBeaver's `credentials-config.json`.
pub struct ImportInput<'a> {
    pub primary: &'a ImportSource,
    pub secondary: Option<&'a ImportSource>,
}

impl<'a> ImportInput<'a> {
    pub fn new(primary: &'a ImportSource) -> Self {
        Self {
            primary,
            secondary: None,
        }
    }

    pub fn with_secondary(primary: &'a ImportSource, secondary: &'a ImportSource) -> Self {
        Self {
            primary,
            secondary: Some(secondary),
        }
    }
}

/// A single connection successfully parsed from an external format.
pub struct ExternalImportCandidate {
    pub name: String,

    /// Target connection configuration.
    pub config: DbConfig,

    /// Authoritative database kind, distinct from `config.kind()` for drivers
    /// that share a `DbConfig` shape (e.g. MariaDB reuses `DbConfig::MySQL`
    /// but must report `DbKind::MariaDB`). Pass this to
    /// `ConnectionProfile::new_with_kind` rather than deriving it from `config`.
    pub kind: DbKind,

    /// Plaintext secret recovered from the source format, when one was found
    /// and could be decrypted. Never a plain `String` so the value cannot be
    /// accidentally logged or serialized by a careless caller.
    pub secret: Option<SecretString>,

    /// Set when a secret exists in the source but could not be carried over
    /// (e.g. Beekeeper's per-install encryption key, or a DBeaver credentials
    /// file that failed to decrypt). The connection itself is still a valid
    /// candidate; only the secret is missing.
    pub secret_skip_reason: Option<String>,
}

/// An entry that could not be turned into a candidate at all.
pub struct ExternalImportSkip {
    /// Best-effort display name for the skipped entry. Empty when the source
    /// entry carried no usable name.
    pub name: String,

    /// Human-readable reason, e.g. "unsupported driver 'oracle'".
    pub reason: String,
}

/// Result of parsing one external source file (or file pair).
#[derive(Default)]
pub struct ExternalImportOutcome {
    pub candidates: Vec<ExternalImportCandidate>,
    pub skips: Vec<ExternalImportSkip>,
}

/// Errors that abort parsing the whole batch.
///
/// Per-entry problems (unsupported driver, missing field, undecryptable
/// secret) are never represented here — they become an `ExternalImportSkip`
/// so one bad entry cannot fail the batch. This type is reserved for cases
/// where the input as a whole is not the format the importer expects.
#[derive(Debug, Error)]
pub enum ExternalImportError {
    #[error("expected in-memory bytes for this input, got a filesystem path")]
    ExpectedBytes,

    #[error("expected a filesystem path for this input, got in-memory bytes")]
    ExpectedPath,

    #[error("input is not valid UTF-8: {0}")]
    InvalidUtf8(String),

    #[error("input is not valid JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),

    #[error("unexpected structure: {0}")]
    Structure(String),

    #[error("could not open SQLite database: {0}")]
    Sqlite(String),

    #[error("not a recognized Beekeeper Studio database: {0}")]
    NotBeekeeperDatabase(String),
}

/// Parses one external connection-storage format into DBflux candidates.
pub trait ConnectionImporter: Send + Sync {
    /// Stable machine identifier, e.g. `"dbeaver"`, `"beekeeper"`.
    fn id(&self) -> &'static str;

    /// Human-readable name for the import-source picker.
    fn display_name(&self) -> &'static str;

    fn parse(&self, input: &ImportInput<'_>) -> Result<ExternalImportOutcome, ExternalImportError>;
}

/// Every known external importer, so the UI can list formats generically.
///
/// Adding a new client's importer is one file plus one entry here.
pub fn importers() -> Vec<Box<dyn ConnectionImporter>> {
    vec![
        Box::new(dbeaver::DBeaverImporter),
        Box::new(beekeeper::BeekeeperImporter),
    ]
}

/// Tolerantly reads a port from a JSON value that may be a string or a number.
///
/// Both DBeaver and Beekeeper have been observed to serialize ports as either
/// type depending on version and connection kind.
pub(crate) fn port_from_json(value: Option<&serde_json::Value>) -> Option<u16> {
    match value? {
        serde_json::Value::String(s) => s.trim().parse::<u16>().ok(),
        serde_json::Value::Number(n) => n.as_u64().and_then(|v| u16::try_from(v).ok()),
        _ => None,
    }
}
