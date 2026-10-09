#![allow(clippy::result_large_err)]

//! SSH tunneling support for DBFlux database drivers.
//!
//! Uses `dbflux_tunnel_core::Tunnel` for the shared RAII lifecycle and
//! implements `TunnelConnector` for SSH-specific forwarding logic.

use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD_NO_PAD;
#[cfg(windows)]
use dbflux_core::secrecy::{ExposeSecret, SecretString};
use dbflux_core::{DbError, SshAuthMethod, SshTunnelConfig};
use dbflux_tunnel_core::{ForwardingConnection, Tunnel, TunnelConnector, adaptive_sleep};
use sha2::{Digest, Sha256};
use ssh2::Session;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// OpenSSH config resolution (feature #837)
// ---------------------------------------------------------------------------

pub mod ssh_config;

pub use ssh_config::{
    ResolvedHost, SshConfigError, SshConfigFile, SshConfigHost, UnsupportedDirective,
};

/// Resolves the SSH config alias a tunnel references, if any, into the parameters to dial.
///
/// A tunnel with no alias is returned unchanged and no SSH config is read. An empty
/// or whitespace-only alias is an error. A resolution failure is an error: the stored
/// host, port and user are never used as a fallback (A7).
pub fn resolve_for_dial(config: &SshTunnelConfig) -> Result<SshTunnelConfig, DbError> {
    let Some(alias) = config.ssh_config_host.as_deref() else {
        let mut resolved = config.clone();
        resolved.ssh_config_host = None;
        return Ok(resolved);
    };
    let Some(home) = dirs::home_dir() else {
        return Err(DbError::connection_failed(format!(
            "No home directory available to resolve SSH config host '{alias}'"
        )));
    };
    resolve_for_dial_in(config, &ssh_config::local_user_from_env(), &home)
}

/// Same as [`resolve_for_dial`] with the home directory and local user injected, so callers
/// and tests never depend on the environment.
pub fn resolve_for_dial_in(
    config: &SshTunnelConfig,
    local_user: &str,
    home: &Path,
) -> Result<SshTunnelConfig, DbError> {
    let Some(alias) = config.ssh_config_host.as_deref() else {
        let mut resolved = config.clone();
        resolved.ssh_config_host = None;
        return Ok(resolved);
    };
    if alias.trim().is_empty() {
        // A hand-edited row or an imported bundle can carry an empty alias;
        // it must never select a block (a `Host *` catch-all would match it)
        // and must never fall back to the stored host/port/user (A7).
        return Err(DbError::connection_failed(
            "SSH config host alias is empty; pick a host from the SSH config or clear the field",
        ));
    }
    let config_dir = home.join(".ssh");
    let config_path = config_dir.join("config");
    let file = SshConfigFile::load_with_home(&config_dir, home).map_err(|error| match error {
        SshConfigError::Read { path, message } => DbError::connection_failed(format!(
            "Failed to read SSH config {}: {message}",
            path.display()
        )),
        other => DbError::connection_failed(format!(
            "Failed to read SSH config {}: {other}",
            config_path.display()
        )),
    })?;
    let resolved = file
        .resolve_with_local_user(alias, local_user, home)
        .map_err(|error| match error {
            SshConfigError::UnknownHost { alias } => DbError::connection_failed(format!(
                "SSH config host '{alias}' was not found in {}",
                config_path.display()
            )),
            SshConfigError::UnsupportedHost { alias, directive } => {
                DbError::connection_failed(format!(
                    "SSH config host '{alias}' uses the {directive} directive, which DBFlux's \
                     SSH tunnel does not support"
                ))
            }
            SshConfigError::Read { path, message } => DbError::connection_failed(format!(
                "Failed to read SSH config {}: {message}",
                path.display()
            )),
        })?;
    Ok(SshTunnelConfig {
        host: resolved.host_name,
        port: resolved.port,
        user: resolved.user.unwrap_or_else(|| local_user.to_string()),
        auth_method: resolve_auth_method(&config.auth_method, resolved.identity_file),
        ssh_config_host: None,
    })
}

/// Auth precedence for a referenced host (A2): a `Password` profile stays
/// `Password` because the user chose it; a `PrivateKey` profile with a
/// non-empty `key_path` keeps that path; an empty or absent profile path
/// takes the config's `IdentityFile`, or stays `None` so the SSH agent and
/// the default keys are tried (S3).
fn resolve_auth_method(
    auth_method: &SshAuthMethod,
    identity_file: Option<PathBuf>,
) -> SshAuthMethod {
    match auth_method {
        SshAuthMethod::Password => SshAuthMethod::Password,
        SshAuthMethod::PrivateKey { key_path } => {
            let profile_path = key_path
                .as_ref()
                .filter(|path| !path.as_os_str().is_empty())
                .cloned();
            SshAuthMethod::PrivateKey {
                key_path: profile_path.or(identity_file),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Session passphrase vault
// ---------------------------------------------------------------------------

/// Session-scoped in-memory store for SSH key passphrases.
///
/// Passphrases are retained for the lifetime of the process and never
/// written to disk, logged, or serialized.
///
/// # Security note
/// Values are stored in process memory in clear text. This is intentional
/// for a local-first client where the OS keyring may not have the passphrase
/// (for example, newly imported keys). NEVER serialize, log, or persist the
/// inner map. The `Debug` implementation deliberately omits all values to
/// prevent accidental passphrase exposure in log output.
pub struct SessionPassphraseVault {
    inner: HashMap<Uuid, String>,
}

impl std::fmt::Debug for SessionPassphraseVault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionPassphraseVault")
            .field("count", &self.inner.len())
            .finish()
    }
}

impl SessionPassphraseVault {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    /// Retrieve the passphrase stored for `id`, if any.
    pub fn get(&self, id: &Uuid) -> Option<&str> {
        self.inner.get(id).map(String::as_str)
    }

    /// Store or replace the passphrase for `id`.
    pub fn insert(&mut self, id: Uuid, passphrase: String) {
        self.inner.insert(id, passphrase);
    }

    /// Remove the passphrase for a specific tunnel.
    pub fn remove(&mut self, id: &Uuid) {
        self.inner.remove(id);
    }

    /// Remove all stored passphrases.
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

impl Default for SessionPassphraseVault {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Passphrase-required detection
// ---------------------------------------------------------------------------

/// Returns `true` when a `DbError` from `establish_session` indicates that
/// the SSH private key file requires a passphrase that was not supplied (or
/// that the supplied passphrase was wrong).
///
/// ssh2/libssh2 surfaces this as error messages containing phrases like
/// "Unable to extract public key" or "Failed to decrypt" or "bad decrypt".
pub fn is_passphrase_required_error(error: &DbError) -> bool {
    is_passphrase_required_error_str(&error.to_string())
}

/// Returns `true` when a plain-string error message from a connect attempt
/// indicates that the SSH private key requires a passphrase.
///
/// Use this variant when the original `DbError` has already been converted to
/// a `String` (e.g., after `Result::map_err(|e| e.to_string())`).
pub fn is_passphrase_required_error_str(msg: &str) -> bool {
    let lower = msg.to_lowercase();
    lower.contains("unable to extract public key")
        || lower.contains("failed to decrypt")
        || lower.contains("bad decrypt")
        || lower.contains("unable to open private key")
        || lower.contains("passphrase")
}

/// An active SSH tunnel that forwards local connections to a remote host.
///
/// All SSH operations are serialized through a single thread to avoid
/// libssh2 thread-safety issues. Shuts down on drop.
pub struct SshTunnel {
    inner: Tunnel,
}

impl SshTunnel {
    /// Start a new SSH tunnel forwarding to the specified remote host and port.
    ///
    /// Returns a tunnel that listens on a random local port. Use `local_port()`
    /// to get the assigned port number.
    pub fn start(session: Session, remote_host: String, remote_port: u16) -> Result<Self, DbError> {
        let connector = SshConnector { session };
        let inner = Tunnel::start(connector, remote_host, remote_port, "SSH")?;
        Ok(Self { inner })
    }

    /// Get the local port the tunnel is listening on.
    pub fn local_port(&self) -> u16 {
        self.inner.local_port()
    }
}

struct SshConnector {
    session: Session,
}

// Safety: all `Session` access is serialized to the tunnel thread.
#[expect(
    unsafe_code,
    reason = "redundant with the auto trait: ssh2 0.9.6 Session is documented Send (Session wraps Arc<Mutex<SessionInner>>); the explicit impl is kept as the recorded contract for tunnel-thread access"
)]
unsafe impl Send for SshConnector {}

impl TunnelConnector for SshConnector {
    fn test_connection(&self, remote_host: &str, remote_port: u16) -> Result<(), DbError> {
        self.session.set_blocking(true);
        let test_channel = self
            .session
            .channel_direct_tcpip(remote_host, remote_port, None)
            .map_err(|e| {
                DbError::connection_failed(format!(
                    "SSH tunnel test failed - cannot reach {}:{} through SSH server: {}",
                    remote_host, remote_port, e
                ))
            })?;

        drop(test_channel);
        Ok(())
    }

    fn run_tunnel_loop(
        self,
        listener: TcpListener,
        remote_host: String,
        remote_port: u16,
        shutdown: Arc<AtomicBool>,
    ) {
        run_ssh_tunnel_loop(listener, self.session, remote_host, remote_port, shutdown);
    }
}

/// Establish an SSH session using the provided configuration.
///
/// This handles TCP connection, handshake, and authentication.
pub fn establish_session(
    config: &SshTunnelConfig,
    secret: Option<&str>,
) -> Result<Session, DbError> {
    let total_start = std::time::Instant::now();

    // Resolve the SSH config alias first (feature #837): every dial below —
    // TCP connect, handshake, TOFU host-key step (keyed on the resolved
    // host and port, A1) and authentication — uses the resolved target.
    let resolved = resolve_for_dial(config)?;
    let config = &resolved;

    log::info!(
        "[SSH] Phase 1/3: TCP connect to {}:{}",
        config.host,
        config.port
    );
    let phase_start = std::time::Instant::now();

    let tcp = TcpStream::connect((&*config.host, config.port)).map_err(|e| {
        DbError::connection_failed(format!(
            "Failed to connect to SSH server {}:{}: {}",
            config.host, config.port, e
        ))
    })?;

    tcp.set_nodelay(true).ok();
    tcp.set_read_timeout(Some(std::time::Duration::from_secs(30)))
        .ok();
    tcp.set_write_timeout(Some(std::time::Duration::from_secs(30)))
        .ok();

    log::info!(
        "[SSH] Phase 1/3: TCP connect completed in {:.2}ms",
        phase_start.elapsed().as_secs_f64() * 1000.0
    );

    log::info!("[SSH] Phase 2/3: Creating SSH session and handshake");
    let phase_start = std::time::Instant::now();

    let mut session = Session::new()
        .map_err(|e| DbError::connection_failed(format!("Failed to create SSH session: {}", e)))?;

    session.set_tcp_stream(tcp);
    session.set_timeout(30000);

    session
        .handshake()
        .map_err(|e| DbError::connection_failed(format!("SSH handshake failed: {}", e)))?;

    verify_or_store_host_key(&session, &config.host, config.port)?;

    log::info!(
        "[SSH] Phase 2/3: Handshake completed in {:.2}ms",
        phase_start.elapsed().as_secs_f64() * 1000.0
    );

    log::info!("[SSH] Phase 3/3: Authenticating as {}", config.user);
    let phase_start = std::time::Instant::now();

    match &config.auth_method {
        SshAuthMethod::PrivateKey { key_path } => {
            authenticate_with_key(&session, &config.user, key_path.as_deref(), secret)?;
        }
        SshAuthMethod::Password => {
            let password = secret.ok_or_else(|| {
                DbError::connection_failed("SSH password required but not provided".to_string())
            })?;
            session
                .userauth_password(&config.user, password)
                .map_err(|e| {
                    DbError::connection_failed(format!("SSH password authentication failed: {}", e))
                })?;
        }
    }

    if !session.authenticated() {
        return Err(DbError::connection_failed(
            "SSH authentication failed".to_string(),
        ));
    }

    log::info!(
        "[SSH] Phase 3/3: Authentication completed in {:.2}ms",
        phase_start.elapsed().as_secs_f64() * 1000.0
    );

    log::info!(
        "[SSH] Session established, total time: {:.2}ms",
        total_start.elapsed().as_secs_f64() * 1000.0
    );

    Ok(session)
}

/// Expand `~` at the start of a path to the user's home directory.
fn expand_tilde(path: &Path) -> std::path::PathBuf {
    let path_str = path.to_string_lossy();

    let Some(home) = dirs::home_dir() else {
        return path.to_path_buf();
    };

    if let Some(stripped) = path_str.strip_prefix("~/") {
        return home.join(stripped);
    }

    if path_str == "~" {
        return home;
    }

    path.to_path_buf()
}

// ---------------------------------------------------------------------------
// Host-key fingerprint helpers
// ---------------------------------------------------------------------------

/// Outcome of comparing a stored known-hosts entry against the server's current key.
#[derive(Debug, PartialEq, Eq)]
enum HostKeyMatch {
    /// Stored entry is already in `SHA256:<base64-nopad>` format and matches.
    NewFormat,
    /// Stored entry is the legacy lowercase-hex encoding of the same key bytes.
    /// The caller should migrate the entry to `NewFormat` in place.
    LegacyHex,
    /// Neither format matches — the key has changed or is unknown.
    Mismatch,
}

/// Returns the `SHA256:<base64-nopad>` fingerprint of `key`, byte-identical to
/// `ssh-keygen -lf` output for the same raw key bytes.
fn sha256_base64_fingerprint(key: &[u8]) -> String {
    format!("SHA256:{}", STANDARD_NO_PAD.encode(Sha256::digest(key)))
}

/// Compares `stored` (a line from `ssh_known_hosts`) against the server's current
/// `key` bytes and classifies the relationship.
///
/// Migration invariant: `LegacyHex` is returned ONLY when re-deriving the legacy
/// hex from the SAME `key` bytes via `hex_encode` equals `stored`. This guarantees
/// migration never widens acceptance — a stored legacy-hex entry that would have been
/// rejected under the old comparison (because the key changed) still yields `Mismatch`.
fn host_key_matches_stored(stored: &str, key: &[u8]) -> HostKeyMatch {
    if stored == sha256_base64_fingerprint(key) {
        return HostKeyMatch::NewFormat;
    }

    if !stored.starts_with("SHA256:") && stored == hex_encode(key) {
        return HostKeyMatch::LegacyHex;
    }

    HostKeyMatch::Mismatch
}

fn verify_or_store_host_key(session: &Session, host: &str, port: u16) -> Result<(), DbError> {
    let (key, _) = session.host_key().ok_or_else(|| {
        DbError::connection_failed("SSH server did not present a host key".to_string())
    })?;

    let fingerprint = current_host_key_fingerprint(session)?;

    let known_hosts_path = tofu_known_hosts_path()?;
    let mut entries = load_tofu_known_hosts(&known_hosts_path)?;
    let entry_key = format!("{}\t{}", host, port);

    if let Some(existing) = entries.get(&entry_key) {
        match host_key_matches_stored(existing, key) {
            HostKeyMatch::NewFormat => return Ok(()),
            HostKeyMatch::LegacyHex => {
                entries.insert(entry_key, fingerprint);
                save_tofu_known_hosts(&known_hosts_path, &entries)?;
                return Ok(());
            }
            HostKeyMatch::Mismatch => {
                return Err(DbError::connection_failed(format!(
                    "SSH host key mismatch for {}:{} (possible MITM attack)",
                    host, port
                )));
            }
        }
    }

    entries.insert(entry_key, fingerprint);
    save_tofu_known_hosts(&known_hosts_path, &entries)?;

    log::warn!(
        "[SSH] First connection to {}:{} -- storing host key (TOFU)",
        host,
        port
    );

    Ok(())
}

fn current_host_key_fingerprint(session: &Session) -> Result<String, DbError> {
    let (key, _) = session.host_key().ok_or_else(|| {
        DbError::connection_failed("SSH server did not present a host key".to_string())
    })?;

    Ok(sha256_base64_fingerprint(key))
}

const KNOWN_HOSTS_FILE: &str = "ssh_known_hosts";

fn tofu_known_hosts_path() -> Result<PathBuf, DbError> {
    let dir = dbflux_storage::paths::data_dir().map_err(|e| {
        DbError::connection_failed(format!(
            "Failed to resolve data directory for SSH known hosts: {e}"
        ))
    })?;
    let new_path = dir.join(KNOWN_HOSTS_FILE);
    migrate_legacy_known_hosts(&new_path);
    Ok(new_path)
}

/// One-time best-effort move of the pre-0.7 SSH known-hosts file from the legacy
/// config directory (`~/.config/dbflux/ssh_known_hosts`) into the data directory.
///
/// Runs at most once per process via `Once`. Never blocks startup and never
/// errors out: any failure leaves the new path absent, which simply triggers a
/// fresh TOFU acceptance on the next SSH connect (the pre-existing clean-break
/// behavior). All best-effort failures are logged with `log::warn!`, never
/// silently dropped via `let _ =`.
fn migrate_legacy_known_hosts(new_path: &Path) {
    static MIGRATED: std::sync::Once = std::sync::Once::new();
    MIGRATED.call_once(|| {
        if new_path.exists() {
            return;
        }
        let Some(config_dir) = dirs::config_dir() else {
            return;
        };
        let old_path = config_dir.join("dbflux").join(KNOWN_HOSTS_FILE);
        if !old_path.exists() {
            return;
        }
        migrate_known_hosts_paths(&old_path, new_path);
    });
}

/// Pure, path-injected core of the known-hosts migration. Best-effort; returns
/// nothing. Caller supplies old and new paths so this is unit-testable without
/// mutating process environment variables.
///
/// Attempts an atomic `fs::rename` first; falls back to copy-then-remove on
/// cross-device or other rename failures. All best-effort failures are logged
/// with `log::warn!`, never silently discarded.
fn migrate_known_hosts_paths(old_path: &Path, new_path: &Path) {
    if new_path.exists() {
        return;
    }
    if !old_path.exists() {
        return;
    }

    match std::fs::rename(old_path, new_path) {
        Ok(()) => {
            if let Err(e) = dbflux_storage::paths::secure_file_permissions(new_path) {
                log::warn!("Migrated SSH known_hosts but failed to set 0o600: {e}");
            }
            log::info!("Migrated SSH known_hosts from {old_path:?} to {new_path:?}");
        }
        Err(rename_err) => match std::fs::copy(old_path, new_path) {
            Ok(_) => {
                if let Err(e) = dbflux_storage::paths::secure_file_permissions(new_path) {
                    log::warn!("Copied SSH known_hosts but failed to set 0o600: {e}");
                }
                if let Err(e) = std::fs::remove_file(old_path) {
                    log::warn!(
                        "Copied SSH known_hosts to new path but failed to remove old file {old_path:?}: {e}"
                    );
                } else {
                    log::info!("Migrated (copy) SSH known_hosts from {old_path:?} to {new_path:?}");
                }
            }
            Err(copy_err) => {
                log::warn!(
                    "Could not migrate SSH known_hosts (rename: {rename_err}; copy: {copy_err}); \
                         a fresh TOFU prompt will occur on next connect"
                );
            }
        },
    }
}

fn load_tofu_known_hosts(path: &Path) -> Result<BTreeMap<String, String>, DbError> {
    if !path.exists() {
        return Ok(BTreeMap::new());
    }

    let content = std::fs::read_to_string(path).map_err(|error| {
        DbError::connection_failed(format!("Failed to read SSH known hosts: {}", error))
    })?;

    let mut entries = BTreeMap::new();

    for line in content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let mut parts = line.splitn(3, '\t');

        let Some(host) = parts.next() else {
            continue;
        };
        let Some(port) = parts.next() else {
            continue;
        };
        let Some(fingerprint) = parts.next() else {
            continue;
        };

        entries.insert(format!("{}\t{}", host, port), fingerprint.to_string());
    }

    Ok(entries)
}

fn save_tofu_known_hosts(path: &Path, entries: &BTreeMap<String, String>) -> Result<(), DbError> {
    let mut output = String::new();

    for (key, fingerprint) in entries {
        let mut parts = key.splitn(2, '\t');
        let Some(host) = parts.next() else {
            continue;
        };
        let Some(port) = parts.next() else {
            continue;
        };

        output.push_str(host);
        output.push('\t');
        output.push_str(port);
        output.push('\t');
        output.push_str(fingerprint);
        output.push('\n');
    }

    std::fs::write(path, output).map_err(|error| {
        DbError::connection_failed(format!("Failed to write SSH known hosts: {}", error))
    })
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);

    // `byte >> 4` and `byte & 0x0f` are always in 0..=15, so each index is
    // within the 16-byte HEX alphabet.
    #[expect(
        clippy::indexing_slicing,
        reason = "byte >> 4 and byte & 0x0f are always in 0..=15, within the 16-byte HEX alphabet"
    )]
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }

    output
}

fn authenticate_with_key(
    session: &Session,
    user: &str,
    key_path: Option<&Path>,
    passphrase: Option<&str>,
) -> Result<(), DbError> {
    if key_path.is_none() {
        log::info!("[SSH] No key path specified, trying SSH agent authentication...");
        match session.userauth_agent(user) {
            Ok(()) if session.authenticated() => {
                log::info!("[SSH] Authenticated via SSH agent");
                return Ok(());
            }
            Ok(()) => {
                log::info!("[SSH] SSH agent returned OK but not authenticated");
            }
            Err(e) => {
                log::info!("[SSH] SSH agent not available or failed: {}", e);
            }
        }
    } else {
        log::info!("[SSH] Key path specified, skipping SSH agent");
    }

    let key_paths: Vec<std::path::PathBuf> = if let Some(path) = key_path {
        let expanded = expand_tilde(path);
        log::info!(
            "[SSH] Using specified key path: {} (expanded: {})",
            path.display(),
            expanded.display()
        );
        vec![expanded]
    } else {
        let home = dirs::home_dir().unwrap_or_default();
        log::info!(
            "[SSH] No key path specified, trying default paths in {}",
            home.display()
        );
        vec![
            home.join(".ssh/id_rsa"),
            home.join(".ssh/id_ed25519"),
            home.join(".ssh/id_ecdsa"),
        ]
    };

    let mut last_error: Option<String> = None;

    for path in &key_paths {
        if !path.exists() {
            log::info!("[SSH] Key file not found: {}", path.display());
            continue;
        }

        log::info!(
            "[SSH] Trying key: {} (passphrase: {})",
            path.display(),
            if passphrase.is_some() { "yes" } else { "no" }
        );

        let result = authenticate_with_key_file(session, user, path, passphrase);

        match result {
            Ok(()) if session.authenticated() => {
                log::info!("[SSH] Authenticated with key: {}", path.display());
                return Ok(());
            }
            Ok(()) => {
                log::info!(
                    "[SSH] Key {} returned OK but not authenticated",
                    path.display()
                );
                last_error = Some(format!("Key {} not accepted by server", path.display()));
            }
            Err(error) => {
                log::info!("[SSH] Key {} failed: {}", path.display(), error);
                last_error = Some(error);
            }
        }
    }

    let error_detail = last_error.unwrap_or_else(|| "No valid SSH keys found".to_string());
    Err(DbError::connection_failed(format!(
        "SSH key authentication failed: {}",
        error_detail
    )))
}

#[cfg(windows)]
fn authenticate_with_key_file(
    session: &Session,
    user: &str,
    path: &Path,
    passphrase: Option<&str>,
) -> Result<(), String> {
    let private_key = SecretString::from(std::fs::read_to_string(path).map_err(|error| {
        format!(
            "Failed to read SSH private key {}: {}",
            path.display(),
            error
        )
    })?);

    session
        .userauth_pubkey_memory(user, None, private_key.expose_secret(), passphrase)
        .map_err(|error| error.to_string())
}

#[cfg(not(windows))]
fn authenticate_with_key_file(
    session: &Session,
    user: &str,
    path: &Path,
    passphrase: Option<&str>,
) -> Result<(), String> {
    session
        .userauth_pubkey_file(user, None, path, passphrase)
        .map_err(|error| error.to_string())
}

// ---------------------------------------------------------------------------
// SSH tunnel loop
// ---------------------------------------------------------------------------

/// Single-threaded tunnel loop that multiplexes all SSH connections.
fn run_ssh_tunnel_loop(
    listener: TcpListener,
    session: Session,
    remote_host: String,
    remote_port: u16,
    shutdown: Arc<AtomicBool>,
) {
    session.set_blocking(false);

    let mut connections: Vec<ForwardingConnection<ssh2::Channel>> = Vec::new();

    while !shutdown.load(Ordering::SeqCst) {
        let mut activity = false;

        match listener.accept() {
            Ok((client_stream, addr)) => {
                log::debug!("[SSH] New tunnel connection from {}", addr);

                // Temporarily set blocking to open the channel
                session.set_blocking(true);
                match session.channel_direct_tcpip(&remote_host, remote_port, None) {
                    Ok(channel) => {
                        session.set_blocking(false);
                        match ForwardingConnection::new(client_stream, channel) {
                            Ok(conn) => {
                                connections.push(conn);
                                activity = true;
                            }
                            Err(e) => {
                                log::error!("[SSH] Failed to setup tunnel connection: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        session.set_blocking(false);
                        log::error!("[SSH] Failed to open SSH channel: {}", e);
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => {
                log::error!("[SSH] Tunnel listener error: {}", e);
                break;
            }
        }

        for conn in &mut connections {
            if conn.poll(
                |channel, data| channel.write_all(data),
                |client, data| client.write_all(data),
            ) {
                activity = true;
            }
        }

        let before = connections.len();
        connections.retain(|c| !c.closed);
        if connections.len() < before {
            log::debug!(
                "[SSH] Removed {} closed connections, {} active",
                before - connections.len(),
                connections.len()
            );
        }

        adaptive_sleep(activity, !connections.is_empty());
    }

    log::info!("[SSH] Tunnel loop shutting down");
}

// ---------------------------------------------------------------------------
// Tests — resolve_for_dial (feature #837, T3)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod resolve_for_dial_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use std::fs;
    use tempfile::TempDir;

    /// Fixture rooted at a `TempDir`: `dir/.ssh/config` is the SSH config and
    /// `dir` doubles as the home directory handed to the resolver. Nothing
    /// outside the temp directory is ever touched (S7).
    struct Fixture {
        dir: TempDir,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                dir: TempDir::new().unwrap(),
            }
        }

        fn home(&self) -> PathBuf {
            self.dir.path().to_path_buf()
        }

        fn write_ssh_config(&self, content: &str) {
            let path = self.dir.path().join(".ssh").join("config");
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, content).unwrap();
        }
    }

    fn aliased_tunnel(alias: &str, key_path: Option<PathBuf>) -> SshTunnelConfig {
        SshTunnelConfig {
            host: String::new(),
            port: 22,
            user: String::new(),
            auth_method: SshAuthMethod::PrivateKey { key_path },
            ssh_config_host: Some(alias.to_string()),
        }
    }

    fn key_path_of(auth: &SshAuthMethod) -> Option<&Path> {
        match auth {
            SshAuthMethod::PrivateKey { key_path } => key_path.as_deref(),
            SshAuthMethod::Password => None,
        }
    }

    #[test]
    fn no_alias_returns_the_input_unchanged_without_reading_ssh_config() {
        // The home directory has no `.ssh` directory at all: the no-alias path
        // must not touch it (and must not fail because it is absent).
        let home = TempDir::new().unwrap();
        let config = SshTunnelConfig {
            host: "db.internal.example.com".to_string(),
            port: 5433,
            user: "manual".to_string(),
            auth_method: SshAuthMethod::PrivateKey {
                key_path: Some(PathBuf::from("/keys/id_ed25519")),
            },
            ssh_config_host: None,
        };

        let resolved = resolve_for_dial_in(&config, "alice", home.path()).unwrap();

        assert_eq!(resolved.host, "db.internal.example.com");
        assert_eq!(resolved.port, 5433);
        assert_eq!(resolved.user, "manual");
        assert_eq!(
            resolved.auth_method,
            SshAuthMethod::PrivateKey {
                key_path: Some(PathBuf::from("/keys/id_ed25519"))
            }
        );
        assert_eq!(resolved.ssh_config_host, None);
    }

    #[test]
    fn alias_resolves_host_port_and_user_and_clears_the_alias() {
        let fixture = Fixture::new();
        fixture
            .write_ssh_config("Host web\n  HostName web.example.com\n  User deploy\n  Port 2222\n");
        let config = aliased_tunnel("web", None);

        let resolved = resolve_for_dial_in(&config, "alice", &fixture.home()).unwrap();

        assert_eq!(resolved.host, "web.example.com");
        assert_eq!(resolved.port, 2222);
        assert_eq!(resolved.user, "deploy");
        assert_eq!(resolved.ssh_config_host, None);
    }

    #[test]
    fn config_without_user_falls_back_to_the_injected_local_user() {
        let fixture = Fixture::new();
        fixture.write_ssh_config("Host web\n  HostName web.example.com\n");
        let config = aliased_tunnel("web", None);

        let resolved = resolve_for_dial_in(&config, "alice", &fixture.home()).unwrap();

        assert_eq!(resolved.user, "alice");
    }

    #[test]
    fn unknown_alias_error_names_the_alias_not_the_stored_host() {
        let fixture = Fixture::new();
        fixture.write_ssh_config("Host web\n  HostName web.example.com\n");
        let config = aliased_tunnel("ghost", None);

        let error = resolve_for_dial_in(&config, "alice", &fixture.home()).unwrap_err();
        let message = error.to_string();

        // The stored host is empty, so a dial attempt could not have produced
        // this message: it must come from resolution, naming the alias.
        assert!(message.contains("ghost"), "got: {message}");
        assert!(message.contains("was not found"), "got: {message}");
        assert!(
            message.contains(fixture.home().join(".ssh/config").to_str().unwrap()),
            "got: {message}"
        );
        assert!(!message.contains("Failed to connect"), "got: {message}");
    }

    #[test]
    fn proxy_jump_error_names_the_directive_and_never_dials() {
        let fixture = Fixture::new();
        fixture.write_ssh_config(
            "Host via\n  HostName bastion.example.com\n  ProxyJump jump.example.com\n",
        );
        let config = aliased_tunnel("via", None);

        let message = resolve_for_dial_in(&config, "alice", &fixture.home())
            .unwrap_err()
            .to_string();

        assert!(message.contains("ProxyJump"), "got: {message}");
        assert!(message.contains("does not support"), "got: {message}");
        assert!(!message.contains("Failed to connect"), "got: {message}");
    }

    #[test]
    fn proxy_command_error_names_the_directive_and_never_dials() {
        let fixture = Fixture::new();
        fixture.write_ssh_config(
            "Host viacmd\n  HostName bastion.example.com\n  ProxyCommand nc -w 1 %h %p\n",
        );
        let config = aliased_tunnel("viacmd", None);

        let message = resolve_for_dial_in(&config, "alice", &fixture.home())
            .unwrap_err()
            .to_string();

        assert!(message.contains("ProxyCommand"), "got: {message}");
        assert!(message.contains("does not support"), "got: {message}");
        assert!(!message.contains("Failed to connect"), "got: {message}");
    }

    #[test]
    fn password_auth_survives_a_config_identity_file() {
        let fixture = Fixture::new();
        fixture.write_ssh_config(
            "Host web\n  HostName web.example.com\n  IdentityFile ~/id_ed25519\n",
        );
        let config = SshTunnelConfig {
            auth_method: SshAuthMethod::Password,
            ..aliased_tunnel("web", None)
        };

        let resolved = resolve_for_dial_in(&config, "alice", &fixture.home()).unwrap();

        assert_eq!(resolved.auth_method, SshAuthMethod::Password);
    }

    #[test]
    fn non_empty_profile_key_path_wins_over_the_config_identity_file() {
        let fixture = Fixture::new();
        fixture.write_ssh_config(
            "Host web\n  HostName web.example.com\n  IdentityFile ~/config_key\n",
        );
        let config = aliased_tunnel("web", Some(PathBuf::from("/profile_key")));

        let resolved = resolve_for_dial_in(&config, "alice", &fixture.home()).unwrap();

        assert_eq!(
            key_path_of(&resolved.auth_method),
            Some(Path::new("/profile_key"))
        );
    }

    #[test]
    fn empty_profile_key_path_takes_the_config_identity_file() {
        let fixture = Fixture::new();
        fixture.write_ssh_config(
            "Host web\n  HostName web.example.com\n  IdentityFile ~/config_key\n",
        );
        let config = aliased_tunnel("web", Some(PathBuf::new()));

        let resolved = resolve_for_dial_in(&config, "alice", &fixture.home()).unwrap();

        assert_eq!(
            key_path_of(&resolved.auth_method),
            Some(&*fixture.home().join("config_key"))
        );
    }

    #[test]
    fn no_identity_file_anywhere_leaves_key_path_none_for_the_agent() {
        let fixture = Fixture::new();
        fixture.write_ssh_config("Host web\n  HostName web.example.com\n");
        let config = aliased_tunnel("web", None);

        let resolved = resolve_for_dial_in(&config, "alice", &fixture.home()).unwrap();

        assert_eq!(key_path_of(&resolved.auth_method), None);
    }

    #[test]
    fn identity_file_tokens_expand_into_the_resolved_auth_method() {
        let fixture = Fixture::new();
        fixture.write_ssh_config(
            "Host web\n  HostName web.example.com\n  User alice\n  IdentityFile ~/keys/%h_%r_%d\n",
        );
        let config = aliased_tunnel("web", None);

        let resolved = resolve_for_dial_in(&config, "alice", &fixture.home()).unwrap();

        let home = fixture.home();
        assert_eq!(
            key_path_of(&resolved.auth_method),
            Some(&*home.join(format!("keys/web.example.com_alice_{}", home.display())) as &Path)
        );
    }

    #[test]
    fn empty_alias_does_not_resolve_through_a_catch_all_block() {
        let fixture = Fixture::new();
        fixture.write_ssh_config("Host *\n  HostName catchall.example.com\n");

        // An empty or whitespace-only alias must fail closed, never falling
        // back to the stored host/port/user (A7), and never matching `Host *`.
        for alias in ["", "   "] {
            let config = aliased_tunnel(alias, None);
            let error = resolve_for_dial_in(&config, "alice", &fixture.home())
                .expect_err("empty alias must not resolve");
            let message = error.to_string();
            assert!(!message.contains("catchall.example.com"), "got: {message}");
            assert!(!message.contains("Failed to connect"), "got: {message}");
        }
    }

    #[test]
    fn unreadable_config_error_names_the_path_and_the_underlying_message() {
        let fixture = Fixture::new();
        // `.ssh/config` is a directory, so reading it fails with a non-NotFound
        // error instead of yielding the empty-config default.
        fs::create_dir_all(fixture.home().join(".ssh").join("config")).unwrap();
        let config = aliased_tunnel("web", None);

        let message = resolve_for_dial_in(&config, "alice", &fixture.home())
            .unwrap_err()
            .to_string();

        assert!(
            message.contains(fixture.home().join(".ssh/config").to_str().unwrap()),
            "got: {message}"
        );
    }

    /// S5's fail-closed requirement, proven at the seam `establish_session`
    /// itself calls first. A direct `establish_session` call would have to read
    /// `dirs::home_dir()` (its alias path does), which S7 forbids in tests, so
    /// the env-dependent outer function is exercised through its injected
    /// core here: `establish_session` starts with `resolve_for_dial(config)?`,
    /// so this error is returned before any `TcpStream::connect`.
    #[test]
    fn unresolved_alias_fails_closed_before_any_tcp_connect_at_the_session_seam() {
        let fixture = Fixture::new();
        fixture.write_ssh_config("Host web\n  HostName web.example.com\n");
        let config = aliased_tunnel("ghost", None);

        let error = resolve_for_dial_in(&config, "alice", &fixture.home()).unwrap_err();

        assert!(error.to_string().contains("ghost"));
    }
}

// ---------------------------------------------------------------------------
// Tests — SessionPassphraseVault pure logic
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_get_roundtrip() {
        let mut vault = SessionPassphraseVault::new();
        let id = Uuid::new_v4();
        vault.insert(id, "hunter2".to_string());
        assert_eq!(vault.get(&id), Some("hunter2"));
    }

    #[test]
    fn get_missing_key_returns_none() {
        let vault = SessionPassphraseVault::new();
        let id = Uuid::new_v4();
        assert_eq!(vault.get(&id), None);
    }

    #[test]
    fn clear_removes_all_entries() {
        let mut vault = SessionPassphraseVault::new();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        vault.insert(id1, "pass1".to_string());
        vault.insert(id2, "pass2".to_string());
        vault.clear();
        assert_eq!(vault.get(&id1), None);
        assert_eq!(vault.get(&id2), None);
    }

    #[test]
    fn remove_removes_single_entry() {
        let mut vault = SessionPassphraseVault::new();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        vault.insert(id1, "pass1".to_string());
        vault.insert(id2, "pass2".to_string());
        vault.remove(&id1);
        assert_eq!(vault.get(&id1), None);
        assert_eq!(vault.get(&id2), Some("pass2"));
    }

    #[test]
    fn debug_impl_does_not_expose_passphrase_values() {
        let mut vault = SessionPassphraseVault::new();
        vault.insert(Uuid::new_v4(), "super_secret_passphrase".to_string());
        let debug_str = format!("{:?}", vault);
        assert!(
            !debug_str.contains("super_secret_passphrase"),
            "Debug output must not expose passphrase: {debug_str}",
        );
        assert!(debug_str.contains("SessionPassphraseVault"));
    }

    // SHA-256 fingerprint helper tests

    #[test]
    fn sha256_base64_fingerprint_matches_ssh_keygen_format() {
        // Known input: 4 zero bytes.  Expected: SHA256-base64-nopad of [0,0,0,0].
        // base64-std-nopad of SHA256([0,0,0,0]) = 3z9hmASpL9tAVxktxD3XSOp3itxSvEmM6AUkwBS4ERk
        // (standard alphabet, no '=' padding)
        let key: &[u8] = &[0u8; 4];
        let fp = sha256_base64_fingerprint(key);
        assert!(fp.starts_with("SHA256:"), "must start with 'SHA256:': {fp}");
        assert!(!fp.contains('='), "must not contain '=' padding: {fp}");
        assert_eq!(fp, "SHA256:3z9hmASpL9tAVxktxD3XSOp3itxSvEmM6AUkwBS4ERk");
    }

    #[test]
    fn legacy_hex_entry_is_recognized_and_upgraded() {
        const KEY: &[u8] = b"test-key-bytes";
        let legacy = hex_encode(KEY);
        let new_fp = sha256_base64_fingerprint(KEY);

        assert_eq!(
            host_key_matches_stored(&legacy, KEY),
            HostKeyMatch::LegacyHex,
            "stored legacy-hex of same key must be LegacyHex"
        );
        assert_eq!(
            host_key_matches_stored(&new_fp, KEY),
            HostKeyMatch::NewFormat,
            "stored SHA256: of same key must be NewFormat"
        );
    }

    #[test]
    fn genuine_key_change_is_mismatch() {
        const KEY_A: &[u8] = b"key-a";
        const KEY_B: &[u8] = b"key-b";

        let legacy_a = hex_encode(KEY_A);
        let new_a = sha256_base64_fingerprint(KEY_A);

        assert_eq!(
            host_key_matches_stored(&legacy_a, KEY_B),
            HostKeyMatch::Mismatch,
            "legacy hex of key_A must not match key_B"
        );
        assert_eq!(
            host_key_matches_stored(&new_a, KEY_B),
            HostKeyMatch::Mismatch,
            "SHA256 fingerprint of key_A must not match key_B"
        );
    }

    // ---------------------------------------------------------------------------
    // Known-hosts path + migration tests
    // ---------------------------------------------------------------------------

    #[test]
    fn known_hosts_path_parent_is_data_dir() {
        let path = tofu_known_hosts_path().expect("must resolve known hosts path");
        let data_dir = dbflux_storage::paths::data_dir().expect("must resolve data dir");

        assert_eq!(
            path.parent().expect("known hosts path must have a parent"),
            data_dir,
            "ssh_known_hosts must be located under the data directory"
        );
        assert_eq!(
            path.file_name().and_then(|n| n.to_str()),
            Some("ssh_known_hosts"),
            "known hosts file must be named 'ssh_known_hosts'"
        );
    }

    #[test]
    fn migration_moves_old_file_to_new() {
        let dir = tempfile::TempDir::new().expect("create temp dir");
        let old_path = dir.path().join("old_known_hosts");
        let new_path = dir.path().join("new_known_hosts");

        let content = "host\t22\tSHA256:abc";
        std::fs::write(&old_path, content).expect("write old file");

        migrate_known_hosts_paths(&old_path, &new_path);

        assert!(new_path.exists(), "new file must exist after migration");
        assert!(
            !old_path.exists(),
            "old file must be removed after migration"
        );
        let new_content = std::fs::read_to_string(&new_path).expect("read new file");
        assert_eq!(new_content, content, "new file must have the same content");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&new_path)
                .expect("metadata readable")
                .permissions()
                .mode();
            assert_eq!(
                mode & 0o777,
                0o600,
                "migrated file must be 0o600, got {:o}",
                mode & 0o777
            );
        }
    }

    #[test]
    fn migration_noop_when_new_exists() {
        let dir = tempfile::TempDir::new().expect("create temp dir");
        let old_path = dir.path().join("old_known_hosts");
        let new_path = dir.path().join("new_known_hosts");

        std::fs::write(&old_path, "old content").expect("write old file");
        std::fs::write(&new_path, "new content").expect("write new file");

        migrate_known_hosts_paths(&old_path, &new_path);

        let new_content = std::fs::read_to_string(&new_path).expect("read new file");
        assert_eq!(
            new_content, "new content",
            "new file must remain unchanged when it already exists"
        );
        assert!(
            old_path.exists(),
            "old file must remain when new already exists"
        );
    }

    #[test]
    fn migration_noop_when_old_absent() {
        let dir = tempfile::TempDir::new().expect("create temp dir");
        let old_path = dir.path().join("old_known_hosts");
        let new_path = dir.path().join("new_known_hosts");

        migrate_known_hosts_paths(&old_path, &new_path);

        assert!(
            !new_path.exists(),
            "new file must not be created when old is absent"
        );
    }

    #[test]
    fn migration_failure_tolerated() {
        let dir = tempfile::TempDir::new().expect("create temp dir");
        let old_path = dir.path().join("old_known_hosts");
        // Point new_path at a location whose parent does not exist.
        let new_path = dir
            .path()
            .join("nonexistent_parent")
            .join("new_known_hosts");

        std::fs::write(&old_path, "content").expect("write old file");

        // Must not panic; rename and copy both fail due to missing parent.
        migrate_known_hosts_paths(&old_path, &new_path);

        assert!(
            !new_path.exists(),
            "new file must not exist after a failed migration"
        );
    }
}
