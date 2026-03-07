#![allow(clippy::result_large_err)]

//! SSH tunneling support for DBFlux database drivers.
//!
//! Uses `dbflux_tunnel_core::Tunnel` for the shared RAII lifecycle and
//! implements `TunnelConnector` for SSH-specific forwarding logic.

use std::collections::BTreeMap;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use dbflux_core::{DbError, SshAuthMethod, SshTunnelConfig};
use dbflux_tunnel_core::{ForwardingConnection, Tunnel, TunnelConnector, adaptive_sleep};
use ssh2::Session;

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

fn verify_or_store_host_key(session: &Session, host: &str, port: u16) -> Result<(), DbError> {
    let fingerprint = current_host_key_fingerprint(session)?;

    let known_hosts_path = tofu_known_hosts_path()?;
    let mut entries = load_tofu_known_hosts(&known_hosts_path)?;
    let entry_key = format!("{}\t{}", host, port);

    if let Some(existing) = entries.get(&entry_key) {
        if existing == &fingerprint {
            return Ok(());
        }

        return Err(DbError::connection_failed(format!(
            "SSH host key mismatch for {}:{} (possible MITM attack)",
            host, port
        )));
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

    Ok(hex_encode(key))
}

fn tofu_known_hosts_path() -> Result<PathBuf, DbError> {
    let config_dir = dirs::config_dir().ok_or_else(|| {
        DbError::connection_failed("Could not find config directory for SSH known hosts")
    })?;

    let app_dir = config_dir.join("dbflux");
    std::fs::create_dir_all(&app_dir).map_err(|error| {
        DbError::connection_failed(format!("Failed to create config directory: {}", error))
    })?;

    Ok(app_dir.join("ssh_known_hosts"))
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

        let result = session.userauth_pubkey_file(user, None, path, passphrase);

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
            Err(e) => {
                let err_msg = format!("{}", e);
                log::info!("[SSH] Key {} failed: {}", path.display(), err_msg);
                last_error = Some(err_msg);
            }
        }
    }

    let error_detail = last_error.unwrap_or_else(|| "No valid SSH keys found".to_string());
    Err(DbError::connection_failed(format!(
        "SSH key authentication failed: {}",
        error_detail
    )))
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
