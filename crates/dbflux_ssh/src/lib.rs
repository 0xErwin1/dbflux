#![allow(clippy::result_large_err)]

//! SSH tunneling support for DBFlux database drivers.
//!
//! This crate provides SSH tunnel functionality that can be shared across
//! different database drivers (PostgreSQL, MySQL, etc.).

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

use dbflux_core::{DbError, SshAuthMethod, SshTunnelConfig};
use ssh2::Session;

/// An active SSH tunnel that forwards local connections to a remote host.
///
/// The tunnel runs in a background thread and automatically shuts down
/// when dropped. All SSH operations are serialized through a single thread
/// to avoid libssh2 thread-safety issues.
pub struct SshTunnel {
    local_port: u16,
    shutdown: Arc<AtomicBool>,
    #[allow(dead_code)]
    forwarder_thread: JoinHandle<()>,
}

impl SshTunnel {
    /// Start a new SSH tunnel forwarding to the specified remote host and port.
    ///
    /// Returns a tunnel that listens on a random local port. Use `local_port()`
    /// to get the assigned port number.
    ///
    /// This function verifies the tunnel can reach the remote host before returning.
    pub fn start(session: Session, remote_host: String, remote_port: u16) -> Result<Self, DbError> {
        // Test that we can actually forward to the remote host before starting the tunnel
        log::info!(
            "[SSH] Testing tunnel connectivity to {}:{}",
            remote_host,
            remote_port
        );

        session.set_blocking(true);
        let test_channel = session
            .channel_direct_tcpip(&remote_host, remote_port, None)
            .map_err(|e| {
                DbError::connection_failed(format!(
                    "SSH tunnel test failed - cannot reach {}:{} through SSH server: {}",
                    remote_host, remote_port, e
                ))
            })?;

        // Close test channel
        drop(test_channel);
        log::info!("[SSH] Tunnel connectivity verified");

        let listener = TcpListener::bind("127.0.0.1:0").map_err(|e| {
            DbError::connection_failed(format!("Failed to bind local tunnel port: {}", e))
        })?;

        let local_port = listener
            .local_addr()
            .map_err(|e| {
                DbError::connection_failed(format!("Failed to get local tunnel address: {}", e))
            })?
            .port();

        listener.set_nonblocking(true).map_err(|e| {
            DbError::connection_failed(format!("Failed to set listener non-blocking: {}", e))
        })?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();

        let thread = thread::spawn(move || {
            run_tunnel_loop(listener, session, remote_host, remote_port, shutdown_clone);
        });

        Ok(Self {
            local_port,
            shutdown,
            forwarder_thread: thread,
        })
    }

    /// Get the local port the tunnel is listening on.
    pub fn local_port(&self) -> u16 {
        self.local_port
    }
}

impl Drop for SshTunnel {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
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

fn authenticate_with_key(
    session: &Session,
    user: &str,
    key_path: Option<&Path>,
    passphrase: Option<&str>,
) -> Result<(), DbError> {
    // Only try SSH agent if no explicit key path was provided.
    // When a key path is specified, the user wants to use that specific key,
    // and the agent call can hang indefinitely in some configurations.
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

    // Build list of key paths to try
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

/// A single tunnel connection pairing a client TCP stream with an SSH channel.
struct TunnelConnection {
    client: TcpStream,
    channel: ssh2::Channel,
    client_buf: Vec<u8>,
    channel_buf: Vec<u8>,
    closed: bool,
}

impl TunnelConnection {
    fn new(client: TcpStream, channel: ssh2::Channel) -> std::io::Result<Self> {
        client.set_nodelay(true)?;
        client.set_nonblocking(true)?;

        Ok(Self {
            client,
            channel,
            client_buf: vec![0u8; 8192],
            channel_buf: vec![0u8; 8192],
            closed: false,
        })
    }

    /// Poll this connection for data transfer. Returns true if any data was transferred.
    fn poll(&mut self) -> bool {
        if self.closed {
            return false;
        }

        let mut activity = false;

        // Client -> SSH channel
        match self.client.read(&mut self.client_buf) {
            Ok(0) => {
                self.closed = true;
                return false;
            }
            Ok(n) => {
                if self.channel.write_all(&self.client_buf[..n]).is_err() {
                    self.closed = true;
                    return false;
                }
                activity = true;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(_) => {
                self.closed = true;
                return false;
            }
        }

        // SSH channel -> Client
        match self.channel.read(&mut self.channel_buf) {
            Ok(0) => {
                self.closed = true;
                return false;
            }
            Ok(n) => {
                if self.client.write_all(&self.channel_buf[..n]).is_err() {
                    self.closed = true;
                    return false;
                }
                activity = true;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(_) => {
                self.closed = true;
                return false;
            }
        }

        activity
    }
}

/// Single-threaded tunnel loop that multiplexes all connections.
///
/// This approach avoids libssh2 thread-safety issues by keeping all SSH
/// operations on a single thread. The session and all its channels are
/// only ever accessed from this one thread.
fn run_tunnel_loop(
    listener: TcpListener,
    session: Session,
    remote_host: String,
    remote_port: u16,
    shutdown: Arc<AtomicBool>,
) {
    session.set_blocking(false);

    let mut connections: Vec<TunnelConnection> = Vec::new();

    while !shutdown.load(Ordering::SeqCst) {
        let mut activity = false;

        // Accept new connections
        match listener.accept() {
            Ok((client_stream, addr)) => {
                log::debug!("[SSH] New tunnel connection from {}", addr);

                // Temporarily set blocking to open the channel
                session.set_blocking(true);
                match session.channel_direct_tcpip(&remote_host, remote_port, None) {
                    Ok(channel) => {
                        session.set_blocking(false);
                        match TunnelConnection::new(client_stream, channel) {
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

        // Poll all active connections
        for conn in &mut connections {
            if conn.poll() {
                activity = true;
            }
        }

        // Remove closed connections
        let before = connections.len();
        connections.retain(|c| !c.closed);
        if connections.len() < before {
            log::debug!(
                "[SSH] Removed {} closed connections, {} active",
                before - connections.len(),
                connections.len()
            );
        }

        // Sleep briefly if no activity to avoid busy-spinning
        if !activity {
            thread::sleep(std::time::Duration::from_micros(500));
        }
    }

    log::info!("[SSH] Tunnel loop shutting down");
}
