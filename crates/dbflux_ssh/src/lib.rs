//! SSH tunneling support for DBFlux database drivers.
//!
//! This crate provides SSH tunnel functionality that can be shared across
//! different database drivers (PostgreSQL, MySQL, etc.).

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use dbflux_core::{DbError, SshAuthMethod, SshTunnelConfig};
use ssh2::Session;

/// An active SSH tunnel that forwards local connections to a remote host.
///
/// The tunnel runs in a background thread and automatically shuts down
/// when dropped.
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
    pub fn start(session: Session, remote_host: String, remote_port: u16) -> Result<Self, DbError> {
        let listener = TcpListener::bind("127.0.0.1:0").map_err(|e| {
            DbError::ConnectionFailed(format!("Failed to bind local tunnel port: {}", e))
        })?;

        let local_port = listener
            .local_addr()
            .map_err(|e| {
                DbError::ConnectionFailed(format!("Failed to get local tunnel address: {}", e))
            })?
            .port();

        listener.set_nonblocking(true).map_err(|e| {
            DbError::ConnectionFailed(format!("Failed to set listener non-blocking: {}", e))
        })?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();

        let session = Arc::new(Mutex::new(session));

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
        DbError::ConnectionFailed(format!(
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
        .map_err(|e| DbError::ConnectionFailed(format!("Failed to create SSH session: {}", e)))?;

    session.set_tcp_stream(tcp);
    session.set_timeout(30000);

    session
        .handshake()
        .map_err(|e| DbError::ConnectionFailed(format!("SSH handshake failed: {}", e)))?;

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
                DbError::ConnectionFailed("SSH password required but not provided".to_string())
            })?;
            session
                .userauth_password(&config.user, password)
                .map_err(|e| {
                    DbError::ConnectionFailed(format!("SSH password authentication failed: {}", e))
                })?;
        }
    }

    if !session.authenticated() {
        return Err(DbError::ConnectionFailed(
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
    Err(DbError::ConnectionFailed(format!(
        "SSH key authentication failed: {}",
        error_detail
    )))
}

fn run_tunnel_loop(
    listener: TcpListener,
    session: Arc<Mutex<Session>>,
    remote_host: String,
    remote_port: u16,
    shutdown: Arc<AtomicBool>,
) {
    while !shutdown.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((client_stream, _)) => {
                let session = session.clone();
                let remote_host = remote_host.clone();
                let shutdown = shutdown.clone();

                thread::spawn(move || {
                    if let Err(e) = handle_tunnel_connection(
                        client_stream,
                        session,
                        &remote_host,
                        remote_port,
                        shutdown,
                    ) {
                        log::error!("SSH tunnel connection error: {}", e);
                    }
                });
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(e) => {
                log::error!("SSH tunnel listener error: {}", e);
                break;
            }
        }
    }
}

fn open_ssh_channel_blocking(
    session: &Session,
    remote_host: &str,
    remote_port: u16,
) -> Result<ssh2::Channel, ssh2::Error> {
    session.set_blocking(true);
    session.channel_direct_tcpip(remote_host, remote_port, None)
}

fn handle_tunnel_connection(
    mut client_stream: TcpStream,
    session: Arc<Mutex<Session>>,
    remote_host: &str,
    remote_port: u16,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut channel = {
        let session = session
            .lock()
            .map_err(|e| format!("Session lock failed: {}", e))?;

        let channel = open_ssh_channel_blocking(&session, remote_host, remote_port)?;
        session.set_blocking(false);
        channel
    };

    client_stream.set_nodelay(true)?;
    client_stream.set_nonblocking(true)?;

    let mut client_buf = [0u8; 8192];
    let mut channel_buf = [0u8; 8192];

    while !shutdown.load(Ordering::SeqCst) {
        let mut activity = false;

        match client_stream.read(&mut client_buf) {
            Ok(0) => break,
            Ok(n) => {
                channel.write_all(&client_buf[..n])?;
                activity = true;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => return Err(Box::new(e)),
        }

        match channel.read(&mut channel_buf) {
            Ok(0) => break,
            Ok(n) => {
                client_stream.write_all(&channel_buf[..n])?;
                activity = true;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => return Err(Box::new(e)),
        }

        if !activity {
            thread::sleep(std::time::Duration::from_micros(100));
        }
    }

    Ok(())
}
