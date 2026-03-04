#![allow(clippy::result_large_err)]

//! Shared TCP tunnel infrastructure for proxy and SSH tunnels.
//!
//! `Tunnel` binds a local listener, spawns a background thread, and shuts
//! down on drop. Protocol-specific behavior is injected via `TunnelConnector`.

use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use dbflux_core::DbError;

/// Protocol-specific tunnel connector (SOCKS5, HTTP CONNECT, SSH, etc.).
pub trait TunnelConnector: Send + 'static {
    /// Verify that the remote target is reachable.
    fn test_connection(&self, remote_host: &str, remote_port: u16) -> Result<(), DbError>;

    /// Run the forwarding loop until `shutdown` is set.
    /// The listener is already bound and non-blocking.
    fn run_tunnel_loop(
        self,
        listener: TcpListener,
        remote_host: String,
        remote_port: u16,
        shutdown: Arc<AtomicBool>,
    );
}

/// RAII tunnel handle. Shuts down its background thread on drop.
pub struct Tunnel {
    local_port: u16,
    shutdown: Arc<AtomicBool>,
    #[allow(dead_code)]
    thread: JoinHandle<()>,
}

impl Tunnel {
    pub fn start<C: TunnelConnector>(
        connector: C,
        remote_host: String,
        remote_port: u16,
        label: &str,
    ) -> Result<Self, DbError> {
        log::info!(
            "[{}] Testing tunnel connectivity to {}:{}",
            label,
            remote_host,
            remote_port,
        );

        connector.test_connection(&remote_host, remote_port)?;
        log::info!("[{}] Tunnel connectivity verified", label);

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
            connector.run_tunnel_loop(listener, remote_host, remote_port, shutdown_clone);
        });

        Ok(Self {
            local_port,
            shutdown,
            thread,
        })
    }

    pub fn local_port(&self) -> u16 {
        self.local_port
    }
}

impl Drop for Tunnel {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }
}

// ---------------------------------------------------------------------------
// Shared tunnel loop utilities
// ---------------------------------------------------------------------------

/// Temporarily switches a non-blocking socket to blocking for `write_all`,
/// avoiding `WouldBlock` when the kernel send buffer is full.
pub fn blocking_write_all(stream: &mut TcpStream, data: &[u8]) -> io::Result<()> {
    stream.set_nonblocking(false)?;
    let result = (&*stream).write_all(data);
    let _ = stream.set_nonblocking(true);
    result
}

/// Bidirectional forwarding between a local `TcpStream` and a remote `R`.
pub struct ForwardingConnection<R: Read + Write> {
    pub client: TcpStream,
    pub remote: R,
    client_buf: Vec<u8>,
    remote_buf: Vec<u8>,
    pub closed: bool,
}

impl<R: Read + Write> ForwardingConnection<R> {
    pub fn new(client: TcpStream, remote: R) -> io::Result<Self> {
        client.set_nodelay(true)?;
        client.set_nonblocking(true)?;

        Ok(Self {
            client,
            remote,
            client_buf: vec![0u8; 8192],
            remote_buf: vec![0u8; 8192],
            closed: false,
        })
    }

    /// Returns `true` if any data was transferred.
    pub fn poll(
        &mut self,
        write_to_remote: fn(&mut R, &[u8]) -> io::Result<()>,
        write_to_client: fn(&mut TcpStream, &[u8]) -> io::Result<()>,
    ) -> bool {
        if self.closed {
            return false;
        }

        let mut activity = false;

        // Client -> Remote
        match self.client.read(&mut self.client_buf) {
            Ok(0) => {
                self.closed = true;
                return false;
            }
            Ok(n) => {
                if write_to_remote(&mut self.remote, &self.client_buf[..n]).is_err() {
                    self.closed = true;
                    return false;
                }
                activity = true;
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(_) => {
                self.closed = true;
                return false;
            }
        }

        // Remote -> Client
        match self.remote.read(&mut self.remote_buf) {
            Ok(0) => {
                self.closed = true;
                return false;
            }
            Ok(n) => {
                if write_to_client(&mut self.client, &self.remote_buf[..n]).is_err() {
                    self.closed = true;
                    return false;
                }
                activity = true;
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(_) => {
                self.closed = true;
                return false;
            }
        }

        activity
    }
}

/// Sleeps 50ms idle / 1ms active / 0 when data transferred.
pub fn adaptive_sleep(activity: bool, has_connections: bool) {
    if !activity {
        if !has_connections {
            thread::sleep(std::time::Duration::from_millis(50));
        } else {
            thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}
