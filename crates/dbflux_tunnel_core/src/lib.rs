#![allow(clippy::result_large_err)]

//! Shared TCP tunnel infrastructure for proxy and SSH tunnels.
//!
//! `Tunnel` binds a local listener, spawns a background thread, and shuts
//! down on drop. Protocol-specific behavior is injected via `TunnelConnector`.

use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use dbflux_core::DbError;

/// Maximum time `Drop` waits for the forwarding thread to observe `shutdown`
/// and exit before detaching it. Blocking sections inside the loop (a stalled
/// `blocking_write_all`, an SSH handshake) may not poll `shutdown` promptly, so
/// the join is bounded to keep the dropping thread (often the UI thread) from
/// hanging.
const DROP_JOIN_GRACE: Duration = Duration::from_secs(2);

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
    thread: Option<JoinHandle<()>>,
    /// Receives on `Disconnect` once the forwarding thread returns and drops
    /// its `Sender`, signalling that `thread.join()` will complete immediately.
    /// Wrapped in a `Mutex` so `Tunnel` stays `Sync` (`Receiver` is `!Sync`),
    /// which the `dbflux_core::Connection: Send + Sync` bound requires.
    thread_exit: Mutex<Receiver<()>>,
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

        let (exit_tx, thread_exit) = mpsc::channel::<()>();

        let thread = thread::spawn(move || {
            let _exit_tx = exit_tx;
            connector.run_tunnel_loop(listener, remote_host, remote_port, shutdown_clone);
        });

        Ok(Self {
            local_port,
            shutdown,
            thread: Some(thread),
            thread_exit: Mutex::new(thread_exit),
        })
    }

    pub fn local_port(&self) -> u16 {
        self.local_port
    }
}

impl Drop for Tunnel {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);

        let Some(handle) = self.thread.take() else {
            return;
        };

        let exit_guard = match self.thread_exit.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        match exit_guard.recv_timeout(DROP_JOIN_GRACE) {
            Err(RecvTimeoutError::Disconnected) => {
                // The forwarding thread has returned and dropped its sender, so
                // join completes immediately and releases the local port.
                if handle.join().is_err() {
                    log::warn!("[Tunnel] forwarding thread panicked");
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                log::warn!(
                    "[Tunnel] forwarding thread did not stop within the grace period; detaching"
                );
            }
            Ok(()) => {
                // The sender should only ever drop, never send. Treat a value as
                // an exit signal and join immediately.
                if handle.join().is_err() {
                    log::warn!("[Tunnel] forwarding thread panicked");
                }
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    struct MockConnector {
        joined: Arc<AtomicBool>,
    }

    impl TunnelConnector for MockConnector {
        fn test_connection(&self, _host: &str, _port: u16) -> Result<(), DbError> {
            Ok(())
        }

        fn run_tunnel_loop(
            self,
            _listener: TcpListener,
            _remote_host: String,
            _remote_port: u16,
            shutdown: Arc<AtomicBool>,
        ) {
            while !shutdown.load(Ordering::SeqCst) {
                adaptive_sleep(false, false);
            }
            self.joined.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn tunnel_drop_joins_thread() {
        let joined = Arc::new(AtomicBool::new(false));
        let t = Tunnel::start(
            MockConnector {
                joined: joined.clone(),
            },
            "127.0.0.1".into(),
            1,
            "TEST",
        )
        .unwrap();
        drop(t);
        assert!(
            joined.load(Ordering::SeqCst),
            "thread must be joined before drop returns"
        );
    }

    struct StuckConnector {
        exited: Arc<AtomicBool>,
    }

    impl TunnelConnector for StuckConnector {
        fn test_connection(&self, _host: &str, _port: u16) -> Result<(), DbError> {
            Ok(())
        }

        fn run_tunnel_loop(
            self,
            _listener: TcpListener,
            _remote_host: String,
            _remote_port: u16,
            _shutdown: Arc<AtomicBool>,
        ) {
            thread::sleep(std::time::Duration::from_secs(10));
            self.exited.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn tunnel_drop_detaches_stuck_thread_within_grace() {
        let exited = Arc::new(AtomicBool::new(false));
        let t = Tunnel::start(
            StuckConnector {
                exited: exited.clone(),
            },
            "127.0.0.1".into(),
            1,
            "TEST",
        )
        .unwrap();

        let started = std::time::Instant::now();
        drop(t);
        let elapsed = started.elapsed();

        assert!(
            elapsed < DROP_JOIN_GRACE + std::time::Duration::from_secs(1),
            "drop must return shortly after the grace period, took {elapsed:?}"
        );
        assert!(
            !exited.load(Ordering::SeqCst),
            "thread ignoring shutdown must be detached, not joined"
        );
    }
}
