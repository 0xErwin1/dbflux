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

    #[test]
    fn start_binds_a_nonzero_ephemeral_port() {
        let joined = Arc::new(AtomicBool::new(false));
        let tunnel = Tunnel::start(
            MockConnector {
                joined: joined.clone(),
            },
            "127.0.0.1".into(),
            1,
            "TEST",
        )
        .unwrap();

        assert_ne!(
            tunnel.local_port(),
            0,
            "binding 127.0.0.1:0 must resolve to a real ephemeral port"
        );
    }

    /// Records whether `test_connection` had already run by the time the
    /// forwarding loop starts. `Tunnel::start` calls `test_connection` and only
    /// then spawns the thread, so the loop must always observe the flag set.
    struct OrderingConnector {
        tested: Arc<AtomicBool>,
        loop_saw_tested: Arc<AtomicBool>,
    }

    impl TunnelConnector for OrderingConnector {
        fn test_connection(&self, _host: &str, _port: u16) -> Result<(), DbError> {
            self.tested.store(true, Ordering::SeqCst);
            Ok(())
        }

        fn run_tunnel_loop(
            self,
            _listener: TcpListener,
            _remote_host: String,
            _remote_port: u16,
            shutdown: Arc<AtomicBool>,
        ) {
            self.loop_saw_tested
                .store(self.tested.load(Ordering::SeqCst), Ordering::SeqCst);

            while !shutdown.load(Ordering::SeqCst) {
                adaptive_sleep(false, false);
            }
        }
    }

    #[test]
    fn test_connection_runs_before_forwarding_thread_starts() {
        let tested = Arc::new(AtomicBool::new(false));
        let loop_saw_tested = Arc::new(AtomicBool::new(false));

        let tunnel = Tunnel::start(
            OrderingConnector {
                tested: tested.clone(),
                loop_saw_tested: loop_saw_tested.clone(),
            },
            "127.0.0.1".into(),
            1,
            "TEST",
        )
        .unwrap();

        assert!(
            tested.load(Ordering::SeqCst),
            "test_connection must run during start()"
        );

        // Dropping joins the thread, so its recorded observation is final afterwards.
        drop(tunnel);

        assert!(
            loop_saw_tested.load(Ordering::SeqCst),
            "the forwarding loop must observe test_connection as already completed"
        );
    }

    /// In-memory remote half for `ForwardingConnection`. `read` drains a queued
    /// inbound buffer; `write` appends to an outbound buffer the test can inspect.
    struct MockRemote {
        inbound: Vec<u8>,
        outbound: Vec<u8>,
    }

    impl Read for MockRemote {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if self.inbound.is_empty() {
                return Err(io::Error::new(io::ErrorKind::WouldBlock, "no data"));
            }

            let n = self.inbound.len().min(buf.len());
            buf[..n].copy_from_slice(&self.inbound[..n]);
            self.inbound.drain(..n);
            Ok(n)
        }
    }

    impl Write for MockRemote {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.outbound.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn write_all_remote(remote: &mut MockRemote, data: &[u8]) -> io::Result<()> {
        remote.write_all(data)
    }

    fn write_all_client(client: &mut TcpStream, data: &[u8]) -> io::Result<()> {
        blocking_write_all(client, data)
    }

    /// Builds a connected loopback `TcpStream` pair. Loopback is in-process and
    /// needs no external network, so this stays deterministic in CI.
    fn loopback_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");

        let client = TcpStream::connect(addr).expect("connect loopback");
        let (server, _) = listener.accept().expect("accept loopback");

        (client, server)
    }

    #[test]
    fn poll_forwards_remote_bytes_to_the_client() {
        let (client, mut peer) = loopback_pair();

        let remote = MockRemote {
            inbound: b"hello-from-remote".to_vec(),
            outbound: Vec::new(),
        };

        let mut conn = ForwardingConnection::new(client, remote).expect("forwarding connection");

        let activity = conn.poll(write_all_remote, write_all_client);

        assert!(activity, "remote had data, so poll must report activity");
        assert!(
            !conn.closed,
            "a successful transfer must not close the tunnel"
        );

        // The peer end of the loopback pair must now hold the forwarded bytes.
        peer.set_read_timeout(Some(Duration::from_secs(1)))
            .expect("set read timeout");
        let mut received = vec![0u8; 64];
        let n = peer.read(&mut received).expect("peer read");
        assert_eq!(&received[..n], b"hello-from-remote");
    }

    #[test]
    fn poll_closes_when_remote_reaches_eof() {
        let (client, _peer) = loopback_pair();

        // inbound empty AND we simulate EOF by returning Ok(0): override read.
        struct EofRemote;
        impl Read for EofRemote {
            fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
                Ok(0)
            }
        }
        impl Write for EofRemote {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                Ok(buf.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        fn write_all_eof(remote: &mut EofRemote, data: &[u8]) -> io::Result<()> {
            remote.write_all(data)
        }

        let mut conn = ForwardingConnection::new(client, EofRemote).expect("forwarding connection");

        let activity = conn.poll(write_all_eof, write_all_client);

        assert!(!activity, "an EOF poll transfers nothing");
        assert!(conn.closed, "remote EOF (Ok(0)) must close the tunnel");
    }
}
