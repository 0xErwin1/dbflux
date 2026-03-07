#![allow(clippy::result_large_err)]

//! TCP-level proxy tunneling for DBFlux database drivers.
//!
//! Provides a local TCP listener that forwards connections through a SOCKS5
//! or HTTP CONNECT proxy, allowing database drivers (which don't natively
//! support proxies) to connect via `127.0.0.1:{local_port}`.
//!
//! Uses `dbflux_tunnel_core::Tunnel` for the shared RAII lifecycle and
//! implements `TunnelConnector` for the proxy-specific forwarding logic.

use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{fmt, fmt::Debug};

use dbflux_core::{DbError, ProxyAuth, ProxyKind, ProxyProfile};
use dbflux_tunnel_core::{
    ForwardingConnection, Tunnel, TunnelConnector, adaptive_sleep, blocking_write_all,
};
use native_tls::{TlsConnector, TlsStream};

/// Proxy protocol to use for tunneling.
#[derive(Debug, Clone)]
pub enum ProxyProtocol {
    Socks5,
    HttpConnect,
    HttpsConnect,
}

enum ProxiedStream {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl ProxiedStream {
    fn set_nodelay(&self) {
        match self {
            Self::Plain(stream) => {
                let _ = stream.set_nodelay(true);
            }
            Self::Tls(stream) => {
                let _ = stream.get_ref().set_nodelay(true);
            }
        }
    }

    fn set_nonblocking(&self, nonblocking: bool) {
        match self {
            Self::Plain(stream) => {
                let _ = stream.set_nonblocking(nonblocking);
            }
            Self::Tls(stream) => {
                let _ = stream.get_ref().set_nonblocking(nonblocking);
            }
        }
    }
}

impl Read for ProxiedStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read(buf),
            Self::Tls(stream) => stream.read(buf),
        }
    }
}

impl Write for ProxiedStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.write(buf),
            Self::Tls(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(stream) => stream.flush(),
            Self::Tls(stream) => stream.flush(),
        }
    }
}

/// Authentication credentials for the proxy server.
#[derive(Clone)]
pub struct ProxyCredentials {
    pub username: String,
    pub password: String,
}

impl Debug for ProxyCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProxyCredentials")
            .field("username", &self.username)
            .field("password", &"***")
            .finish()
    }
}

/// Configuration for establishing a proxy tunnel.
#[derive(Clone)]
pub struct ProxyTunnelConfig {
    pub protocol: ProxyProtocol,
    pub proxy_host: String,
    pub proxy_port: u16,
    pub credentials: Option<ProxyCredentials>,
}

impl Debug for ProxyTunnelConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProxyTunnelConfig")
            .field("protocol", &self.protocol)
            .field("proxy_host", &self.proxy_host)
            .field("proxy_port", &self.proxy_port)
            .field("credentials", &self.credentials)
            .finish()
    }
}

impl ProxyTunnelConfig {
    /// Build a tunnel config from a stored `ProxyProfile` and an optional
    /// password (retrieved from the system keyring).
    pub fn from_profile(profile: &ProxyProfile, password: Option<&str>) -> Self {
        let protocol = match profile.kind {
            ProxyKind::Socks5 => ProxyProtocol::Socks5,
            ProxyKind::Http => ProxyProtocol::HttpConnect,
            ProxyKind::Https => ProxyProtocol::HttpsConnect,
        };

        let credentials = match &profile.auth {
            ProxyAuth::None => None,
            ProxyAuth::Basic { username } => Some(ProxyCredentials {
                username: username.clone(),
                password: password.unwrap_or("").to_string(),
            }),
        };

        Self {
            protocol,
            proxy_host: profile.host.clone(),
            proxy_port: profile.port,
            credentials,
        }
    }
}

/// An active proxy tunnel that forwards local connections through a proxy.
///
/// Wraps `dbflux_tunnel_core::Tunnel` with proxy-specific connection logic.
/// The tunnel shuts down automatically when dropped (RAII).
pub struct ProxyTunnel {
    inner: Tunnel,
}

impl ProxyTunnel {
    /// Start a new proxy tunnel forwarding to the specified remote host and port.
    ///
    /// Verifies connectivity through the proxy before returning. Use
    /// `local_port()` to get the assigned local port for the database driver.
    pub fn start(
        config: ProxyTunnelConfig,
        remote_host: String,
        remote_port: u16,
    ) -> Result<Self, DbError> {
        let inner = Tunnel::start(config, remote_host, remote_port, "Proxy")?;
        Ok(Self { inner })
    }

    /// The local port the tunnel is listening on.
    pub fn local_port(&self) -> u16 {
        self.inner.local_port()
    }
}

impl TunnelConnector for ProxyTunnelConfig {
    fn test_connection(&self, remote_host: &str, remote_port: u16) -> Result<(), DbError> {
        let stream = open_proxied_stream(self, remote_host, remote_port)?;
        drop(stream);
        Ok(())
    }

    fn run_tunnel_loop(
        self,
        listener: TcpListener,
        remote_host: String,
        remote_port: u16,
        shutdown: Arc<AtomicBool>,
    ) {
        run_proxy_tunnel_loop(listener, self, remote_host, remote_port, shutdown);
    }
}

// ---------------------------------------------------------------------------
// Proxied stream establishment
// ---------------------------------------------------------------------------

/// Open a TCP stream to `(remote_host, remote_port)` through the proxy.
fn open_proxied_stream(
    config: &ProxyTunnelConfig,
    remote_host: &str,
    remote_port: u16,
) -> Result<ProxiedStream, DbError> {
    match config.protocol {
        ProxyProtocol::Socks5 => open_socks5_stream(config, remote_host, remote_port),
        ProxyProtocol::HttpConnect => open_http_connect_stream(config, remote_host, remote_port),
        ProxyProtocol::HttpsConnect => open_https_connect_stream(config, remote_host, remote_port),
    }
}

fn open_socks5_stream(
    config: &ProxyTunnelConfig,
    remote_host: &str,
    remote_port: u16,
) -> Result<ProxiedStream, DbError> {
    let proxy_addr = (&*config.proxy_host, config.proxy_port);
    let target = (remote_host, remote_port);

    let stream = match &config.credentials {
        Some(creds) => socks::Socks5Stream::connect_with_password(
            proxy_addr,
            target,
            &creds.username,
            &creds.password,
        ),
        None => socks::Socks5Stream::connect(proxy_addr, target),
    };

    stream
        .map(|s| ProxiedStream::Plain(s.into_inner()))
        .map_err(|e| {
            DbError::connection_failed(format!(
                "SOCKS5 proxy connection to {}:{} failed: {}",
                remote_host, remote_port, e
            ))
        })
}

fn open_http_connect_stream(
    config: &ProxyTunnelConfig,
    remote_host: &str,
    remote_port: u16,
) -> Result<ProxiedStream, DbError> {
    let stream = TcpStream::connect((&*config.proxy_host, config.proxy_port)).map_err(|e| {
        DbError::connection_failed(format!(
            "Failed to connect to HTTP proxy {}:{}: {}",
            config.proxy_host, config.proxy_port, e
        ))
    })?;

    stream.set_nodelay(true).ok();

    let stream = perform_connect_handshake(stream, config, remote_host, remote_port)?;
    Ok(ProxiedStream::Plain(stream))
}

fn open_https_connect_stream(
    config: &ProxyTunnelConfig,
    remote_host: &str,
    remote_port: u16,
) -> Result<ProxiedStream, DbError> {
    let stream = TcpStream::connect((&*config.proxy_host, config.proxy_port)).map_err(|e| {
        DbError::connection_failed(format!(
            "Failed to connect to HTTPS proxy {}:{}: {}",
            config.proxy_host, config.proxy_port, e
        ))
    })?;

    stream.set_nodelay(true).ok();

    let connector = TlsConnector::new().map_err(|e| {
        DbError::connection_failed(format!("Failed to initialize TLS connector: {}", e))
    })?;

    let tls_stream = connector.connect(&config.proxy_host, stream).map_err(|e| {
        DbError::connection_failed(format!(
            "TLS handshake with HTTPS proxy {}:{} failed: {}",
            config.proxy_host, config.proxy_port, e
        ))
    })?;

    let tls_stream = perform_connect_handshake(tls_stream, config, remote_host, remote_port)?;
    Ok(ProxiedStream::Tls(tls_stream))
}

fn perform_connect_handshake<S: Read + Write>(
    mut stream: S,
    config: &ProxyTunnelConfig,
    remote_host: &str,
    remote_port: u16,
) -> Result<S, DbError> {
    let mut request = format!(
        "CONNECT {}:{} HTTP/1.1\r\nHost: {}:{}\r\n",
        remote_host, remote_port, remote_host, remote_port,
    );

    if let Some(creds) = &config.credentials {
        use std::fmt::Write as _;
        let raw = format!("{}:{}", creds.username, creds.password);
        let encoded = base64_encode(raw.as_bytes());
        write!(request, "Proxy-Authorization: Basic {}\r\n", encoded)
            .expect("write to String cannot fail");
    }

    request.push_str("\r\n");

    stream.write_all(request.as_bytes()).map_err(|e| {
        DbError::connection_failed(format!("Failed to send CONNECT request: {}", e))
    })?;

    stream.flush().map_err(|e| {
        DbError::connection_failed(format!("Failed to flush CONNECT request: {}", e))
    })?;

    let status_line = read_http_line(&mut stream).map_err(|e| {
        DbError::connection_failed(format!("Failed to read CONNECT response: {}", e))
    })?;

    let status_ok = status_line
        .split_whitespace()
        .nth(1)
        .is_some_and(|code| code == "200");

    if !status_ok {
        return Err(DbError::connection_failed(format!(
            "HTTP CONNECT proxy rejected connection to {}:{}: {}",
            remote_host,
            remote_port,
            status_line.trim(),
        )));
    }

    // Consume remaining headers until the blank line.
    loop {
        let header = read_http_line(&mut stream).map_err(|e| {
            DbError::connection_failed(format!("Failed to read CONNECT response headers: {}", e))
        })?;

        if header.trim().is_empty() {
            break;
        }
    }

    Ok(stream)
}

/// Read a single HTTP header line (up to `\n`) byte-by-byte from a stream.
///
/// Reads directly from the raw `TcpStream` without buffering so no bytes
/// beyond the line boundary are consumed. This avoids data loss that would
/// occur with `BufReader` when the proxy sends payload data immediately
/// after the header block (e.g. MySQL server greeting).
fn read_http_line<R: Read>(stream: &mut R) -> io::Result<String> {
    let mut line = Vec::with_capacity(128);
    let mut byte = [0u8; 1];

    loop {
        match stream.read_exact(&mut byte) {
            Ok(()) => {
                line.push(byte[0]);
                if byte[0] == b'\n' {
                    break;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
    }

    Ok(String::from_utf8_lossy(&line).into_owned())
}

fn write_to_proxied_stream(stream: &mut ProxiedStream, data: &[u8]) -> io::Result<()> {
    match stream {
        ProxiedStream::Plain(tcp_stream) => blocking_write_all(tcp_stream, data),
        ProxiedStream::Tls(tls_stream) => {
            {
                let inner = tls_stream.get_mut();
                inner.set_nonblocking(false)?;
            }

            let result = tls_stream.write_all(data);

            {
                let inner = tls_stream.get_mut();
                let _ = inner.set_nonblocking(true);
            }

            result
        }
    }
}

/// Minimal Base64 encoder (RFC 4648) — avoids pulling in a dependency for
/// the single use in `Proxy-Authorization`.
fn base64_encode(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut output = String::with_capacity(input.len().div_ceil(3) * 4);

    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;

        output.push(ALPHABET[((triple >> 18) & 0x3F) as usize] as char);
        output.push(ALPHABET[((triple >> 12) & 0x3F) as usize] as char);

        if chunk.len() > 1 {
            output.push(ALPHABET[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            output.push('=');
        }

        if chunk.len() > 2 {
            output.push(ALPHABET[(triple & 0x3F) as usize] as char);
        } else {
            output.push('=');
        }
    }

    output
}

// ---------------------------------------------------------------------------
// Proxy tunnel loop
// ---------------------------------------------------------------------------

/// Single-threaded tunnel loop that multiplexes all proxy connections.
fn run_proxy_tunnel_loop(
    listener: TcpListener,
    config: ProxyTunnelConfig,
    remote_host: String,
    remote_port: u16,
    shutdown: Arc<AtomicBool>,
) {
    let mut connections: Vec<ForwardingConnection<ProxiedStream>> = Vec::new();

    while !shutdown.load(Ordering::SeqCst) {
        let mut activity = false;

        match listener.accept() {
            Ok((client_stream, addr)) => {
                log::debug!("[Proxy] New tunnel connection from {}", addr);

                match open_proxied_stream(&config, &remote_host, remote_port) {
                    Ok(proxied_stream) => {
                        proxied_stream.set_nodelay();
                        proxied_stream.set_nonblocking(true);

                        match ForwardingConnection::new(client_stream, proxied_stream) {
                            Ok(conn) => {
                                connections.push(conn);
                                activity = true;
                            }
                            Err(e) => {
                                log::error!("[Proxy] Failed to setup tunnel connection: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("[Proxy] Failed to open proxied stream: {}", e);
                    }
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(e) => {
                log::error!("[Proxy] Tunnel listener error: {}", e);
                break;
            }
        }

        for conn in &mut connections {
            if conn.poll(write_to_proxied_stream, blocking_write_all) {
                activity = true;
            }
        }

        let before = connections.len();
        connections.retain(|c| !c.closed);
        if connections.len() < before {
            log::debug!(
                "[Proxy] Removed {} closed connections, {} active",
                before - connections.len(),
                connections.len(),
            );
        }

        adaptive_sleep(activity, !connections.is_empty());
    }

    log::info!("[Proxy] Tunnel loop shutting down");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_encode_empty() {
        assert_eq!(base64_encode(b""), "");
    }

    #[test]
    fn base64_encode_single_byte() {
        assert_eq!(base64_encode(b"f"), "Zg==");
    }

    #[test]
    fn base64_encode_two_bytes() {
        assert_eq!(base64_encode(b"fo"), "Zm8=");
    }

    #[test]
    fn base64_encode_three_bytes() {
        assert_eq!(base64_encode(b"foo"), "Zm9v");
    }

    #[test]
    fn base64_encode_rfc_vectors() {
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
    }

    #[test]
    fn base64_encode_credentials() {
        assert_eq!(base64_encode(b"user:pass"), "dXNlcjpwYXNz");
    }

    #[test]
    fn proxy_tunnel_config_debug() {
        let config = ProxyTunnelConfig {
            protocol: ProxyProtocol::Socks5,
            proxy_host: "proxy.local".to_string(),
            proxy_port: 1080,
            credentials: None,
        };
        let debug = format!("{:?}", config);
        assert!(debug.contains("Socks5"));
        assert!(debug.contains("proxy.local"));
    }

    #[test]
    fn proxy_credentials_debug_redacts_password() {
        let credentials = ProxyCredentials {
            username: "admin".to_string(),
            password: "super-secret".to_string(),
        };

        let debug = format!("{:?}", credentials);
        assert!(debug.contains("admin"));
        assert!(debug.contains("***"));
        assert!(!debug.contains("super-secret"));
    }

    #[test]
    fn from_profile_https_maps_to_https_connect() {
        let profile = ProxyProfile {
            id: uuid::Uuid::new_v4(),
            name: "test".to_string(),
            kind: ProxyKind::Https,
            host: "proxy.local".to_string(),
            port: 3128,
            auth: ProxyAuth::None,
            no_proxy: None,
            enabled: true,
            save_secret: false,
        };

        let config = ProxyTunnelConfig::from_profile(&profile, None);
        assert!(matches!(config.protocol, ProxyProtocol::HttpsConnect));
    }

    #[test]
    fn from_profile_basic_auth_no_password() {
        let profile = ProxyProfile {
            id: uuid::Uuid::new_v4(),
            name: "test".to_string(),
            kind: ProxyKind::Socks5,
            host: "proxy.local".to_string(),
            port: 1080,
            auth: ProxyAuth::Basic {
                username: "admin".to_string(),
            },
            no_proxy: None,
            enabled: true,
            save_secret: false,
        };

        let config = ProxyTunnelConfig::from_profile(&profile, None);
        let creds = config.credentials.expect("should have credentials");
        assert_eq!(creds.username, "admin");
        assert_eq!(creds.password, "");
    }

    #[test]
    fn read_http_line_normal() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let writer = std::thread::spawn(move || {
            let mut stream = TcpStream::connect(addr).unwrap();
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nX-Header: val\r\n\r\nextra-data")
                .unwrap();
        });

        let (mut stream, _) = listener.accept().unwrap();
        let line1 = read_http_line(&mut stream).unwrap();
        assert_eq!(line1, "HTTP/1.1 200 OK\r\n");

        let line2 = read_http_line(&mut stream).unwrap();
        assert_eq!(line2, "X-Header: val\r\n");

        let line3 = read_http_line(&mut stream).unwrap();
        assert_eq!(line3, "\r\n");

        // The extra data after headers is still available on the stream.
        let mut remaining = vec![0u8; 10];
        let n = stream.read(&mut remaining).unwrap();
        assert_eq!(&remaining[..n], b"extra-data");

        writer.join().unwrap();
    }

    #[test]
    fn read_http_line_eof_mid_line() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let writer = std::thread::spawn(move || {
            let mut stream = TcpStream::connect(addr).unwrap();
            stream.write_all(b"partial").unwrap();
            drop(stream);
        });

        let (mut stream, _) = listener.accept().unwrap();
        let line = read_http_line(&mut stream).unwrap();
        assert_eq!(line, "partial");

        writer.join().unwrap();
    }
}
