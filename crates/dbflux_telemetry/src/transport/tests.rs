use super::*;

#[cfg(debug_assertions)]
mod debug_http {
    use super::*;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
    };

    const PAYLOAD: &str = r#"{"schema":"dbflux.telemetry-event/v1","event":"ping","install_id":"11111111-1111-4111-8111-111111111111","delivery_id":"22222222-2222-4222-8222-222222222222","sent_at":"2026-01-01T00:00:00Z","version":"1.2.3","channel":"stable","os":"linux","arch":"amd64"}"#;
    const STORED: &str = r#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored"}"#;
    const DUPLICATE: &str = r#"{"schema":"dbflux.telemetry-delivery/v1","decision":"duplicate"}"#;

    fn mock(responses: Vec<Vec<u8>>) -> (Endpoint, thread::JoinHandle<Vec<Vec<u8>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("loopback listener");
        let endpoint = Endpoint::parse(
            Some(&format!(
                "http://{}/v1/events",
                listener.local_addr().expect("address")
            )),
            true,
        )
        .expect("debug endpoint");
        let handle = thread::spawn(move || {
            let mut requests = Vec::new();
            for response in responses {
                let (mut stream, _) = listener.accept().expect("accept");
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("read bound");
                let mut request = Vec::new();
                let mut byte = [0];
                while !request.ends_with(b"\r\n\r\n") {
                    stream.read_exact(&mut byte).expect("headers");
                    request.push(byte[0]);
                    assert!(request.len() < 16384);
                }
                let headers = String::from_utf8(request.clone()).expect("headers UTF8");
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().expect("length"))
                    })
                    .expect("content length");
                let mut body = vec![0; length];
                stream.read_exact(&mut body).expect("body");
                request.extend(body);
                requests.push(request);
                stream.write_all(&response).expect("response");
            }
            requests
        });
        (endpoint, handle)
    }

    fn response(status: u16, body: &[u8], extra: &str) -> Vec<u8> {
        let mut bytes = format!(
            "HTTP/1.1 {status} fixture\r\nContent-Length: {}\r\nConnection: close\r\n{extra}\r\n",
            body.len()
        )
        .into_bytes();
        bytes.extend(body);
        bytes
    }

    #[test]
    fn exact_retry_and_closed_status_codes() {
        let (endpoint, server) = mock(vec![
            response(200, STORED.as_bytes(), ""),
            response(200, DUPLICATE.as_bytes(), ""),
        ]);
        let transport = Transport::new(endpoint).expect("client");
        assert!(matches!(
            transport.attempt(PAYLOAD).expect("stored").decision,
            crate::Decision::Stored
        ));
        assert!(matches!(
            transport
                .clone()
                .attempt(PAYLOAD)
                .expect("duplicate")
                .decision,
            crate::Decision::Duplicate
        ));
        let requests = server.join().expect("server");
        assert_eq!(requests[0], requests[1]);
        for request in requests {
            let text = String::from_utf8(request).expect("request UTF8");
            assert_eq!(text.split_once("\r\n\r\n").expect("body").1, PAYLOAD);
            assert!(!text.to_ascii_lowercase().contains("authorization"));
        }
        for (status, expected) in [
            (400, TransportError::BadRequest),
            (409, TransportError::Conflict),
            (429, TransportError::RateLimited),
            (503, TransportError::Unavailable),
            (302, TransportError::UnexpectedStatus),
        ] {
            let (endpoint, server) = mock(vec![response(
                status,
                b"secret body",
                "Location: http://127.0.0.1:1/secret\r\n",
            )]);
            let error = Transport::new(endpoint)
                .expect("client")
                .attempt(PAYLOAD)
                .expect_err("status");
            assert_eq!(error, expected);
            assert!(!error.to_string().contains("secret"));
            server.join().expect("server");
        }
    }

    // A gzip stream with a single uncompressed DEFLATE block, no new test dependency.
    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let length = u16::try_from(bytes.len()).expect("small fixture");
        let mut compressed = vec![31, 139, 8, 0, 0, 0, 0, 0, 0, 255, 1];
        compressed.extend(length.to_le_bytes());
        compressed.extend((!length).to_le_bytes());
        compressed.extend(bytes);
        let mut checksum = u32::MAX;
        for byte in bytes {
            checksum ^= u32::from(*byte);
            for _ in 0..8 {
                checksum = (checksum >> 1) ^ (0xedb88320 & 0_u32.wrapping_sub(checksum & 1));
            }
        }
        compressed.extend((!checksum).to_le_bytes());
        compressed.extend((bytes.len() as u32).to_le_bytes());
        compressed
    }

    #[test]
    fn bounded_strict_and_decompressed_acknowledgements() {
        for body in [b"{}".to_vec(), br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"unknown"}"#.to_vec(), br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored","decision":"duplicate"}"#.to_vec(), vec![b' '; 1025]] {
        let (endpoint, server) = mock(vec![response(200, &body, "")]);
        assert_eq!(Transport::new(endpoint).expect("client").attempt(PAYLOAD).expect_err("bad acknowledgement"), TransportError::BadResponse);
        server.join().expect("server");
    }
        for (body, valid) in [
            (STORED.as_bytes().to_vec(), true),
            (vec![b' '; 2048], false),
        ] {
            let (endpoint, server) = mock(vec![response(
                200,
                &gzip(&body),
                "Content-Encoding: gzip\r\n",
            )]);
            assert_eq!(
                Transport::new(endpoint)
                    .expect("client")
                    .attempt(PAYLOAD)
                    .is_ok(),
                valid
            );
            server.join().expect("server");
        }
        let (endpoint, server) = mock(vec![
            b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n{}".to_vec(),
        ]);
        assert_eq!(
            Transport::new(endpoint)
                .expect("client")
                .attempt(PAYLOAD)
                .expect_err("truncated"),
            TransportError::Network
        );
        server.join().expect("server");
    }

    #[test]
    fn validation_precedes_io_and_does_not_repair() {
        let endpoint =
            Endpoint::parse(Some("http://127.0.0.1:1/v1/events"), true).expect("endpoint");
        let transport = Transport::new(endpoint).expect("client");
        for raw in [
            "{}".to_owned(),
            " ".repeat(4097),
            PAYLOAD.replace("\"event\":", "\"event\":\"ping\",\"event\":"),
            PAYLOAD.replace("1.2.3", "invalid"),
        ] {
            assert_eq!(
                transport.attempt(&raw).expect_err("invalid before IO"),
                TransportError::InvalidPayload
            );
        }
        assert!(TransportError::Network.retryable());
        assert!(!TransportError::BadResponse.retryable());
    }

    #[test]
    fn deadline_is_bounded() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
        let endpoint = Endpoint::parse(
            Some(&format!(
                "http://{}/v1/events",
                listener.local_addr().expect("address")
            )),
            true,
        )
        .expect("endpoint");
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept");
            thread::sleep(Duration::from_millis(200));
            drop(stream);
        });
        let transport =
            Transport::with_deadline(endpoint, Duration::from_millis(50)).expect("client");
        let start = std::time::Instant::now();
        assert_eq!(
            transport.attempt(PAYLOAD).expect_err("timeout"),
            TransportError::Timeout
        );
        assert!(start.elapsed() < Duration::from_secs(1));
        server.join().expect("server");
    }

    #[test]
    fn response_body_deadline_is_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
        let endpoint = Endpoint::parse(
            Some(&format!(
                "http://{}/v1/events",
                listener.local_addr().expect("address")
            )),
            true,
        )
        .expect("endpoint");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut headers = Vec::new();
            let mut byte = [0];
            while !headers.ends_with(b"\r\n\r\n") {
                stream.read_exact(&mut byte).expect("request headers");
                headers.push(byte[0]);
            }
            let mut body = vec![0; PAYLOAD.len()];
            stream.read_exact(&mut body).expect("request body");
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n{")
                .expect("partial response");
            thread::sleep(Duration::from_millis(200));
        });
        let transport =
            Transport::with_deadline(endpoint, Duration::from_millis(50)).expect("client");
        assert_eq!(
            transport.attempt(PAYLOAD).expect_err("body timeout"),
            TransportError::Timeout
        );
        server.join().expect("server");
    }

    /// Each wait fits the budget on its own (headers after 400 ms, body 400 ms
    /// later, budget 600 ms), so only a deadline over the whole exchange fires.
    #[test]
    fn deadline_spans_headers_and_body_together() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
        let endpoint = Endpoint::parse(
            Some(&format!(
                "http://{}/v1/events",
                listener.local_addr().expect("address")
            )),
            true,
        )
        .expect("endpoint");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut request = Vec::new();
            let mut byte = [0];
            while !request.ends_with(b"\r\n\r\n") {
                stream.read_exact(&mut byte).expect("request headers");
                request.push(byte[0]);
            }
            let mut body = vec![0; PAYLOAD.len()];
            stream.read_exact(&mut body).expect("request body");
            thread::sleep(Duration::from_millis(400));
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                STORED.len()
            );
            // The client gives up at 600 ms, so the late body write may hit a
            // closed socket; either outcome is fine for this test.
            if stream.write_all(head.as_bytes()).is_ok() {
                thread::sleep(Duration::from_millis(400));
                stream.write_all(STORED.as_bytes()).ok();
            }
        });
        let transport =
            Transport::with_deadline(endpoint, Duration::from_millis(600)).expect("client");
        assert_eq!(
            transport.attempt(PAYLOAD).expect_err("total deadline"),
            TransportError::Timeout
        );
        server.join().expect("server");
    }
}

// Live HTTPS coverage over the real production builder: an in-process
// rustls server bound to 127.0.0.1:0 and the static public fixtures under
// tests/fixtures/tls (see that README). No TLS knob is weakened; the only
// test additions are a trusted fixture root and a loopback override, so
// untrusted chains and wrong names still fail through normal validation.
#[cfg(test)]
mod tls_live {
    use super::*;
    use rustls::{
        ServerConnection,
        pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    };
    use std::{
        io::{self, Read, Write},
        net::{SocketAddr, TcpListener, TcpStream},
        path::Path,
        sync::Arc,
        thread,
        time::Instant,
    };

    const FIXTURE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/tls");

    // Total finite bounds: accept wait, whole-exchange deadline checked per read
    // (socket timeouts reset and cannot bound intermittent progress), settle
    // window for zero-socket assertions.
    const ACCEPT_DEADLINE: Duration = Duration::from_secs(10);
    const EXCHANGE_DEADLINE: Duration = Duration::from_secs(5);
    const QUIET_WINDOW: Duration = Duration::from_millis(200);

    const PAYLOAD: &str = r#"{"schema":"dbflux.telemetry-event/v1","event":"ping","install_id":"11111111-1111-4111-8111-111111111111","delivery_id":"22222222-2222-4222-8222-222222222222","sent_at":"2026-01-01T00:00:00Z","version":"1.2.3","channel":"stable","os":"linux","arch":"amd64"}"#;
    const STORED: &str = r#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored"}"#;
    const DUPLICATE: &str = r#"{"schema":"dbflux.telemetry-delivery/v1","decision":"duplicate"}"#;

    fn fixture(name: &str) -> Vec<u8> {
        std::fs::read(Path::new(FIXTURE_DIR).join(name))
            .unwrap_or_else(|error| panic!("fixture {name}: {error}"))
    }

    fn test_root_certificate() -> reqwest::Certificate {
        reqwest::Certificate::from_der(&fixture("ca.der")).expect("fixture CA")
    }

    fn server_config() -> rustls::ServerConfig {
        // Explicit ring provider: no global default provider is installed
        // or consulted inside the test process.
        rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("protocol versions")
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(fixture("server.der"))],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(fixture("server-key.pk8.der"))),
        )
        .expect("server certificate chain")
    }

    fn https_endpoint(port: u16, host: &str) -> Endpoint {
        Endpoint::parse(Some(&format!("https://{host}:{port}/v1/events")), false)
            .expect("https endpoint")
    }

    /// Test-only transport: production builder, localhost loopback override, fixture root iff trusted.
    fn test_transport_trusting(
        endpoint: Endpoint,
        trust_fixture_root: bool,
    ) -> Result<Transport, TransportError> {
        let mut builder = client_builder(Duration::from_secs(10))
            .resolve("localhost", SocketAddr::from(([127, 0, 0, 1], 0)));
        if trust_fixture_root {
            builder = builder.add_root_certificate(test_root_certificate());
        }
        let client = builder
            .build()
            .map_err(|_| TransportError::ClientConfiguration)?;
        Ok(Transport {
            endpoint,
            client,
            deadline: Duration::from_secs(10),
        })
    }

    type TlsStream = rustls::StreamOwned<ServerConnection, BoundedStream>;

    /// Absolute-deadline view of the owned connection: the fixed exchange
    /// deadline is checked BEFORE each underlying read/write/flush, which
    /// re-caps the socket timeout to min(remaining, 2s); handshake,
    /// header, body and response writes all flow through it. Subject to
    /// normal OS scheduler/syscall granularity slack, this is not a hard
    /// guarantee that no operation completes after the deadline.
    struct BoundedStream {
        stream: TcpStream,
        deadline: Instant,
    }

    impl BoundedStream {
        fn check_deadline(&mut self) -> io::Result<()> {
            let remaining = self.deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "exchange deadline exceeded",
                ));
            }
            let capped = remaining.min(Duration::from_secs(2));
            self.stream.set_read_timeout(Some(capped))?;
            self.stream.set_write_timeout(Some(capped))
        }
    }

    impl Read for BoundedStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.check_deadline()?;
            self.stream.read(buf)
        }
    }

    impl Write for BoundedStream {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.check_deadline()?;
            self.stream.write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.check_deadline()?;
            self.stream.flush()
        }
    }

    /// Blocking in-process rustls server bound to 127.0.0.1:0. Every
    /// exchange is bounded; handshake and read failures are expected
    /// vectors (untrusted chains, name mismatches) that stop the loop and
    /// return the requests observed so far instead of panicking.
    fn tls_server(responses: Vec<Vec<u8>>) -> (u16, thread::JoinHandle<Vec<Vec<u8>>>) {
        let config = Arc::new(server_config());
        let listener = TcpListener::bind("127.0.0.1:0").expect("loopback listener");
        let port = listener.local_addr().expect("bound address").port();
        listener.set_nonblocking(true).expect("nonblocking");
        let handle = thread::spawn(move || {
            let mut requests = Vec::new();
            for response in responses {
                let Some(stream) = bounded_accept(&listener) else {
                    break;
                };
                if stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .is_err()
                    || stream
                        .set_write_timeout(Some(Duration::from_secs(2)))
                        .is_err()
                {
                    break;
                }
                let Ok(connection) = ServerConnection::new(Arc::clone(&config)) else {
                    break;
                };
                let mut tls = rustls::StreamOwned::new(
                    connection,
                    BoundedStream {
                        stream,
                        deadline: Instant::now() + EXCHANGE_DEADLINE,
                    },
                );
                if exchange(&mut tls, &response, &mut requests).is_err() {
                    break;
                }
            }
            requests
        });
        (port, handle)
    }

    fn bounded_accept(listener: &TcpListener) -> Option<TcpStream> {
        let deadline = Instant::now() + ACCEPT_DEADLINE;
        loop {
            match listener.accept() {
                Ok((stream, _)) => return Some(stream),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(_) => return None,
            }
            if Instant::now() >= deadline {
                return None;
            }
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn exchange(
        tls: &mut TlsStream,
        response: &[u8],
        requests: &mut Vec<Vec<u8>>,
    ) -> io::Result<()> {
        let mut request = Vec::new();
        let mut byte = [0_u8];
        while !request.ends_with(b"\r\n\r\n") {
            tls.read_exact(&mut byte)?;
            request.push(byte[0]);
            assert!(request.len() < 16384, "unbounded request headers");
        }
        let length = String::from_utf8_lossy(&request)
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .filter(|length| *length <= 8192)
            .ok_or_else(|| io::Error::other("unusable content length"))?;
        let mut body = vec![0_u8; length];
        tls.read_exact(&mut body)?;
        request.extend_from_slice(&body);
        requests.push(request);
        tls.write_all(response)?;
        tls.flush()
    }

    fn tls_response(status: u16, body: &[u8], extra_headers: &str) -> Vec<u8> {
        let mut bytes = format!(
            "HTTP/1.1 {status} fixture\r\nContent-Length: {}\r\nConnection: close\r\n{extra_headers}\r\n",
            body.len()
        )
        .into_bytes();
        bytes.extend_from_slice(body);
        bytes
    }

    /// Owned live/nonblocking loopback trap: URLs at its port route here, never to a user port.
    fn quiet_trap() -> (TcpListener, u16) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("loopback trap");
        let port = listener.local_addr().expect("bound address").port();
        listener.set_nonblocking(true).expect("nonblocking trap");
        (listener, port)
    }

    /// TCP connections arriving within a settle window; zero proves no socket opened.
    fn observed_connections(listener: &TcpListener) -> usize {
        let mut connections = 0;
        let deadline = Instant::now() + QUIET_WINDOW;
        while Instant::now() < deadline {
            match listener.accept() {
                Ok(_) => connections += 1,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("unexpected trap accept error: {error}"),
            }
        }
        connections
    }

    #[test]
    fn tls_live_stored_then_duplicate_replays_identical_request_bytes() {
        let (port, server) = tls_server(vec![
            tls_response(200, STORED.as_bytes(), ""),
            tls_response(200, DUPLICATE.as_bytes(), ""),
        ]);
        let transport =
            test_transport_trusting(https_endpoint(port, "localhost"), true).expect("client");
        assert!(matches!(
            transport.attempt(PAYLOAD).expect("stored").decision,
            crate::Decision::Stored
        ));
        assert!(matches!(
            transport
                .clone()
                .attempt(PAYLOAD)
                .expect("duplicate")
                .decision,
            crate::Decision::Duplicate
        ));
        let requests = server.join().expect("server");
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0], requests[1],
            "delivery replay must be byte-identical"
        );
        for request in &requests {
            let text = String::from_utf8(request.clone()).expect("request UTF8");
            assert_eq!(text.split_once("\r\n\r\n").expect("body").1, PAYLOAD);
            assert!(!text.to_ascii_lowercase().contains("authorization"));
        }
    }

    #[test]
    fn tls_live_untrusted_certificate_is_network_error() {
        let (port, server) = tls_server(vec![tls_response(200, STORED.as_bytes(), "")]);
        // Matching DNS localhost, no fixture root: the untrusted chain alone must fail.
        let endpoint = https_endpoint(port, "localhost");
        let error = test_transport_trusting(endpoint, false)
            .expect("client")
            .attempt(PAYLOAD)
            .expect_err("untrusted chain");
        assert_eq!(error, TransportError::Network);
        assert!(server.join().expect("server").is_empty());
    }

    #[test]
    fn tls_live_localhost_only_san_rejects_ip_hostname() {
        let (port, server) = tls_server(vec![tls_response(200, STORED.as_bytes(), "")]);
        // Same trusted fixture root, but the URL names 127.0.0.1 while the
        // leaf carries a DNS "localhost" SAN only: hostname validation
        // must fail like any other certificate mismatch.
        let endpoint = https_endpoint(port, "127.0.0.1");
        let error = test_transport_trusting(endpoint, true)
            .expect("client")
            .attempt(PAYLOAD)
            .expect_err("name mismatch");
        assert_eq!(error, TransportError::Network);
        assert!(server.join().expect("server").is_empty());
    }

    #[test]
    fn tls_live_redirect_is_unexpected_status_without_following() {
        // Owned redirect trap: a followed redirect would route here via the loopback override.
        let (trap, trap_port) = quiet_trap();
        let (port, server) = tls_server(vec![tls_response(
            302,
            b"moved",
            &format!("Location: https://localhost:{trap_port}/secret\r\n"),
        )]);
        let endpoint = https_endpoint(port, "localhost");
        let error = test_transport_trusting(endpoint, true)
            .expect("client")
            .attempt(PAYLOAD)
            .expect_err("redirect");
        assert_eq!(error, TransportError::UnexpectedStatus);
        assert_eq!(
            server.join().expect("server").len(),
            1,
            "the redirect must not be followed"
        );
        assert_eq!(observed_connections(&trap), 0, "no redirect follow");
    }

    #[test]
    fn tls_live_bounded_and_malformed_acknowledgements_are_bad_response() {
        let mut oversized = STORED.as_bytes().to_vec();
        oversized.extend(std::iter::repeat_n(b' ', 1200));
        for body in [
            oversized,
            b"{}".to_vec(),
            br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"unknown"}"#.to_vec(),
        ] {
            let (port, server) = tls_server(vec![tls_response(200, &body, "")]);
            let endpoint = https_endpoint(port, "localhost");
            let error = test_transport_trusting(endpoint, true)
                .expect("client")
                .attempt(PAYLOAD)
                .expect_err("bad acknowledgement");
            assert_eq!(error, TransportError::BadResponse);
            assert_eq!(server.join().expect("server").len(), 1);
        }
    }

    #[test]
    fn tls_live_invalid_payload_fails_before_any_socket_io() {
        // Owned trap listener: validation must fail before any socket I/O (zero connections).
        let (trap, port) = quiet_trap();
        let transport =
            test_transport_trusting(https_endpoint(port, "localhost"), true).expect("client");
        for raw in [
            "{}".to_owned(),
            " ".repeat(4097),
            PAYLOAD.replace("\"event\":", "\"event\":\"ping\",\"event\":"),
        ] {
            assert_eq!(
                transport.attempt(&raw).expect_err("invalid before IO"),
                TransportError::InvalidPayload
            );
            assert_eq!(observed_connections(&trap), 0, "no socket opened");
        }
    }

    /// Precondition test for the parent-owned proxy proof. The harness
    /// launches this binary with an owned environment: all six proxy
    /// variables aimed at the deliberately unreachable
    /// http://127.0.0.1:0, empty NO_PROXY/no_proxy, and
    /// DBFLUX_TLS_PROXY_TEST=1. Run it explicitly via --include-ignored;
    /// the guard below never lets it pass silently elsewhere. The test
    /// process never mutates its own environment.
    #[test]
    #[ignore = "requires harness-owned proxy environment: run with --include-ignored and DBFLUX_TLS_PROXY_TEST=1"]
    fn tls_live_proxy_environment_is_ignored_by_production_client() {
        // Exact harness contract: exact flag, six proxy vars at the
        // unreachable http://127.0.0.1:0, NO_PROXY/no_proxy empty.
        assert_eq!(
            std::env::var("DBFLUX_TLS_PROXY_TEST").as_deref(),
            Ok("1"),
            "must run under the harness proxy environment (DBFLUX_TLS_PROXY_TEST=1)"
        );
        for name in [
            "HTTPS_PROXY",
            "https_proxy",
            "HTTP_PROXY",
            "http_proxy",
            "ALL_PROXY",
            "all_proxy",
        ] {
            let exact = std::env::var(name).as_deref() == Ok("http://127.0.0.1:0");
            assert!(exact, "{name} must be the exact unreachable proxy");
        }
        for name in ["NO_PROXY", "no_proxy"] {
            let exempt = std::env::var(name).as_deref() == Ok("");
            assert!(exempt, "{name} must be present and exactly empty");
        }

        let (port, server) = tls_server(vec![tls_response(200, STORED.as_bytes(), "")]);

        // Negative control FIRST, while the owned endpoint is live: a
        // client equal to production except it honors the ambient
        // environment. It must fail through the unreachable proxy;
        // bypassing it reaches the valid server and fails this proof.
        let control = Client::builder()
            .use_rustls_tls()
            .redirect(Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .gzip(true)
            .add_root_certificate(test_root_certificate())
            .resolve("localhost", SocketAddr::from(([127, 0, 0, 1], 0)))
            .build()
            .expect("control client");
        control
            .post(https_endpoint(port, "localhost").0)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(PAYLOAD)
            .send()
            .expect_err(
                "control client must fail through the unreachable harness proxy; success \
                 means localhost proxies are bypassed and this proof is void",
            );

        // Production: .no_proxy() keeps the ambient proxy out; endpoint served this delivery.
        let transport =
            test_transport_trusting(https_endpoint(port, "localhost"), true).expect("client");
        assert!(matches!(
            transport.attempt(PAYLOAD).expect("stored").decision,
            crate::Decision::Stored
        ));

        let requests = server.join().expect("server");
        assert_eq!(requests.len(), 1, "exactly one production delivery");
        let text = String::from_utf8(requests[0].clone()).expect("request UTF8");
        assert_eq!(text.split_once("\r\n\r\n").expect("body").1, PAYLOAD);
    }
}

#[test]
fn proxies_disabled_in_builder_without_environment_mutation() {
    let builder = client_builder(Duration::from_secs(10));
    let debug = format!("{builder:?}");
    // Reqwest's Debug omits an empty proxy list rather than printing [].
    assert!(!debug.contains("proxies:"), "{debug}");
    let with_proxy = Client::builder()
        .proxy(reqwest::Proxy::all("http://127.0.0.1:1").expect("synthetic proxy"));
    assert!(format!("{with_proxy:?}").contains("proxies:"));
    assert!(!format!("{:?}", with_proxy.no_proxy()).contains("proxies:"));
}

#[test]
fn endpoint_admission() {
    assert_eq!(
        Endpoint::parse(None, false),
        Err(TransportError::MissingEndpoint)
    );
    assert!(Endpoint::parse(Some("https://collector.invalid/v1/events"), false).is_ok());
    for raw in [
        "ftp://127.0.0.1/v1/events",
        "http://localhost/v1/events",
        "http://192.0.2.1/v1/events",
        "https://user:secret@collector.invalid/v1/events",
        "https://collector.invalid/v1/events?secret=value",
        "https://collector.invalid/v1/events#fragment",
        "https://collector.invalid/",
        "https://collector.invalid/v1/events/",
    ] {
        assert_eq!(
            Endpoint::parse(Some(raw), true),
            Err(TransportError::InvalidEndpoint)
        );
    }
    assert!(Endpoint::parse(Some("http://127.0.0.1/v1/events"), false).is_err());
    assert_eq!(
        Endpoint::parse(Some("http://127.0.0.1/v1/events"), true).is_ok(),
        cfg!(debug_assertions)
    );
}
