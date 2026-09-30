//! One blocking attempt; construct off the UI thread and reuse clones.
//! Retry only retryable codes with caller-owned backoff. No error carries a URL,
//! response body, credentials, or the underlying client's diagnostic string.

use crate::{Acknowledgement, validate_payload};
use reqwest::{Url, blocking::Client, redirect::Policy};
use std::{io::Read, net::IpAddr, time::Duration};

#[derive(Clone, PartialEq, Eq)]
pub struct Endpoint(Url);

impl std::fmt::Debug for Endpoint {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Endpoint([redacted])")
    }
}

impl Endpoint {
    /// Full explicit URL, not an origin. HTTP is debug-only numeric loopback.
    pub fn parse(raw: Option<&str>, allow_debug_loopback: bool) -> Result<Self, TransportError> {
        let raw = raw.ok_or(TransportError::MissingEndpoint)?;
        // URL parsers normalize whitespace, backslashes and dot segments; do
        // not admit those as alternative spellings of the configured route.
        if raw
            .chars()
            .any(|character| character.is_whitespace() || character == '\\')
            || !raw.ends_with("/v1/events")
        {
            return Err(TransportError::InvalidEndpoint);
        }
        let url = Url::parse(raw).map_err(|_| TransportError::InvalidEndpoint)?;
        let authority = raw
            .split_once("://")
            .ok_or(TransportError::InvalidEndpoint)?
            .1
            .split('/')
            .next()
            .ok_or(TransportError::InvalidEndpoint)?;
        if authority.contains('@')
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() != "/v1/events"
            || raw
                .split_once("://")
                .map(|(_, tail)| tail.find('/').map(|index| &tail[index..]))
                != Some(Some("/v1/events"))
        {
            return Err(TransportError::InvalidEndpoint);
        }
        match url.scheme() {
            "https" => {}
            "http" if cfg!(debug_assertions) && allow_debug_loopback => {
                let host = url.host_str().ok_or(TransportError::InvalidEndpoint)?;
                let address: IpAddr = host
                    .trim_matches(['[', ']'])
                    .parse()
                    .map_err(|_| TransportError::InvalidEndpoint)?;
                if !address.is_loopback() || !authority.starts_with(host) {
                    return Err(TransportError::InvalidEndpoint);
                }
            }
            _ => return Err(TransportError::InvalidEndpoint),
        }
        Ok(Self(url))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("telemetry transport: {self:?}")]
pub enum TransportError {
    MissingEndpoint,
    InvalidEndpoint,
    InvalidPayload,
    ClientConfiguration,
    BadRequest,
    Conflict,
    RateLimited,
    Unavailable,
    UnexpectedStatus,
    BadResponse,
    Timeout,
    /// Includes connection, DNS, TLS/certificate and response stream failures.
    /// Reqwest does not expose a stable TLS-specific classifier; never inspect
    /// diagnostic strings. Retry with bounded backoff, not relaxed TLS policy.
    Network,
}

impl TransportError {
    pub fn retryable(self) -> bool {
        matches!(
            self,
            Self::Timeout | Self::Network | Self::RateLimited | Self::Unavailable
        )
    }
}

#[derive(Clone)]
pub struct Transport {
    endpoint: Endpoint,
    client: Client,
}

impl Transport {
    pub fn new(endpoint: Endpoint) -> Result<Self, TransportError> {
        Self::with_deadline(endpoint, Duration::from_secs(10))
    }

    fn with_deadline(endpoint: Endpoint, deadline: Duration) -> Result<Self, TransportError> {
        let client = client_builder(deadline)
            .build()
            .map_err(|_| TransportError::ClientConfiguration)?;
        Ok(Self { endpoint, client })
    }

    /// Validates and sends exactly these bytes once. No retries or admin auth.
    pub fn attempt(&self, frozen_payload: &str) -> Result<Acknowledgement, TransportError> {
        validate_payload(frozen_payload).map_err(|_| TransportError::InvalidPayload)?;
        let response = self
            .client
            .post(self.endpoint.0.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(frozen_payload.to_owned())
            .send()
            .map_err(network_error)?;
        match response.status().as_u16() {
            200 => {}
            400 => return Err(TransportError::BadRequest),
            409 => return Err(TransportError::Conflict),
            429 => return Err(TransportError::RateLimited),
            503 => return Err(TransportError::Unavailable),
            _ => return Err(TransportError::UnexpectedStatus),
        }
        let mut bytes = Vec::with_capacity(1025);
        response
            .take(1025)
            .read_to_end(&mut bytes)
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::TimedOut
                    || error
                        .get_ref()
                        .and_then(|source| source.downcast_ref::<reqwest::Error>())
                        .is_some_and(reqwest::Error::is_timeout)
                {
                    TransportError::Timeout
                } else {
                    TransportError::Network
                }
            })?;
        Acknowledgement::parse(200, &bytes).map_err(|_| TransportError::BadResponse)
    }
}

fn client_builder(deadline: Duration) -> reqwest::blocking::ClientBuilder {
    Client::builder()
        .use_rustls_tls()
        .no_proxy()
        .redirect(Policy::none())
        .connect_timeout(Duration::from_secs(5).min(deadline))
        .timeout(deadline)
        .gzip(true)
}

fn network_error(error: reqwest::Error) -> TransportError {
    if error.is_timeout() {
        TransportError::Timeout
    } else {
        TransportError::Network
    }
}

#[cfg(test)]
mod tests {
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
        const DUPLICATE: &str =
            r#"{"schema":"dbflux.telemetry-delivery/v1","decision":"duplicate"}"#;

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
            let mut bytes = format!("HTTP/1.1 {status} fixture\r\nContent-Length: {}\r\nConnection: close\r\n{extra}\r\n", body.len()).into_bytes();
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
}
