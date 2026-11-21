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
    deadline: Duration,
}

impl Transport {
    pub fn new(endpoint: Endpoint) -> Result<Self, TransportError> {
        Self::with_deadline(endpoint, Duration::from_secs(10))
    }

    fn with_deadline(endpoint: Endpoint, deadline: Duration) -> Result<Self, TransportError> {
        let client = client_builder(deadline)
            .build()
            .map_err(|_| TransportError::ClientConfiguration)?;
        Ok(Self {
            endpoint,
            client,
            deadline,
        })
    }

    /// Validates and sends exactly these bytes once. No retries or admin auth.
    pub fn attempt(&self, frozen_payload: &str) -> Result<Acknowledgement, TransportError> {
        validate_payload(frozen_payload).map_err(|_| TransportError::InvalidPayload)?;
        let response = self
            .client
            .post(self.endpoint.0.clone())
            // The client-level timeout gives the header wait and every body
            // read their own budget; this one spans connect to the last byte.
            .timeout(self.deadline)
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
mod tests;
