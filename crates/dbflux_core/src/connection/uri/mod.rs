//! Authority helpers shared by the drivers' `parse_uri` and `build_uri`.

use std::borrow::Cow;

#[cfg(test)]
mod tests;

/// Splits the `host[:port]` part of a URI authority. A bracketed IPv6 host
/// (`[::1]:5432`) is returned without its brackets, and the colons inside
/// it are never taken for the port separator.
pub fn split_uri_host_port(host_port: &str) -> (&str, Option<&str>) {
    if let Some(bracketed) = host_port.strip_prefix('[')
        && let Some((host, rest)) = bracketed.split_once(']')
    {
        return (host, rest.strip_prefix(':'));
    }

    match host_port.rsplit_once(':') {
        Some((host, port)) => (host, Some(port)),
        None => (host_port, None),
    }
}

/// Writes a host for a URI authority, bracketing an IPv6 literal so its
/// colons are not read as the port separator.
pub fn uri_host(host: &str) -> Cow<'_, str> {
    if host.contains(':') && !host.starts_with('[') {
        Cow::Owned(format!("[{host}]"))
    } else {
        Cow::Borrowed(host)
    }
}

/// Percent-decodes a URI path or userinfo component, keeping the raw text
/// when it does not decode to UTF-8.
pub fn decode_uri_component(component: &str) -> String {
    urlencoding::decode(component)
        .map(Cow::into_owned)
        .unwrap_or_else(|_| component.to_string())
}
