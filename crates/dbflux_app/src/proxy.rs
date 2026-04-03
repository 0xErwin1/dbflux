use std::any::Any;

use dbflux_core::ResolvedProxy;
use dbflux_core::secrecy::ExposeSecret;
use dbflux_proxy::{ProxyTunnel, ProxyTunnelConfig};

/// Creates a proxy tunnel and returns a type-erased handle plus the local port.
///
/// This is the bridge between `dbflux_core` (which cannot depend on
/// `dbflux_proxy`) and the actual tunnel implementation.
pub fn create_proxy_tunnel(
    resolved: &ResolvedProxy,
    remote_host: &str,
    remote_port: u16,
) -> Result<(Box<dyn Any + Send + Sync>, u16), String> {
    let config = ProxyTunnelConfig::from_profile(
        &resolved.profile,
        resolved.secret.as_ref().map(|value| value.expose_secret()),
    );

    let tunnel = ProxyTunnel::start(config, remote_host.to_string(), remote_port)
        .map_err(|e| e.to_string())?;

    let local_port = tunnel.local_port();

    Ok((Box::new(tunnel), local_port))
}
