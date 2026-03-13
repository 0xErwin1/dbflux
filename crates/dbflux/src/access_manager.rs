#[cfg(feature = "aws")]
use std::sync::Arc;

use dbflux_core::DbError;
use dbflux_core::access::{AccessHandle, AccessKind, AccessManager};

/// Concrete access manager for the app crate.
///
/// Dispatches to the right tunnel infrastructure based on the `AccessKind`
/// variant. SSH and proxy tunnels are currently handled by the legacy connect
/// path in `ConnectProfileParams::execute()` — this manager only handles
/// direct connections and managed tunnels (e.g. `aws-ssm`).
pub struct AppAccessManager {
    #[cfg(feature = "aws")]
    ssm_factory: Option<Arc<dbflux_ssm::SsmTunnelFactory>>,
}

impl AppAccessManager {
    #[cfg(feature = "aws")]
    pub fn new(ssm_factory: Option<Arc<dbflux_ssm::SsmTunnelFactory>>) -> Self {
        Self { ssm_factory }
    }

    #[cfg(not(feature = "aws"))]
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl AccessManager for AppAccessManager {
    async fn open(
        &self,
        access_kind: &AccessKind,
        remote_host: &str,
        _remote_port: u16,
    ) -> Result<AccessHandle, DbError> {
        match access_kind {
            AccessKind::Direct => Ok(AccessHandle::direct()),

            AccessKind::Ssh { .. } => Err(DbError::connection_failed(
                "SSH tunnels are managed by the legacy connect path",
            )),

            AccessKind::Proxy { .. } => Err(DbError::connection_failed(
                "Proxy tunnels are managed by the legacy connect path",
            )),

            AccessKind::Managed { provider, params } => {
                self.open_managed(provider, params, remote_host).await
            }
        }
    }
}

impl AppAccessManager {
    async fn open_managed(
        &self,
        provider: &str,
        params: &std::collections::HashMap<String, String>,
        remote_host: &str,
    ) -> Result<AccessHandle, DbError> {
        match provider {
            #[cfg(feature = "aws")]
            "aws-ssm" => {
                let instance_id = params.get("instance_id").map(String::as_str).unwrap_or("");
                let region = params
                    .get("region")
                    .map(String::as_str)
                    .unwrap_or("us-east-1");
                let remote_port: u16 = params
                    .get("remote_port")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);

                let factory = self.ssm_factory.as_ref().ok_or_else(|| {
                    DbError::connection_failed("SSM tunnel factory not available")
                })?;

                let tunnel = factory.start(instance_id, region, remote_host, remote_port)?;
                let local_port = tunnel.local_port();

                Ok(AccessHandle::tunnel(local_port, Box::new(tunnel)))
            }

            other => Err(DbError::connection_failed(format!(
                "Unknown managed access provider: '{}'. No handler registered.",
                other
            ))),
        }
    }
}
