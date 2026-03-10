#[cfg(feature = "aws")]
use std::sync::Arc;

use dbflux_core::DbError;
use dbflux_core::access::{AccessHandle, AccessKind, AccessManager};

/// Concrete access manager for the app crate.
///
/// Dispatches to the right tunnel infrastructure based on the `AccessKind`
/// variant. SSH and proxy tunnels are currently handled by the legacy connect
/// path in `ConnectProfileParams::execute()` — this manager only handles
/// direct connections and SSM tunnels (new in the pipeline).
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

            #[cfg(feature = "aws")]
            AccessKind::Ssm {
                instance_id,
                region,
                remote_port,
                ..
            } => {
                let factory = self.ssm_factory.as_ref().ok_or_else(|| {
                    DbError::connection_failed("SSM tunnel factory not available")
                })?;

                let tunnel = factory.start(instance_id, region, remote_host, *remote_port)?;
                let local_port = tunnel.local_port();

                Ok(AccessHandle::tunnel(local_port, Box::new(tunnel)))
            }

            #[cfg(not(feature = "aws"))]
            AccessKind::Ssm { .. } => Err(DbError::connection_failed(
                "SSM tunnel support requires the 'aws' feature",
            )),
        }
    }
}
