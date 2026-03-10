use crate::DbError;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum AccessKind {
    #[default]
    Direct,
    Ssh {
        ssh_tunnel_profile_id: Uuid,
    },
    Proxy {
        proxy_profile_id: Uuid,
    },
    Ssm {
        instance_id: String,
        region: String,
        remote_port: u16,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth_profile_id: Option<Uuid>,
    },
}

pub struct AccessHandle {
    local_port: u16,
    _handle: Option<Box<dyn std::any::Any + Send + Sync>>,
}

impl AccessHandle {
    pub fn direct() -> Self {
        Self {
            local_port: 0,
            _handle: None,
        }
    }

    pub fn tunnel(local_port: u16, handle: Box<dyn std::any::Any + Send + Sync>) -> Self {
        Self {
            local_port,
            _handle: Some(handle),
        }
    }

    pub fn local_port(&self) -> u16 {
        self.local_port
    }

    pub fn is_tunneled(&self) -> bool {
        self._handle.is_some()
    }
}

/// Abstraction over tunnel/access setup (SSH, proxy, SSM, direct).
///
/// The app crate provides the concrete implementation that dispatches
/// to the right tunnel infrastructure based on the `AccessKind` variant.
#[async_trait::async_trait]
pub trait AccessManager: Send + Sync {
    async fn open(
        &self,
        access_kind: &AccessKind,
        remote_host: &str,
        remote_port: u16,
    ) -> Result<AccessHandle, DbError>;
}
