use crate::DbError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// How the app connects to the remote database host.
///
/// `Serialize` always writes the new format. `Deserialize` uses `AccessKindWire`
/// as an intermediate to migrate the legacy `"ssm"` JSON format transparently.
#[derive(Debug, Clone, Serialize, Default)]
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
    /// Generic managed-access variant. `provider` identifies the access
    /// backend (e.g. `"aws-ssm"`). `params` carries provider-specific keys
    /// (e.g. `"instance_id"`, `"region"`, `"remote_port"`).
    Managed {
        provider: String,
        #[serde(default)]
        params: HashMap<String, String>,
    },
}

impl<'de> Deserialize<'de> for AccessKind {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        AccessKindWire::deserialize(deserializer).map(AccessKind::from)
    }
}

/// Internal wire type used only for deserialization.
///
/// Handles both the current `"managed"` format and the legacy `"ssm"` format
/// so that saved profiles round-trip correctly after the migration.
#[derive(Debug, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
enum AccessKindWire {
    Direct,
    Ssh {
        ssh_tunnel_profile_id: Uuid,
    },
    Proxy {
        proxy_profile_id: Uuid,
    },
    Managed {
        provider: String,
        #[serde(default)]
        params: HashMap<String, String>,
    },
    /// Legacy variant written by older DBFlux versions. Migrated to
    /// `Managed { provider: "aws-ssm", params: { ... } }` on read.
    Ssm {
        instance_id: String,
        region: String,
        remote_port: u16,
        #[serde(default)]
        auth_profile_id: Option<Uuid>,
    },
}

impl From<AccessKindWire> for AccessKind {
    fn from(wire: AccessKindWire) -> Self {
        match wire {
            AccessKindWire::Direct => AccessKind::Direct,
            AccessKindWire::Ssh {
                ssh_tunnel_profile_id,
            } => AccessKind::Ssh {
                ssh_tunnel_profile_id,
            },
            AccessKindWire::Proxy { proxy_profile_id } => AccessKind::Proxy { proxy_profile_id },
            AccessKindWire::Managed { provider, params } => AccessKind::Managed { provider, params },
            AccessKindWire::Ssm {
                instance_id,
                region,
                remote_port,
                auth_profile_id,
            } => {
                let mut params = HashMap::new();
                params.insert("instance_id".to_string(), instance_id);
                params.insert("region".to_string(), region);
                params.insert("remote_port".to_string(), remote_port.to_string());
                if let Some(id) = auth_profile_id {
                    params.insert("auth_profile_id".to_string(), id.to_string());
                }
                AccessKind::Managed {
                    provider: "aws-ssm".to_string(),
                    params,
                }
            }
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn old_ssm_json_migrates_to_managed() {
        let json = serde_json::json!({
            "method": "ssm",
            "instance_id": "i-abc",
            "region": "us-east-1",
            "remote_port": 5432
        });

        let kind: AccessKind = serde_json::from_value(json).unwrap();

        match kind {
            AccessKind::Managed { provider, params } => {
                assert_eq!(provider, "aws-ssm");
                assert_eq!(params["instance_id"], "i-abc");
                assert_eq!(params["region"], "us-east-1");
                assert_eq!(params["remote_port"], "5432");
                assert!(!params.contains_key("auth_profile_id"));
            }
            other => panic!("expected Managed, got {:?}", other),
        }
    }

    #[test]
    fn old_ssm_with_auth_profile_id_migrates() {
        let profile_id = Uuid::new_v4();
        let json = serde_json::json!({
            "method": "ssm",
            "instance_id": "i-xyz",
            "region": "eu-west-1",
            "remote_port": 5433,
            "auth_profile_id": profile_id.to_string()
        });

        let kind: AccessKind = serde_json::from_value(json).unwrap();

        match kind {
            AccessKind::Managed { params, .. } => {
                assert_eq!(params["auth_profile_id"], profile_id.to_string());
            }
            other => panic!("expected Managed, got {:?}", other),
        }
    }

    #[test]
    fn managed_roundtrip() {
        let mut params = HashMap::new();
        params.insert("instance_id".to_string(), "i-test".to_string());
        params.insert("region".to_string(), "ap-east-1".to_string());

        let original = AccessKind::Managed {
            provider: "aws-ssm".to_string(),
            params: params.clone(),
        };

        let serialized = serde_json::to_value(&original).unwrap();
        let deserialized: AccessKind = serde_json::from_value(serialized).unwrap();

        match deserialized {
            AccessKind::Managed { provider, params: p } => {
                assert_eq!(provider, "aws-ssm");
                assert_eq!(p, params);
            }
            other => panic!("expected Managed, got {:?}", other),
        }
    }

    #[test]
    fn direct_ssh_proxy_unaffected() {
        let cases = vec![
            serde_json::json!({"method": "direct"}),
            serde_json::json!({"method": "ssh", "ssh_tunnel_profile_id": Uuid::new_v4().to_string()}),
            serde_json::json!({"method": "proxy", "proxy_profile_id": Uuid::new_v4().to_string()}),
        ];

        for case in cases {
            let method = case["method"].as_str().unwrap().to_string();
            let kind: AccessKind = serde_json::from_value(case).unwrap();
            match (&kind, method.as_str()) {
                (AccessKind::Direct, "direct") => {}
                (AccessKind::Ssh { .. }, "ssh") => {}
                (AccessKind::Proxy { .. }, "proxy") => {}
                _ => panic!("unexpected variant for method '{}'", method),
            }
        }
    }
}
