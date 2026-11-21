use crate::envelope::{APP_CONTROL_VERSION, ProtocolVersion};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IpcMessage {
    Ping,
    OpenScript {
        path: PathBuf,
    },
    Focus,
    #[cfg(feature = "telemetry")]
    BeginMcpToolUsage {
        session_nonce: uuid::Uuid,
    },
    #[cfg(feature = "telemetry")]
    FinishMcpToolUsage {
        session_nonce: uuid::Uuid,
        capability: uuid::Uuid,
        success: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum IpcResponse {
    Pong {
        version: String,
    },
    #[default]
    Ok,
    Error {
        message: String,
    },
    #[cfg(feature = "telemetry")]
    McpToolUsageCapability {
        capability: Option<uuid::Uuid>,
    },
    #[cfg(feature = "telemetry")]
    McpToolUsageFinished {
        accepted: bool,
    },
}

/// Versioned request envelope for app-control IPC messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppControlRequest {
    pub protocol_version: ProtocolVersion,
    pub request_id: u64,
    #[serde(default)]
    pub auth_token: Option<String>,
    pub body: IpcMessage,
}

impl AppControlRequest {
    pub fn new(request_id: u64, auth_token: Option<String>, body: IpcMessage) -> Self {
        Self {
            protocol_version: APP_CONTROL_VERSION,
            request_id,
            auth_token,
            body,
        }
    }
}

/// Versioned response envelope for app-control IPC messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppControlResponse {
    pub protocol_version: ProtocolVersion,
    pub request_id: u64,
    pub body: IpcResponse,
}

impl AppControlResponse {
    pub fn ok(request_id: u64, body: IpcResponse) -> Self {
        Self {
            protocol_version: APP_CONTROL_VERSION,
            request_id,
            body,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_wire_bytes_are_frozen() {
        for (body, bytes) in [
            (IpcMessage::Ping, vec![0]),
            (
                IpcMessage::OpenScript { path: "a".into() },
                vec![1, 1, b'a'],
            ),
            (IpcMessage::Focus, vec![2]),
        ] {
            assert_eq!(postcard::to_allocvec(&body).unwrap(), bytes);
            let request = AppControlRequest {
                protocol_version: ProtocolVersion::new(1, 0),
                request_id: 7,
                auth_token: Some("t".into()),
                body,
            };
            let mut expected = vec![1, 0, 7, 1, 1, b't'];
            expected.extend(bytes);
            assert_eq!(postcard::to_allocvec(&request).unwrap(), expected);
        }
        assert_eq!(postcard::to_allocvec(&IpcResponse::Ok).unwrap(), vec![1]);
        assert_eq!(
            postcard::to_allocvec(&IpcResponse::Pong {
                version: "v".into()
            })
            .unwrap(),
            vec![0, 1, b'v']
        );
        assert_eq!(
            postcard::to_allocvec(&IpcResponse::Error {
                message: "e".into()
            })
            .unwrap(),
            vec![2, 1, b'e']
        );
    }

    #[cfg(feature = "telemetry")]
    #[test]
    fn telemetry_variants_append_and_roundtrip() {
        let session_nonce = uuid::Uuid::new_v4();
        let capability = uuid::Uuid::new_v4();
        for (body, index) in [
            (IpcMessage::BeginMcpToolUsage { session_nonce }, 3),
            (
                IpcMessage::FinishMcpToolUsage {
                    session_nonce,
                    capability,
                    success: true,
                },
                4,
            ),
        ] {
            let bytes = postcard::to_allocvec(&body).unwrap();
            assert_eq!(bytes[0], index);
            let decoded: IpcMessage = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(postcard::to_allocvec(&decoded).unwrap(), bytes);
        }
        for (body, index) in [
            (
                IpcResponse::McpToolUsageCapability {
                    capability: Some(capability),
                },
                3,
            ),
            (IpcResponse::McpToolUsageFinished { accepted: false }, 4),
        ] {
            let bytes = postcard::to_allocvec(&body).unwrap();
            assert_eq!(bytes[0], index);
            let decoded: IpcResponse = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(postcard::to_allocvec(&decoded).unwrap(), bytes);
        }
    }
}
