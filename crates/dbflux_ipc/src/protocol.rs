use crate::envelope::{ProtocolVersion, APP_CONTROL_VERSION};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IpcMessage {
    Ping,
    OpenScript { path: PathBuf },
    Focus,
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
}

/// Versioned request envelope for app-control IPC messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppControlRequest {
    pub protocol_version: ProtocolVersion,
    pub request_id: u64,
    pub body: IpcMessage,
}

impl AppControlRequest {
    pub fn new(request_id: u64, body: IpcMessage) -> Self {
        Self {
            protocol_version: APP_CONTROL_VERSION,
            request_id,
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
