use serde::{Deserialize, Serialize};

/// Wire-level protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolVersion {
    pub major: u16,
    pub minor: u16,
}

impl ProtocolVersion {
    pub const fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }

    pub const fn is_compatible_with(self, other: Self) -> bool {
        self.major == other.major
    }
}

pub const APP_CONTROL_VERSION: ProtocolVersion = ProtocolVersion::new(1, 0);
pub const DRIVER_RPC_VERSION: ProtocolVersion = ProtocolVersion::new(1, 0);
