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
pub const DRIVER_RPC_V1_0: ProtocolVersion = ProtocolVersion::new(1, 0);
pub const DRIVER_RPC_VERSION: ProtocolVersion = ProtocolVersion::new(1, 1);
pub const AUTH_PROVIDER_RPC_V1_0: ProtocolVersion = ProtocolVersion::new(1, 0);
pub const AUTH_PROVIDER_RPC_V1_1: ProtocolVersion = ProtocolVersion::new(1, 1);
pub const AUTH_PROVIDER_RPC_V1_2: ProtocolVersion = ProtocolVersion::new(1, 2);
/// Current highest auth-provider protocol version.
pub const AUTH_PROVIDER_RPC_VERSION: ProtocolVersion = AUTH_PROVIDER_RPC_V1_2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcApiFamily {
    DriverRpc,
    AuthProviderRpc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcApiContract {
    pub family: RpcApiFamily,
    pub version: ProtocolVersion,
}

impl RpcApiContract {
    pub const fn new(family: RpcApiFamily, version: ProtocolVersion) -> Self {
        Self { family, version }
    }

    pub fn is_compatible_with(self, other: Self) -> bool {
        self.family == other.family && self.version.is_compatible_with(other.version)
    }
}

pub const DRIVER_RPC_API_CONTRACT: RpcApiContract =
    RpcApiContract::new(RpcApiFamily::DriverRpc, DRIVER_RPC_VERSION);

pub const AUTH_PROVIDER_RPC_API_CONTRACT: RpcApiContract =
    RpcApiContract::new(RpcApiFamily::AuthProviderRpc, AUTH_PROVIDER_RPC_VERSION);

pub const DRIVER_RPC_SUPPORTED_VERSIONS: [ProtocolVersion; 2] =
    [DRIVER_RPC_V1_0, DRIVER_RPC_VERSION];

pub const AUTH_PROVIDER_RPC_SUPPORTED_VERSIONS: [ProtocolVersion; 3] =
    [AUTH_PROVIDER_RPC_V1_2, AUTH_PROVIDER_RPC_V1_1, AUTH_PROVIDER_RPC_V1_0];

pub const fn driver_rpc_supported_versions() -> &'static [ProtocolVersion] {
    &DRIVER_RPC_SUPPORTED_VERSIONS
}

pub const fn auth_provider_rpc_supported_versions() -> &'static [ProtocolVersion] {
    &AUTH_PROVIDER_RPC_SUPPORTED_VERSIONS
}

pub fn negotiate_highest_mutual_version(
    family: RpcApiFamily,
    local_versions: &[ProtocolVersion],
    remote_versions: &[ProtocolVersion],
) -> Option<ProtocolVersion> {
    let _family = family;

    local_versions
        .iter()
        .copied()
        .filter(|local| remote_versions.contains(local))
        .max_by_key(|version| (version.major, version.minor))
}

#[cfg(test)]
mod tests {
    use super::{
        DRIVER_RPC_VERSION, ProtocolVersion, RpcApiContract, RpcApiFamily,
        negotiate_highest_mutual_version,
    };

    #[test]
    fn rpc_api_contract_requires_matching_family_and_major() {
        let driver_v1 = RpcApiContract::new(RpcApiFamily::DriverRpc, ProtocolVersion::new(1, 1));
        let same_family_same_major =
            RpcApiContract::new(RpcApiFamily::DriverRpc, ProtocolVersion::new(1, 0));
        let different_family =
            RpcApiContract::new(RpcApiFamily::AuthProviderRpc, ProtocolVersion::new(1, 1));
        let different_major =
            RpcApiContract::new(RpcApiFamily::DriverRpc, ProtocolVersion::new(2, 0));

        assert!(driver_v1.is_compatible_with(same_family_same_major));
        assert!(!driver_v1.is_compatible_with(different_family));
        assert!(!driver_v1.is_compatible_with(different_major));
    }

    #[test]
    fn negotiate_highest_mutual_minor_is_deterministic() {
        let selected = negotiate_highest_mutual_version(
            RpcApiFamily::DriverRpc,
            &[ProtocolVersion::new(1, 1), ProtocolVersion::new(1, 0)],
            &[
                ProtocolVersion::new(1, 0),
                ProtocolVersion::new(1, 1),
                ProtocolVersion::new(1, 3),
            ],
        );

        assert_eq!(selected, Some(DRIVER_RPC_VERSION));
    }

    #[test]
    fn negotiate_highest_mutual_minor_returns_none_without_overlap() {
        let selected = negotiate_highest_mutual_version(
            RpcApiFamily::DriverRpc,
            &[ProtocolVersion::new(1, 1)],
            &[ProtocolVersion::new(2, 0)],
        );

        assert_eq!(selected, None);
    }
}
