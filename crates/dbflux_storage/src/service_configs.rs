use dbflux_core::{RpcServiceKind, ServiceConfig, ServiceRpcApiContract};

use crate::bootstrap::StorageRuntime;

pub fn load_service_configs(runtime: &StorageRuntime) -> Vec<ServiceConfig> {
    let repo = runtime.services();

    if let Ok(entries) = repo.all() {
        entries
            .into_iter()
            .map(|dto| {
                let args = repo.get_args(&dto.socket_id).unwrap_or_default();
                let env = repo.get_env(&dto.socket_id).unwrap_or_default();
                let api_contract = service_api_contract_from_dto(&dto);

                ServiceConfig {
                    socket_id: dto.socket_id,
                    enabled: dto.enabled,
                    command: dto.command,
                    args,
                    env,
                    startup_timeout_ms: dto.startup_timeout_ms.map(|value| value as u64),
                    kind: rpc_service_kind_from_storage(&dto.service_kind),
                    api_contract,
                }
            })
            .collect()
    } else {
        Vec::new()
    }
}

fn rpc_service_kind_from_storage(kind: &str) -> RpcServiceKind {
    match kind {
        "driver" => RpcServiceKind::Driver,
        "auth_provider" => RpcServiceKind::AuthProvider,
        other => {
            log::warn!(
                "Unknown RPC service kind '{}'; defaulting to driver for compatibility",
                other
            );
            RpcServiceKind::Driver
        }
    }
}

fn service_api_contract_from_dto(
    dto: &crate::repositories::services::ServiceDto,
) -> Option<ServiceRpcApiContract> {
    match (&dto.api_family, dto.api_major, dto.api_minor) {
        (Some(family), Some(major), Some(minor)) => {
            let major = match u16::try_from(major) {
                Ok(major) => major,
                Err(_) => {
                    log::warn!(
                        "Ignoring persisted RPC API contract for service '{}' because api_major={} is out of range for u16",
                        dto.socket_id,
                        major,
                    );
                    return None;
                }
            };

            let minor = match u16::try_from(minor) {
                Ok(minor) => minor,
                Err(_) => {
                    log::warn!(
                        "Ignoring persisted RPC API contract for service '{}' because api_minor={} is out of range for u16",
                        dto.socket_id,
                        minor,
                    );
                    return None;
                }
            };

            Some(ServiceRpcApiContract::new(family.clone(), major, minor))
        }
        _ => None,
    }
}
