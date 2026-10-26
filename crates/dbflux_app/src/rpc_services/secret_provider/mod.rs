use std::sync::Arc;

use dbflux_core::values::DynSecretProvider;
use dbflux_core::{DbError, RpcServiceKind, ServiceConfig, ServiceRpcApiContract};
use dbflux_ipc::{
    BUILT_IN_SECRET_PROVIDER_IDS, IpcServiceLaunchConfig, RpcSecretProvider,
    SECRET_PROVIDER_RPC_API_CONTRACT,
};

use super::{RpcServiceDescriptor, RpcServiceLaunch, normalize_error_message};

#[cfg(test)]
mod tests;

const SECRET_PROVIDER_RPC_FAMILY: &str = "secret_provider_rpc";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExternalSecretProviderStage {
    Config,
    Compatibility,
    Launch,
    Probe,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalSecretProviderDiagnostic {
    pub socket_id: String,
    pub stage: ExternalSecretProviderStage,
    pub summary: String,
    pub details: Option<String>,
}

pub(crate) enum SecretProviderServiceAdaptation<T> {
    Registered {
        provider_id: String,
        service: T,
    },
    SkippedDisabled {
        socket_id: String,
    },
    SkippedNonSecretProvider {
        socket_id: String,
        kind: RpcServiceKind,
    },
    SkippedDuplicate {
        socket_id: String,
        provider_id: String,
    },
    Incompatible {
        diagnostic: ExternalSecretProviderDiagnostic,
    },
    ProbeFailed {
        diagnostic: ExternalSecretProviderDiagnostic,
    },
}

impl RpcServiceDescriptor {
    fn secret_provider_launch(&self) -> Option<&IpcServiceLaunchConfig> {
        match &self.launch {
            Some(RpcServiceLaunch::SecretProvider(launch)) => Some(launch),
            _ => None,
        }
    }
}

/// Probes a secret-provider service and wraps it for the value resolver.
///
/// Blocking: the probe launches the managed host when needed and performs the Hello handshake.
pub(crate) fn adapt_secret_provider_service(
    descriptor: RpcServiceDescriptor,
    provider_exists: impl FnOnce(&str) -> bool,
) -> SecretProviderServiceAdaptation<Arc<dyn DynSecretProvider>> {
    adapt_secret_provider_service_with(
        descriptor,
        provider_exists,
        |socket_id, launch, reserved_ids| {
            RpcSecretProvider::probe(socket_id, launch.cloned(), reserved_ids)
                .map(|provider| Arc::new(provider) as Arc<dyn DynSecretProvider>)
                .map_err(Box::new)
        },
    )
}

pub(crate) fn adapt_secret_provider_service_with<Probe>(
    descriptor: RpcServiceDescriptor,
    provider_exists: impl FnOnce(&str) -> bool,
    probe: Probe,
) -> SecretProviderServiceAdaptation<Arc<dyn DynSecretProvider>>
where
    Probe: FnOnce(
        &str,
        Option<&IpcServiceLaunchConfig>,
        &[&str],
    ) -> Result<Arc<dyn DynSecretProvider>, Box<DbError>>,
{
    if !descriptor.config.enabled {
        return SecretProviderServiceAdaptation::SkippedDisabled {
            socket_id: descriptor.config.socket_id,
        };
    }

    if descriptor.config.kind != RpcServiceKind::SecretProvider {
        return SecretProviderServiceAdaptation::SkippedNonSecretProvider {
            socket_id: descriptor.config.socket_id,
            kind: descriptor.config.kind,
        };
    }

    if let Err(diagnostic) = validate_secret_provider_contract(&descriptor.config) {
        return SecretProviderServiceAdaptation::Incompatible { diagnostic };
    }

    let launch = descriptor.secret_provider_launch().cloned();

    let service = match probe(
        &descriptor.config.socket_id,
        launch.as_ref(),
        &BUILT_IN_SECRET_PROVIDER_IDS,
    ) {
        Ok(service) => service,
        Err(error) => {
            return SecretProviderServiceAdaptation::ProbeFailed {
                diagnostic: secret_provider_diagnostic_from_error(
                    &descriptor.config.socket_id,
                    classify_probe_failure_stage(launch.as_ref(), &error),
                    &error,
                ),
            };
        }
    };

    let provider_id = service.provider_id().to_string();
    if provider_exists(&provider_id) {
        return SecretProviderServiceAdaptation::SkippedDuplicate {
            socket_id: descriptor.config.socket_id,
            provider_id,
        };
    }

    SecretProviderServiceAdaptation::Registered {
        provider_id,
        service,
    }
}

fn validate_secret_provider_contract(
    config: &ServiceConfig,
) -> Result<ServiceRpcApiContract, ExternalSecretProviderDiagnostic> {
    let contract = config.resolved_api_contract();

    if contract.family != SECRET_PROVIDER_RPC_FAMILY {
        return Err(ExternalSecretProviderDiagnostic {
            socket_id: config.socket_id.clone(),
            stage: ExternalSecretProviderStage::Compatibility,
            summary: format!(
                "Secret-provider service '{}' declares incompatible RPC family '{}'",
                config.socket_id, contract.family
            ),
            details: None,
        });
    }

    if contract.major != SECRET_PROVIDER_RPC_API_CONTRACT.version.major {
        return Err(ExternalSecretProviderDiagnostic {
            socket_id: config.socket_id.clone(),
            stage: ExternalSecretProviderStage::Compatibility,
            summary: format!(
                "Secret-provider service '{}' declares incompatible RPC major version {}",
                config.socket_id, contract.major
            ),
            details: Some(format!(
                "Expected {}.x for family '{}'",
                SECRET_PROVIDER_RPC_API_CONTRACT.version.major, contract.family
            )),
        });
    }

    Ok(contract)
}

fn classify_probe_failure_stage(
    launch: Option<&IpcServiceLaunchConfig>,
    error: &DbError,
) -> ExternalSecretProviderStage {
    if launch.is_none() {
        return ExternalSecretProviderStage::Probe;
    }

    if matches!(error, DbError::IoError(_)) {
        return ExternalSecretProviderStage::Launch;
    }

    let summary = normalize_error_message(error);
    if summary.contains("exited before socket was ready")
        || summary.contains("did not become ready within")
        || summary.contains("socket is unavailable")
    {
        ExternalSecretProviderStage::Launch
    } else {
        ExternalSecretProviderStage::Probe
    }
}

fn secret_provider_diagnostic_from_error(
    socket_id: &str,
    stage: ExternalSecretProviderStage,
    error: &DbError,
) -> ExternalSecretProviderDiagnostic {
    let message = normalize_error_message(error);
    let (summary, details) = match message.split_once("\n\n") {
        Some((summary, details)) => (
            summary.trim().to_string(),
            Some(details.trim().to_string()).filter(|details| !details.is_empty()),
        ),
        None => (message, None),
    };

    ExternalSecretProviderDiagnostic {
        socket_id: socket_id.to_string(),
        stage,
        summary,
        details,
    }
}
