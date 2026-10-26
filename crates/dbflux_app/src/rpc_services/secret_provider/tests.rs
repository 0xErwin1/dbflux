use std::collections::HashMap;
use std::sync::Arc;

use dbflux_core::auth::DynAuthProvider;
use dbflux_core::secrecy::SecretString;
use dbflux_core::values::DynSecretProvider;
use dbflux_core::{DbError, RpcServiceKind, ServiceConfig, ServiceRpcApiContract};

use super::*;
use crate::rpc_services::{
    AuthProviderServiceAdaptation, DriverServiceAdaptation, RpcServiceDiscovery,
    adapt_auth_provider_service_with, adapt_driver_service_with, discover_services,
};

struct FakeSecretProvider {
    provider_id: String,
}

#[async_trait::async_trait]
impl DynSecretProvider for FakeSecretProvider {
    fn provider_id(&self) -> &str {
        &self.provider_id
    }

    fn display_name(&self) -> &str {
        "Fake Secret Provider"
    }

    async fn get_secret(
        &self,
        _locator: &str,
        _json_key: Option<&str>,
    ) -> Result<SecretString, DbError> {
        Ok(SecretString::from("unused".to_string()))
    }
}

fn fake_provider(provider_id: &str) -> Arc<dyn DynSecretProvider> {
    Arc::new(FakeSecretProvider {
        provider_id: provider_id.to_string(),
    })
}

fn service(kind: RpcServiceKind, enabled: bool) -> ServiceConfig {
    ServiceConfig {
        socket_id: "secret-socket".to_string(),
        enabled,
        command: Some("dbflux-secret-host".to_string()),
        args: vec!["--stdio".to_string()],
        env: HashMap::new(),
        startup_timeout_ms: Some(2_500),
        kind,
        api_contract: None,
    }
}

fn descriptor(config: ServiceConfig) -> RpcServiceDescriptor {
    match discover_services(vec![config]).into_iter().next() {
        Some(RpcServiceDiscovery::Descriptor(descriptor)) => descriptor,
        _ => panic!("expected a valid descriptor"),
    }
}

#[test]
fn secret_provider_service_gets_a_dedicated_launch_config() {
    let descriptor = descriptor(service(RpcServiceKind::SecretProvider, true));

    let launch = descriptor
        .secret_provider_launch()
        .expect("managed secret provider keeps its launch config");

    assert_eq!(launch.program, "dbflux-secret-host");
    assert_eq!(launch.startup_timeout.as_millis(), 2_500);
    assert!(descriptor.auth_provider_launch().is_none());
}

#[test]
fn driver_adapter_skips_secret_provider_services() {
    let adaptation = adapt_driver_service_with(
        descriptor(service(RpcServiceKind::SecretProvider, true)),
        |_| false,
        |_, _| panic!("secret-provider services must not be probed as drivers"),
        |driver_id, _, _, _| driver_id,
    );

    match adaptation {
        DriverServiceAdaptation::SkippedNonDriver { socket_id, kind } => {
            assert_eq!(socket_id, "secret-socket");
            assert_eq!(kind, RpcServiceKind::SecretProvider);
        }
        _ => panic!("expected the driver adapter to skip the secret provider"),
    }
}

#[test]
fn auth_adapter_skips_secret_provider_services() {
    let adaptation: AuthProviderServiceAdaptation<Arc<dyn DynAuthProvider>> =
        adapt_auth_provider_service_with(
            descriptor(service(RpcServiceKind::SecretProvider, true)),
            |_| false,
            |_, _| panic!("secret-provider services must not be probed as auth providers"),
        );

    match adaptation {
        AuthProviderServiceAdaptation::SkippedNonAuthProvider { socket_id, kind } => {
            assert_eq!(socket_id, "secret-socket");
            assert_eq!(kind, RpcServiceKind::SecretProvider);
        }
        _ => panic!("expected the auth adapter to skip the secret provider"),
    }
}

#[test]
fn secret_adapter_skips_driver_and_auth_provider_services() {
    for kind in [RpcServiceKind::Driver, RpcServiceKind::AuthProvider] {
        let mut config = service(kind, true);
        config.args = Vec::new();

        let adaptation = adapt_secret_provider_service_with(
            descriptor(config),
            |_| false,
            |_, _, _| panic!("other service kinds must not be probed as secret providers"),
        );

        match adaptation {
            SecretProviderServiceAdaptation::SkippedNonSecretProvider {
                socket_id,
                kind: skipped_kind,
            } => {
                assert_eq!(socket_id, "secret-socket");
                assert_eq!(skipped_kind, kind);
            }
            _ => panic!("expected the secret adapter to skip {kind:?}"),
        }
    }
}

#[test]
fn secret_adapter_skips_disabled_services_before_probe() {
    let adaptation = adapt_secret_provider_service_with(
        descriptor(service(RpcServiceKind::SecretProvider, false)),
        |_| false,
        |_, _, _| panic!("disabled services must not be probed"),
    );

    match adaptation {
        SecretProviderServiceAdaptation::SkippedDisabled { socket_id } => {
            assert_eq!(socket_id, "secret-socket");
        }
        _ => panic!("expected the disabled service to be skipped"),
    }
}

#[test]
fn secret_adapter_rejects_incompatible_family_before_probe() {
    let mut config = service(RpcServiceKind::SecretProvider, true);
    config.api_contract = Some(ServiceRpcApiContract::new("auth_provider_rpc", 1, 0));

    let adaptation = adapt_secret_provider_service_with(
        descriptor(config),
        |_| false,
        |_, _, _| panic!("incompatible services must not be probed"),
    );

    match adaptation {
        SecretProviderServiceAdaptation::Incompatible { diagnostic } => {
            assert_eq!(diagnostic.stage, ExternalSecretProviderStage::Compatibility);
            assert_eq!(
                diagnostic.summary,
                "Secret-provider service 'secret-socket' declares incompatible RPC family 'auth_provider_rpc'"
            );
        }
        _ => panic!("expected a compatibility rejection"),
    }
}

#[test]
fn secret_adapter_rejects_incompatible_major_before_probe() {
    let mut config = service(RpcServiceKind::SecretProvider, true);
    config.api_contract = Some(ServiceRpcApiContract::new(
        "secret_provider_rpc",
        SECRET_PROVIDER_RPC_API_CONTRACT.version.major + 1,
        0,
    ));

    let adaptation = adapt_secret_provider_service_with(
        descriptor(config),
        |_| false,
        |_, _, _| panic!("incompatible services must not be probed"),
    );

    match adaptation {
        SecretProviderServiceAdaptation::Incompatible { diagnostic } => {
            assert_eq!(diagnostic.stage, ExternalSecretProviderStage::Compatibility);
            assert!(
                diagnostic
                    .summary
                    .contains("declares incompatible RPC major version")
            );
            assert_eq!(
                diagnostic.details.as_deref(),
                Some("Expected 1.x for family 'secret_provider_rpc'")
            );
        }
        _ => panic!("expected a compatibility rejection"),
    }
}

#[test]
fn secret_adapter_reports_probe_failure_as_diagnostic() {
    let adaptation = adapt_secret_provider_service_with(
        descriptor(service(RpcServiceKind::SecretProvider, true)),
        |_| false,
        |_, _, _| Err(Box::new(DbError::connection_failed("handshake refused"))),
    );

    match adaptation {
        SecretProviderServiceAdaptation::ProbeFailed { diagnostic } => {
            assert_eq!(diagnostic.socket_id, "secret-socket");
            assert_eq!(diagnostic.stage, ExternalSecretProviderStage::Probe);
            assert_eq!(diagnostic.summary, "handshake refused");
        }
        _ => panic!("expected a probe failure"),
    }
}

#[test]
fn secret_adapter_passes_built_in_ids_as_reserved() {
    let adaptation = adapt_secret_provider_service_with(
        descriptor(service(RpcServiceKind::SecretProvider, true)),
        |_| false,
        |socket_id, launch, reserved_ids| {
            assert_eq!(socket_id, "secret-socket");
            assert!(launch.is_some());
            assert_eq!(reserved_ids, ["aws-secrets-manager", "aws-ssm"]);
            Ok(fake_provider("vault"))
        },
    );

    assert!(matches!(
        adaptation,
        SecretProviderServiceAdaptation::Registered { .. }
    ));
}

#[test]
fn secret_adapter_skips_duplicate_provider_ids() {
    let adaptation = adapt_secret_provider_service_with(
        descriptor(service(RpcServiceKind::SecretProvider, true)),
        |provider_id| provider_id == "vault",
        |_, _, _| Ok(fake_provider("vault")),
    );

    match adaptation {
        SecretProviderServiceAdaptation::SkippedDuplicate {
            socket_id,
            provider_id,
        } => {
            assert_eq!(socket_id, "secret-socket");
            assert_eq!(provider_id, "vault");
        }
        _ => panic!("expected the duplicate provider to be skipped"),
    }
}

#[test]
fn secret_adapter_registers_with_declared_provider_id() {
    let adaptation = adapt_secret_provider_service_with(
        descriptor(service(RpcServiceKind::SecretProvider, true)),
        |_| false,
        |_, _, _| Ok(fake_provider("team-vault")),
    );

    match adaptation {
        SecretProviderServiceAdaptation::Registered {
            provider_id,
            service,
        } => {
            assert_eq!(provider_id, "team-vault");
            assert_eq!(service.provider_id(), "team-vault");
        }
        _ => panic!("expected the provider to register"),
    }
}
