use dbflux_core::{
    DbDriver, DbError, DbKind, DriverFormDef, DriverMetadata, RpcServiceKind, ServiceConfig,
    ServiceRpcApiContract,
};
use dbflux_driver_ipc::{IpcDriver, driver::IpcDriverLaunchConfig};
use dbflux_ipc::{AUTH_PROVIDER_RPC_API_CONTRACT, IpcServiceLaunchConfig, RpcAuthProvider};
use std::sync::Arc;

use dbflux_core::auth::DynAuthProvider;

pub(crate) type DriverProbe = (DbKind, DriverMetadata, DriverFormDef, Option<DriverFormDef>);

#[derive(Clone, Debug)]
enum RpcServiceLaunch {
    Driver(IpcDriverLaunchConfig),
    AuthProvider(IpcServiceLaunchConfig),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExternalDriverStage {
    Config,
    Launch,
    Probe,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalDriverDiagnostic {
    pub socket_id: String,
    pub stage: ExternalDriverStage,
    pub summary: String,
    pub details: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExternalAuthProviderStage {
    Config,
    Compatibility,
    Launch,
    Probe,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalAuthProviderDiagnostic {
    pub socket_id: String,
    pub stage: ExternalAuthProviderStage,
    pub summary: String,
    pub details: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct RpcServiceDescriptor {
    pub(crate) config: ServiceConfig,
    launch: Option<RpcServiceLaunch>,
}

impl RpcServiceDescriptor {
    fn driver_launch(&self) -> Option<&IpcDriverLaunchConfig> {
        match &self.launch {
            Some(RpcServiceLaunch::Driver(launch)) => Some(launch),
            _ => None,
        }
    }

    fn auth_provider_launch(&self) -> Option<&IpcServiceLaunchConfig> {
        match &self.launch {
            Some(RpcServiceLaunch::AuthProvider(launch)) => Some(launch),
            _ => None,
        }
    }
}

pub(crate) enum RpcServiceDiscovery {
    Descriptor(RpcServiceDescriptor),
    InvalidConfig {
        diagnostic: ExternalDriverDiagnostic,
    },
}

pub(crate) enum DriverServiceAdaptation<T> {
    Registered {
        driver_id: String,
        service: T,
    },
    SkippedDisabled {
        socket_id: String,
    },
    SkippedNonDriver {
        socket_id: String,
        kind: RpcServiceKind,
    },
    SkippedDuplicate {
        socket_id: String,
    },
    ProbeFailed {
        diagnostic: ExternalDriverDiagnostic,
    },
}

pub(crate) enum AuthProviderServiceAdaptation<T> {
    Registered {
        provider_id: String,
        service: T,
    },
    SkippedDisabled {
        socket_id: String,
    },
    SkippedNonAuthProvider {
        socket_id: String,
        kind: RpcServiceKind,
    },
    SkippedDuplicate {
        socket_id: String,
        provider_id: String,
    },
    Incompatible {
        diagnostic: ExternalAuthProviderDiagnostic,
    },
    ProbeFailed {
        diagnostic: ExternalAuthProviderDiagnostic,
    },
}

pub(crate) fn rpc_registry_id(socket_id: &str) -> String {
    format!("rpc:{}", socket_id)
}

pub(crate) fn discover_services(services: Vec<ServiceConfig>) -> Vec<RpcServiceDiscovery> {
    services
        .into_iter()
        .map(|config| match build_service_launch_config(&config) {
            Ok(launch) => RpcServiceDiscovery::Descriptor(RpcServiceDescriptor { config, launch }),
            Err(error) => RpcServiceDiscovery::InvalidConfig {
                diagnostic: diagnostic_from_error(
                    &config.socket_id,
                    ExternalDriverStage::Config,
                    &error,
                ),
            },
        })
        .collect()
}

pub(crate) fn adapt_driver_service(
    descriptor: RpcServiceDescriptor,
    driver_exists: impl FnOnce(&str) -> bool,
) -> DriverServiceAdaptation<Arc<dyn DbDriver>> {
    adapt_driver_service_with(
        descriptor,
        driver_exists,
        |socket_id, launch| IpcDriver::probe_driver(socket_id, launch).map_err(Box::new),
        |_, socket_id, (kind, metadata, form_definition, settings_schema), launch| {
            let driver =
                IpcDriver::new(socket_id, kind, metadata, form_definition, settings_schema);
            let driver = match launch {
                Some(launch) => driver.with_launch_config(launch),
                None => driver,
            };

            Arc::new(driver) as Arc<dyn DbDriver>
        },
    )
}

pub(crate) fn adapt_driver_service_with<T, Probe, Build>(
    descriptor: RpcServiceDescriptor,
    driver_exists: impl FnOnce(&str) -> bool,
    probe: Probe,
    build: Build,
) -> DriverServiceAdaptation<T>
where
    Probe: FnOnce(&str, Option<&IpcDriverLaunchConfig>) -> Result<DriverProbe, Box<DbError>>,
    Build: FnOnce(String, String, DriverProbe, Option<IpcDriverLaunchConfig>) -> T,
{
    if !descriptor.config.enabled {
        return DriverServiceAdaptation::SkippedDisabled {
            socket_id: descriptor.config.socket_id,
        };
    }

    if descriptor.config.kind != RpcServiceKind::Driver {
        return DriverServiceAdaptation::SkippedNonDriver {
            socket_id: descriptor.config.socket_id,
            kind: descriptor.config.kind,
        };
    }

    let driver_id = rpc_registry_id(&descriptor.config.socket_id);
    if driver_exists(&driver_id) {
        return DriverServiceAdaptation::SkippedDuplicate {
            socket_id: descriptor.config.socket_id,
        };
    }

    let driver_launch = descriptor.driver_launch().cloned();

    let probe_result = match probe(&descriptor.config.socket_id, driver_launch.as_ref()) {
        Ok(probe_result) => probe_result,
        Err(error) => {
            return DriverServiceAdaptation::ProbeFailed {
                diagnostic: diagnostic_from_error(
                    &descriptor.config.socket_id,
                    classify_driver_probe_failure_stage(driver_launch.as_ref(), &error),
                    &error,
                ),
            };
        }
    };

    let socket_id = descriptor.config.socket_id;
    let service = build(driver_id.clone(), socket_id, probe_result, driver_launch);

    DriverServiceAdaptation::Registered { driver_id, service }
}

pub(crate) fn adapt_auth_provider_service(
    descriptor: RpcServiceDescriptor,
    provider_exists: impl FnOnce(&str) -> bool,
) -> AuthProviderServiceAdaptation<Arc<dyn DynAuthProvider>> {
    adapt_auth_provider_service_with(descriptor, provider_exists, |socket_id, launch| {
        RpcAuthProvider::probe(socket_id, launch.cloned())
            .map(|provider| Arc::new(provider) as Arc<dyn DynAuthProvider>)
            .map_err(Box::new)
    })
}

pub(crate) fn adapt_auth_provider_service_with<T, Probe>(
    descriptor: RpcServiceDescriptor,
    provider_exists: impl FnOnce(&str) -> bool,
    probe: Probe,
) -> AuthProviderServiceAdaptation<T>
where
    T: DynAuthProvider,
    Probe: FnOnce(&str, Option<&IpcServiceLaunchConfig>) -> Result<T, Box<DbError>>,
{
    if !descriptor.config.enabled {
        return AuthProviderServiceAdaptation::SkippedDisabled {
            socket_id: descriptor.config.socket_id,
        };
    }

    if descriptor.config.kind != RpcServiceKind::AuthProvider {
        return AuthProviderServiceAdaptation::SkippedNonAuthProvider {
            socket_id: descriptor.config.socket_id,
            kind: descriptor.config.kind,
        };
    }

    if let Err(diagnostic) = validate_auth_provider_contract(&descriptor.config) {
        return AuthProviderServiceAdaptation::Incompatible { diagnostic };
    }

    let auth_launch = descriptor.auth_provider_launch().cloned();

    let service = match probe(&descriptor.config.socket_id, auth_launch.as_ref()) {
        Ok(service) => service,
        Err(error) => {
            return AuthProviderServiceAdaptation::ProbeFailed {
                diagnostic: auth_provider_diagnostic_from_error(
                    &descriptor.config.socket_id,
                    classify_auth_provider_probe_failure_stage(auth_launch.as_ref(), &error),
                    &error,
                ),
            };
        }
    };

    let provider_id = service.provider_id().to_string();
    if provider_exists(&provider_id) {
        return AuthProviderServiceAdaptation::SkippedDuplicate {
            socket_id: descriptor.config.socket_id,
            provider_id,
        };
    }

    AuthProviderServiceAdaptation::Registered {
        provider_id,
        service,
    }
}

fn build_service_launch_config(
    config: &ServiceConfig,
) -> Result<Option<RpcServiceLaunch>, Box<DbError>> {
    match config.kind {
        RpcServiceKind::Driver => IpcDriver::build_launch_config(
            &config.socket_id,
            config.command.as_deref(),
            &config.args,
            &config.env,
            config.startup_timeout_ms,
        )
        .map(|launch| launch.map(RpcServiceLaunch::Driver))
        .map_err(Box::new),
        RpcServiceKind::AuthProvider => RpcAuthProvider::build_launch_config(
            &config.socket_id,
            config.command.as_deref(),
            &config.args,
            &config.env,
            config.startup_timeout_ms,
        )
        .map(|launch| launch.map(RpcServiceLaunch::AuthProvider))
        .map_err(Box::new),
    }
}

fn classify_driver_probe_failure_stage(
    launch: Option<&IpcDriverLaunchConfig>,
    error: &DbError,
) -> ExternalDriverStage {
    if launch.is_none() {
        return ExternalDriverStage::Probe;
    }

    let summary = normalize_error_message(error);
    if summary.contains("Failed to start driver host")
        || summary.contains("exited before socket was ready")
        || summary.contains("did not become ready within")
        || summary.contains("socket is unavailable")
    {
        ExternalDriverStage::Launch
    } else {
        ExternalDriverStage::Probe
    }
}

fn classify_auth_provider_probe_failure_stage(
    launch: Option<&IpcServiceLaunchConfig>,
    error: &DbError,
) -> ExternalAuthProviderStage {
    if launch.is_none() {
        return ExternalAuthProviderStage::Probe;
    }

    let summary = normalize_error_message(error);
    if summary.contains("Failed to start driver host")
        || summary.contains("exited before socket was ready")
        || summary.contains("did not become ready within")
        || summary.contains("socket is unavailable")
    {
        ExternalAuthProviderStage::Launch
    } else {
        ExternalAuthProviderStage::Probe
    }
}

fn diagnostic_from_error(
    socket_id: &str,
    stage: ExternalDriverStage,
    error: &DbError,
) -> ExternalDriverDiagnostic {
    let message = normalize_error_message(error);
    let (summary, details) = match message.split_once("\n\n") {
        Some((summary, details)) => (
            summary.trim().to_string(),
            Some(details.trim().to_string()).filter(|details| !details.is_empty()),
        ),
        None => (message, None),
    };

    ExternalDriverDiagnostic {
        socket_id: socket_id.to_string(),
        stage,
        summary,
        details,
    }
}

fn normalize_error_message(error: &DbError) -> String {
    let rendered = error.to_string();

    rendered
        .strip_prefix("Connection failed: ")
        .unwrap_or(&rendered)
        .to_string()
}

fn validate_auth_provider_contract(
    config: &ServiceConfig,
) -> Result<ServiceRpcApiContract, ExternalAuthProviderDiagnostic> {
    let contract = config.resolved_api_contract();

    if contract.family != "auth_provider_rpc" {
        return Err(ExternalAuthProviderDiagnostic {
            socket_id: config.socket_id.clone(),
            stage: ExternalAuthProviderStage::Compatibility,
            summary: format!(
                "Auth-provider service '{}' declares incompatible RPC family '{}'",
                config.socket_id, contract.family
            ),
            details: None,
        });
    }

    if contract.major != AUTH_PROVIDER_RPC_API_CONTRACT.version.major {
        return Err(ExternalAuthProviderDiagnostic {
            socket_id: config.socket_id.clone(),
            stage: ExternalAuthProviderStage::Compatibility,
            summary: format!(
                "Auth-provider service '{}' declares incompatible RPC major version {}",
                config.socket_id, contract.major
            ),
            details: Some(format!(
                "Expected {}.x for family '{}'",
                AUTH_PROVIDER_RPC_API_CONTRACT.version.major, contract.family
            )),
        });
    }

    Ok(contract)
}

fn auth_provider_diagnostic_from_error(
    socket_id: &str,
    stage: ExternalAuthProviderStage,
    error: &DbError,
) -> ExternalAuthProviderDiagnostic {
    let message = normalize_error_message(error);
    let (summary, details) = match message.split_once("\n\n") {
        Some((summary, details)) => (
            summary.trim().to_string(),
            Some(details.trim().to_string()).filter(|details| !details.is_empty()),
        ),
        None => (message, None),
    };

    ExternalAuthProviderDiagnostic {
        socket_id: socket_id.to_string(),
        stage,
        summary,
        details,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use dbflux_core::auth::{
        AuthFormDef, AuthProfile, AuthSession, AuthSessionState, ResolvedCredentials, UrlCallback,
    };
    use dbflux_core::{DatabaseCategory, DriverMetadataBuilder, QueryLanguage};

    fn fake_probe() -> DriverProbe {
        let metadata = DriverMetadataBuilder::new(
            "sqlite",
            "SQLite",
            DatabaseCategory::Relational,
            QueryLanguage::Sql,
        )
        .build();

        (
            DbKind::SQLite,
            metadata,
            DriverFormDef { tabs: vec![] },
            None,
        )
    }

    fn test_service(kind: RpcServiceKind, enabled: bool) -> ServiceConfig {
        ServiceConfig {
            socket_id: "svc-socket".to_string(),
            enabled,
            command: Some("dbflux-driver-host".to_string()),
            args: vec!["--stdio".to_string()],
            env: std::collections::HashMap::from([("RUST_LOG".to_string(), "info".to_string())]),
            startup_timeout_ms: Some(7_500),
            kind,
            api_contract: None,
        }
    }

    fn manual_service() -> ServiceConfig {
        ServiceConfig {
            socket_id: "manual-socket".to_string(),
            enabled: true,
            command: None,
            args: Vec::new(),
            env: HashMap::new(),
            startup_timeout_ms: None,
            kind: RpcServiceKind::Driver,
            api_contract: None,
        }
    }

    struct FakeAuthProvider {
        provider_id: String,
    }

    #[async_trait::async_trait]
    impl DynAuthProvider for FakeAuthProvider {
        fn provider_id(&self) -> &str {
            &self.provider_id
        }

        fn display_name(&self) -> &str {
            "Fake Auth Provider"
        }

        fn form_def(&self) -> &AuthFormDef {
            static FORM: std::sync::OnceLock<AuthFormDef> = std::sync::OnceLock::new();
            FORM.get_or_init(|| AuthFormDef { tabs: vec![] })
        }

        async fn validate_session(
            &self,
            _profile: &AuthProfile,
        ) -> Result<AuthSessionState, DbError> {
            Ok(AuthSessionState::LoginRequired)
        }

        async fn login(
            &self,
            profile: &AuthProfile,
            url_callback: UrlCallback,
        ) -> Result<AuthSession, DbError> {
            url_callback(None);
            Ok(AuthSession {
                provider_id: self.provider_id.clone(),
                profile_id: profile.id,
                expires_at: None,
                data: None,
            })
        }

        async fn resolve_credentials(
            &self,
            _profile: &AuthProfile,
        ) -> Result<ResolvedCredentials, DbError> {
            Ok(ResolvedCredentials::default())
        }
    }

    #[test]
    fn discover_and_adapt_manual_driver_service_keeps_manual_launch_and_rpc_registry_id() {
        let descriptor = discover_services(vec![manual_service()])
            .into_iter()
            .next()
            .expect("descriptor");

        let RpcServiceDiscovery::Descriptor(descriptor) = descriptor else {
            panic!("expected valid descriptor");
        };

        assert!(descriptor.launch.is_none());

        let adaptation = adapt_driver_service_with(
            descriptor,
            |_| false,
            |socket_id, launch| {
                assert_eq!(socket_id, "manual-socket");
                assert!(launch.is_none());
                Ok(fake_probe())
            },
            |driver_id, socket_id, _, launch| (driver_id, socket_id, launch),
        );

        match adaptation {
            DriverServiceAdaptation::Registered { driver_id, service } => {
                assert_eq!(driver_id, "rpc:manual-socket");
                assert_eq!(service.0, "rpc:manual-socket");
                assert_eq!(service.1, "manual-socket");
                assert!(service.2.is_none());
            }
            _ => panic!("expected manual driver registration"),
        }
    }

    #[test]
    fn discover_services_returns_config_diagnostic_for_missing_default_host_flags() {
        let invalid_service = ServiceConfig {
            socket_id: "svc-socket".to_string(),
            enabled: true,
            command: None,
            args: vec!["--stdio".to_string()],
            env: HashMap::new(),
            startup_timeout_ms: Some(1_000),
            kind: RpcServiceKind::Driver,
            api_contract: None,
        };

        let discovery = discover_services(vec![invalid_service])
            .into_iter()
            .next()
            .expect("discovery");

        match discovery {
            RpcServiceDiscovery::InvalidConfig { diagnostic } => {
                assert_eq!(diagnostic.socket_id, "svc-socket");
                assert_eq!(diagnostic.stage, ExternalDriverStage::Config);
                assert!(diagnostic.summary.contains("--driver"));
            }
            _ => panic!("expected invalid config diagnostic"),
        }
    }

    #[test]
    fn discover_services_returns_config_diagnostic_for_socket_mismatch() {
        let invalid_service = ServiceConfig {
            socket_id: "svc-socket".to_string(),
            enabled: true,
            command: None,
            args: vec![
                "--driver".to_string(),
                "example".to_string(),
                "--socket".to_string(),
                "other.sock".to_string(),
            ],
            env: HashMap::new(),
            startup_timeout_ms: Some(1_000),
            kind: RpcServiceKind::Driver,
            api_contract: None,
        };

        let discovery = discover_services(vec![invalid_service])
            .into_iter()
            .next()
            .expect("discovery");

        match discovery {
            RpcServiceDiscovery::InvalidConfig { diagnostic } => {
                assert_eq!(diagnostic.socket_id, "svc-socket");
                assert_eq!(diagnostic.stage, ExternalDriverStage::Config);
                assert!(diagnostic.summary.contains("socket mismatch"));
            }
            _ => panic!("expected invalid config diagnostic"),
        }
    }

    #[test]
    fn build_service_launch_config_preserves_manual_services_without_boxing_workarounds() {
        let launch: Result<Option<RpcServiceLaunch>, Box<DbError>> =
            build_service_launch_config(&manual_service());

        assert!(launch.expect("manual service should stay valid").is_none());
    }

    #[test]
    fn build_service_launch_config_boxes_invalid_default_host_errors() {
        let invalid_service = ServiceConfig {
            socket_id: "svc-socket".to_string(),
            enabled: true,
            command: None,
            args: vec!["--stdio".to_string()],
            env: HashMap::new(),
            startup_timeout_ms: Some(1_000),
            kind: RpcServiceKind::Driver,
            api_contract: None,
        };

        let launch: Result<Option<RpcServiceLaunch>, Box<DbError>> =
            build_service_launch_config(&invalid_service);
        let error = launch.expect_err("invalid default host config should fail");

        assert!(error.to_string().contains("--driver"));
    }

    #[test]
    fn discover_and_adapt_driver_service_preserves_rpc_registry_id() {
        let descriptor = discover_services(vec![test_service(RpcServiceKind::Driver, true)])
            .into_iter()
            .next()
            .expect("descriptor");

        let RpcServiceDiscovery::Descriptor(descriptor) = descriptor else {
            panic!("expected valid descriptor");
        };

        let adaptation = adapt_driver_service_with(
            descriptor,
            |_| false,
            |socket_id, launch| {
                let launch = launch.expect("managed service should have launch config");
                assert_eq!(socket_id, "svc-socket");
                assert_eq!(launch.program, "dbflux-driver-host");
                assert_eq!(launch.args, vec!["--stdio".to_string()]);
                assert_eq!(launch.startup_timeout.as_millis(), 7_500);
                Ok(fake_probe())
            },
            |driver_id, socket_id, _, launch| {
                let launch = launch.expect("managed service should keep launch config");
                (driver_id, socket_id, launch.program)
            },
        );

        match adaptation {
            DriverServiceAdaptation::Registered { driver_id, service } => {
                assert_eq!(driver_id, "rpc:svc-socket");
                assert_eq!(service.0, "rpc:svc-socket");
                assert_eq!(service.1, "svc-socket");
                assert_eq!(service.2, "dbflux-driver-host");
            }
            _ => panic!("expected driver registration"),
        }
    }

    #[test]
    fn adapt_driver_service_skips_non_driver_descriptors() {
        let descriptor = discover_services(vec![test_service(RpcServiceKind::AuthProvider, true)])
            .into_iter()
            .next()
            .expect("descriptor");

        let RpcServiceDiscovery::Descriptor(descriptor) = descriptor else {
            panic!("expected valid descriptor");
        };

        let adaptation = adapt_driver_service_with(
            descriptor,
            |_| false,
            |_, _| Ok(fake_probe()),
            |driver_id, _, _, _| driver_id,
        );

        match adaptation {
            DriverServiceAdaptation::SkippedNonDriver { socket_id, kind } => {
                assert_eq!(socket_id, "svc-socket");
                assert_eq!(kind, RpcServiceKind::AuthProvider);
            }
            _ => panic!("expected non-driver service to stay inert"),
        }
    }

    #[test]
    fn adapt_driver_service_skips_disabled_descriptors_before_probe() {
        let descriptor = discover_services(vec![test_service(RpcServiceKind::Driver, false)])
            .into_iter()
            .next()
            .expect("descriptor");

        let RpcServiceDiscovery::Descriptor(descriptor) = descriptor else {
            panic!("expected valid descriptor");
        };

        let adaptation = adapt_driver_service_with(
            descriptor,
            |_| false,
            |_, _| panic!("disabled services must not be probed"),
            |driver_id, _, _, _| driver_id,
        );

        match adaptation {
            DriverServiceAdaptation::SkippedDisabled { socket_id } => {
                assert_eq!(socket_id, "svc-socket");
            }
            _ => panic!("expected disabled service to be skipped"),
        }
    }

    #[test]
    fn adapt_driver_service_returns_probe_failure_without_registration() {
        let descriptor = discover_services(vec![test_service(RpcServiceKind::Driver, true)])
            .into_iter()
            .next()
            .expect("descriptor");

        let RpcServiceDiscovery::Descriptor(descriptor) = descriptor else {
            panic!("expected valid descriptor");
        };

        let adaptation = adapt_driver_service_with(
            descriptor,
            |_| false,
            |_, _| Err(Box::new(DbError::connection_failed("probe failed"))),
            |driver_id, _, _, _| driver_id,
        );

        match adaptation {
            DriverServiceAdaptation::ProbeFailed { diagnostic } => {
                assert_eq!(diagnostic.socket_id, "svc-socket");
                assert_eq!(diagnostic.stage, ExternalDriverStage::Probe);
                assert_eq!(diagnostic.summary, "probe failed");
            }
            _ => panic!("expected probe failure"),
        }
    }

    #[test]
    fn discover_services_returns_config_diagnostic_for_auth_provider_without_explicit_command() {
        let invalid_service = ServiceConfig {
            socket_id: "svc-socket".to_string(),
            enabled: true,
            command: None,
            args: vec!["--stdio".to_string()],
            env: HashMap::new(),
            startup_timeout_ms: Some(1_000),
            kind: RpcServiceKind::AuthProvider,
            api_contract: None,
        };

        let discovery = discover_services(vec![invalid_service])
            .into_iter()
            .next()
            .expect("discovery");

        match discovery {
            RpcServiceDiscovery::InvalidConfig { diagnostic } => {
                assert_eq!(diagnostic.stage, ExternalDriverStage::Config);
                assert!(diagnostic.summary.contains("explicit command"));
            }
            _ => panic!("expected invalid auth-provider config"),
        }
    }

    #[test]
    fn adapt_auth_provider_service_registers_compatible_provider() {
        let descriptor = discover_services(vec![test_service(RpcServiceKind::AuthProvider, true)])
            .into_iter()
            .next()
            .expect("descriptor");

        let RpcServiceDiscovery::Descriptor(descriptor) = descriptor else {
            panic!("expected valid descriptor");
        };

        let adaptation = adapt_auth_provider_service_with(
            descriptor,
            |_| false,
            |socket_id, launch| {
                let launch = launch.expect("managed auth provider should keep launch config");
                assert_eq!(socket_id, "svc-socket");
                assert_eq!(launch.program, "dbflux-driver-host");

                Ok(FakeAuthProvider {
                    provider_id: "rpc-auth".to_string(),
                })
            },
        );

        match adaptation {
            AuthProviderServiceAdaptation::Registered {
                provider_id,
                service,
            } => {
                assert_eq!(provider_id, "rpc-auth");
                assert_eq!(service.provider_id(), "rpc-auth");
            }
            _ => panic!("expected auth-provider registration"),
        }
    }

    #[test]
    fn adapt_auth_provider_service_preserves_declared_provider_id_on_registration() {
        let descriptor = discover_services(vec![test_service(RpcServiceKind::AuthProvider, true)])
            .into_iter()
            .next()
            .expect("descriptor");

        let RpcServiceDiscovery::Descriptor(descriptor) = descriptor else {
            panic!("expected valid descriptor");
        };

        let adaptation = adapt_auth_provider_service_with(
            descriptor,
            |_| false,
            |_, _| {
                Ok(FakeAuthProvider {
                    provider_id: "custom-oidc".to_string(),
                })
            },
        );

        match adaptation {
            AuthProviderServiceAdaptation::Registered { provider_id, .. } => {
                assert_eq!(provider_id, "custom-oidc");
            }
            _ => panic!("expected auth-provider registration"),
        }
    }

    #[test]
    fn adapt_auth_provider_service_rejects_incompatible_contract_before_probe() {
        let mut service = test_service(RpcServiceKind::AuthProvider, true);
        service.api_contract = Some(ServiceRpcApiContract::new("driver_rpc", 1, 1));

        let descriptor = discover_services(vec![service])
            .into_iter()
            .next()
            .expect("descriptor");

        let RpcServiceDiscovery::Descriptor(descriptor) = descriptor else {
            panic!("expected valid descriptor");
        };

        let adaptation: AuthProviderServiceAdaptation<FakeAuthProvider> =
            adapt_auth_provider_service_with(
                descriptor,
                |_| false,
                |_, _| panic!("incompatible auth-provider descriptors must not be probed"),
            );

        match adaptation {
            AuthProviderServiceAdaptation::Incompatible { diagnostic } => {
                assert_eq!(diagnostic.stage, ExternalAuthProviderStage::Compatibility);
                assert!(diagnostic.summary.contains("incompatible RPC family"));
            }
            _ => panic!("expected compatibility rejection"),
        }
    }

    #[test]
    fn adapt_auth_provider_service_skips_duplicate_provider_ids() {
        let descriptor = discover_services(vec![test_service(RpcServiceKind::AuthProvider, true)])
            .into_iter()
            .next()
            .expect("descriptor");

        let RpcServiceDiscovery::Descriptor(descriptor) = descriptor else {
            panic!("expected valid descriptor");
        };

        let adaptation = adapt_auth_provider_service_with(
            descriptor,
            |provider_id| provider_id == "aws-sso",
            |_, _| {
                Ok(FakeAuthProvider {
                    provider_id: "aws-sso".to_string(),
                })
            },
        );

        match adaptation {
            AuthProviderServiceAdaptation::SkippedDuplicate {
                socket_id,
                provider_id,
            } => {
                assert_eq!(socket_id, "svc-socket");
                assert_eq!(provider_id, "aws-sso");
            }
            _ => panic!("expected duplicate provider rejection"),
        }
    }

    #[test]
    fn adapt_auth_provider_service_rejects_duplicate_non_aws_provider_ids() {
        let descriptor = discover_services(vec![test_service(RpcServiceKind::AuthProvider, true)])
            .into_iter()
            .next()
            .expect("descriptor");

        let RpcServiceDiscovery::Descriptor(descriptor) = descriptor else {
            panic!("expected valid descriptor");
        };

        let adaptation = adapt_auth_provider_service_with(
            descriptor,
            |provider_id| provider_id == "custom-oidc",
            |_, _| {
                Ok(FakeAuthProvider {
                    provider_id: "custom-oidc".to_string(),
                })
            },
        );

        match adaptation {
            AuthProviderServiceAdaptation::SkippedDuplicate {
                socket_id,
                provider_id,
            } => {
                assert_eq!(socket_id, "svc-socket");
                assert_eq!(provider_id, "custom-oidc");
            }
            _ => panic!("expected duplicate provider rejection"),
        }
    }
}
