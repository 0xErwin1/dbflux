use dbflux_core::{
    DbDriver, DbError, DbKind, DriverFormDef, DriverMetadata, RpcServiceKind, ServiceConfig,
};
use dbflux_driver_ipc::{IpcDriver, driver::IpcDriverLaunchConfig};
use std::sync::Arc;

pub(crate) type DriverProbe = (DbKind, DriverMetadata, DriverFormDef, Option<DriverFormDef>);

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

#[derive(Clone, Debug)]
pub(crate) struct RpcServiceDescriptor {
    pub(crate) config: ServiceConfig,
    pub(crate) launch: Option<IpcDriverLaunchConfig>,
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

    let probe_result = match probe(&descriptor.config.socket_id, descriptor.launch.as_ref()) {
        Ok(probe_result) => probe_result,
        Err(error) => {
            return DriverServiceAdaptation::ProbeFailed {
                diagnostic: diagnostic_from_error(
                    &descriptor.config.socket_id,
                    classify_probe_failure_stage(descriptor.launch.as_ref(), &error),
                    &error,
                ),
            };
        }
    };

    let socket_id = descriptor.config.socket_id;
    let service = build(
        driver_id.clone(),
        socket_id,
        probe_result,
        descriptor.launch,
    );

    DriverServiceAdaptation::Registered { driver_id, service }
}

fn build_service_launch_config(
    config: &ServiceConfig,
) -> Result<Option<IpcDriverLaunchConfig>, Box<DbError>> {
    IpcDriver::build_launch_config(
        &config.socket_id,
        config.command.as_deref(),
        &config.args,
        &config.env,
        config.startup_timeout_ms,
    )
    .map_err(Box::new)
}

fn classify_probe_failure_stage(
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

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
        let launch: Result<Option<IpcDriverLaunchConfig>, Box<DbError>> =
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

        let launch: Result<Option<IpcDriverLaunchConfig>, Box<DbError>> =
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
}
