use std::sync::Arc;
use std::time::Duration;

use dbflux_core::{
    DbDriver, DbError, DbKind, DriverFormDef, DriverMetadata, RpcServiceKind, ServiceConfig,
};
use dbflux_driver_ipc::{IpcDriver, driver::IpcDriverLaunchConfig};

pub(crate) type DriverProbe = (DbKind, DriverMetadata, DriverFormDef, Option<DriverFormDef>);

#[derive(Clone, Debug)]
pub(crate) struct RpcServiceDescriptor {
    pub(crate) config: ServiceConfig,
    pub(crate) launch: IpcDriverLaunchConfig,
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
        socket_id: String,
        error: Box<DbError>,
    },
}

pub(crate) fn rpc_registry_id(socket_id: &str) -> String {
    format!("rpc:{}", socket_id)
}

pub(crate) fn discover_services(services: Vec<ServiceConfig>) -> Vec<RpcServiceDescriptor> {
    services
        .into_iter()
        .map(|config| RpcServiceDescriptor {
            launch: IpcDriverLaunchConfig {
                program: config
                    .command
                    .clone()
                    .unwrap_or_else(|| "dbflux-driver-host".to_string()),
                args: config.args.clone(),
                env: config
                    .env
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
                startup_timeout: Duration::from_millis(config.startup_timeout_ms.unwrap_or(5_000)),
            },
            config,
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
        |socket_id, launch| IpcDriver::probe_driver(socket_id, Some(launch)).map_err(Box::new),
        |_, socket_id, (kind, metadata, form_definition, settings_schema), launch| {
            Arc::new(
                IpcDriver::new(socket_id, kind, metadata, form_definition, settings_schema)
                    .with_launch_config(launch),
            ) as Arc<dyn DbDriver>
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
    Probe: FnOnce(&str, &IpcDriverLaunchConfig) -> Result<DriverProbe, Box<DbError>>,
    Build: FnOnce(String, String, DriverProbe, IpcDriverLaunchConfig) -> T,
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

    let probe_result = match probe(&descriptor.config.socket_id, &descriptor.launch) {
        Ok(probe_result) => probe_result,
        Err(error) => {
            return DriverServiceAdaptation::ProbeFailed {
                socket_id: descriptor.config.socket_id,
                error,
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

#[cfg(test)]
mod tests {
    use super::*;
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
            command: None,
            args: vec!["--stdio".to_string()],
            env: std::collections::HashMap::from([("RUST_LOG".to_string(), "info".to_string())]),
            startup_timeout_ms: Some(7_500),
            kind,
        }
    }

    #[test]
    fn discover_and_adapt_driver_service_preserves_rpc_registry_id() {
        let descriptor = discover_services(vec![test_service(RpcServiceKind::Driver, true)])
            .into_iter()
            .next()
            .expect("descriptor");

        let adaptation = adapt_driver_service_with(
            descriptor,
            |_| false,
            |socket_id, launch| {
                assert_eq!(socket_id, "svc-socket");
                assert_eq!(launch.program, "dbflux-driver-host");
                assert_eq!(launch.args, vec!["--stdio".to_string()]);
                assert_eq!(launch.startup_timeout, Duration::from_millis(7_500));
                Ok(fake_probe())
            },
            |driver_id, socket_id, _, launch| (driver_id, socket_id, launch.program),
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

        let adaptation = adapt_driver_service_with(
            descriptor,
            |_| false,
            |_, _| Err(Box::new(DbError::connection_failed("probe failed"))),
            |driver_id, _, _, _| driver_id,
        );

        match adaptation {
            DriverServiceAdaptation::ProbeFailed { socket_id, error } => {
                assert_eq!(socket_id, "svc-socket");
                assert_eq!(error.to_string(), "Connection failed: probe failed");
            }
            _ => panic!("expected probe failure"),
        }
    }
}
