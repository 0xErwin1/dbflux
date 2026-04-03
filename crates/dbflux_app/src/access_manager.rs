use std::collections::HashMap;
#[cfg(feature = "aws")]
use std::sync::Arc;

use dbflux_core::DbError;
use dbflux_core::SshTunnelConfig;
use dbflux_core::access::{AccessHandle, AccessKind, AccessManager};
use dbflux_core::secrecy::{ExposeSecret, SecretString};
use uuid::Uuid;

#[derive(Clone)]
pub struct ResolvedSshTunnel {
    pub config: SshTunnelConfig,
    pub secret: Option<SecretString>,
}

/// Concrete access manager for the app crate.
///
/// Dispatches to the right tunnel infrastructure based on the `AccessKind`
/// variant. Direct, SSH, and managed access are handled here. Proxy tunnels
/// still use the app connection flow and are not yet supported by this manager.
pub struct AppAccessManager {
    ssh_tunnels: HashMap<Uuid, ResolvedSshTunnel>,
    #[cfg(feature = "aws")]
    ssm_factory: Option<Arc<dbflux_ssm::SsmTunnelFactory>>,
}

impl AppAccessManager {
    #[cfg(feature = "aws")]
    pub fn new(
        ssh_tunnels: HashMap<Uuid, ResolvedSshTunnel>,
        ssm_factory: Option<Arc<dbflux_ssm::SsmTunnelFactory>>,
    ) -> Self {
        Self {
            ssh_tunnels,
            ssm_factory,
        }
    }

    #[cfg(not(feature = "aws"))]
    pub fn new(ssh_tunnels: HashMap<Uuid, ResolvedSshTunnel>) -> Self {
        Self { ssh_tunnels }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    use dbflux_core::DbError;
    use dbflux_core::access::{AccessKind, AccessManager};
    use uuid::Uuid;

    use super::AppAccessManager;

    #[cfg(feature = "aws")]
    fn test_manager() -> AppAccessManager {
        AppAccessManager::new(HashMap::new(), None)
    }

    #[cfg(not(feature = "aws"))]
    fn test_manager() -> AppAccessManager {
        AppAccessManager::new(HashMap::new())
    }

    fn run_ready_future<F>(future: F) -> F::Output
    where
        F: Future,
    {
        fn raw_waker() -> RawWaker {
            fn clone(_: *const ()) -> RawWaker {
                raw_waker()
            }
            fn wake(_: *const ()) {}
            fn wake_by_ref(_: *const ()) {}
            fn drop(_: *const ()) {}

            RawWaker::new(
                std::ptr::null(),
                &RawWakerVTable::new(clone, wake, wake_by_ref, drop),
            )
        }

        // SAFETY: the vtable functions are no-ops and never dereference the data pointer.
        let waker = unsafe { Waker::from_raw(raw_waker()) };
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(future);

        loop {
            match Pin::as_mut(&mut future).poll(&mut context) {
                Poll::Ready(value) => return value,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    #[test]
    fn direct_mode_opens_without_tunnel_handle() {
        let manager = test_manager();
        let handle = run_ready_future(manager.open(&AccessKind::Direct, "localhost", 5432))
            .expect("direct access should open");

        assert_eq!(handle.local_port(), 0);
        assert!(!handle.is_tunneled());
    }

    #[test]
    fn ssh_mode_reports_missing_profile_without_legacy_wording() {
        let manager = test_manager();
        let missing_profile_id = Uuid::new_v4();

        let result = run_ready_future(manager.open(
            &AccessKind::Ssh {
                ssh_tunnel_profile_id: missing_profile_id,
            },
            "localhost",
            5432,
        ));

        let error = match result {
            Ok(_) => panic!("missing ssh tunnel profile should fail explicitly"),
            Err(error) => error,
        };

        let DbError::ConnectionFailed(error) = error else {
            panic!("ssh mode should return a connection error");
        };

        assert_eq!(
            error.message,
            format!("SSH tunnel profile '{}' was not found", missing_profile_id)
        );
    }

    #[test]
    fn proxy_mode_returns_structured_pipeline_error() {
        let manager = test_manager();

        let proxy_result = run_ready_future(manager.open(
            &AccessKind::Proxy {
                proxy_profile_id: Uuid::new_v4(),
            },
            "localhost",
            5432,
        ));

        let proxy_error = match proxy_result {
            Ok(_) => panic!("proxy mode should fail explicitly"),
            Err(error) => error,
        };

        let DbError::ConnectionFailed(proxy_error) = proxy_error else {
            panic!("proxy mode should return a connection error");
        };

        assert_eq!(
            proxy_error.message,
            "Proxy tunnels are not supported by the connect pipeline yet"
        );
    }

    #[test]
    fn unknown_managed_provider_returns_structured_failure() {
        let manager = test_manager();
        let result = run_ready_future(manager.open(
            &AccessKind::Managed {
                provider: "custom-provider".to_string(),
                params: std::collections::HashMap::new(),
            },
            "localhost",
            5432,
        ));

        let error = match result {
            Ok(_) => panic!("unknown managed providers should fail explicitly"),
            Err(error) => error,
        };

        let DbError::ConnectionFailed(error) = error else {
            panic!("managed mode should return a connection error");
        };

        assert_eq!(
            error.message,
            "Unknown managed access provider: 'custom-provider'. No handler registered."
        );
    }
}

#[async_trait::async_trait]
impl AccessManager for AppAccessManager {
    async fn open(
        &self,
        access_kind: &AccessKind,
        remote_host: &str,
        remote_port: u16,
    ) -> Result<AccessHandle, DbError> {
        match access_kind {
            AccessKind::Direct => Ok(AccessHandle::direct()),

            AccessKind::Ssh {
                ssh_tunnel_profile_id,
            } => self.open_ssh(ssh_tunnel_profile_id, remote_host, remote_port),

            AccessKind::Proxy { .. } => Err(DbError::connection_failed(
                "Proxy tunnels are not supported by the connect pipeline yet",
            )),

            AccessKind::Managed { provider, params } => {
                self.open_managed(provider, params, remote_host).await
            }
        }
    }
}

impl AppAccessManager {
    #[allow(clippy::result_large_err)]
    fn open_ssh(
        &self,
        ssh_tunnel_profile_id: &Uuid,
        remote_host: &str,
        remote_port: u16,
    ) -> Result<AccessHandle, DbError> {
        let resolved = self.ssh_tunnels.get(ssh_tunnel_profile_id).ok_or_else(|| {
            DbError::connection_failed(format!(
                "SSH tunnel profile '{}' was not found",
                ssh_tunnel_profile_id
            ))
        })?;

        let session = dbflux_ssh::establish_session(
            &resolved.config,
            resolved
                .secret
                .as_ref()
                .map(|secret| secret.expose_secret()),
        )?;

        let tunnel = dbflux_ssh::SshTunnel::start(session, remote_host.to_string(), remote_port)?;
        let local_port = tunnel.local_port();

        Ok(AccessHandle::tunnel(local_port, Box::new(tunnel)))
    }

    async fn open_managed(
        &self,
        provider: &str,
        params: &std::collections::HashMap<String, String>,
        remote_host: &str,
    ) -> Result<AccessHandle, DbError> {
        match provider {
            #[cfg(feature = "aws")]
            "aws-ssm" => {
                let instance_id = params.get("instance_id").map(String::as_str).unwrap_or("");
                let region = params
                    .get("region")
                    .map(String::as_str)
                    .unwrap_or("us-east-1");
                let remote_port: u16 = params
                    .get("remote_port")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);

                let factory = self.ssm_factory.as_ref().ok_or_else(|| {
                    DbError::connection_failed("SSM tunnel factory not available")
                })?;

                let tunnel = factory.start(instance_id, region, remote_host, remote_port)?;
                let local_port = tunnel.local_port();

                Ok(AccessHandle::tunnel(local_port, Box::new(tunnel)))
            }

            other => Err(DbError::connection_failed(format!(
                "Unknown managed access provider: '{}'. No handler registered.",
                other
            ))),
        }
    }
}
