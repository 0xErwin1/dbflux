#[cfg(feature = "aws")]
use std::sync::Arc;

use dbflux_core::DbError;
use dbflux_core::access::{AccessHandle, AccessKind, AccessManager};

/// Concrete access manager for the app crate.
///
/// Dispatches to the right tunnel infrastructure based on the `AccessKind`
/// variant. SSH and proxy tunnels are currently handled by the legacy connect
/// path in `ConnectProfileParams::execute()` — this manager only handles
/// direct connections and managed tunnels (e.g. `aws-ssm`).
pub struct AppAccessManager {
    #[cfg(feature = "aws")]
    ssm_factory: Option<Arc<dbflux_ssm::SsmTunnelFactory>>,
}

impl AppAccessManager {
    #[cfg(feature = "aws")]
    pub fn new(ssm_factory: Option<Arc<dbflux_ssm::SsmTunnelFactory>>) -> Self {
        Self { ssm_factory }
    }

    #[cfg(not(feature = "aws"))]
    pub fn new() -> Self {
        Self {}
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    use dbflux_core::DbError;
    use dbflux_core::access::{AccessKind, AccessManager};
    use uuid::Uuid;

    use super::AppAccessManager;

    #[cfg(feature = "aws")]
    fn test_manager() -> AppAccessManager {
        AppAccessManager::new(None)
    }

    #[cfg(not(feature = "aws"))]
    fn test_manager() -> AppAccessManager {
        AppAccessManager::new()
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
    fn ssh_and_proxy_modes_return_structured_legacy_path_errors() {
        let manager = test_manager();

        let ssh_result = run_ready_future(manager.open(
            &AccessKind::Ssh {
                ssh_tunnel_profile_id: Uuid::new_v4(),
            },
            "localhost",
            5432,
        ));

        let proxy_result = run_ready_future(manager.open(
            &AccessKind::Proxy {
                proxy_profile_id: Uuid::new_v4(),
            },
            "localhost",
            5432,
        ));

        let ssh_error = match ssh_result {
            Ok(_) => panic!("ssh mode should route to explicit legacy-path failure"),
            Err(error) => error,
        };

        let proxy_error = match proxy_result {
            Ok(_) => panic!("proxy mode should route to explicit legacy-path failure"),
            Err(error) => error,
        };

        let DbError::ConnectionFailed(ssh_error) = ssh_error else {
            panic!("ssh mode should return a connection error");
        };

        let DbError::ConnectionFailed(proxy_error) = proxy_error else {
            panic!("proxy mode should return a connection error");
        };

        assert_eq!(
            ssh_error.message,
            "SSH tunnels are managed by the legacy connect path"
        );
        assert_eq!(
            proxy_error.message,
            "Proxy tunnels are managed by the legacy connect path"
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
        _remote_port: u16,
    ) -> Result<AccessHandle, DbError> {
        match access_kind {
            AccessKind::Direct => Ok(AccessHandle::direct()),

            AccessKind::Ssh { .. } => Err(DbError::connection_failed(
                "SSH tunnels are managed by the legacy connect path",
            )),

            AccessKind::Proxy { .. } => Err(DbError::connection_failed(
                "Proxy tunnels are managed by the legacy connect path",
            )),

            AccessKind::Managed { provider, params } => {
                self.open_managed(provider, params, remote_host).await
            }
        }
    }
}

impl AppAccessManager {
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
