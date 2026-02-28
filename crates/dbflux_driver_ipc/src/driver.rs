use std::collections::HashMap;
use std::process::Child;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use std::{process::Stdio, thread};

use dbflux_core::{
    ConnectionProfile, DbConfig, DbError, DbKind, DriverFormDef, DriverMetadata, FormValues,
};
use dbflux_ipc::driver_protocol::DriverResponseBody;
use interprocess::local_socket::{GenericNamespaced, Name, Stream as IpcStream, prelude::*};

use crate::connection::IpcConnection;
use crate::transport::RpcClient;

static MANAGED_HOSTS: OnceLock<Mutex<HashMap<String, Child>>> = OnceLock::new();

fn managed_hosts() -> &'static Mutex<HashMap<String, Child>> {
    MANAGED_HOSTS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Stops all RPC host processes that were started by DBFlux.
///
/// Returns the number of processes that were terminated.
pub fn shutdown_managed_hosts() -> usize {
    let mut children = {
        let Ok(mut hosts) = managed_hosts().lock() else {
            log::error!("Managed RPC host registry is poisoned");
            return 0;
        };

        std::mem::take(&mut *hosts)
    };

    let mut stopped = 0;
    for (socket_id, mut child) in children.drain() {
        match child.try_wait() {
            Ok(Some(status)) => {
                log::info!(
                    "RPC host for '{}' already exited before shutdown ({})",
                    socket_id,
                    status
                );
            }
            Ok(None) => {
                if let Err(error) = child.kill() {
                    log::warn!(
                        "Failed to kill managed RPC host for '{}': {}",
                        socket_id,
                        error
                    );
                    continue;
                }

                if let Err(error) = child.wait() {
                    log::warn!(
                        "Failed to wait for managed RPC host '{}' after kill: {}",
                        socket_id,
                        error
                    );
                }

                stopped += 1;
            }
            Err(error) => {
                log::warn!(
                    "Failed to inspect managed RPC host for '{}': {}",
                    socket_id,
                    error
                );
            }
        }
    }

    stopped
}

/// An IPC-based driver that proxies all operations to a remote driver-host process.
///
/// The driver connects to a driver-host over a local socket identified by a
/// string name (not a filesystem path). The underlying transport is cross-platform:
/// abstract namespace UDS on Linux, UDS in /tmp on macOS, named pipes on Windows.
///
/// `kind`, `metadata`, and `form_definition` are provided at construction time
/// (typically from a probe against the driver host), so the driver can satisfy
/// `DbDriver` metadata APIs without needing an active connection.
pub struct IpcDriver {
    socket_id: String,
    kind: DbKind,
    metadata: DriverMetadata,
    form_definition: DriverFormDef,
    launch: Option<IpcDriverLaunchConfig>,
}

#[derive(Clone, Debug)]
pub struct IpcDriverLaunchConfig {
    pub program: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub startup_timeout: Duration,
}

impl IpcDriver {
    pub fn new(
        socket_id: String,
        kind: DbKind,
        metadata: DriverMetadata,
        form_definition: DriverFormDef,
    ) -> Self {
        Self {
            socket_id,
            kind,
            metadata,
            form_definition,
            launch: None,
        }
    }

    pub fn with_launch_config(mut self, launch: IpcDriverLaunchConfig) -> Self {
        self.launch = Some(launch);
        self
    }

    pub fn socket_id(&self) -> &str {
        &self.socket_id
    }

    #[allow(clippy::result_large_err)]
    pub fn probe_driver(
        socket_id: &str,
        launch: Option<&IpcDriverLaunchConfig>,
    ) -> Result<(DbKind, DriverMetadata, DriverFormDef), DbError> {
        Self::ensure_host_running_for(socket_id, launch)?;

        let name = Self::parse_socket_name(socket_id)?;

        let client = RpcClient::connect(name).map_err(DbError::from)?;
        let hello = client.hello_response();

        Ok((
            hello.driver_kind,
            hello.driver_metadata.clone(),
            hello.form_definition.clone(),
        ))
    }

    #[allow(clippy::result_large_err)]
    fn socket_is_live_for(socket_id: &str) -> Result<bool, DbError> {
        let name = Self::parse_socket_name(socket_id)?;

        match IpcStream::connect(name) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    #[allow(clippy::result_large_err)]
    fn managed_host_is_running(socket_id: &str) -> Result<bool, DbError> {
        let mut hosts = managed_hosts().lock().map_err(|_| {
            DbError::ConnectionFailed("Managed RPC host registry is poisoned".into())
        })?;

        let mut should_remove = false;
        let is_running = if let Some(child) = hosts.get_mut(socket_id) {
            match child.try_wait().map_err(DbError::IoError)? {
                Some(_) => {
                    should_remove = true;
                    false
                }
                None => true,
            }
        } else {
            false
        };

        if should_remove {
            hosts.remove(socket_id);
        }

        Ok(is_running)
    }

    #[allow(clippy::result_large_err)]
    fn register_managed_host(socket_id: &str, mut child: Child) -> Result<(), DbError> {
        let mut hosts = match managed_hosts().lock() {
            Ok(hosts) => hosts,
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(DbError::ConnectionFailed(
                    "Managed RPC host registry is poisoned".into(),
                ));
            }
        };

        if let Some(mut previous) = hosts.insert(socket_id.to_string(), child)
            && let Ok(None) = previous.try_wait()
        {
            let _ = previous.kill();
            let _ = previous.wait();
        }

        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn parse_socket_name(socket_id: &str) -> Result<Name<'static>, DbError> {
        socket_id
            .to_string()
            .to_ns_name::<GenericNamespaced>()
            .map_err(|e| DbError::ConnectionFailed(e.to_string().into()))
    }

    #[allow(clippy::result_large_err)]
    fn ensure_host_running_for(
        socket_id: &str,
        launch: Option<&IpcDriverLaunchConfig>,
    ) -> Result<(), DbError> {
        if Self::socket_is_live_for(socket_id)? {
            return Ok(());
        }

        if Self::managed_host_is_running(socket_id)? {
            let startup_timeout = launch
                .map(|config| config.startup_timeout)
                .unwrap_or_else(|| Duration::from_millis(2_000));
            let deadline = Instant::now() + startup_timeout;

            while Instant::now() < deadline {
                if Self::socket_is_live_for(socket_id)? {
                    return Ok(());
                }

                if !Self::managed_host_is_running(socket_id)? {
                    break;
                }

                thread::sleep(Duration::from_millis(75));
            }

            if Self::managed_host_is_running(socket_id)? {
                return Err(DbError::ConnectionFailed(
                    format!(
                        "Managed RPC host for '{}' is running but socket is unavailable",
                        socket_id
                    )
                    .into(),
                ));
            }
        }

        let Some(launch) = launch else {
            return Err(DbError::ConnectionFailed(
                format!("Driver host socket '{}' is not available", socket_id).into(),
            ));
        };

        let mut command = std::process::Command::new(&launch.program);
        command
            .args(&launch.args)
            .envs(launch.env.iter().map(|(k, v)| (k, v)))
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        let mut child = command.spawn().map_err(|e| {
            DbError::ConnectionFailed(
                format!("Failed to start driver host '{}': {}", launch.program, e).into(),
            )
        })?;

        log::info!(
            "Started managed RPC host '{}' for socket '{}' (pid={})",
            launch.program,
            socket_id,
            child.id()
        );

        let deadline = Instant::now() + launch.startup_timeout;
        while Instant::now() < deadline {
            if Self::socket_is_live_for(socket_id)? {
                Self::register_managed_host(socket_id, child)?;
                return Ok(());
            }

            if let Some(status) = child.try_wait().map_err(DbError::IoError)? {
                return Err(DbError::ConnectionFailed(
                    format!(
                        "Driver host '{}' exited before socket was ready ({})",
                        launch.program, status
                    )
                    .into(),
                ));
            }

            thread::sleep(Duration::from_millis(75));
        }

        let _ = child.kill();
        let _ = child.wait();

        Err(DbError::ConnectionFailed(
            format!(
                "Driver host '{}' did not become ready within {} ms",
                launch.program,
                launch.startup_timeout.as_millis()
            )
            .into(),
        ))
    }

    #[allow(clippy::result_large_err)]
    fn ensure_host_running(&self) -> Result<(), DbError> {
        Self::ensure_host_running_for(&self.socket_id, self.launch.as_ref())
    }
}

impl dbflux_core::DbDriver for IpcDriver {
    fn kind(&self) -> DbKind {
        self.kind
    }

    fn metadata(&self) -> &DriverMetadata {
        &self.metadata
    }

    fn driver_key(&self) -> dbflux_core::DriverKey {
        format!("rpc:{}", self.socket_id)
    }

    fn form_definition(&self) -> &DriverFormDef {
        &self.form_definition
    }

    fn build_config(&self, values: &FormValues) -> Result<DbConfig, DbError> {
        Ok(DbConfig::External {
            kind: self.kind,
            values: values.clone(),
        })
    }

    fn extract_values(&self, config: &DbConfig) -> FormValues {
        match config {
            DbConfig::External { values, .. } => values.clone(),
            _ => FormValues::new(),
        }
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        password: Option<&str>,
        ssh_secret: Option<&str>,
    ) -> Result<Box<dyn dbflux_core::Connection>, DbError> {
        self.ensure_host_running()?;

        let name = Self::parse_socket_name(&self.socket_id)?;

        let client = RpcClient::connect(name).map_err(DbError::from)?;

        let profile_json = serde_json::to_string(profile)
            .map_err(|e| DbError::InvalidProfile(format!("JSON serialization failed: {e}")))?;

        let response = client
            .open_session(&profile_json, password, ssh_secret)
            .map_err(DbError::from)?;

        let DriverResponseBody::SessionOpened {
            session_id,
            kind,
            metadata,
            schema_loading_strategy,
            schema_features,
            code_gen_capabilities,
        } = response
        else {
            return Err(DbError::ConnectionFailed(
                "Unexpected response from driver host".into(),
            ));
        };

        let capabilities = metadata.capabilities;

        Ok(Box::new(IpcConnection::new(
            Arc::new(client),
            session_id,
            kind,
            metadata,
            capabilities,
            schema_loading_strategy,
            schema_features,
            code_gen_capabilities,
        )))
    }

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), DbError> {
        self.ensure_host_running()?;

        let name = Self::parse_socket_name(&self.socket_id)?;

        let client = RpcClient::connect(name).map_err(DbError::from)?;

        let profile_json = serde_json::to_string(profile)
            .map_err(|e| DbError::InvalidProfile(format!("JSON serialization failed: {e}")))?;

        let response = client
            .open_session(&profile_json, None, None)
            .map_err(DbError::from)?;

        let DriverResponseBody::SessionOpened { session_id, .. } = response else {
            return Err(DbError::ConnectionFailed(
                "Unexpected response from driver host".into(),
            ));
        };

        let result = client.ping(session_id).map_err(DbError::from);

        let _ = client.close_session(session_id);

        result
    }
}
