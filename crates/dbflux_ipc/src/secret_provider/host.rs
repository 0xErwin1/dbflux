use std::collections::HashMap;
use std::process::{Child, Command};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use dbflux_core::DbError;
use interprocess::local_socket::{Stream as IpcStream, prelude::*};

use crate::auth::{SECRET_PROVIDER_RPC_AUTH_TOKEN_ENV, process_auth_token};
use crate::auth_provider_client::IpcServiceLaunchConfig;
use crate::socket::secret_provider_socket_name;

const UNMANAGED_STARTUP_WAIT: Duration = Duration::from_millis(2_000);
const POLL_INTERVAL: Duration = Duration::from_millis(75);

fn managed_hosts() -> &'static Mutex<HashMap<String, Child>> {
    static MANAGED_HOSTS: OnceLock<Mutex<HashMap<String, Child>>> = OnceLock::new();
    MANAGED_HOSTS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Stops all secret-provider host processes started by DBFlux. Returns count terminated.
pub fn shutdown_managed_secret_provider_hosts() -> usize {
    let mut children = {
        let Ok(mut hosts) = managed_hosts().lock() else {
            log::error!("Managed secret-provider host registry is poisoned");
            return 0;
        };
        std::mem::take(&mut *hosts)
    };

    let mut stopped = 0;

    for (socket_id, mut child) in children.drain() {
        match child.try_wait() {
            Ok(Some(status)) => {
                log::info!(
                    "Secret-provider host for '{}' already exited before shutdown ({})",
                    socket_id,
                    status
                );
            }
            Ok(None) => {
                if let Err(error) = child.kill() {
                    log::warn!(
                        "Failed to kill secret-provider host '{}': {}",
                        socket_id,
                        error
                    );
                    continue;
                }
                if let Err(error) = child.wait() {
                    log::warn!(
                        "Failed to wait for secret-provider host '{}' after kill: {}",
                        socket_id,
                        error
                    );
                }
                stopped += 1;
            }
            Err(error) => {
                log::warn!(
                    "Failed to inspect secret-provider host '{}': {}",
                    socket_id,
                    error
                );
            }
        }
    }

    stopped
}

#[allow(clippy::result_large_err)]
pub(super) fn validate_socket_id(socket_id: &str) -> Result<(), DbError> {
    if socket_id.is_empty()
        || !socket_id.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
        })
    {
        return Err(DbError::connection_failed(format!(
            "Invalid socket ID '{}': use only letters, numbers, '.', '_' or '-'",
            socket_id
        )));
    }

    secret_provider_socket_name(socket_id)
        .map(|_| ())
        .map_err(|error| DbError::connection_failed(error.to_string()))
}

#[allow(clippy::result_large_err)]
pub(super) fn connect(socket_id: &str) -> Result<IpcStream, DbError> {
    let name = secret_provider_socket_name(socket_id)
        .map_err(|error| DbError::connection_failed(error.to_string()))?;

    IpcStream::connect(name).map_err(|error| DbError::connection_failed(error.to_string()))
}

#[allow(clippy::result_large_err)]
fn socket_is_live_for(socket_id: &str) -> Result<bool, DbError> {
    let name = secret_provider_socket_name(socket_id)
        .map_err(|error| DbError::connection_failed(error.to_string()))?;

    Ok(IpcStream::connect(name).is_ok())
}

#[allow(clippy::result_large_err)]
fn managed_host_is_running(socket_id: &str) -> Result<bool, DbError> {
    let mut hosts = managed_hosts().lock().map_err(|_| {
        DbError::connection_failed("Managed secret-provider host registry is poisoned".to_string())
    })?;

    let Some(child) = hosts.get_mut(socket_id) else {
        return Ok(false);
    };

    match child.try_wait().map_err(DbError::IoError)? {
        Some(_) => {
            hosts.remove(socket_id);
            Ok(false)
        }
        None => Ok(true),
    }
}

fn terminate_child(socket_id: &str, child: &mut Child) {
    if let Err(error) = child.kill() {
        log::warn!(
            "Failed to kill secret-provider host '{}': {}",
            socket_id,
            error
        );
        return;
    }

    if let Err(error) = child.wait() {
        log::warn!(
            "Failed to wait for secret-provider host '{}' after kill: {}",
            socket_id,
            error
        );
    }
}

#[allow(clippy::result_large_err)]
fn register_managed_host(socket_id: &str, mut child: Child) -> Result<(), DbError> {
    let mut hosts = match managed_hosts().lock() {
        Ok(hosts) => hosts,
        Err(_) => {
            terminate_child(socket_id, &mut child);
            return Err(DbError::connection_failed(
                "Managed secret-provider host registry is poisoned".to_string(),
            ));
        }
    };

    if let Some(mut previous) = hosts.insert(socket_id.to_string(), child)
        && let Ok(None) = previous.try_wait()
    {
        terminate_child(socket_id, &mut previous);
    }

    Ok(())
}

/// Makes sure the provider socket accepts connections, relaunching a managed host when it
/// has exited, so a provider that was restarted or crashed recovers on the next request.
#[allow(clippy::result_large_err)]
pub(super) fn ensure_host_running_for(
    socket_id: &str,
    launch: Option<&IpcServiceLaunchConfig>,
) -> Result<(), DbError> {
    if socket_is_live_for(socket_id)? {
        return Ok(());
    }

    if managed_host_is_running(socket_id)? {
        let startup_timeout = launch
            .map(|config| config.startup_timeout)
            .unwrap_or(UNMANAGED_STARTUP_WAIT);
        let deadline = Instant::now() + startup_timeout;

        while Instant::now() < deadline {
            if socket_is_live_for(socket_id)? {
                return Ok(());
            }

            if !managed_host_is_running(socket_id)? {
                break;
            }

            thread::sleep(POLL_INTERVAL);
        }

        if managed_host_is_running(socket_id)? {
            return Err(DbError::connection_failed(format!(
                "Managed secret-provider host for '{}' is running but socket is unavailable",
                socket_id
            )));
        }
    }

    let Some(launch) = launch else {
        return Err(DbError::connection_failed(format!(
            "Secret-provider socket '{}' is not available",
            socket_id
        )));
    };

    launch_managed_host(socket_id, launch)
}

#[allow(clippy::result_large_err)]
fn launch_managed_host(socket_id: &str, launch: &IpcServiceLaunchConfig) -> Result<(), DbError> {
    let mut command = Command::new(&launch.program);
    command.args(&launch.args);
    command.envs(launch.env.iter().cloned());

    if let Some(token) = process_auth_token() {
        command.env(SECRET_PROVIDER_RPC_AUTH_TOKEN_ENV, token);
    }

    let child = command.spawn().map_err(DbError::IoError)?;
    register_managed_host(socket_id, child)?;

    let deadline = Instant::now() + launch.startup_timeout;
    while Instant::now() < deadline {
        if socket_is_live_for(socket_id)? {
            return Ok(());
        }

        if !managed_host_is_running(socket_id)? {
            return Err(DbError::connection_failed(format!(
                "Managed secret-provider host for '{}' exited before socket was ready",
                socket_id
            )));
        }

        thread::sleep(POLL_INTERVAL);
    }

    Err(DbError::connection_failed(format!(
        "Managed secret-provider host for '{}' did not become ready within {} ms",
        socket_id,
        launch.startup_timeout.as_millis()
    )))
}
