use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::{BufRead, BufReader};
use std::process::Child;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use std::{process::Stdio, thread};

use dbflux_core::secrecy::{ExposeSecret, SecretString};
use dbflux_core::{
    ConnectionProfile, DbConfig, DbError, DbKind, DriverFormDef, DriverMetadata, FormValues,
};
use dbflux_ipc::driver_protocol::DriverResponseBody;
use interprocess::local_socket::{GenericNamespaced, Name, Stream as IpcStream, prelude::*};

use crate::connection::IpcConnection;
use crate::transport::RpcClient;

static MANAGED_HOSTS: OnceLock<Mutex<HashMap<String, Child>>> = OnceLock::new();

const DEFAULT_MANAGED_HOST_PROGRAM: &str = "dbflux-driver-host";
const DEFAULT_STARTUP_TIMEOUT_MS: u64 = 5_000;
const MIN_STARTUP_TIMEOUT_MS: u64 = 1;
const STARTUP_OUTPUT_TAIL_LINES: usize = 6;
const STARTUP_OUTPUT_TAIL_BYTES: usize = 4_096;

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
    settings_schema: Option<Arc<DriverFormDef>>,
    launch: Option<IpcDriverLaunchConfig>,
}

#[derive(Clone, Debug)]
pub struct IpcDriverLaunchConfig {
    pub program: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub startup_timeout: Duration,
}

#[derive(Clone, Default)]
struct StartupOutputCollector {
    stdout: Arc<Mutex<VecDeque<String>>>,
    stderr: Arc<Mutex<VecDeque<String>>>,
    readers: Arc<Mutex<Vec<thread::JoinHandle<()>>>>,
}

impl IpcDriver {
    pub fn new(
        socket_id: String,
        kind: DbKind,
        metadata: DriverMetadata,
        form_definition: DriverFormDef,
        settings_schema: Option<DriverFormDef>,
    ) -> Self {
        Self {
            socket_id,
            kind,
            metadata,
            form_definition,
            settings_schema: settings_schema.map(Arc::new),
            launch: None,
        }
    }

    pub fn with_launch_config(mut self, launch: IpcDriverLaunchConfig) -> Self {
        self.launch = Some(launch);
        self
    }

    #[allow(clippy::result_large_err)]
    pub fn build_launch_config(
        socket_id: &str,
        command: Option<&str>,
        args: &[String],
        env: &HashMap<String, String>,
        startup_timeout_ms: Option<u64>,
    ) -> Result<Option<IpcDriverLaunchConfig>, DbError> {
        Self::validate_socket_id(socket_id)?;

        let program = match command.map(str::trim).filter(|value| !value.is_empty()) {
            Some(program) => Some(program.to_string()),
            None if args.is_empty() => None,
            None => {
                Self::validate_default_host_launch_args(socket_id, args)?;
                Some(DEFAULT_MANAGED_HOST_PROGRAM.to_string())
            }
        };

        let Some(program) = program else {
            return Ok(None);
        };

        let startup_timeout_ms = match startup_timeout_ms {
            Some(0) => {
                return Err(DbError::ConnectionFailed(
                    format!(
                        "Startup timeout for service '{}' must be at least {} ms",
                        socket_id, MIN_STARTUP_TIMEOUT_MS
                    )
                    .into(),
                ));
            }
            Some(timeout) => timeout,
            None => DEFAULT_STARTUP_TIMEOUT_MS,
        };

        let mut env_pairs = env
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        env_pairs.sort_by(|left, right| left.0.cmp(&right.0));

        Ok(Some(IpcDriverLaunchConfig {
            program,
            args: args.to_vec(),
            env: env_pairs,
            startup_timeout: Duration::from_millis(startup_timeout_ms),
        }))
    }

    pub fn socket_id(&self) -> &str {
        &self.socket_id
    }

    #[allow(clippy::result_large_err)]
    pub fn validate_socket_id(socket_id: &str) -> Result<(), DbError> {
        if socket_id.is_empty()
            || !socket_id
                .chars()
                .all(|char| char.is_ascii_alphanumeric() || matches!(char, '.' | '_' | '-'))
        {
            return Err(DbError::ConnectionFailed(
                format!(
                    "Invalid socket ID '{}': use only letters, numbers, '.', '_' or '-'",
                    socket_id
                )
                .into(),
            ));
        }

        Self::parse_socket_name(socket_id).map(|_| ())
    }

    #[allow(clippy::result_large_err)]
    pub fn probe_driver(
        socket_id: &str,
        launch: Option<&IpcDriverLaunchConfig>,
    ) -> Result<(DbKind, DriverMetadata, DriverFormDef, Option<DriverFormDef>), DbError> {
        Self::ensure_host_running_for(socket_id, launch)?;

        let name = Self::parse_socket_name(socket_id)?;

        let client = RpcClient::connect(name).map_err(DbError::from)?;
        let hello = client.hello_response();

        Ok((
            hello.driver_kind,
            hello.driver_metadata.clone(),
            hello.form_definition.clone(),
            hello.settings_schema.clone(),
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
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

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

        let output_collector = StartupOutputCollector::capture_from_child(&mut child);

        let deadline = Instant::now() + launch.startup_timeout;
        while Instant::now() < deadline {
            if Self::socket_is_live_for(socket_id)? {
                Self::register_managed_host(socket_id, child)?;
                return Ok(());
            }

            if let Some(status) = child.try_wait().map_err(DbError::IoError)? {
                let details = output_collector.failure_details();
                return Err(DbError::ConnectionFailed(
                    Self::with_startup_details(
                        format!(
                            "Driver host '{}' exited before socket was ready ({})",
                            launch.program, status
                        ),
                        details,
                    )
                    .into(),
                ));
            }

            thread::sleep(Duration::from_millis(75));
        }

        let _ = child.kill();
        let _ = child.wait();

        let details = output_collector.failure_details();

        Err(DbError::ConnectionFailed(
            Self::with_startup_details(
                format!(
                    "Driver host '{}' did not become ready within {} ms",
                    launch.program,
                    launch.startup_timeout.as_millis()
                ),
                details,
            )
            .into(),
        ))
    }

    #[allow(clippy::result_large_err)]
    fn ensure_host_running(&self) -> Result<(), DbError> {
        Self::ensure_host_running_for(&self.socket_id, self.launch.as_ref())
    }

    fn missing_default_host_flag_value(flag: &str) -> DbError {
        DbError::ConnectionFailed(
            format!(
                "Managed external drivers using the default 'dbflux-driver-host' command require a value immediately after '{flag}'"
            )
            .into(),
        )
    }

    #[allow(clippy::result_large_err)]
    fn parse_default_host_launch_args(args: &[String]) -> Result<(String, String), DbError> {
        let mut driver = None;
        let mut configured_socket = None;

        let mut index = 0;
        while index < args.len() {
            match args[index].as_str() {
                "--driver" => {
                    let value = args
                        .get(index + 1)
                        .ok_or_else(|| Self::missing_default_host_flag_value("--driver"))?
                        .clone();
                    driver = Some(value);
                    index += 2;
                }
                "--socket" => {
                    let value = args
                        .get(index + 1)
                        .ok_or_else(|| Self::missing_default_host_flag_value("--socket"))?
                        .clone();
                    configured_socket = Some(value);
                    index += 2;
                }
                "--help" | "-h" => {
                    return Err(DbError::ConnectionFailed(
                        "Managed external drivers using the default 'dbflux-driver-host' command cannot use '--help' or '-h'"
                            .into(),
                    ));
                }
                other => {
                    return Err(DbError::ConnectionFailed(
                        format!(
                            "Managed external drivers using the default 'dbflux-driver-host' command does not accept argument '{}'; use '--driver <name>' and '--socket <name>'",
                            other
                        )
                        .into(),
                    ));
                }
            }
        }

        let Some(driver) = driver else {
            return Err(DbError::ConnectionFailed(
                "Managed external drivers using the default 'dbflux-driver-host' command must include both '--driver' and '--socket' arguments"
                    .into(),
            ));
        };

        if driver.trim().is_empty() || driver.starts_with("--") {
            return Err(DbError::ConnectionFailed(
                "Managed external drivers using the default 'dbflux-driver-host' command require a non-empty value for '--driver'"
                    .into(),
            ));
        }

        let Some(configured_socket) = configured_socket else {
            return Err(DbError::ConnectionFailed(
                "Managed external drivers using the default 'dbflux-driver-host' command must include both '--driver' and '--socket' arguments"
                    .into(),
            ));
        };

        if configured_socket.trim().is_empty() || configured_socket.starts_with("--") {
            return Err(DbError::ConnectionFailed(
                "Managed external drivers using the default 'dbflux-driver-host' command require a non-empty value for '--socket'"
                    .into(),
            ));
        }

        Ok((driver, configured_socket))
    }

    #[allow(clippy::result_large_err)]
    fn validate_default_host_launch_args(socket_id: &str, args: &[String]) -> Result<(), DbError> {
        let (_, configured_socket) = Self::parse_default_host_launch_args(args)?;

        if configured_socket != socket_id {
            return Err(DbError::ConnectionFailed(
                format!(
                    "Managed external driver socket mismatch: service '{}' must launch 'dbflux-driver-host' with '--socket {}'",
                    socket_id, socket_id
                )
                .into(),
            ));
        }

        Ok(())
    }

    fn startup_output_tail(stdout: &str, stderr: &str) -> String {
        let stdout_tail = Self::trim_recent_bytes(
            &Self::tail_lines(stdout, STARTUP_OUTPUT_TAIL_LINES).join("\n"),
            STARTUP_OUTPUT_TAIL_BYTES,
        );
        let stderr_tail = Self::trim_recent_bytes(
            &Self::tail_lines(stderr, STARTUP_OUTPUT_TAIL_LINES).join("\n"),
            STARTUP_OUTPUT_TAIL_BYTES,
        );

        let mut sections = Vec::new();

        if !stdout_tail.is_empty() {
            sections.push(format!("stdout:\n{stdout_tail}"));
        }

        if !stderr_tail.is_empty() {
            sections.push(format!("stderr:\n{stderr_tail}"));
        }

        sections.join("\n\n")
    }

    fn tail_lines(output: &str, limit: usize) -> Vec<&str> {
        let lines = output
            .lines()
            .filter(|line| !line.trim().is_empty())
            .collect::<Vec<_>>();

        let start = lines.len().saturating_sub(limit);
        lines.into_iter().skip(start).collect()
    }

    fn with_startup_details(summary: String, details: Option<String>) -> String {
        match details {
            Some(details) if !details.is_empty() => {
                format!("{summary}\n\nRecent host output:\n{details}")
            }
            _ => summary,
        }
    }

    fn trim_recent_bytes(output: &str, limit: usize) -> String {
        if output.len() <= limit {
            return output.to_string();
        }

        let ellipsis = "…";
        let content_limit = limit.saturating_sub(ellipsis.len());
        if content_limit == 0 {
            return ellipsis.to_string();
        }

        let mut start = output.len().saturating_sub(content_limit);
        while !output.is_char_boundary(start) {
            start += 1;
        }

        format!("{ellipsis}{}", &output[start..])
    }
}

impl StartupOutputCollector {
    fn capture_from_child(child: &mut Child) -> Self {
        let collector = Self::default();

        if let Some(stdout) = child.stdout.take() {
            collector.capture_reader(stdout, collector.stdout.clone());
        }

        if let Some(stderr) = child.stderr.take() {
            collector.capture_reader(stderr, collector.stderr.clone());
        }

        collector
    }

    fn capture_reader<R>(&self, reader: R, target: Arc<Mutex<VecDeque<String>>>)
    where
        R: std::io::Read + Send + 'static,
    {
        let handle = thread::spawn(move || {
            for line in BufReader::new(reader).lines().map_while(Result::ok) {
                let Ok(mut tail) = target.lock() else {
                    return;
                };

                tail.push_back(line);
                if tail.len() > STARTUP_OUTPUT_TAIL_LINES {
                    tail.pop_front();
                }
            }
        });

        if let Ok(mut readers) = self.readers.lock() {
            readers.push(handle);
        }
    }

    fn wait_for_readers(&self) {
        let Ok(mut readers) = self.readers.lock() else {
            return;
        };

        for handle in readers.drain(..) {
            let _ = handle.join();
        }
    }

    fn failure_details(&self) -> Option<String> {
        self.wait_for_readers();

        let stdout = self
            .stdout
            .lock()
            .ok()?
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join("\n");
        let stderr = self
            .stderr
            .lock()
            .ok()?
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join("\n");
        let details = IpcDriver::startup_output_tail(&stdout, &stderr);

        if details.is_empty() {
            None
        } else {
            Some(details)
        }
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

    fn settings_schema(&self) -> Option<Arc<DriverFormDef>> {
        self.settings_schema.clone()
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
        password: Option<&SecretString>,
        ssh_secret: Option<&SecretString>,
    ) -> Result<Box<dyn dbflux_core::Connection>, DbError> {
        self.ensure_host_running()?;

        let name = Self::parse_socket_name(&self.socket_id)?;

        let client = RpcClient::connect(name).map_err(DbError::from)?;

        let profile_json = serde_json::to_string(profile)
            .map_err(|e| DbError::InvalidProfile(format!("JSON serialization failed: {e}")))?;

        let response = client
            .open_session(
                &profile_json,
                password.map(|value| value.expose_secret()),
                ssh_secret.map(|value| value.expose_secret()),
            )
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_launch_config_keeps_explicit_command_and_args() {
        let config = IpcDriver::build_launch_config(
            "demo.sock",
            Some("custom-host"),
            &["--flag".to_string()],
            &HashMap::from([("KEY".to_string(), "VALUE".to_string())]),
            Some(9_000),
        )
        .unwrap();

        let config = config.expect("explicit command should build launch config");

        assert_eq!(config.program, "custom-host");
        assert_eq!(config.args, vec!["--flag"]);
        assert_eq!(config.env, vec![("KEY".to_string(), "VALUE".to_string())]);
        assert_eq!(config.startup_timeout, Duration::from_millis(9_000));
    }

    #[test]
    fn build_launch_config_defaults_timeout_and_driver_host_when_valid() {
        let config = IpcDriver::build_launch_config(
            "demo.sock",
            None,
            &[
                "--driver".to_string(),
                "demo".to_string(),
                "--socket".to_string(),
                "demo.sock".to_string(),
            ],
            &HashMap::new(),
            None,
        )
        .unwrap();

        let config = config.expect("default host args should build launch config");

        assert_eq!(config.program, "dbflux-driver-host");
        assert_eq!(config.startup_timeout, Duration::from_millis(5_000));
    }

    #[test]
    fn build_launch_config_rejects_default_driver_host_without_required_args() {
        let error = IpcDriver::build_launch_config(
            "demo.sock",
            None,
            &["--driver".to_string(), "demo".to_string()],
            &HashMap::new(),
            None,
        )
        .unwrap_err();

        let message = error.to_string();
        assert!(message.contains("--socket"));
        assert!(message.contains("dbflux-driver-host"));
    }

    #[test]
    fn build_launch_config_rejects_default_driver_host_socket_mismatch() {
        let error = IpcDriver::build_launch_config(
            "expected.sock",
            None,
            &[
                "--driver".to_string(),
                "demo".to_string(),
                "--socket".to_string(),
                "other.sock".to_string(),
            ],
            &HashMap::new(),
            None,
        )
        .unwrap_err();

        let message = error.to_string();
        assert!(message.contains("socket mismatch"));
        assert!(message.contains("expected.sock"));
    }

    #[test]
    fn build_launch_config_uses_last_duplicate_socket_flag_value() {
        let config = IpcDriver::build_launch_config(
            "expected.sock",
            None,
            &[
                "--driver".to_string(),
                "demo".to_string(),
                "--socket".to_string(),
                "wrong.sock".to_string(),
                "--socket".to_string(),
                "expected.sock".to_string(),
            ],
            &HashMap::new(),
            None,
        )
        .unwrap();

        let config = config.expect("duplicate socket args should still build launch config");

        assert_eq!(config.program, "dbflux-driver-host");
    }

    #[test]
    fn build_launch_config_rejects_equals_syntax_for_default_driver_host_flags() {
        let error = IpcDriver::build_launch_config(
            "demo.sock",
            None,
            &[
                "--driver=demo".to_string(),
                "--socket=demo.sock".to_string(),
            ],
            &HashMap::new(),
            None,
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("does not accept argument '--driver=demo'")
        );
    }

    #[test]
    fn build_launch_config_rejects_unknown_default_driver_host_flags() {
        let error = IpcDriver::build_launch_config(
            "demo.sock",
            None,
            &[
                "--driver".to_string(),
                "demo".to_string(),
                "--socket".to_string(),
                "demo.sock".to_string(),
                "--verbose".to_string(),
            ],
            &HashMap::new(),
            None,
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("does not accept argument '--verbose'")
        );
    }

    #[test]
    fn build_launch_config_allows_manual_service_without_launch_command() {
        let config =
            IpcDriver::build_launch_config("live.sock", None, &[], &HashMap::new(), None).unwrap();

        assert!(config.is_none());
    }

    #[test]
    fn build_launch_config_allows_manual_service_with_unused_zero_timeout() {
        let config =
            IpcDriver::build_launch_config("live.sock", None, &[], &HashMap::new(), Some(0))
                .unwrap();

        assert!(config.is_none());
    }

    #[test]
    fn build_launch_config_rejects_zero_timeout() {
        let error = IpcDriver::build_launch_config(
            "demo.sock",
            Some("custom-host"),
            &[],
            &HashMap::new(),
            Some(0),
        )
        .unwrap_err();

        assert!(error.to_string().contains("at least 1 ms"));
    }

    #[test]
    fn startup_output_tail_keeps_recent_stdout_and_stderr_lines() {
        let stdout = (1..=8)
            .map(|index| format!("stdout-{index}"))
            .collect::<Vec<_>>()
            .join("\n");
        let stderr = (1..=8)
            .map(|index| format!("stderr-{index}"))
            .collect::<Vec<_>>()
            .join("\n");

        let details = IpcDriver::startup_output_tail(&stdout, &stderr);

        assert!(details.contains("stdout-8"));
        assert!(details.contains("stderr-8"));
        assert!(!details.contains("stdout-1"));
        assert!(!details.contains("stderr-1"));
    }

    #[test]
    fn startup_output_tail_truncates_large_output_by_bytes() {
        let details =
            IpcDriver::startup_output_tail(&"x".repeat(STARTUP_OUTPUT_TAIL_LINES * 2_000), "");

        assert!(details.len() <= 4_096 + "stdout:\n".len());
    }

    #[test]
    fn startup_output_collector_waits_for_readers_before_reporting_failure_details() {
        struct SlowReader {
            bytes: std::io::Cursor<Vec<u8>>,
            delay_applied: bool,
        }

        impl SlowReader {
            fn new(contents: &str) -> Self {
                Self {
                    bytes: std::io::Cursor::new(contents.as_bytes().to_vec()),
                    delay_applied: false,
                }
            }
        }

        impl std::io::Read for SlowReader {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                if !self.delay_applied {
                    self.delay_applied = true;
                    thread::sleep(Duration::from_millis(25));
                }

                self.bytes.read(buf)
            }
        }

        let collector = StartupOutputCollector::default();
        collector.capture_reader(SlowReader::new("stderr-line\n"), collector.stderr.clone());

        let details = collector
            .failure_details()
            .expect("collector should include stderr");

        assert!(details.contains("stderr-line"));
    }
}
