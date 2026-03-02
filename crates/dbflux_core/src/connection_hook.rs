use crate::profile::{ConnectionProfile, DbConfig};
use crate::task::CancelToken;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Read;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum HookFailureMode {
    #[default]
    Disconnect,
    Warn,
    Ignore,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookPhase {
    PreConnect,
    PostConnect,
    PreDisconnect,
    PostDisconnect,
}

impl HookPhase {
    pub fn label(&self) -> &'static str {
        match self {
            Self::PreConnect => "Pre-connect",
            Self::PostConnect => "Post-connect",
            Self::PreDisconnect => "Pre-disconnect",
            Self::PostDisconnect => "Post-disconnect",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionHook {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub cwd: Option<PathBuf>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default = "default_inherit_env")]
    pub inherit_env: bool,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub on_failure: HookFailureMode,
}

fn default_enabled() -> bool {
    true
}

fn default_inherit_env() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ConnectionHooks {
    #[serde(default)]
    pub pre_connect: Vec<ConnectionHook>,
    #[serde(default)]
    pub post_connect: Vec<ConnectionHook>,
    #[serde(default)]
    pub pre_disconnect: Vec<ConnectionHook>,
    #[serde(default)]
    pub post_disconnect: Vec<ConnectionHook>,
}

impl ConnectionHooks {
    pub fn phase_hooks(&self, phase: HookPhase) -> &[ConnectionHook] {
        match phase {
            HookPhase::PreConnect => &self.pre_connect,
            HookPhase::PostConnect => &self.post_connect,
            HookPhase::PreDisconnect => &self.pre_disconnect,
            HookPhase::PostDisconnect => &self.post_disconnect,
        }
    }

    pub fn phase_hooks_mut(&mut self, phase: HookPhase) -> &mut Vec<ConnectionHook> {
        match phase {
            HookPhase::PreConnect => &mut self.pre_connect,
            HookPhase::PostConnect => &mut self.post_connect,
            HookPhase::PreDisconnect => &mut self.pre_disconnect,
            HookPhase::PostDisconnect => &mut self.post_disconnect,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ConnectionHookBindings {
    #[serde(default)]
    pub pre_connect: Vec<String>,
    #[serde(default)]
    pub post_connect: Vec<String>,
    #[serde(default)]
    pub pre_disconnect: Vec<String>,
    #[serde(default)]
    pub post_disconnect: Vec<String>,
}

impl ConnectionHookBindings {
    pub fn phase_bindings(&self, phase: HookPhase) -> &[String] {
        match phase {
            HookPhase::PreConnect => &self.pre_connect,
            HookPhase::PostConnect => &self.post_connect,
            HookPhase::PreDisconnect => &self.pre_disconnect,
            HookPhase::PostDisconnect => &self.post_disconnect,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookContext {
    pub profile_id: Uuid,
    pub profile_name: String,
    pub db_kind: String,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
}

impl HookContext {
    pub fn from_profile(profile: &ConnectionProfile) -> Self {
        let (host, port, database) = profile_config_context(&profile.config);

        Self {
            profile_id: profile.id,
            profile_name: profile.name.clone(),
            db_kind: format!("{:?}", profile.kind()),
            host,
            port,
            database,
        }
    }
}

fn profile_config_context(config: &DbConfig) -> (Option<String>, Option<u16>, Option<String>) {
    match config {
        DbConfig::Postgres {
            host,
            port,
            database,
            ..
        } => (Some(host.clone()), Some(*port), Some(database.clone())),
        DbConfig::SQLite { path } => (None, None, Some(path.to_string_lossy().to_string())),
        DbConfig::MySQL {
            host,
            port,
            database,
            ..
        } => (Some(host.clone()), Some(*port), database.clone()),
        DbConfig::MongoDB {
            host,
            port,
            database,
            ..
        } => (Some(host.clone()), Some(*port), database.clone()),
        DbConfig::Redis {
            host,
            port,
            database,
            ..
        } => (
            Some(host.clone()),
            Some(*port),
            database.map(|db| db.to_string()),
        ),
        DbConfig::External { values, .. } => {
            let host = values.get("host").cloned();
            let port = values
                .get("port")
                .and_then(|value| value.parse::<u16>().ok());
            let database = values.get("database").cloned();
            (host, port, database)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookResult {
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub timed_out: bool,
}

impl HookResult {
    pub fn is_success(&self) -> bool {
        !self.timed_out && self.exit_code == Some(0)
    }
}

#[derive(Debug, Clone)]
pub struct HookExecution {
    pub hook: ConnectionHook,
    pub result: Result<HookResult, String>,
}

#[derive(Debug, Clone)]
pub enum HookPhaseOutcome {
    Success {
        executions: Vec<HookExecution>,
    },
    Aborted {
        executions: Vec<HookExecution>,
        error: String,
    },
    CompletedWithWarnings {
        executions: Vec<HookExecution>,
        warnings: Vec<String>,
    },
}

impl ConnectionHook {
    pub fn display_command(&self) -> String {
        if self.args.is_empty() {
            return self.command.clone();
        }

        format!("{} {}", self.command, self.args.join(" "))
    }

    pub fn execute(
        &self,
        context: &HookContext,
        cancel_token: &CancelToken,
        parent_cancel_token: Option<&CancelToken>,
    ) -> Result<HookResult, String> {
        let mut command = Command::new(&self.command);
        command.args(&self.args);
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());

        if let Some(cwd) = &self.cwd {
            command.current_dir(cwd);
        }

        if self.inherit_env {
            command.envs(std::env::vars());
        } else {
            command.env_clear();
        }

        command.envs(self.context_env(context));
        command.envs(self.env.iter());

        let mut child = command.spawn().map_err(|error| {
            format!("Failed to execute '{}': {}", self.display_command(), error)
        })?;

        let start = Instant::now();
        let timeout = self.timeout_ms.map(Duration::from_millis);
        let wait_interval = Duration::from_millis(50);

        loop {
            if cancel_token.is_cancelled()
                || parent_cancel_token.is_some_and(CancelToken::is_cancelled)
            {
                let _ = child.kill();
                let _ = child.wait();
                let (stdout, stderr) = collect_output(&mut child);

                return Err(format!(
                    "Hook '{}' cancelled\n{}{}",
                    self.display_command(),
                    stdout,
                    stderr
                ));
            }

            if timeout.is_some_and(|max| start.elapsed() > max) {
                let _ = child.kill();
                let _ = child.wait();
                let (stdout, stderr) = collect_output(&mut child);

                return Ok(HookResult {
                    exit_code: None,
                    stdout,
                    stderr,
                    timed_out: true,
                });
            }

            match child.try_wait() {
                Ok(Some(status)) => {
                    let (stdout, stderr) = collect_output(&mut child);

                    return Ok(HookResult {
                        exit_code: status.code(),
                        stdout,
                        stderr,
                        timed_out: false,
                    });
                }
                Ok(None) => {
                    thread::sleep(wait_interval);
                }
                Err(error) => {
                    return Err(format!(
                        "Failed to wait for hook '{}': {}",
                        self.display_command(),
                        error
                    ));
                }
            }
        }
    }

    pub fn failure_message(&self, phase: HookPhase, result: &Result<HookResult, String>) -> String {
        match result {
            Ok(output) if output.timed_out => {
                let timeout = self
                    .timeout_ms
                    .map(|value| format!("{}ms", value))
                    .unwrap_or_else(|| "timeout".to_string());

                format!(
                    "{} hook timed out after {}: {}",
                    phase.label(),
                    timeout,
                    self.display_command()
                )
            }
            Ok(output) => {
                let details = if !output.stderr.trim().is_empty() {
                    output.stderr.trim().to_string()
                } else if !output.stdout.trim().is_empty() {
                    output.stdout.trim().to_string()
                } else {
                    "no output".to_string()
                };

                format!(
                    "{} hook failed (exit code {:?}): {} ({})",
                    phase.label(),
                    output.exit_code,
                    self.display_command(),
                    details
                )
            }
            Err(error) => {
                format!(
                    "{} hook failed: {} ({})",
                    phase.label(),
                    self.display_command(),
                    error
                )
            }
        }
    }

    fn context_env(&self, context: &HookContext) -> HashMap<String, String> {
        let mut environment = HashMap::new();

        environment.insert(
            "DBFLUX_PROFILE_ID".to_string(),
            context.profile_id.to_string(),
        );
        environment.insert(
            "DBFLUX_PROFILE_NAME".to_string(),
            context.profile_name.clone(),
        );
        environment.insert("DBFLUX_DB_KIND".to_string(), context.db_kind.clone());

        if let Some(host) = &context.host {
            environment.insert("DBFLUX_HOST".to_string(), host.clone());
        }

        if let Some(port) = context.port {
            environment.insert("DBFLUX_PORT".to_string(), port.to_string());
        }

        if let Some(database) = &context.database {
            environment.insert("DBFLUX_DATABASE".to_string(), database.clone());
        }

        environment
    }
}

fn collect_output(child: &mut Child) -> (String, String) {
    let stdout = child
        .stdout
        .as_mut()
        .map(|stream| {
            let mut buffer = Vec::new();
            let _ = stream.read_to_end(&mut buffer);
            String::from_utf8_lossy(&buffer).to_string()
        })
        .unwrap_or_default();

    let stderr = child
        .stderr
        .as_mut()
        .map(|stream| {
            let mut buffer = Vec::new();
            let _ = stream.read_to_end(&mut buffer);
            String::from_utf8_lossy(&buffer).to_string()
        })
        .unwrap_or_default();

    (stdout, stderr)
}

pub struct HookRunner;

impl HookRunner {
    pub fn run_phase(
        phase: HookPhase,
        hooks: &[ConnectionHook],
        context: &HookContext,
        cancel_token: &CancelToken,
        parent_cancel_token: Option<&CancelToken>,
    ) -> HookPhaseOutcome {
        let mut warnings = Vec::new();
        let mut executions = Vec::new();

        for hook in hooks {
            if !hook.enabled {
                continue;
            }

            let result = hook.execute(context, cancel_token, parent_cancel_token);
            let succeeded = result.as_ref().is_ok_and(HookResult::is_success);

            executions.push(HookExecution {
                hook: hook.clone(),
                result: result.clone(),
            });

            if succeeded {
                continue;
            }

            let message = hook.failure_message(phase, &result);

            match hook.on_failure {
                HookFailureMode::Disconnect => {
                    return HookPhaseOutcome::Aborted {
                        executions,
                        error: message,
                    };
                }
                HookFailureMode::Warn => {
                    warnings.push(message);
                }
                HookFailureMode::Ignore => {
                    log::warn!("{}", message);
                }
            }
        }

        if warnings.is_empty() {
            HookPhaseOutcome::Success { executions }
        } else {
            HookPhaseOutcome::CompletedWithWarnings {
                executions,
                warnings,
            }
        }
    }
}
