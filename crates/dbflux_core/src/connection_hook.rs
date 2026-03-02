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

    /// Resolves hook bindings from a profile against a global definitions map.
    ///
    /// If the profile has `hook_bindings`, each binding ID is looked up in
    /// `definitions` and placed into the corresponding phase. Missing IDs are
    /// silently skipped (logged as warnings). If the profile has no bindings,
    /// falls back to `profile.hooks` (inline hooks) or an empty default.
    pub fn resolve_from_bindings(
        profile: &ConnectionProfile,
        definitions: &HashMap<String, ConnectionHook>,
    ) -> Self {
        if let Some(bindings) = &profile.hook_bindings {
            let mut hooks = Self::default();

            for phase in [
                HookPhase::PreConnect,
                HookPhase::PostConnect,
                HookPhase::PreDisconnect,
                HookPhase::PostDisconnect,
            ] {
                for hook_id in bindings.phase_bindings(phase) {
                    if let Some(hook) = definitions.get(hook_id) {
                        hooks.phase_hooks_mut(phase).push(hook.clone());
                    } else {
                        log::warn!(
                            "Profile '{}' references missing {} hook '{}'",
                            profile.name,
                            phase.label().to_ascii_lowercase(),
                            hook_id
                        );
                    }
                }
            }

            return hooks;
        }

        profile.hooks.clone().unwrap_or_default()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile::{ConnectionProfile, DbConfig};
    use crate::AppConfig;

    // =========================================================================
    // Helpers
    // =========================================================================

    fn test_context() -> HookContext {
        HookContext {
            profile_id: Uuid::nil(),
            profile_name: "test-profile".to_string(),
            db_kind: "Postgres".to_string(),
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: Some("mydb".to_string()),
        }
    }

    fn echo_hook(message: &str) -> ConnectionHook {
        ConnectionHook {
            enabled: true,
            command: "echo".to_string(),
            args: vec![message.to_string()],
            cwd: None,
            env: HashMap::new(),
            inherit_env: true,
            timeout_ms: None,
            on_failure: HookFailureMode::Disconnect,
        }
    }

    fn failing_hook() -> ConnectionHook {
        ConnectionHook {
            enabled: true,
            command: "false".to_string(),
            args: vec![],
            cwd: None,
            env: HashMap::new(),
            inherit_env: true,
            timeout_ms: None,
            on_failure: HookFailureMode::Disconnect,
        }
    }

    fn disabled_hook() -> ConnectionHook {
        ConnectionHook {
            enabled: false,
            ..echo_hook("disabled")
        }
    }

    // =========================================================================
    // Serde
    // =========================================================================

    #[test]
    fn hook_failure_mode_serializes_as_snake_case() {
        assert_eq!(
            serde_json::to_string(&HookFailureMode::Disconnect).unwrap(),
            "\"disconnect\""
        );
        assert_eq!(
            serde_json::to_string(&HookFailureMode::Warn).unwrap(),
            "\"warn\""
        );
        assert_eq!(
            serde_json::to_string(&HookFailureMode::Ignore).unwrap(),
            "\"ignore\""
        );
    }

    #[test]
    fn hook_phase_serializes_as_snake_case() {
        assert_eq!(
            serde_json::to_string(&HookPhase::PreConnect).unwrap(),
            "\"pre_connect\""
        );
        assert_eq!(
            serde_json::to_string(&HookPhase::PostConnect).unwrap(),
            "\"post_connect\""
        );
        assert_eq!(
            serde_json::to_string(&HookPhase::PreDisconnect).unwrap(),
            "\"pre_disconnect\""
        );
        assert_eq!(
            serde_json::to_string(&HookPhase::PostDisconnect).unwrap(),
            "\"post_disconnect\""
        );
    }

    #[test]
    fn connection_hook_serde_roundtrip() {
        let hook = ConnectionHook {
            enabled: true,
            command: "pg_isready".to_string(),
            args: vec!["-h".to_string(), "localhost".to_string()],
            cwd: Some(PathBuf::from("/tmp")),
            env: HashMap::from([("PG_COLOR".to_string(), "always".to_string())]),
            inherit_env: false,
            timeout_ms: Some(5000),
            on_failure: HookFailureMode::Warn,
        };

        let json = serde_json::to_string(&hook).unwrap();
        let deserialized: ConnectionHook = serde_json::from_str(&json).unwrap();

        assert_eq!(hook, deserialized);
    }

    #[test]
    fn connection_hook_defaults_on_minimal_json() {
        let hook: ConnectionHook = serde_json::from_str(r#"{"command": "echo"}"#).unwrap();

        assert!(hook.enabled);
        assert_eq!(hook.command, "echo");
        assert!(hook.args.is_empty());
        assert!(hook.cwd.is_none());
        assert!(hook.env.is_empty());
        assert!(hook.inherit_env);
        assert!(hook.timeout_ms.is_none());
        assert_eq!(hook.on_failure, HookFailureMode::Disconnect);
    }

    #[test]
    fn connection_hooks_defaults_all_phases_empty() {
        let hooks = ConnectionHooks::default();

        assert!(hooks.pre_connect.is_empty());
        assert!(hooks.post_connect.is_empty());
        assert!(hooks.pre_disconnect.is_empty());
        assert!(hooks.post_disconnect.is_empty());
    }

    #[test]
    fn connection_hooks_serde_roundtrip() {
        let hooks = ConnectionHooks {
            pre_connect: vec![echo_hook("pre")],
            post_connect: vec![echo_hook("post")],
            pre_disconnect: vec![],
            post_disconnect: vec![failing_hook()],
        };

        let json = serde_json::to_string(&hooks).unwrap();
        let deserialized: ConnectionHooks = serde_json::from_str(&json).unwrap();

        assert_eq!(hooks, deserialized);
    }

    #[test]
    fn connection_hook_bindings_serde_roundtrip() {
        let bindings = ConnectionHookBindings {
            pre_connect: vec!["setup-vpn".to_string()],
            post_connect: vec!["warm-cache".to_string(), "notify".to_string()],
            pre_disconnect: vec![],
            post_disconnect: vec!["cleanup".to_string()],
        };

        let json = serde_json::to_string(&bindings).unwrap();
        let deserialized: ConnectionHookBindings = serde_json::from_str(&json).unwrap();

        assert_eq!(bindings, deserialized);
    }

    #[test]
    fn connection_hook_bindings_defaults_on_empty_json() {
        let bindings: ConnectionHookBindings = serde_json::from_str("{}").unwrap();

        assert!(bindings.pre_connect.is_empty());
        assert!(bindings.post_connect.is_empty());
        assert!(bindings.pre_disconnect.is_empty());
        assert!(bindings.post_disconnect.is_empty());
    }

    // =========================================================================
    // Backward compatibility
    // =========================================================================

    #[test]
    fn profile_without_hooks_deserializes_cleanly() {
        let profile = ConnectionProfile::new("test", DbConfig::default_postgres());

        let json = serde_json::to_string(&profile).unwrap();
        let deserialized: ConnectionProfile = serde_json::from_str(&json).unwrap();

        assert!(deserialized.hooks.is_none());
        assert!(deserialized.hook_bindings.is_none());
    }

    #[test]
    fn profile_with_hooks_roundtrip() {
        let mut profile = ConnectionProfile::new("hooked", DbConfig::default_postgres());

        profile.hooks = Some(ConnectionHooks {
            pre_connect: vec![echo_hook("before")],
            ..Default::default()
        });

        profile.hook_bindings = Some(ConnectionHookBindings {
            post_connect: vec!["warm-cache".to_string()],
            ..Default::default()
        });

        let json = serde_json::to_string(&profile).unwrap();
        let deserialized: ConnectionProfile = serde_json::from_str(&json).unwrap();

        assert!(deserialized.hooks.is_some());
        assert_eq!(deserialized.hooks.unwrap().pre_connect.len(), 1);
        assert!(deserialized.hook_bindings.is_some());
        assert_eq!(deserialized.hook_bindings.unwrap().post_connect.len(), 1);
    }

    #[test]
    fn profile_hooks_none_omitted_from_json() {
        let profile = ConnectionProfile::new(
            "plain",
            DbConfig::SQLite {
                path: PathBuf::from("/tmp/test.db"),
            },
        );

        let json = serde_json::to_string(&profile).unwrap();

        assert!(!json.contains("\"hooks\""));
        assert!(!json.contains("hook_bindings"));
    }

    #[test]
    fn app_config_without_hook_definitions_deserializes() {
        let config: AppConfig = serde_json::from_str(r#"{"version": 1}"#).unwrap();

        assert!(config.hook_definitions.is_empty());
    }

    #[test]
    fn app_config_empty_hook_definitions_omitted_from_json() {
        let config = AppConfig::default();
        let json = serde_json::to_string(&config).unwrap();

        assert!(!json.contains("hook_definitions"));
    }

    // =========================================================================
    // HookContext from profile
    // =========================================================================

    #[test]
    fn hook_context_from_postgres_profile() {
        let profile = ConnectionProfile::new(
            "pg",
            DbConfig::Postgres {
                use_uri: false,
                uri: None,
                host: "db.example.com".to_string(),
                port: 5433,
                user: "admin".to_string(),
                database: "production".to_string(),
                ssl_mode: Default::default(),
                ssh_tunnel: None,
                ssh_tunnel_profile_id: None,
            },
        );

        let ctx = HookContext::from_profile(&profile);

        assert_eq!(ctx.host.as_deref(), Some("db.example.com"));
        assert_eq!(ctx.port, Some(5433));
        assert_eq!(ctx.database.as_deref(), Some("production"));
    }

    #[test]
    fn hook_context_from_sqlite_profile() {
        let profile = ConnectionProfile::new(
            "lite",
            DbConfig::SQLite {
                path: PathBuf::from("/data/app.db"),
            },
        );

        let ctx = HookContext::from_profile(&profile);

        assert!(ctx.host.is_none());
        assert!(ctx.port.is_none());
        assert_eq!(ctx.database.as_deref(), Some("/data/app.db"));
    }

    #[test]
    fn hook_context_from_external_profile() {
        let values = HashMap::from([
            ("host".to_string(), "ext-host".to_string()),
            ("port".to_string(), "9999".to_string()),
            ("database".to_string(), "ext-db".to_string()),
        ]);

        let profile = ConnectionProfile::new_with_kind(
            "external",
            crate::DbKind::Postgres,
            DbConfig::External {
                kind: crate::DbKind::Postgres,
                values,
            },
        );

        let ctx = HookContext::from_profile(&profile);

        assert_eq!(ctx.host.as_deref(), Some("ext-host"));
        assert_eq!(ctx.port, Some(9999));
        assert_eq!(ctx.database.as_deref(), Some("ext-db"));
    }

    #[test]
    fn hook_context_preserves_profile_id_and_name() {
        let profile = ConnectionProfile::new(
            "my-db",
            DbConfig::SQLite {
                path: PathBuf::from("/tmp/test.db"),
            },
        );

        let ctx = HookContext::from_profile(&profile);

        assert_eq!(ctx.profile_id, profile.id);
        assert_eq!(ctx.profile_name, "my-db");
    }

    // =========================================================================
    // HookResult
    // =========================================================================

    #[test]
    fn hook_result_success_when_exit_zero() {
        let result = HookResult {
            exit_code: Some(0),
            stdout: String::new(),
            stderr: String::new(),
            timed_out: false,
        };

        assert!(result.is_success());
    }

    #[test]
    fn hook_result_failure_on_nonzero_exit() {
        let result = HookResult {
            exit_code: Some(1),
            stdout: String::new(),
            stderr: String::new(),
            timed_out: false,
        };

        assert!(!result.is_success());
    }

    #[test]
    fn hook_result_failure_on_timeout() {
        let result = HookResult {
            exit_code: Some(0),
            stdout: String::new(),
            stderr: String::new(),
            timed_out: true,
        };

        assert!(!result.is_success());
    }

    #[test]
    fn hook_result_failure_on_none_exit_code() {
        let result = HookResult {
            exit_code: None,
            stdout: String::new(),
            stderr: String::new(),
            timed_out: false,
        };

        assert!(!result.is_success());
    }

    // =========================================================================
    // ConnectionHook::execute
    // =========================================================================

    #[test]
    fn execute_successful_command() {
        let hook = echo_hook("hello");
        let result = hook
            .execute(&test_context(), &CancelToken::new(), None)
            .unwrap();

        assert_eq!(result.exit_code, Some(0));
        assert!(result.stdout.contains("hello"));
        assert!(!result.timed_out);
    }

    #[test]
    fn execute_captures_stderr() {
        let hook = ConnectionHook {
            command: "sh".to_string(),
            args: vec!["-c".to_string(), "echo errmsg >&2".to_string()],
            ..echo_hook("")
        };

        let result = hook
            .execute(&test_context(), &CancelToken::new(), None)
            .unwrap();

        assert!(result.stderr.contains("errmsg"));
    }

    #[test]
    fn execute_nonzero_exit_code() {
        let hook = failing_hook();
        let result = hook
            .execute(&test_context(), &CancelToken::new(), None)
            .unwrap();

        assert!(!result.is_success());
        assert_ne!(result.exit_code, Some(0));
    }

    #[test]
    fn execute_invalid_command_returns_error() {
        let hook = ConnectionHook {
            command: "nonexistent_command_xyz_12345".to_string(),
            args: vec![],
            ..echo_hook("")
        };

        let result = hook.execute(&test_context(), &CancelToken::new(), None);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Failed to execute"));
    }

    #[test]
    fn execute_timeout_kills_process() {
        let hook = ConnectionHook {
            command: "sleep".to_string(),
            args: vec!["10".to_string()],
            timeout_ms: Some(100),
            ..echo_hook("")
        };

        let result = hook
            .execute(&test_context(), &CancelToken::new(), None)
            .unwrap();

        assert!(result.timed_out);
        assert!(!result.is_success());
    }

    #[test]
    fn execute_cancellation_returns_error() {
        let token = CancelToken::new();
        token.cancel();

        let hook = ConnectionHook {
            command: "sleep".to_string(),
            args: vec!["10".to_string()],
            ..echo_hook("")
        };

        let result = hook.execute(&test_context(), &token, None);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cancelled"));
    }

    #[test]
    fn execute_parent_cancellation_returns_error() {
        let token = CancelToken::new();
        let parent = CancelToken::new();
        parent.cancel();

        let hook = ConnectionHook {
            command: "sleep".to_string(),
            args: vec!["10".to_string()],
            ..echo_hook("")
        };

        let result = hook.execute(&test_context(), &token, Some(&parent));

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cancelled"));
    }

    #[test]
    fn execute_injects_context_env_vars() {
        let hook = ConnectionHook {
            command: "sh".to_string(),
            args: vec![
                "-c".to_string(),
                "echo $DBFLUX_PROFILE_NAME:$DBFLUX_HOST:$DBFLUX_PORT:$DBFLUX_DATABASE".to_string(),
            ],
            ..echo_hook("")
        };

        let result = hook
            .execute(&test_context(), &CancelToken::new(), None)
            .unwrap();

        assert!(result.stdout.contains("test-profile:localhost:5432:mydb"));
    }

    #[test]
    fn execute_custom_env_overrides_context() {
        let hook = ConnectionHook {
            command: "sh".to_string(),
            args: vec!["-c".to_string(), "echo $DBFLUX_HOST".to_string()],
            env: HashMap::from([("DBFLUX_HOST".to_string(), "override-host".to_string())]),
            ..echo_hook("")
        };

        let result = hook
            .execute(&test_context(), &CancelToken::new(), None)
            .unwrap();

        assert!(result.stdout.contains("override-host"));
    }

    #[test]
    fn execute_inherit_env_false_clears_environment() {
        let hook = ConnectionHook {
            command: "sh".to_string(),
            args: vec!["-c".to_string(), "echo ${HOME:-empty}".to_string()],
            inherit_env: false,
            ..echo_hook("")
        };

        let result = hook
            .execute(&test_context(), &CancelToken::new(), None)
            .unwrap();

        assert_eq!(result.stdout.trim(), "empty");
    }

    #[test]
    fn execute_respects_cwd() {
        let hook = ConnectionHook {
            command: "pwd".to_string(),
            args: vec![],
            cwd: Some(PathBuf::from("/tmp")),
            ..echo_hook("")
        };

        let result = hook
            .execute(&test_context(), &CancelToken::new(), None)
            .unwrap();

        let output = result.stdout.trim();
        assert!(
            output == "/tmp" || output.ends_with("/tmp"),
            "expected /tmp, got: {}",
            output
        );
    }

    // =========================================================================
    // ConnectionHooks::phase_hooks
    // =========================================================================

    #[test]
    fn phase_hooks_returns_correct_phase() {
        let hooks = ConnectionHooks {
            pre_connect: vec![echo_hook("pre")],
            post_connect: vec![echo_hook("post1"), echo_hook("post2")],
            pre_disconnect: vec![],
            post_disconnect: vec![failing_hook()],
        };

        assert_eq!(hooks.phase_hooks(HookPhase::PreConnect).len(), 1);
        assert_eq!(hooks.phase_hooks(HookPhase::PostConnect).len(), 2);
        assert_eq!(hooks.phase_hooks(HookPhase::PreDisconnect).len(), 0);
        assert_eq!(hooks.phase_hooks(HookPhase::PostDisconnect).len(), 1);
    }

    #[test]
    fn phase_hooks_mut_allows_modification() {
        let mut hooks = ConnectionHooks::default();

        hooks
            .phase_hooks_mut(HookPhase::PostConnect)
            .push(echo_hook("added"));

        assert_eq!(hooks.post_connect.len(), 1);
        assert_eq!(hooks.post_connect[0].args, vec!["added"]);
    }

    // =========================================================================
    // ConnectionHookBindings::phase_bindings
    // =========================================================================

    #[test]
    fn phase_bindings_returns_correct_phase() {
        let bindings = ConnectionHookBindings {
            pre_connect: vec!["a".to_string()],
            post_connect: vec!["b".to_string(), "c".to_string()],
            pre_disconnect: vec![],
            post_disconnect: vec!["d".to_string()],
        };

        assert_eq!(bindings.phase_bindings(HookPhase::PreConnect), &["a"]);
        assert_eq!(bindings.phase_bindings(HookPhase::PostConnect), &["b", "c"]);
        assert!(bindings.phase_bindings(HookPhase::PreDisconnect).is_empty());
        assert_eq!(bindings.phase_bindings(HookPhase::PostDisconnect), &["d"]);
    }

    // =========================================================================
    // HookRunner::run_phase
    // =========================================================================

    #[test]
    fn run_phase_empty_hooks_returns_success() {
        let outcome = HookRunner::run_phase(
            HookPhase::PreConnect,
            &[],
            &test_context(),
            &CancelToken::new(),
            None,
        );

        assert!(
            matches!(outcome, HookPhaseOutcome::Success { executions } if executions.is_empty())
        );
    }

    #[test]
    fn run_phase_single_success() {
        let hooks = [echo_hook("ok")];

        let outcome = HookRunner::run_phase(
            HookPhase::PreConnect,
            &hooks,
            &test_context(),
            &CancelToken::new(),
            None,
        );

        assert!(
            matches!(outcome, HookPhaseOutcome::Success { executions } if executions.len() == 1)
        );
    }

    #[test]
    fn run_phase_multiple_all_succeed() {
        let hooks = [echo_hook("a"), echo_hook("b"), echo_hook("c")];

        let outcome = HookRunner::run_phase(
            HookPhase::PreConnect,
            &hooks,
            &test_context(),
            &CancelToken::new(),
            None,
        );

        assert!(
            matches!(outcome, HookPhaseOutcome::Success { executions } if executions.len() == 3)
        );
    }

    #[test]
    fn run_phase_skips_disabled_hooks() {
        let hooks = [echo_hook("a"), disabled_hook(), echo_hook("c")];

        let outcome = HookRunner::run_phase(
            HookPhase::PreConnect,
            &hooks,
            &test_context(),
            &CancelToken::new(),
            None,
        );

        match outcome {
            HookPhaseOutcome::Success { executions } => {
                assert_eq!(executions.len(), 2);
            }
            other => panic!("expected Success, got {:?}", other),
        }
    }

    #[test]
    fn run_phase_disconnect_failure_aborts() {
        let mut hook = failing_hook();
        hook.on_failure = HookFailureMode::Disconnect;

        let hooks = [hook];

        let outcome = HookRunner::run_phase(
            HookPhase::PreConnect,
            &hooks,
            &test_context(),
            &CancelToken::new(),
            None,
        );

        assert!(matches!(outcome, HookPhaseOutcome::Aborted { .. }));
    }

    #[test]
    fn run_phase_warn_failure_continues() {
        let mut warn_hook = failing_hook();
        warn_hook.on_failure = HookFailureMode::Warn;

        let hooks = [warn_hook, echo_hook("after")];

        let outcome = HookRunner::run_phase(
            HookPhase::PreConnect,
            &hooks,
            &test_context(),
            &CancelToken::new(),
            None,
        );

        match outcome {
            HookPhaseOutcome::CompletedWithWarnings {
                executions,
                warnings,
            } => {
                assert_eq!(executions.len(), 2);
                assert_eq!(warnings.len(), 1);
            }
            other => panic!("expected CompletedWithWarnings, got {:?}", other),
        }
    }

    #[test]
    fn run_phase_ignore_failure_continues_silently() {
        let mut ignore_hook = failing_hook();
        ignore_hook.on_failure = HookFailureMode::Ignore;

        let hooks = [ignore_hook, echo_hook("after")];

        let outcome = HookRunner::run_phase(
            HookPhase::PreConnect,
            &hooks,
            &test_context(),
            &CancelToken::new(),
            None,
        );

        match outcome {
            HookPhaseOutcome::Success { executions } => {
                assert_eq!(executions.len(), 2);
            }
            other => panic!(
                "expected Success (ignore swallows warnings), got {:?}",
                other
            ),
        }
    }

    #[test]
    fn run_phase_abort_stops_remaining_hooks() {
        let mut abort_hook = failing_hook();
        abort_hook.on_failure = HookFailureMode::Disconnect;

        let hooks = [echo_hook("first"), abort_hook, echo_hook("never")];

        let outcome = HookRunner::run_phase(
            HookPhase::PreConnect,
            &hooks,
            &test_context(),
            &CancelToken::new(),
            None,
        );

        match outcome {
            HookPhaseOutcome::Aborted { executions, .. } => {
                assert_eq!(
                    executions.len(),
                    2,
                    "only first + aborting hook should execute"
                );
            }
            other => panic!("expected Aborted, got {:?}", other),
        }
    }

    #[test]
    fn run_phase_mixed_failure_modes() {
        let mut warn_hook = failing_hook();
        warn_hook.on_failure = HookFailureMode::Warn;

        let mut abort_hook = failing_hook();
        abort_hook.on_failure = HookFailureMode::Disconnect;

        let hooks = [warn_hook, abort_hook, echo_hook("never")];

        let outcome = HookRunner::run_phase(
            HookPhase::PreConnect,
            &hooks,
            &test_context(),
            &CancelToken::new(),
            None,
        );

        assert!(matches!(outcome, HookPhaseOutcome::Aborted { .. }));
    }

    #[test]
    fn run_phase_cancelled_token_aborts_immediately() {
        let token = CancelToken::new();
        token.cancel();

        let hooks = [ConnectionHook {
            command: "sleep".to_string(),
            args: vec!["10".to_string()],
            ..echo_hook("")
        }];

        let outcome =
            HookRunner::run_phase(HookPhase::PreConnect, &hooks, &test_context(), &token, None);

        match outcome {
            HookPhaseOutcome::Aborted { executions, .. } => {
                assert_eq!(executions.len(), 1);
                assert!(executions[0].result.is_err());
            }
            other => panic!("expected Aborted on cancellation, got {:?}", other),
        }
    }

    // =========================================================================
    // failure_message
    // =========================================================================

    #[test]
    fn failure_message_on_timeout() {
        let hook = ConnectionHook {
            timeout_ms: Some(3000),
            ..echo_hook("slow")
        };

        let result = Ok(HookResult {
            exit_code: None,
            stdout: String::new(),
            stderr: String::new(),
            timed_out: true,
        });

        let message = hook.failure_message(HookPhase::PreConnect, &result);

        assert!(message.contains("timed out"));
        assert!(message.contains("3000ms"));
        assert!(message.contains("Pre-connect"));
    }

    #[test]
    fn failure_message_on_nonzero_exit() {
        let hook = echo_hook("fail");

        let result = Ok(HookResult {
            exit_code: Some(1),
            stdout: String::new(),
            stderr: "something went wrong".to_string(),
            timed_out: false,
        });

        let message = hook.failure_message(HookPhase::PostConnect, &result);

        assert!(message.contains("exit code"));
        assert!(message.contains("something went wrong"));
        assert!(message.contains("Post-connect"));
    }

    #[test]
    fn failure_message_on_execution_error() {
        let hook = echo_hook("broken");
        let result: Result<HookResult, String> = Err("spawn failed".to_string());

        let message = hook.failure_message(HookPhase::PreDisconnect, &result);

        assert!(message.contains("spawn failed"));
        assert!(message.contains("Pre-disconnect"));
    }

    // =========================================================================
    // display_command
    // =========================================================================

    #[test]
    fn display_command_no_args() {
        let hook = ConnectionHook {
            args: vec![],
            ..echo_hook("")
        };

        assert_eq!(hook.display_command(), "echo");
    }

    #[test]
    fn display_command_with_args() {
        let hook = echo_hook("hello world");

        assert_eq!(hook.display_command(), "echo hello world");
    }
}
