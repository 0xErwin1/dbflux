use crate::connection::profile::{ConnectionProfile, DbConfig};
use crate::core::task::CancelToken;
use serde::de::{self, DeserializeOwned, Deserializer};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptLanguage {
    Bash,
    Python,
}

impl ScriptLanguage {
    pub fn extension(&self) -> &'static str {
        match self {
            Self::Bash => "sh",
            Self::Python => "py",
        }
    }

    pub fn default_interpreter(&self) -> Option<&'static str> {
        match self {
            Self::Bash => {
                if cfg!(target_os = "windows") {
                    None
                } else {
                    Some("bash")
                }
            }
            Self::Python => {
                if cfg!(target_os = "windows") {
                    Some("python")
                } else {
                    Some("python3")
                }
            }
        }
    }

    pub fn supported_on_current_platform(&self) -> bool {
        self.default_interpreter().is_some()
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Bash => "Bash",
            Self::Python => "Python",
        }
    }

    pub fn available() -> Vec<Self> {
        [Self::Bash, Self::Python]
            .into_iter()
            .filter(|language| language.supported_on_current_platform())
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum ScriptSource {
    Inline { content: String },
    File { path: PathBuf },
}

impl ScriptSource {
    fn summary_label(&self) -> &'static str {
        match self {
            Self::Inline { .. } => "inline",
            Self::File { .. } => "file",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LuaCapabilities {
    #[serde(default = "default_true")]
    pub logging: bool,
    #[serde(default = "default_true")]
    pub env_read: bool,
    #[serde(default = "default_true")]
    pub connection_metadata: bool,
    #[serde(default)]
    pub process_run: bool,
}

impl Default for LuaCapabilities {
    fn default() -> Self {
        Self {
            logging: true,
            env_read: true,
            connection_metadata: true,
            process_run: false,
        }
    }
}

impl LuaCapabilities {
    pub fn all_enabled() -> Self {
        Self {
            logging: true,
            env_read: true,
            connection_metadata: true,
            process_run: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum HookKind {
    Command {
        command: String,
        #[serde(default)]
        args: Vec<String>,
    },
    Script {
        language: ScriptLanguage,
        source: ScriptSource,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        interpreter: Option<String>,
    },
    Lua {
        source: ScriptSource,
        #[serde(default)]
        capabilities: LuaCapabilities,
    },
}

impl HookKind {
    pub fn resolve_interpreter(&self) -> Result<String, String> {
        match self {
            Self::Command { command, .. } => Ok(command.clone()),
            Self::Script {
                language,
                interpreter,
                ..
            } => {
                interpreter
                    .clone()
                    .or_else(|| {
                        language
                            .default_interpreter()
                            .map(std::string::ToString::to_string)
                    })
                    .ok_or_else(|| {
                        match language {
                    ScriptLanguage::Bash => {
                        "Bash is not supported on Windows. Set an explicit interpreter override."
                            .to_string()
                    }
                    _ => format!(
                    "{} is not supported on this platform. Set an explicit interpreter override.",
                    language.label()
                ),
                }
                    })
            }
            Self::Lua { .. } => {
                Err("Lua hooks run in-process and do not use an interpreter".into())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConnectionHook {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(flatten)]
    pub kind: HookKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub env: HashMap<String, String>,
    #[serde(default = "default_inherit_env")]
    pub inherit_env: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub on_failure: HookFailureMode,
}

impl<'de> Deserialize<'de> for ConnectionHook {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut object = match Value::deserialize(deserializer)? {
            Value::Object(object) => object,
            other => {
                return Err(de::Error::custom(format!(
                    "expected hook object, got {other:?}"
                )));
            }
        };

        let enabled = take_field(&mut object, "enabled")
            .map_err(de::Error::custom)?
            .unwrap_or_else(default_enabled);
        let cwd = take_field(&mut object, "cwd").map_err(de::Error::custom)?;
        let env = take_field(&mut object, "env")
            .map_err(de::Error::custom)?
            .unwrap_or_default();
        let inherit_env = take_field(&mut object, "inherit_env")
            .map_err(de::Error::custom)?
            .unwrap_or_else(default_inherit_env);
        let timeout_ms = take_field(&mut object, "timeout_ms").map_err(de::Error::custom)?;
        let on_failure = take_field(&mut object, "on_failure")
            .map_err(de::Error::custom)?
            .unwrap_or_default();

        let kind = if object.contains_key("kind") {
            serde_json::from_value(Value::Object(object)).map_err(de::Error::custom)?
        } else if object.contains_key("command") {
            let command = take_required_field(&mut object, "command").map_err(de::Error::custom)?;
            let args = take_field(&mut object, "args")
                .map_err(de::Error::custom)?
                .unwrap_or_default();

            if let Some(unexpected) = object.keys().next().cloned() {
                return Err(de::Error::custom(format!(
                    "unexpected field '{unexpected}' in legacy hook definition"
                )));
            }

            HookKind::Command { command, args }
        } else {
            return Err(de::Error::custom(
                "hook definition must include either 'kind' or legacy 'command'",
            ));
        };

        Ok(Self {
            enabled,
            kind,
            cwd,
            env,
            inherit_env,
            timeout_ms,
            on_failure,
        })
    }
}

fn default_enabled() -> bool {
    true
}

fn default_inherit_env() -> bool {
    true
}

fn default_true() -> bool {
    true
}

fn take_field<T>(object: &mut Map<String, Value>, key: &str) -> Result<Option<T>, String>
where
    T: DeserializeOwned,
{
    object
        .remove(key)
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| format!("invalid '{key}' field: {error}"))
}

fn take_required_field<T>(object: &mut Map<String, Value>, key: &str) -> Result<T, String>
where
    T: DeserializeOwned,
{
    take_field(object, key)?.ok_or_else(|| format!("missing required '{key}' field"))
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
    pub phase: Option<HookPhase>,
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
            phase: None,
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
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OutputStreamKind {
    Stdout,
    Stderr,
    Log,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputEvent {
    pub stream: OutputStreamKind,
    pub text: String,
}

impl OutputEvent {
    pub fn new(stream: OutputStreamKind, text: impl Into<String>) -> Self {
        Self {
            stream,
            text: text.into(),
        }
    }
}

pub type OutputSender = mpsc::Sender<OutputEvent>;
pub type OutputReceiver = mpsc::Receiver<OutputEvent>;

pub fn output_channel() -> (OutputSender, OutputReceiver) {
    mpsc::channel()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessExecutionError {
    Spawn(String),
    Wait(String),
    Cancelled { stdout: String, stderr: String },
    TimedOut { stdout: String, stderr: String },
}

struct ResolvedExecution {
    program: String,
    args: Vec<String>,
    _temp_file: Option<NamedTempFile>,
}

const MAX_OUTPUT_BYTES: usize = 4 * 1024 * 1024;
const OUTPUT_TRUNCATED_NOTICE: &str = "\n[output truncated]\n";

#[derive(Default)]
struct OutputCollector {
    stdout: String,
    stderr: String,
    total_bytes: usize,
    truncated: bool,
}

impl OutputCollector {
    fn push(&mut self, event: OutputEvent, output: Option<&OutputSender>) {
        if self.truncated {
            return;
        }

        let mut text = event.text;

        if text.is_empty() {
            return;
        }

        let remaining = MAX_OUTPUT_BYTES.saturating_sub(self.total_bytes);

        if text.len() > remaining {
            self.truncated = true;

            if remaining == 0 {
                return;
            }

            if remaining > OUTPUT_TRUNCATED_NOTICE.len() {
                let prefix = safe_prefix_by_bytes(&text, remaining - OUTPUT_TRUNCATED_NOTICE.len());
                text = format!("{prefix}{OUTPUT_TRUNCATED_NOTICE}");
            } else {
                text = safe_prefix_by_bytes(OUTPUT_TRUNCATED_NOTICE, remaining);
            }
        }

        if text.is_empty() {
            return;
        }

        if let Some(sender) = output {
            let _ = sender.send(OutputEvent::new(event.stream, text.clone()));
        }

        match event.stream {
            OutputStreamKind::Stdout | OutputStreamKind::Log => self.stdout.push_str(&text),
            OutputStreamKind::Stderr => self.stderr.push_str(&text),
        }

        self.total_bytes += text.len();
    }

    fn into_hook_result(self, exit_code: Option<i32>, timed_out: bool) -> HookResult {
        HookResult {
            exit_code,
            stdout: self.stdout,
            stderr: self.stderr,
            timed_out,
            warnings: Vec::new(),
        }
    }
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

pub trait HookExecutor: Send + Sync {
    fn execute_hook(
        &self,
        hook: &ConnectionHook,
        context: &HookContext,
        cancel_token: &CancelToken,
        parent_cancel_token: Option<&CancelToken>,
        output: Option<&OutputSender>,
    ) -> Result<HookResult, String>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessExecutor;

impl HookExecutor for ProcessExecutor {
    fn execute_hook(
        &self,
        hook: &ConnectionHook,
        context: &HookContext,
        cancel_token: &CancelToken,
        parent_cancel_token: Option<&CancelToken>,
        output: Option<&OutputSender>,
    ) -> Result<HookResult, String> {
        match &hook.kind {
            HookKind::Command { .. } | HookKind::Script { .. } => {
                hook.execute_process(context, cancel_token, parent_cancel_token, output)
            }
            HookKind::Lua { .. } => {
                Err("Lua hooks require the 'lua' feature to be enabled".to_string())
            }
        }
    }
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
        match &self.kind {
            HookKind::Command { command, args } => {
                if args.is_empty() {
                    command.clone()
                } else {
                    format!("{} {}", command, args.join(" "))
                }
            }
            HookKind::Script {
                source,
                interpreter,
                language,
            } => {
                let program = interpreter.clone().or_else(|| {
                    language
                        .default_interpreter()
                        .map(std::string::ToString::to_string)
                });

                match program {
                    Some(program) => match source {
                        ScriptSource::Inline { .. } => format!("{} <inline script>", program),
                        ScriptSource::File { path } => {
                            format!("{} {}", program, path.display())
                        }
                    },
                    None => "Unsupported on this platform".to_string(),
                }
            }
            HookKind::Lua { source, .. } => match source {
                ScriptSource::Inline { .. } => "lua <inline script>".to_string(),
                ScriptSource::File { path } => format!("lua {}", path.display()),
            },
        }
    }

    pub fn is_script(&self) -> bool {
        matches!(self.kind, HookKind::Script { .. })
    }

    pub fn is_command(&self) -> bool {
        matches!(self.kind, HookKind::Command { .. })
    }

    pub fn summary(&self) -> String {
        match &self.kind {
            HookKind::Command { .. } => self.display_command(),
            HookKind::Script {
                language, source, ..
            } => {
                format!("{} · {}", language.label(), source.summary_label())
            }
            HookKind::Lua { source, .. } => format!("Lua · {}", source.summary_label()),
        }
    }

    fn resolve_execution(&self) -> Result<ResolvedExecution, String> {
        match &self.kind {
            HookKind::Command { command, args } => Ok(ResolvedExecution {
                program: command.clone(),
                args: args.clone(),
                _temp_file: None,
            }),
            HookKind::Script { source, .. } => {
                let program = self.kind.resolve_interpreter()?;

                match source {
                    ScriptSource::File { path } => Ok(ResolvedExecution {
                        program,
                        args: vec![path.to_string_lossy().into_owned()],
                        _temp_file: None,
                    }),
                    ScriptSource::Inline { content } => {
                        let mut temp_file = tempfile::Builder::new()
                            .suffix(&format!(
                                ".{}",
                                match &self.kind {
                                    HookKind::Script { language, .. } => language.extension(),
                                    HookKind::Command { .. } => unreachable!(),
                                    HookKind::Lua { .. } => unreachable!(),
                                }
                            ))
                            .tempfile()
                            .map_err(|error| {
                                format!("Failed to create temp file for hook script: {error}")
                            })?;

                        temp_file.write_all(content.as_bytes()).map_err(|error| {
                            format!("Failed to write temp file for hook script: {error}")
                        })?;

                        let temp_path = temp_file.path().to_string_lossy().into_owned();

                        Ok(ResolvedExecution {
                            program,
                            args: vec![temp_path],
                            _temp_file: Some(temp_file),
                        })
                    }
                }
            }
            HookKind::Lua { .. } => {
                Err("Lua hooks run in-process and cannot be executed as child processes".into())
            }
        }
    }

    pub fn execute(
        &self,
        context: &HookContext,
        cancel_token: &CancelToken,
        parent_cancel_token: Option<&CancelToken>,
    ) -> Result<HookResult, String> {
        ProcessExecutor.execute_hook(self, context, cancel_token, parent_cancel_token, None)
    }

    pub fn execute_with_output(
        &self,
        context: &HookContext,
        cancel_token: &CancelToken,
        parent_cancel_token: Option<&CancelToken>,
        output: Option<&OutputSender>,
    ) -> Result<HookResult, String> {
        ProcessExecutor.execute_hook(self, context, cancel_token, parent_cancel_token, output)
    }

    pub(crate) fn execute_process(
        &self,
        context: &HookContext,
        cancel_token: &CancelToken,
        parent_cancel_token: Option<&CancelToken>,
        output: Option<&OutputSender>,
    ) -> Result<HookResult, String> {
        let resolved = self.resolve_execution()?;

        let mut command = Command::new(&resolved.program);
        command.args(&resolved.args);
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

        let _temp_file = resolved._temp_file;

        match execute_streaming_process(
            &mut child,
            cancel_token,
            parent_cancel_token,
            self.timeout_ms.map(Duration::from_millis),
            None,
            output,
        ) {
            Ok(result) => Ok(result),
            Err(ProcessExecutionError::Spawn(error)) => Err(format!(
                "Failed to execute '{}': {}",
                self.display_command(),
                error
            )),
            Err(ProcessExecutionError::Wait(error)) => Err(format!(
                "Failed to wait for hook '{}': {}",
                self.display_command(),
                error
            )),
            Err(ProcessExecutionError::Cancelled { stdout, stderr }) => Err(format!(
                "Hook '{}' cancelled\n{}{}",
                self.display_command(),
                stdout,
                stderr
            )),
            Err(ProcessExecutionError::TimedOut { stdout, stderr }) => Err(format!(
                "Hook '{}' cancelled\n{}{}",
                self.display_command(),
                stdout,
                stderr
            )),
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

pub fn execute_streaming_process(
    child: &mut Child,
    cancel_token: &CancelToken,
    parent_cancel_token: Option<&CancelToken>,
    timeout: Option<Duration>,
    abort_timeout: Option<Duration>,
    output: Option<&OutputSender>,
) -> Result<HookResult, ProcessExecutionError> {
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| ProcessExecutionError::Wait("missing stdout pipe".to_string()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| ProcessExecutionError::Wait("missing stderr pipe".to_string()))?;

    let (chunk_sender, chunk_receiver) = mpsc::channel();
    let stdout_reader = spawn_output_reader(stdout, OutputStreamKind::Stdout, chunk_sender.clone());
    let stderr_reader = spawn_output_reader(stderr, OutputStreamKind::Stderr, chunk_sender.clone());
    drop(chunk_sender);

    let start = Instant::now();
    let wait_interval = Duration::from_millis(50);
    let mut collector = OutputCollector::default();

    let outcome = loop {
        match chunk_receiver.recv_timeout(wait_interval) {
            Ok(event) => {
                collector.push(event, output);
                drain_output_events(&chunk_receiver, &mut collector, output);
            }
            Err(RecvTimeoutError::Timeout) | Err(RecvTimeoutError::Disconnected) => {}
        }

        if cancel_token.is_cancelled() || parent_cancel_token.is_some_and(CancelToken::is_cancelled)
        {
            terminate_child(child)?;
            break ProcessMonitorOutcome::Cancelled;
        }

        if abort_timeout.is_some_and(|limit| start.elapsed() > limit) {
            terminate_child(child)?;
            break ProcessMonitorOutcome::AbortTimedOut;
        }

        if timeout.is_some_and(|limit| start.elapsed() > limit) {
            terminate_child(child)?;
            break ProcessMonitorOutcome::TimedOut;
        }

        match child.try_wait() {
            Ok(Some(status)) => break ProcessMonitorOutcome::Exited(status.code()),
            Ok(None) => {}
            Err(error) => {
                terminate_child(child)?;
                break ProcessMonitorOutcome::WaitFailed(error.to_string());
            }
        }
    };

    join_output_reader(stdout_reader);
    join_output_reader(stderr_reader);
    drain_output_events(&chunk_receiver, &mut collector, output);

    match outcome {
        ProcessMonitorOutcome::Exited(exit_code) => {
            Ok(collector.into_hook_result(exit_code, false))
        }
        ProcessMonitorOutcome::TimedOut => Ok(collector.into_hook_result(None, true)),
        ProcessMonitorOutcome::Cancelled => Err(ProcessExecutionError::Cancelled {
            stdout: collector.stdout,
            stderr: collector.stderr,
        }),
        ProcessMonitorOutcome::AbortTimedOut => Err(ProcessExecutionError::TimedOut {
            stdout: collector.stdout,
            stderr: collector.stderr,
        }),
        ProcessMonitorOutcome::WaitFailed(error) => Err(ProcessExecutionError::Wait(error)),
    }
}

enum ProcessMonitorOutcome {
    Exited(Option<i32>),
    TimedOut,
    AbortTimedOut,
    Cancelled,
    WaitFailed(String),
}

fn spawn_output_reader<R>(
    reader: R,
    stream: OutputStreamKind,
    sender: mpsc::Sender<OutputEvent>,
) -> thread::JoinHandle<()>
where
    R: std::io::Read + Send + 'static,
{
    thread::spawn(move || {
        let mut reader = BufReader::new(reader);

        loop {
            let mut line = String::new();

            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {
                    if sender.send(OutputEvent::new(stream, line)).is_err() {
                        break;
                    }
                }
                Err(error) => {
                    log::warn!("Failed to read {:?} output: {}", stream, error);
                    break;
                }
            }
        }
    })
}

fn drain_output_events(
    receiver: &mpsc::Receiver<OutputEvent>,
    collector: &mut OutputCollector,
    output: Option<&OutputSender>,
) {
    while let Ok(event) = receiver.try_recv() {
        collector.push(event, output);
    }
}

fn terminate_child(child: &mut Child) -> Result<(), ProcessExecutionError> {
    let _ = child.kill();
    child
        .wait()
        .map(|_| ())
        .map_err(|error| ProcessExecutionError::Wait(error.to_string()))
}

fn join_output_reader(handle: thread::JoinHandle<()>) {
    if handle.join().is_err() {
        log::warn!("process output reader thread panicked");
    }
}

fn safe_prefix_by_bytes(input: &str, max_bytes: usize) -> String {
    if input.len() <= max_bytes {
        return input.to_string();
    }

    let mut safe_end = 0;

    for (index, ch) in input.char_indices() {
        let next = index + ch.len_utf8();

        if next > max_bytes {
            break;
        }

        safe_end = next;
    }

    input[..safe_end].to_string()
}

pub struct HookRunner;

impl HookRunner {
    pub fn run_phase(
        phase: HookPhase,
        hooks: &[ConnectionHook],
        context: &HookContext,
        cancel_token: &CancelToken,
        parent_cancel_token: Option<&CancelToken>,
        executor: &dyn HookExecutor,
    ) -> HookPhaseOutcome {
        let mut warnings = Vec::new();
        let mut executions = Vec::new();

        for hook in hooks {
            if !hook.enabled {
                continue;
            }

            let result =
                executor.execute_hook(hook, context, cancel_token, parent_cancel_token, None);

            if let Ok(output) = &result {
                warnings.extend(output.warnings.iter().cloned());
            }

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
    use crate::AppConfig;
    use crate::connection::profile::{ConnectionProfile, DbConfig};

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
            phase: None,
        }
    }

    fn echo_hook(message: &str) -> ConnectionHook {
        ConnectionHook {
            enabled: true,
            kind: HookKind::Command {
                command: "echo".to_string(),
                args: vec![message.to_string()],
            },
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
            kind: HookKind::Command {
                command: "false".to_string(),
                args: vec![],
            },
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
            kind: HookKind::Command {
                command: "pg_isready".to_string(),
                args: vec!["-h".to_string(), "localhost".to_string()],
            },
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
        assert_eq!(
            hook.kind,
            HookKind::Command {
                command: "echo".to_string(),
                args: vec![],
            }
        );
        assert!(hook.cwd.is_none());
        assert!(hook.env.is_empty());
        assert!(hook.inherit_env);
        assert!(hook.timeout_ms.is_none());
        assert_eq!(hook.on_failure, HookFailureMode::Disconnect);
    }

    #[test]
    fn connection_hook_new_command_kind_roundtrip() {
        let hook: ConnectionHook = serde_json::from_str(
            r#"{
                "kind": "command",
                "command": "echo",
                "args": ["hello"]
            }"#,
        )
        .unwrap();

        assert_eq!(hook.display_command(), "echo hello");
        assert!(hook.is_command());
        assert!(!hook.is_script());
    }

    #[test]
    fn connection_hook_new_script_kind_roundtrip() {
        let hook: ConnectionHook = serde_json::from_str(
            r#"{
                "kind": "script",
                "language": "python",
                "source": {
                    "type": "inline",
                    "content": "print('hello')"
                }
            }"#,
        )
        .unwrap();

        assert_eq!(hook.summary(), "Python · inline");
        assert!(hook.is_script());
        assert!(!hook.is_command());
    }

    #[test]
    fn connection_hook_new_lua_kind_roundtrip() {
        let hook: ConnectionHook = serde_json::from_str(
            r#"{
                "kind": "lua",
                "source": {
                    "type": "inline",
                    "content": "hook.ok()"
                }
            }"#,
        )
        .unwrap();

        assert_eq!(hook.summary(), "Lua · inline");
        assert_eq!(hook.display_command(), "lua <inline script>");

        let HookKind::Lua { capabilities, .. } = hook.kind else {
            panic!("expected lua hook kind");
        };

        assert!(capabilities.logging);
        assert!(capabilities.env_read);
        assert!(capabilities.connection_metadata);
        assert!(!capabilities.process_run);
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
        assert_eq!(ctx.phase, None);
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
            warnings: Vec::new(),
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
            warnings: Vec::new(),
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
            warnings: Vec::new(),
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
            warnings: Vec::new(),
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
            kind: HookKind::Command {
                command: "sh".to_string(),
                args: vec!["-c".to_string(), "echo errmsg >&2".to_string()],
            },
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
            kind: HookKind::Command {
                command: "nonexistent_command_xyz_12345".to_string(),
                args: vec![],
            },
            ..echo_hook("")
        };

        let result = hook.execute(&test_context(), &CancelToken::new(), None);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Failed to execute"));
    }

    #[test]
    fn execute_timeout_kills_process() {
        let hook = ConnectionHook {
            kind: HookKind::Command {
                command: "sleep".to_string(),
                args: vec!["10".to_string()],
            },
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
            kind: HookKind::Command {
                command: "sleep".to_string(),
                args: vec!["10".to_string()],
            },
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
            kind: HookKind::Command {
                command: "sleep".to_string(),
                args: vec!["10".to_string()],
            },
            ..echo_hook("")
        };

        let result = hook.execute(&test_context(), &token, Some(&parent));

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cancelled"));
    }

    #[test]
    fn execute_injects_context_env_vars() {
        let hook = ConnectionHook {
            kind: HookKind::Command {
                command: "sh".to_string(),
                args: vec![
                    "-c".to_string(),
                    "echo $DBFLUX_PROFILE_NAME:$DBFLUX_HOST:$DBFLUX_PORT:$DBFLUX_DATABASE"
                        .to_string(),
                ],
            },
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
            kind: HookKind::Command {
                command: "sh".to_string(),
                args: vec!["-c".to_string(), "echo $DBFLUX_HOST".to_string()],
            },
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
            kind: HookKind::Command {
                command: "sh".to_string(),
                args: vec!["-c".to_string(), "echo ${HOME:-empty}".to_string()],
            },
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
            kind: HookKind::Command {
                command: "pwd".to_string(),
                args: vec![],
            },
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
        assert_eq!(hooks.post_connect[0].display_command(), "echo added");
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
            &ProcessExecutor,
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
            &ProcessExecutor,
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
            &ProcessExecutor,
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
            &ProcessExecutor,
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
            &ProcessExecutor,
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
            &ProcessExecutor,
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
    fn run_phase_collects_executor_warnings() {
        struct WarningExecutor;

        impl HookExecutor for WarningExecutor {
            fn execute_hook(
                &self,
                _hook: &ConnectionHook,
                _context: &HookContext,
                _cancel_token: &CancelToken,
                _parent_cancel_token: Option<&CancelToken>,
                _output: Option<&OutputSender>,
            ) -> Result<HookResult, String> {
                Ok(HookResult {
                    exit_code: Some(0),
                    stdout: String::new(),
                    stderr: String::new(),
                    timed_out: false,
                    warnings: vec!["be careful".to_string()],
                })
            }
        }

        let hooks = [echo_hook("ok")];

        let outcome = HookRunner::run_phase(
            HookPhase::PreConnect,
            &hooks,
            &test_context(),
            &CancelToken::new(),
            None,
            &WarningExecutor,
        );

        match outcome {
            HookPhaseOutcome::CompletedWithWarnings { warnings, .. } => {
                assert_eq!(warnings, vec!["be careful"]);
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
            &ProcessExecutor,
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
            &ProcessExecutor,
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
            &ProcessExecutor,
        );

        assert!(matches!(outcome, HookPhaseOutcome::Aborted { .. }));
    }

    #[test]
    fn run_phase_cancelled_token_aborts_immediately() {
        let token = CancelToken::new();
        token.cancel();

        let hooks = [ConnectionHook {
            kind: HookKind::Command {
                command: "sleep".to_string(),
                args: vec!["10".to_string()],
            },
            ..echo_hook("")
        }];

        let outcome = HookRunner::run_phase(
            HookPhase::PreConnect,
            &hooks,
            &test_context(),
            &token,
            None,
            &ProcessExecutor,
        );

        match outcome {
            HookPhaseOutcome::Aborted { executions, .. } => {
                assert_eq!(executions.len(), 1);
                assert!(executions[0].result.is_err());
            }
            other => panic!("expected Aborted on cancellation, got {:?}", other),
        }
    }

    #[test]
    fn process_executor_rejects_lua_hooks() {
        let hook = ConnectionHook {
            enabled: true,
            kind: HookKind::Lua {
                source: ScriptSource::Inline {
                    content: "hook.ok()".to_string(),
                },
                capabilities: LuaCapabilities::default(),
            },
            cwd: None,
            env: HashMap::new(),
            inherit_env: true,
            timeout_ms: None,
            on_failure: HookFailureMode::Disconnect,
        };

        let result =
            ProcessExecutor.execute_hook(&hook, &test_context(), &CancelToken::new(), None, None);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("lua"));
    }

    #[test]
    fn process_executor_emits_streaming_output_events() {
        let hook = if cfg!(target_os = "windows") {
            ConnectionHook {
                enabled: true,
                kind: HookKind::Command {
                    command: "cmd".to_string(),
                    args: vec![
                        "/C".to_string(),
                        "echo stdout-line && echo stderr-line 1>&2".to_string(),
                    ],
                },
                cwd: None,
                env: HashMap::new(),
                inherit_env: true,
                timeout_ms: None,
                on_failure: HookFailureMode::Disconnect,
            }
        } else {
            ConnectionHook {
                enabled: true,
                kind: HookKind::Command {
                    command: "sh".to_string(),
                    args: vec![
                        "-c".to_string(),
                        "printf 'stdout-line\n'; printf 'stderr-line\n' >&2".to_string(),
                    ],
                },
                cwd: None,
                env: HashMap::new(),
                inherit_env: true,
                timeout_ms: None,
                on_failure: HookFailureMode::Disconnect,
            }
        };

        let (sender, receiver) = output_channel();
        let result = ProcessExecutor
            .execute_hook(
                &hook,
                &test_context(),
                &CancelToken::new(),
                None,
                Some(&sender),
            )
            .unwrap();
        drop(sender);

        let events: Vec<_> = receiver.try_iter().collect();

        assert!(result.stdout.contains("stdout-line"));
        assert!(result.stderr.contains("stderr-line"));
        assert!(events.iter().any(|event| {
            event.stream == OutputStreamKind::Stdout && event.text.contains("stdout-line")
        }));
        assert!(events.iter().any(|event| {
            event.stream == OutputStreamKind::Stderr && event.text.contains("stderr-line")
        }));
    }

    #[test]
    fn output_collector_truncates_large_output() {
        let mut collector = OutputCollector::default();
        let oversized = "x".repeat(MAX_OUTPUT_BYTES + 128);

        collector.push(OutputEvent::new(OutputStreamKind::Stdout, oversized), None);

        assert!(collector.truncated);
        assert!(collector.stdout.len() <= MAX_OUTPUT_BYTES);
        assert!(collector.stdout.contains("[output truncated]"));
    }

    #[test]
    fn safe_prefix_by_bytes_preserves_utf8_boundaries() {
        assert_eq!(safe_prefix_by_bytes("abc", 2), "ab");
        assert_eq!(safe_prefix_by_bytes("aé漢", 3), "aé");
        assert_eq!(safe_prefix_by_bytes("aé漢", 4), "aé");
        assert_eq!(safe_prefix_by_bytes("aé漢", 6), "aé漢");
    }

    #[test]
    fn execute_streaming_process_uses_abort_timeout_for_forced_stop() {
        let mut command = if cfg!(target_os = "windows") {
            let mut command = Command::new("cmd");
            command.args(["/C", "echo before-timeout && ping 127.0.0.1 -n 6 >nul"]);
            command
        } else {
            let mut command = Command::new("sh");
            command.args(["-c", "printf 'before-timeout\n'; sleep 5"]);
            command
        };

        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());

        let mut child = command.spawn().unwrap();
        let result = execute_streaming_process(
            &mut child,
            &CancelToken::new(),
            None,
            None,
            Some(Duration::from_millis(100)),
            None,
        );

        match result {
            Err(ProcessExecutionError::TimedOut { stdout, stderr }) => {
                assert!(stdout.contains("before-timeout"));
                assert!(stderr.is_empty());
            }
            other => panic!("expected TimedOut, got {other:?}"),
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
            warnings: Vec::new(),
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
            warnings: Vec::new(),
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
            kind: HookKind::Command {
                command: "echo".to_string(),
                args: vec![],
            },
            ..echo_hook("")
        };

        assert_eq!(hook.display_command(), "echo");
    }

    #[test]
    fn display_command_with_args() {
        let hook = echo_hook("hello world");

        assert_eq!(hook.display_command(), "echo hello world");
    }

    #[test]
    fn display_command_for_inline_script() {
        let hook = ConnectionHook {
            enabled: true,
            kind: HookKind::Script {
                language: ScriptLanguage::Python,
                source: ScriptSource::Inline {
                    content: "print('hello')".to_string(),
                },
                interpreter: Some("python-custom".to_string()),
            },
            cwd: None,
            env: HashMap::new(),
            inherit_env: true,
            timeout_ms: None,
            on_failure: HookFailureMode::Disconnect,
        };

        assert_eq!(hook.display_command(), "python-custom <inline script>");
        assert_eq!(hook.summary(), "Python · inline");
    }

    #[test]
    fn display_command_for_file_script() {
        let hook = ConnectionHook {
            enabled: true,
            kind: HookKind::Script {
                language: ScriptLanguage::Python,
                source: ScriptSource::File {
                    path: PathBuf::from("/tmp/test_hook.py"),
                },
                interpreter: Some("python-custom".to_string()),
            },
            cwd: None,
            env: HashMap::new(),
            inherit_env: true,
            timeout_ms: None,
            on_failure: HookFailureMode::Disconnect,
        };

        assert_eq!(hook.display_command(), "python-custom /tmp/test_hook.py");
        assert_eq!(hook.summary(), "Python · file");
    }

    #[test]
    fn execute_inline_script() {
        let interpreter = ScriptLanguage::Python
            .default_interpreter()
            .unwrap_or("python")
            .to_string();

        let hook = ConnectionHook {
            enabled: true,
            kind: HookKind::Script {
                language: ScriptLanguage::Python,
                source: ScriptSource::Inline {
                    content: "print('hello from script')".to_string(),
                },
                interpreter: Some(interpreter),
            },
            cwd: None,
            env: HashMap::new(),
            inherit_env: true,
            timeout_ms: None,
            on_failure: HookFailureMode::Disconnect,
        };

        let result = hook
            .execute(&test_context(), &CancelToken::new(), None)
            .unwrap();

        assert_eq!(result.exit_code, Some(0));
        assert!(result.stdout.contains("hello from script"));
    }

    #[test]
    fn execute_file_backed_script() {
        let interpreter = ScriptLanguage::Python
            .default_interpreter()
            .unwrap_or("python")
            .to_string();

        let mut script = tempfile::Builder::new().suffix(".py").tempfile().unwrap();
        script
            .write_all(b"print('hello from file script')")
            .unwrap();

        let hook = ConnectionHook {
            enabled: true,
            kind: HookKind::Script {
                language: ScriptLanguage::Python,
                source: ScriptSource::File {
                    path: script.path().to_path_buf(),
                },
                interpreter: Some(interpreter),
            },
            cwd: None,
            env: HashMap::new(),
            inherit_env: true,
            timeout_ms: None,
            on_failure: HookFailureMode::Disconnect,
        };

        let result = hook
            .execute(&test_context(), &CancelToken::new(), None)
            .unwrap();

        assert_eq!(result.exit_code, Some(0));
        assert!(result.stdout.contains("hello from file script"));
    }

    #[test]
    fn execute_inline_script_timeout() {
        let interpreter = ScriptLanguage::Python
            .default_interpreter()
            .unwrap_or("python")
            .to_string();

        let hook = ConnectionHook {
            enabled: true,
            kind: HookKind::Script {
                language: ScriptLanguage::Python,
                source: ScriptSource::Inline {
                    content: "import time\ntime.sleep(10)".to_string(),
                },
                interpreter: Some(interpreter),
            },
            cwd: None,
            env: HashMap::new(),
            inherit_env: true,
            timeout_ms: Some(100),
            on_failure: HookFailureMode::Disconnect,
        };

        let result = hook
            .execute(&test_context(), &CancelToken::new(), None)
            .unwrap();

        assert!(result.timed_out);
        assert!(!result.is_success());
    }

    #[test]
    fn execute_inline_script_cancellation_returns_error() {
        let interpreter = ScriptLanguage::Python
            .default_interpreter()
            .unwrap_or("python")
            .to_string();

        let token = CancelToken::new();
        token.cancel();

        let hook = ConnectionHook {
            enabled: true,
            kind: HookKind::Script {
                language: ScriptLanguage::Python,
                source: ScriptSource::Inline {
                    content: "import time\ntime.sleep(10)".to_string(),
                },
                interpreter: Some(interpreter),
            },
            cwd: None,
            env: HashMap::new(),
            inherit_env: true,
            timeout_ms: None,
            on_failure: HookFailureMode::Disconnect,
        };

        let result = hook.execute(&test_context(), &token, None);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cancelled"));
    }
}
