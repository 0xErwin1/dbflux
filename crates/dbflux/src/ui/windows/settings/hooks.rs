use crate::app::AppStateChanged;
use crate::ui::components::toast::ToastExt;
use crate::ui::icons::AppIcon;
use dbflux_core::{
    AppConfig, AppConfigStore, ConnectionHook, HookExecutionMode, HookFailureMode, HookKind,
    ScriptLanguage, ScriptSource,
};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::checkbox::Checkbox;
use gpui_component::input::Input;
use gpui_component::input::InputState;
use gpui_component::scroll::ScrollableElement;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use super::SettingsEvent;
use super::hooks_section::{HookFocus, HookKindSelection, HooksSection, ScriptSourceSelection};
use super::layout;

impl HooksSection {
    fn hook_script_editor_mode(&self, cx: &App) -> &'static str {
        match self.selected_hook_kind(cx) {
            HookKindSelection::Lua => "lua",
            HookKindSelection::Script => match self.selected_script_language(cx) {
                ScriptLanguage::Bash => "bash",
                ScriptLanguage::Python => "python",
            },
            HookKindSelection::Command => "plaintext",
        }
    }

    pub(super) fn refresh_hook_script_content_editor(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let value = self.input_hook_script_content.read(cx).value().to_string();
        let editor_mode = self.hook_script_editor_mode(cx);

        let input = cx.new(|cx| {
            let mut state = InputState::new(window, cx)
                .code_editor(editor_mode)
                .line_number(true)
                .soft_wrap(true)
                .placeholder("Enter script content...");

            state.set_value(value.clone(), window, cx);
            state
        });

        let sub = cx.subscribe_in(
            &input,
            window,
            |_, _, event: &gpui_component::input::InputEvent, _window, cx| {
                if matches!(event, gpui_component::input::InputEvent::Change) {
                    cx.notify();
                }
            },
        );

        self.input_hook_script_content = input;
        self.hook_script_content_subscription = Some(sub);
        cx.notify();
    }

    pub(super) fn on_script_source_changed(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.refresh_hook_script_content_editor(window, cx);
    }

    pub(super) fn hook_sorted_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.hook_definitions.keys().cloned().collect();
        ids.sort();
        ids
    }

    fn selected_hook_kind(&self, cx: &App) -> HookKindSelection {
        match self
            .hook_kind_dropdown
            .read(cx)
            .selected_value()
            .map(|value| value.to_string())
            .as_deref()
        {
            Some("script") => HookKindSelection::Script,
            Some("lua") => HookKindSelection::Lua,
            _ => HookKindSelection::Command,
        }
    }

    fn selected_script_source(&self, cx: &App) -> ScriptSourceSelection {
        let _ = cx;
        ScriptSourceSelection::File
    }

    fn selected_script_language(&self, cx: &App) -> ScriptLanguage {
        match self
            .script_language_dropdown
            .read(cx)
            .selected_value()
            .map(|value| value.to_string())
            .as_deref()
        {
            Some("bash") => ScriptLanguage::Bash,
            _ => ScriptLanguage::Python,
        }
    }

    fn selected_hook_execution_mode(&self, cx: &App) -> HookExecutionMode {
        match self
            .hook_execution_mode_dropdown
            .read(cx)
            .selected_value()
            .map(|value| value.to_string())
            .as_deref()
        {
            Some("detached") => HookExecutionMode::Detached,
            _ => HookExecutionMode::Blocking,
        }
    }

    fn set_hook_kind_dropdown(&self, kind: HookKindSelection, cx: &mut Context<Self>) {
        let index = match kind {
            HookKindSelection::Command => 0,
            HookKindSelection::Script => 1,
            HookKindSelection::Lua => 2,
        };

        self.hook_kind_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_selected_index(Some(index), cx);
        });
    }

    fn set_script_source_dropdown(&self, source: ScriptSourceSelection, cx: &mut Context<Self>) {
        let index = match source {
            ScriptSourceSelection::File => 0,
        };

        self.script_source_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_selected_index(Some(index), cx);
        });
    }

    fn set_script_language_dropdown(&self, language: ScriptLanguage, cx: &mut Context<Self>) {
        let index = match language {
            ScriptLanguage::Bash => 0,
            ScriptLanguage::Python => {
                if cfg!(target_os = "windows") {
                    0
                } else {
                    1
                }
            }
        };

        self.script_language_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_selected_index(Some(index), cx);
        });
    }

    fn set_hook_execution_mode_dropdown(&self, mode: HookExecutionMode, cx: &mut Context<Self>) {
        let index = match mode {
            HookExecutionMode::Blocking => 0,
            HookExecutionMode::Detached => 1,
        };

        self.hook_execution_mode_dropdown
            .update(cx, |dropdown, cx| {
                dropdown.set_selected_index(Some(index), cx);
            });
    }

    fn hook_interpreter_override(&self, cx: &App) -> Option<String> {
        let interpreter = self
            .input_hook_interpreter
            .read(cx)
            .value()
            .trim()
            .to_string();

        if interpreter.is_empty() {
            None
        } else {
            Some(interpreter)
        }
    }

    fn resolved_script_interpreter(&self, cx: &App) -> Option<String> {
        self.hook_interpreter_override(cx).or_else(|| {
            self.selected_script_language(cx)
                .default_interpreter()
                .map(ToString::to_string)
        })
    }

    fn default_script_interpreter_label(&self, cx: &App) -> String {
        self.selected_script_language(cx)
            .default_interpreter()
            .map(|value| format!("auto ({value})"))
            .unwrap_or_else(|| "unsupported on this platform".to_string())
    }

    fn hook_form_preview(&self, cx: &App) -> String {
        match self.selected_hook_kind(cx) {
            HookKindSelection::Command => {
                let command = self.input_hook_command.read(cx).value().trim().to_string();
                let args = self.input_hook_args.read(cx).value().trim().to_string();

                if command.is_empty() {
                    "<enter a command>".to_string()
                } else if args.is_empty() {
                    command
                } else {
                    format!("{command} {args}")
                }
            }
            HookKindSelection::Script => match self.resolved_script_interpreter(cx) {
                Some(interpreter) => {
                    let path = self
                        .input_hook_script_file_path
                        .read(cx)
                        .value()
                        .trim()
                        .to_string();

                    if path.is_empty() {
                        format!("{interpreter} <script file>")
                    } else {
                        format!("{interpreter} {path}")
                    }
                }
                None => "Unsupported on this platform".to_string(),
            },
            HookKindSelection::Lua => {
                let path = self
                    .input_hook_script_file_path
                    .read(cx)
                    .value()
                    .trim()
                    .to_string();

                if path.is_empty() {
                    "lua <script file>".to_string()
                } else {
                    format!("lua {path}")
                }
            }
        }
    }

    fn hook_form_warnings(&self, cx: &App) -> Vec<String> {
        let hook_kind = self.selected_hook_kind(cx);

        if !matches!(
            hook_kind,
            HookKindSelection::Script | HookKindSelection::Lua
        ) {
            return Vec::new();
        }

        let mut warnings = Vec::new();

        if self.selected_script_source(cx) == ScriptSourceSelection::File {
            let path = self
                .input_hook_script_file_path
                .read(cx)
                .value()
                .trim()
                .to_string();

            if !path.is_empty() && !Path::new(&path).exists() {
                warnings.push("Script file does not exist yet".to_string());
            }
        }

        if hook_kind == HookKindSelection::Script {
            match self.resolved_script_interpreter(cx) {
                Some(interpreter) => {
                    if !interpreter_exists(&interpreter) {
                        warnings.push(format!("Interpreter '{interpreter}' was not found in PATH"));
                    }
                }
                None => {
                    warnings.push("Selected language is unsupported on this platform".to_string())
                }
            }
        }

        if hook_kind == HookKindSelection::Lua && self.hook_lua_process_run {
            warnings.push(
                "Lua process.run is enabled: this hook can execute external programs with your user permissions"
                    .to_string(),
            );
        }

        warnings
    }

    fn open_script_in_default_editor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(path) = self.ensure_hook_script_file(window, cx, true) else {
            return;
        };

        if let Err(error) = open::that(&path) {
            cx.toast_error(format!("Failed to open script: {error}"), window);
        }
    }

    fn open_script_in_app(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(path) = self.ensure_hook_script_file(window, cx, true) else {
            return;
        };

        cx.emit(SettingsEvent::OpenScript { path });
    }

    fn current_script_file_path(&self, cx: &App) -> Option<PathBuf> {
        let path = self
            .input_hook_script_file_path
            .read(cx)
            .value()
            .trim()
            .to_string();

        if path.is_empty() {
            None
        } else {
            Some(PathBuf::from(path))
        }
    }

    fn ensure_hook_script_file(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
        persist_hook: bool,
    ) -> Option<PathBuf> {
        let hook_id = self.input_hook_id.read(cx).value().trim().to_string();

        if hook_id.is_empty() {
            cx.toast_error("Hook ID is required", window);
            return None;
        }

        let (extension, content) = match self.selected_hook_kind(cx) {
            HookKindSelection::Script => (
                self.selected_script_language(cx).extension().to_string(),
                self.input_hook_script_content.read(cx).value().to_string(),
            ),
            HookKindSelection::Lua => (
                "lua".to_string(),
                self.input_hook_script_content.read(cx).value().to_string(),
            ),
            HookKindSelection::Command => {
                cx.toast_warning("Commands do not open in the script editor", window);
                return None;
            }
        };

        if let Some(path) = self.current_script_file_path(cx) {
            if !path.exists()
                && let Err(error) = std::fs::write(&path, &content)
            {
                cx.toast_error(format!("Failed to write script file: {error}"), window);
                return None;
            }

            if persist_hook {
                self.save_hook(window, cx);
            }

            return Some(path);
        }

        let path = match self.app_state.update(cx, |state, cx| {
            let scripts_dir = state
                .scripts_directory_mut()
                .ok_or_else(|| "Scripts directory is not available in this session".to_string())?;

            let hooks_dir = scripts_dir
                .hooks_directory()
                .map_err(|error| format!("Failed to create hooks directory: {error}"))?;

            let path = hooks_dir.join(format!("{}.{}", hook_id, extension));

            std::fs::write(&path, &content)
                .map_err(|error| format!("Failed to write script file: {error}"))?;

            scripts_dir.refresh();
            cx.emit(AppStateChanged);

            Ok::<PathBuf, String>(path)
        }) {
            Ok(path) => path,
            Err(error) => {
                cx.toast_error(error, window);
                return None;
            }
        };

        self.set_script_source_dropdown(ScriptSourceSelection::File, cx);
        self.input_hook_script_file_path.update(cx, |input, cx| {
            input.set_value(path.to_string_lossy().to_string(), window, cx)
        });

        if persist_hook {
            self.save_hook(window, cx);
        }

        Some(path)
    }

    pub(super) fn has_unsaved_hook_changes(&self, cx: &App) -> bool {
        if self.hook_definitions != *self.app_state.read(cx).hook_definitions() {
            return true;
        }

        if let Some(editing_id) = &self.editing_hook_id {
            let Ok(Some((hook_id, hook))) = self.hook_from_form(cx, false) else {
                return false;
            };

            if &hook_id != editing_id {
                return true;
            }

            return self
                .hook_definitions
                .get(editing_id)
                .is_some_and(|saved| saved != &hook);
        }

        self.form_has_hook_content(cx)
    }

    fn form_has_hook_content(&self, cx: &App) -> bool {
        !self.input_hook_id.read(cx).value().trim().is_empty()
            || !self.input_hook_command.read(cx).value().trim().is_empty()
            || !self.input_hook_args.read(cx).value().trim().is_empty()
            || !self
                .input_hook_script_file_path
                .read(cx)
                .value()
                .trim()
                .is_empty()
            || !self
                .input_hook_script_content
                .read(cx)
                .value()
                .trim()
                .is_empty()
            || !self
                .input_hook_interpreter
                .read(cx)
                .value()
                .trim()
                .is_empty()
            || !self
                .input_hook_ready_signal
                .read(cx)
                .value()
                .trim()
                .is_empty()
            || !self.input_hook_cwd.read(cx).value().trim().is_empty()
            || !self.input_hook_env.read(cx).value().trim().is_empty()
            || !self.input_hook_timeout.read(cx).value().trim().is_empty()
    }

    fn hook_from_form(
        &self,
        cx: &App,
        strict: bool,
    ) -> Result<Option<(String, ConnectionHook)>, String> {
        let hook_id = self.input_hook_id.read(cx).value().trim().to_string();
        let command = self.input_hook_command.read(cx).value().trim().to_string();
        let args_text = self.input_hook_args.read(cx).value().trim().to_string();
        let script_file_path = self
            .input_hook_script_file_path
            .read(cx)
            .value()
            .trim()
            .to_string();
        let script_content = self.input_hook_script_content.read(cx).value().to_string();
        let script_content_trimmed = script_content.trim().to_string();
        let cwd_text = self.input_hook_cwd.read(cx).value().trim().to_string();
        let env_text = self.input_hook_env.read(cx).value().trim().to_string();
        let timeout_text = self.input_hook_timeout.read(cx).value().trim().to_string();
        let ready_signal = self
            .input_hook_ready_signal
            .read(cx)
            .value()
            .trim()
            .to_string();
        let interpreter = self.hook_interpreter_override(cx);

        if !strict
            && hook_id.is_empty()
            && command.is_empty()
            && args_text.is_empty()
            && script_file_path.is_empty()
            && script_content_trimmed.is_empty()
            && interpreter.is_none()
            && cwd_text.is_empty()
            && env_text.is_empty()
            && ready_signal.is_empty()
        {
            return Ok(None);
        }

        if hook_id.is_empty() {
            return Err("Hook ID is required".to_string());
        }

        let selected_kind = self.selected_hook_kind(cx);

        let timeout_ms = if timeout_text.is_empty() {
            None
        } else {
            match timeout_text.parse::<u64>() {
                Ok(value) => Some(value),
                Err(_) => return Err("Timeout must be a valid number (milliseconds)".to_string()),
            }
        };

        let on_failure = match self
            .hook_failure_dropdown
            .read(cx)
            .selected_value()
            .map(|value| value.to_string())
            .as_deref()
        {
            Some("warn") => HookFailureMode::Warn,
            Some("ignore") => HookFailureMode::Ignore,
            _ => HookFailureMode::Disconnect,
        };

        let cwd = if selected_kind == HookKindSelection::Lua || cwd_text.is_empty() {
            None
        } else {
            Some(std::path::PathBuf::from(cwd_text))
        };

        let env = if selected_kind == HookKindSelection::Lua {
            HashMap::new()
        } else {
            Self::parse_hook_env_pairs(&env_text)?
        };

        let kind = match selected_kind {
            HookKindSelection::Command => {
                if command.is_empty() {
                    return Err("Command is required".to_string());
                }

                HookKind::Command {
                    command,
                    args: args_text
                        .split_whitespace()
                        .map(ToString::to_string)
                        .collect(),
                }
            }
            HookKindSelection::Script => {
                let language = self.selected_script_language(cx);
                if script_file_path.is_empty() {
                    return Err("Script file path is required".to_string());
                }

                let source = ScriptSource::File {
                    path: PathBuf::from(script_file_path),
                };

                HookKind::Script {
                    language,
                    source,
                    interpreter,
                }
            }
            HookKindSelection::Lua => {
                if script_file_path.is_empty() {
                    return Err("Lua script file path is required".to_string());
                }

                let source = ScriptSource::File {
                    path: PathBuf::from(script_file_path),
                };

                HookKind::Lua {
                    source,
                    capabilities: dbflux_core::LuaCapabilities {
                        logging: self.hook_lua_logging,
                        env_read: self.hook_lua_env_read,
                        connection_metadata: self.hook_lua_connection_metadata,
                        process_run: self.hook_lua_process_run,
                    },
                }
            }
        };

        let hook = ConnectionHook {
            enabled: self.hook_enabled,
            kind,
            cwd,
            env,
            inherit_env: if selected_kind == HookKindSelection::Lua {
                true
            } else {
                self.hook_inherit_env
            },
            timeout_ms,
            execution_mode: if selected_kind == HookKindSelection::Lua {
                HookExecutionMode::Blocking
            } else {
                self.selected_hook_execution_mode(cx)
            },
            ready_signal: if selected_kind == HookKindSelection::Lua || ready_signal.is_empty() {
                None
            } else {
                Some(ready_signal)
            },
            on_failure,
        };

        Ok(Some((hook_id, hook)))
    }

    fn persist_hooks(&self, window: &mut Window, cx: &mut Context<Self>) {
        let store = match AppConfigStore::new() {
            Ok(store) => store,
            Err(error) => {
                cx.toast_error(format!("Cannot save: {}", error), window);
                return;
            }
        };

        let mut config = match store.load() {
            Ok(config) => config,
            Err(error) => {
                log::error!("Failed to load config before hooks save: {}", error);
                AppConfig::default()
            }
        };

        config.hook_definitions = self.hook_definitions.clone();

        if let Err(error) = store.save(&config) {
            log::error!("Failed to save hooks: {}", error);
            cx.toast_error(format!("Failed to save hooks: {}", error), window);
            return;
        }

        let hooks = self.hook_definitions.clone();
        self.app_state.update(cx, move |state, _cx| {
            state.set_hook_definitions(hooks);
        });
    }

    pub(super) fn clear_hook_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.editing_hook_id = None;
        self.hook_selected_id = None;
        self.hook_list_idx = None;
        self.hook_enabled = true;
        self.hook_inherit_env = true;
        self.hook_lua_logging = true;
        self.hook_lua_env_read = true;
        self.hook_lua_connection_metadata = true;
        self.hook_lua_process_run = false;
        self.set_hook_execution_mode_dropdown(HookExecutionMode::Blocking, cx);

        self.set_hook_kind_dropdown(HookKindSelection::Command, cx);
        self.set_script_language_dropdown(ScriptLanguage::Python, cx);
        self.set_script_source_dropdown(ScriptSourceSelection::File, cx);
        self.refresh_hook_script_content_editor(window, cx);

        self.input_hook_id
            .update(cx, |input, cx| input.set_value("", window, cx));
        self.input_hook_command
            .update(cx, |input, cx| input.set_value("", window, cx));
        self.input_hook_args
            .update(cx, |input, cx| input.set_value("", window, cx));
        self.input_hook_script_file_path
            .update(cx, |input, cx| input.set_value("", window, cx));
        self.input_hook_script_content
            .update(cx, |input, cx| input.set_value("", window, cx));
        self.input_hook_interpreter
            .update(cx, |input, cx| input.set_value("", window, cx));
        self.input_hook_ready_signal
            .update(cx, |input, cx| input.set_value("", window, cx));
        self.input_hook_cwd
            .update(cx, |input, cx| input.set_value("", window, cx));
        self.input_hook_env
            .update(cx, |input, cx| input.set_value("", window, cx));
        self.input_hook_timeout
            .update(cx, |input, cx| input.set_value("", window, cx));

        self.hook_failure_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_selected_index(Some(0), cx);
        });

        cx.notify();
    }

    pub(super) fn edit_hook(&mut self, hook_id: &str, window: &mut Window, cx: &mut Context<Self>) {
        let Some(hook) = self.hook_definitions.get(hook_id).cloned() else {
            return;
        };

        self.editing_hook_id = Some(hook_id.to_string());
        self.hook_selected_id = Some(hook_id.to_string());
        let ids = self.hook_sorted_ids();
        self.hook_list_idx = ids.iter().position(|id| id == hook_id);
        self.hook_enabled = hook.enabled;
        self.hook_inherit_env = hook.inherit_env;

        self.input_hook_id.update(cx, |input, cx| {
            input.set_value(hook_id.to_string(), window, cx)
        });

        let (command, args, script_file_path, script_content, interpreter) = match &hook.kind {
            HookKind::Command { command, args } => {
                self.set_hook_kind_dropdown(HookKindSelection::Command, cx);
                self.set_hook_execution_mode_dropdown(hook.execution_mode, cx);
                self.set_script_source_dropdown(ScriptSourceSelection::File, cx);
                self.set_script_language_dropdown(ScriptLanguage::Python, cx);
                self.hook_lua_logging = true;
                self.hook_lua_env_read = true;
                self.hook_lua_connection_metadata = true;
                self.hook_lua_process_run = false;

                (
                    command.clone(),
                    args.join(" "),
                    String::new(),
                    String::new(),
                    String::new(),
                )
            }
            HookKind::Script {
                language,
                source,
                interpreter,
            } => {
                self.set_hook_kind_dropdown(HookKindSelection::Script, cx);
                self.set_hook_execution_mode_dropdown(hook.execution_mode, cx);
                self.set_script_language_dropdown(*language, cx);
                self.hook_lua_logging = true;
                self.hook_lua_env_read = true;
                self.hook_lua_connection_metadata = true;
                self.hook_lua_process_run = false;

                let (script_file_path, script_content) = match source {
                    ScriptSource::File { path } => {
                        self.set_script_source_dropdown(ScriptSourceSelection::File, cx);
                        (path.to_string_lossy().to_string(), String::new())
                    }
                    ScriptSource::Inline { content } => {
                        self.set_script_source_dropdown(ScriptSourceSelection::File, cx);
                        (String::new(), content.clone())
                    }
                };

                (
                    String::new(),
                    String::new(),
                    script_file_path,
                    script_content,
                    interpreter.clone().unwrap_or_default(),
                )
            }
            HookKind::Lua {
                source,
                capabilities,
            } => {
                self.set_hook_kind_dropdown(HookKindSelection::Lua, cx);
                self.set_hook_execution_mode_dropdown(HookExecutionMode::Blocking, cx);
                self.set_script_language_dropdown(ScriptLanguage::Python, cx);
                self.hook_lua_logging = capabilities.logging;
                self.hook_lua_env_read = capabilities.env_read;
                self.hook_lua_connection_metadata = capabilities.connection_metadata;
                self.hook_lua_process_run = capabilities.process_run;

                let (script_file_path, script_content) = match source {
                    ScriptSource::File { path } => {
                        self.set_script_source_dropdown(ScriptSourceSelection::File, cx);
                        (path.to_string_lossy().to_string(), String::new())
                    }
                    ScriptSource::Inline { content } => {
                        self.set_script_source_dropdown(ScriptSourceSelection::File, cx);
                        (String::new(), content.clone())
                    }
                };

                (
                    String::new(),
                    String::new(),
                    script_file_path,
                    script_content,
                    String::new(),
                )
            }
        };

        self.refresh_hook_script_content_editor(window, cx);

        self.input_hook_command
            .update(cx, |input, cx| input.set_value(command, window, cx));
        self.input_hook_args
            .update(cx, |input, cx| input.set_value(args, window, cx));
        self.input_hook_script_file_path.update(cx, |input, cx| {
            input.set_value(script_file_path, window, cx)
        });
        self.input_hook_script_content
            .update(cx, |input, cx| input.set_value(script_content, window, cx));
        self.input_hook_interpreter
            .update(cx, |input, cx| input.set_value(interpreter, window, cx));
        self.input_hook_ready_signal.update(cx, |input, cx| {
            input.set_value(hook.ready_signal.unwrap_or_default(), window, cx)
        });
        self.input_hook_cwd.update(cx, |input, cx| {
            input.set_value(
                hook.cwd
                    .as_ref()
                    .map(|path| path.to_string_lossy().to_string())
                    .unwrap_or_default(),
                window,
                cx,
            )
        });
        let mut env_pairs: Vec<String> = hook
            .env
            .iter()
            .map(|(key, value)| format!("{}={}", key, value))
            .collect();
        env_pairs.sort();
        self.input_hook_env.update(cx, |input, cx| {
            input.set_value(env_pairs.join(", "), window, cx)
        });
        self.input_hook_timeout.update(cx, |input, cx| {
            input.set_value(
                hook.timeout_ms
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                window,
                cx,
            )
        });

        let failure_index = match hook.on_failure {
            HookFailureMode::Disconnect => 0,
            HookFailureMode::Warn => 1,
            HookFailureMode::Ignore => 2,
        };
        self.hook_failure_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_selected_index(Some(failure_index), cx);
        });

        cx.notify();
    }

    pub(super) fn save_hook(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if matches!(
            self.selected_hook_kind(cx),
            HookKindSelection::Script | HookKindSelection::Lua
        ) && self.current_script_file_path(cx).is_none()
            && self.ensure_hook_script_file(window, cx, false).is_none()
        {
            return;
        }

        let (hook_id, hook) = match self.hook_from_form(cx, true) {
            Ok(Some(hook)) => hook,
            Ok(None) => return,
            Err(error) => {
                cx.toast_error(error, window);
                return;
            }
        };

        let duplicate = self.hook_definitions.contains_key(&hook_id)
            && self.editing_hook_id.as_deref() != Some(hook_id.as_str());

        if duplicate {
            cx.toast_error(
                format!("A hook with ID '{}' already exists", hook_id),
                window,
            );
            return;
        }

        if let Some(previous_id) = self.editing_hook_id.clone()
            && previous_id != hook_id
        {
            self.hook_definitions.remove(&previous_id);
        }

        self.hook_definitions.insert(hook_id.clone(), hook);
        self.persist_hooks(window, cx);

        self.edit_hook(&hook_id, window, cx);
        self.hook_focus = HookFocus::Form;
        cx.toast_success("Hook saved", window);
    }

    pub(super) fn request_delete_hook(&mut self, hook_id: String, cx: &mut Context<Self>) {
        self.pending_delete_hook_id = Some(hook_id);
        cx.notify();
    }

    pub(super) fn confirm_delete_hook(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(hook_id) = self.pending_delete_hook_id.take() else {
            return;
        };

        self.hook_definitions.remove(&hook_id);

        if self.editing_hook_id.as_deref() == Some(hook_id.as_str()) {
            self.clear_hook_form(window, cx);
        }

        if self.hook_selected_id.as_deref() == Some(hook_id.as_str()) {
            self.hook_selected_id = None;
            self.hook_list_idx = None;
        }

        self.persist_hooks(window, cx);
        cx.toast_success("Hook deleted", window);
        cx.notify();
    }

    pub(super) fn cancel_delete_hook(&mut self, cx: &mut Context<Self>) {
        self.pending_delete_hook_id = None;
        cx.notify();
    }

    fn parse_hook_env_pairs(
        text: &str,
    ) -> Result<std::collections::HashMap<String, String>, String> {
        let mut env = std::collections::HashMap::new();

        if text.trim().is_empty() {
            return Ok(env);
        }

        for raw_pair in text.split(',') {
            let pair = raw_pair.trim();
            if pair.is_empty() {
                continue;
            }

            let Some((key, value)) = pair.split_once('=') else {
                return Err(format!(
                    "Invalid env pair '{}'. Expected KEY=value format",
                    pair
                ));
            };

            let key = key.trim();
            if key.is_empty() {
                return Err("Environment variable key cannot be empty".to_string());
            }

            env.insert(key.to_string(), value.to_string());
        }

        Ok(env)
    }

    pub(super) fn render_hooks_section(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        layout::section_container(
            div()
                .flex_1()
                .min_h_0()
                .flex()
                .flex_col()
                .overflow_hidden()
                .child(layout::section_header(
                    "Hooks",
                    "Create reusable hooks and associate them from connection settings",
                    theme,
                ))
                .child(
                    div()
                        .flex_1()
                        .min_h_0()
                        .flex()
                        .overflow_hidden()
                        .child(self.render_hooks_list(cx))
                        .child(self.render_hook_form(cx)),
                ),
        )
    }

    fn render_hooks_list(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let hook_ids = self.hook_sorted_ids();
        let list_focused = self.content_focused && self.hook_focus == HookFocus::List;
        let is_new_button_focused = list_focused && self.hook_list_idx.is_none();

        if let Some(scroll_idx) = self.hook_pending_scroll_idx.take() {
            self.hook_list_scroll_handle.scroll_to_item(scroll_idx);
        }

        div()
            .w(px(280.0))
            .h_full()
            .min_h_0()
            .border_r_1()
            .border_color(theme.border)
            .flex()
            .flex_col()
            .child(
                div().p_2().border_b_1().border_color(theme.border).child(
                    div()
                        .rounded(px(4.0))
                        .border_1()
                        .border_color(if is_new_button_focused {
                            theme.primary
                        } else {
                            gpui::transparent_black()
                        })
                        .child(
                            Button::new("new-hook")
                                .label("New Hook")
                                .small()
                                .w_full()
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.hook_focus = HookFocus::Form;
                                    this.clear_hook_form(window, cx);
                                })),
                        ),
                ),
            )
            .child(
                div()
                    .id("hooks-list-scroll")
                    .flex_1()
                    .min_h_0()
                    .overflow_scroll()
                    .track_scroll(&self.hook_list_scroll_handle)
                    .p_2()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .when(hook_ids.is_empty(), |container| {
                        container.child(
                            div()
                                .p_4()
                                .text_sm()
                                .text_color(theme.muted_foreground)
                                .child("No hooks defined"),
                        )
                    })
                    .children(hook_ids.into_iter().enumerate().map(|(idx, hook_id)| {
                        let selected = self.editing_hook_id.as_deref() == Some(hook_id.as_str());
                        let focused = list_focused && self.hook_list_idx == Some(idx);
                        let hook = self.hook_definitions.get(&hook_id).cloned();
                        let hook_id_for_click = hook_id.clone();

                        div()
                            .id(SharedString::from(format!("hook-item-{}", hook_id)))
                            .px_3()
                            .py_2()
                            .rounded(px(4.0))
                            .cursor_pointer()
                            .border_1()
                            .border_color(if focused && !selected {
                                theme.primary
                            } else {
                                gpui::transparent_black()
                            })
                            .when(selected, |div| div.bg(theme.secondary))
                            .hover(|div| div.bg(theme.secondary))
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.hook_focus = HookFocus::Form;
                                this.edit_hook(&hook_id_for_click, window, cx);
                            }))
                            .child(
                                div()
                                    .flex()
                                    .items_start()
                                    .gap_2()
                                    .child(
                                        svg()
                                            .path(AppIcon::SquareTerminal.path())
                                            .size_4()
                                            .text_color(theme.muted_foreground)
                                            .mt(px(2.0)),
                                    )
                                    .child(
                                        div()
                                            .flex()
                                            .flex_col()
                                            .gap_1()
                                            .child(
                                                div()
                                                    .text_sm()
                                                    .font_weight(FontWeight::MEDIUM)
                                                    .child(hook_id.clone()),
                                            )
                                            .when_some(hook, |container, hook| {
                                                container.child(
                                                    div()
                                                        .text_xs()
                                                        .text_color(theme.muted_foreground)
                                                        .child(hook.summary()),
                                                )
                                            }),
                                    ),
                            )
                    })),
            )
    }

    fn render_hook_form(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        let editing = self.editing_hook_id.is_some();
        let title = if editing { "Edit Hook" } else { "New Hook" };
        let hook_kind = self.selected_hook_kind(cx);
        let is_script = hook_kind == HookKindSelection::Script;
        let is_lua = hook_kind == HookKindSelection::Lua;
        let uses_script_source = is_script || is_lua;
        let warnings = self.hook_form_warnings(cx);
        let preview = self.hook_form_preview(cx);
        let default_interpreter = self.default_script_interpreter_label(cx);

        div()
            .flex_1()
            .min_h_0()
            .h_full()
            .flex()
            .flex_col()
            .overflow_hidden()
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _, _| {
                    this.switching_input = true;
                }),
            )
            .child(
                div().p_4().border_b_1().border_color(theme.border).child(
                    div()
                        .text_base()
                        .font_weight(FontWeight::MEDIUM)
                        .child(title),
                ),
            )
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scrollbar()
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_4()
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .child("Hook ID"),
                            )
                            .child(Input::new(&self.input_hook_id).small()),
                    )
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .child("Type"),
                            )
                            .child(div().w(px(220.0)).child(self.hook_kind_dropdown.clone())),
                    )
                    .when(hook_kind == HookKindSelection::Command, |container| {
                        container.child(
                            div()
                                .flex()
                                .flex_col()
                                .gap_4()
                                .child(
                                    div()
                                        .flex()
                                        .flex_col()
                                        .gap_1()
                                        .child(
                                            div()
                                                .text_sm()
                                                .font_weight(FontWeight::MEDIUM)
                                                .child("Command"),
                                        )
                                        .child(Input::new(&self.input_hook_command).small()),
                                )
                                .child(
                                    div()
                                        .flex()
                                        .flex_col()
                                        .gap_1()
                                        .child(
                                            div()
                                                .text_sm()
                                                .font_weight(FontWeight::MEDIUM)
                                                .child("Arguments"),
                                        )
                                        .child(
                                            div()
                                                .text_xs()
                                                .text_color(theme.muted_foreground)
                                                .child("Arguments separated by spaces"),
                                        )
                                        .child(Input::new(&self.input_hook_args).small()),
                                ),
                        )
                    })
                    .when(uses_script_source, |container| {
                        container.child(
                            div()
                                .flex()
                                .flex_col()
                                .gap_4()
                                .when(is_script, |container| {
                                    container.child(
                                        div()
                                            .flex()
                                            .flex_col()
                                            .gap_1()
                                            .child(
                                                div()
                                                    .text_sm()
                                                    .font_weight(FontWeight::MEDIUM)
                                                    .child("Language"),
                                            )
                                            .child(
                                                div()
                                                    .w(px(220.0))
                                                    .child(self.script_language_dropdown.clone()),
                                            ),
                                    )
                                })
                                .child(
                                    div()
                                        .flex()
                                        .flex_col()
                                        .gap_1()
                                        .child(
                                            div()
                                                .text_sm()
                                                .font_weight(FontWeight::MEDIUM)
                                                .child("File Path"),
                                        ),
                                )
                                .child(
                                    div()
                                        .flex()
                                        .flex_col()
                                        .gap_1()
                                        .child(
                                            div()
                                                .text_xs()
                                                .text_color(theme.muted_foreground)
                                                .child("Scripts are edited in the app editor and stored under hooks/ by default"),
                                        )
                                        .child(
                                            Input::new(&self.input_hook_script_file_path).small(),
                                        )
                                        .child(
                                            div()
                                                .flex()
                                                .gap_2()
                                                .child(
                                                    Button::new("open-script-app")
                                                        .label("Open in App")
                                                        .small()
                                                        .on_click(cx.listener(|this, _, window, cx| {
                                                            this.open_script_in_app(window, cx);
                                                        })),
                                                )
                                                .child(
                                                    Button::new("open-script-editor")
                                                        .label("Open in Editor")
                                                        .small()
                                                        .on_click(cx.listener(|this, _, window, cx| {
                                                            this.open_script_in_default_editor(window, cx);
                                                        })),
                                                ),
                                        ),
                                )
                                .when(is_script, |container| {
                                    container.child(
                                        div()
                                            .flex()
                                            .flex_col()
                                            .gap_1()
                                            .child(
                                                div()
                                                    .text_sm()
                                                    .font_weight(FontWeight::MEDIUM)
                                                    .child("Interpreter"),
                                            )
                                            .child(
                                                div()
                                                    .text_xs()
                                                    .text_color(theme.muted_foreground)
                                                    .child(format!("Leave empty for {default_interpreter}")),
                                            )
                                            .child(Input::new(&self.input_hook_interpreter).small()),
                                    )
                                })
                                .when(is_lua, |container| {
                                    container.child(
                                        div()
                                            .flex()
                                            .flex_col()
                                            .gap_2()
                                            .child(
                                                div()
                                                    .text_sm()
                                                    .font_weight(FontWeight::MEDIUM)
                                                    .child("Capabilities"),
                                            )
                                            .child(
                                                div()
                                                    .flex()
                                                    .items_center()
                                                    .gap_2()
                                                    .child(
                                                        Checkbox::new("hook-lua-logging")
                                                            .checked(self.hook_lua_logging)
                                                            .on_click(cx.listener(|this, checked: &bool, _, cx| {
                                                                this.hook_lua_logging = *checked;
                                                                cx.notify();
                                                            })),
                                                    )
                                                    .child(div().text_sm().child("Logging")),
                                            )
                                            .child(
                                                div()
                                                    .flex()
                                                    .items_center()
                                                    .gap_2()
                                                    .child(
                                                        Checkbox::new("hook-lua-env-read")
                                                            .checked(self.hook_lua_env_read)
                                                            .on_click(cx.listener(|this, checked: &bool, _, cx| {
                                                                this.hook_lua_env_read = *checked;
                                                                cx.notify();
                                                            })),
                                                    )
                                                    .child(div().text_sm().child("Environment read")),
                                            )
                                            .child(
                                                div()
                                                    .flex()
                                                    .items_center()
                                                    .gap_2()
                                                    .child(
                                                        Checkbox::new("hook-lua-connection-metadata")
                                                            .checked(self.hook_lua_connection_metadata)
                                                            .on_click(cx.listener(|this, checked: &bool, _, cx| {
                                                                this.hook_lua_connection_metadata = *checked;
                                                                cx.notify();
                                                            })),
                                                    )
                                                    .child(div().text_sm().child("Connection metadata")),
                                            )
                                            .child(
                                                div()
                                                    .flex()
                                                    .items_center()
                                                    .gap_2()
                                                    .child(
                                                        Checkbox::new("hook-lua-process-run")
                                                            .checked(self.hook_lua_process_run)
                                                            .on_click(cx.listener(|this, checked: &bool, _, cx| {
                                                                this.hook_lua_process_run = *checked;
                                                                cx.notify();
                                                            })),
                                                    )
                                                    .child(div().text_sm().child("Controlled process run")),
                                            )
                                            .child(
                                                div()
                                                    .text_xs()
                                                    .text_color(theme.muted_foreground)
                                                    .child("Enables `dbflux.process.run(...)` without exposing the Lua `os` library"),
                                            ),
                                    )
                                }),
                        )
                    })
                    .when(!is_lua, |container| {
                        container.child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .child("Execution Mode"),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(theme.muted_foreground)
                                    .child("Detached runs in background and does not block connect/disconnect"),
                            )
                            .child(div().w(px(220.0)).child(self.hook_execution_mode_dropdown.clone())),
                    )
                    })
                    .when(!is_lua && self.selected_hook_execution_mode(cx) == HookExecutionMode::Detached, |container| {
                        container.child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .child("Ready Signal"),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(theme.muted_foreground)
                                    .child("DBFlux waits for this text in hook output before continuing. Required for detached pre-connect hooks."),
                            )
                            .child(Input::new(&self.input_hook_ready_signal).small()),
                    )
                    })
                    .when(!is_lua, |container| {
                        container.child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .child("Working Directory"),
                            )
                            .child(Input::new(&self.input_hook_cwd).small()),
                    )
                    })
                    .when(!is_lua, |container| {
                        container.child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .child("Environment"),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(theme.muted_foreground)
                                    .child("Comma-separated KEY=value pairs"),
                            )
                            .child(Input::new(&self.input_hook_env).small()),
                    )
                    })
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .child("Timeout (ms)"),
                            )
                            .child(Input::new(&self.input_hook_timeout).small()),
                    )
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .child("Resolved Command"),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(theme.muted_foreground)
                                    .child(preview),
                            ),
                    )
                    .when(!warnings.is_empty(), |container| {
                        container.child(
                            div().flex().flex_col().gap_2().children(warnings.iter().map(|warning| {
                                div()
                                    .flex()
                                    .items_start()
                                    .gap_2()
                                    .px_3()
                                    .py_2()
                                    .rounded(px(6.0))
                                    .bg(theme.warning.opacity(0.12))
                                    .border_1()
                                    .border_color(theme.warning.opacity(0.3))
                                    .child(
                                        svg()
                                            .path(AppIcon::TriangleAlert.path())
                                            .size_4()
                                            .text_color(theme.warning)
                                            .mt(px(1.0)),
                                    )
                                    .child(
                                        div()
                                            .text_sm()
                                            .text_color(theme.warning)
                                            .child(warning.clone()),
                                    )
                            })),
                        )
                    })
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .child(
                                Checkbox::new("hook-enabled")
                                    .checked(self.hook_enabled)
                                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                                        this.hook_enabled = *checked;
                                        cx.notify();
                                    })),
                            )
                            .child(div().text_sm().child("Enabled")),
                    )
                    .when(!is_lua, |container| {
                        container.child(
                            div()
                                .flex()
                                .items_center()
                                .gap_2()
                                .child(
                                    Checkbox::new("hook-inherit-env")
                                        .checked(self.hook_inherit_env)
                                        .on_click(cx.listener(|this, checked: &bool, _, cx| {
                                            this.hook_inherit_env = *checked;
                                            cx.notify();
                                        })),
                                )
                                .child(div().text_sm().child("Inherit parent environment")),
                        )
                    })
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .gap_1()
                            .child(
                                div()
                                    .text_sm()
                                    .font_weight(FontWeight::MEDIUM)
                                    .child("On Failure"),
                            )
                            .child(div().w(px(220.0)).child(self.hook_failure_dropdown.clone())),
                    ),
            )
            .child(
                div()
                    .flex_shrink_0()
                    .p_4()
                    .border_t_1()
                    .border_color(theme.border)
                    .flex()
                    .gap_2()
                    .justify_end()
                    .when(editing, |container| {
                        let hook_id = self.editing_hook_id.clone().unwrap_or_default();
                        container.child(
                            Button::new("delete-hook")
                                .label("Delete")
                                .small()
                                .danger()
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.request_delete_hook(hook_id.clone(), cx);
                                })),
                        )
                    })
                    .child(div().flex_1())
                    .child(
                        Button::new("save-hook")
                            .label(if editing { "Update" } else { "Create" })
                            .small()
                            .primary()
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.save_hook(window, cx);
                            })),
                    ),
            )
    }
}

fn interpreter_exists(program: &str) -> bool {
    let path = Path::new(program);

    if path.is_absolute() || program.contains(std::path::MAIN_SEPARATOR) {
        return path.exists();
    }

    let Some(path_value) = std::env::var_os("PATH") else {
        return false;
    };

    std::env::split_paths(&path_value).any(|dir| dir.join(program).exists())
}
