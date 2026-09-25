use crate::tokens::{FormMetrics, SettingsMetrics};
use dbflux_app::config_loader::{EditableGlobalHook, HookDefinitionSave};
use dbflux_app::keymap::Modifiers;
use dbflux_components::controls::InputEvent;
use dbflux_components::controls::{Button, Checkbox, Input, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{
    BannerBlock, BannerVariant, Chamfer, SegmentedControl, SegmentedItem, Text,
};
use dbflux_components::tokens::{ChamferCut, ChromeColors};
use dbflux_components::typography::AppFonts;
use dbflux_core::{
    ConnectionHook, HookExecutionMode, HookFailureMode, HookKind, ScriptLanguage, ScriptSource,
};
use dbflux_ui_base::AppStateChanged;
use dbflux_ui_base::keymap::key_chord_from_gpui;
use dbflux_ui_base::toast::{Toast, copy_action, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::input::EditorState;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use super::SettingsEvent;
use super::form_section::FormSection;
use super::hooks_section::{
    HookFocus, HookFormField, HookKindSelection, HooksSection, ScriptSourceSelection,
};
use super::layout;
use crate::labels::{
    hooks_create_dir_failed, hooks_duplicate_id, hooks_env_pair_invalid,
    hooks_form_interpreter_hint, hooks_interpreter_auto_label, hooks_interpreter_missing,
    hooks_open_script_failed, hooks_write_script_failed,
};

fn commit_saved_hook_definitions(
    current: &mut HashMap<String, EditableGlobalHook>,
    save_result: Result<HashMap<String, EditableGlobalHook>, dbflux_storage::error::StorageError>,
) -> Result<HashMap<String, EditableGlobalHook>, dbflux_storage::error::StorageError> {
    let saved = save_result?;
    *current = saved.clone();
    Ok(saved)
}

fn update_hook_definition(
    definitions: &mut HashMap<String, EditableGlobalHook>,
    existing_name: Option<&str>,
    name: String,
    hook: ConnectionHook,
) {
    let id = existing_name
        .and_then(|previous_name| definitions.get(previous_name))
        .and_then(|definition| definition.id.clone());

    if let Some(previous_name) = existing_name
        && previous_name != name
    {
        definitions.remove(previous_name);
    }

    definitions.insert(name, EditableGlobalHook { id, hook });
}

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
            // soft_wrap defaults to true in 0.6.1, so the old explicit builder is gone.
            let mut state = EditorState::new(window, cx)
                .language(editor_mode)
                .line_number(true)
                .placeholder(dbflux_i18n::t!("hooks.script.placeholder"));

            state.set_value(value.clone(), window, cx);
            state
        });

        let sub = cx.subscribe_in(&input, window, |_, _, event: &InputEvent, _window, cx| {
            if matches!(event, InputEvent::Change) {
                cx.notify();
            }
        });

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

    fn selected_hook_kind(&self, _cx: &App) -> HookKindSelection {
        self.hook_kind_selection
    }

    fn selected_hook_execution_mode(&self, _cx: &App) -> HookExecutionMode {
        self.hook_execution_mode
    }

    fn selected_script_source(&self, _cx: &App) -> ScriptSourceSelection {
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

    fn set_hook_kind_dropdown(&mut self, kind: HookKindSelection, cx: &mut Context<Self>) {
        let mut selection = self.hook_editor_selection();
        selection.set_keyboard_kind(kind);
        self.apply_hook_editor_selection(selection, cx);

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

    fn set_hook_execution_mode_dropdown(
        &mut self,
        mode: HookExecutionMode,
        cx: &mut Context<Self>,
    ) {
        let mut selection = self.hook_editor_selection();
        selection.set_keyboard_execution_mode(mode);
        self.apply_hook_editor_selection(selection, cx);

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
            .map(hooks_interpreter_auto_label)
            .unwrap_or_else(|| dbflux_i18n::t!("settings.hooks.form.interpreter_unsupported"))
    }

    fn hook_form_preview(&self, cx: &App) -> String {
        match self.selected_hook_kind(cx) {
            HookKindSelection::Command => {
                let command = self.input_hook_command.read(cx).value().trim().to_string();
                let args = self.input_hook_args.read(cx).value().trim().to_string();

                if command.is_empty() {
                    dbflux_i18n::t!("settings.hooks.form.preview_placeholder")
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
                None => dbflux_i18n::t!("settings.hooks.status.unsupported_platform"),
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
                warnings.push(dbflux_i18n::t!("settings.hooks.status.script_missing"));
            }
        }

        if hook_kind == HookKindSelection::Script {
            match self.resolved_script_interpreter(cx) {
                Some(interpreter) => {
                    if !interpreter_exists(&interpreter) {
                        warnings.push(hooks_interpreter_missing(&interpreter));
                    }
                }
                None => warnings.push(dbflux_i18n::t!(
                    "settings.hooks.status.language_unsupported"
                )),
            }
        }

        if hook_kind == HookKindSelection::Lua && self.hook_lua_process_run {
            warnings.push(dbflux_i18n::t!(
                "settings.hooks.status.lua_process_run_warning"
            ));
        }

        warnings
    }

    fn open_script_in_default_editor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(path) = self.ensure_hook_script_file(window, cx, true) else {
            return;
        };

        if let Err(error) = open::that(&path) {
            report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    hooks_open_script_failed(&error.to_string()),
                ),
                cx,
            );
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
            let toast_msg = dbflux_i18n::t!("settings.hooks.validation.id_required");
            Toast::error(toast_msg.clone())
                .meta_right(now_hms())
                .action(copy_action(toast_msg))
                .push(cx);
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
                Toast::warning(dbflux_i18n::t!("settings.hooks.error.command_not_editable"))
                    .meta_right(now_hms())
                    .push(cx);
                return None;
            }
        };

        if let Some(path) = self.current_script_file_path(cx) {
            if !path.exists()
                && let Err(error) = std::fs::write(&path, &content)
            {
                let toast_msg = hooks_write_script_failed(&error.to_string());
                Toast::error(toast_msg.clone())
                    .meta_right(now_hms())
                    .action(copy_action(toast_msg))
                    .push(cx);
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
                .ok_or_else(|| dbflux_i18n::t!("settings.hooks.error.no_scripts_dir"))?;

            let hooks_dir = scripts_dir
                .hooks_directory()
                .map_err(|error| hooks_create_dir_failed(&error.to_string()))?;

            let path = hooks_dir.join(format!("{}.{}", hook_id, extension));

            std::fs::write(&path, &content)
                .map_err(|error| hooks_write_script_failed(&error.to_string()))?;

            scripts_dir.refresh();
            cx.emit(AppStateChanged);

            Ok::<PathBuf, String>(path)
        }) {
            Ok(path) => path,
            Err(error) => {
                let toast_msg = error.to_string();
                Toast::error(toast_msg.clone())
                    .meta_right(now_hms())
                    .action(copy_action(toast_msg))
                    .push(cx);
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
                .is_some_and(|saved| saved.hook != hook);
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
            || !self
                .input_hook_env_denylist
                .read(cx)
                .value()
                .trim()
                .is_empty()
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
        let env_denylist_text = self
            .input_hook_env_denylist
            .read(cx)
            .value()
            .trim()
            .to_string();
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
            && env_denylist_text.is_empty()
            && ready_signal.is_empty()
        {
            return Ok(None);
        }

        if hook_id.is_empty() {
            return Err(dbflux_i18n::t!("settings.hooks.validation.id_required"));
        }

        let selected = self.hook_editor_selection().save_selection();
        let selected_kind = selected.kind;

        let timeout_ms = if timeout_text.is_empty() {
            None
        } else {
            match timeout_text.parse::<u64>() {
                Ok(value) => Some(value),
                Err(_) => return Err(dbflux_i18n::t!("settings.hooks.validation.timeout")),
            }
        };

        let on_failure = self.selected_failure_mode(cx);

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
                    return Err(dbflux_i18n::t!(
                        "settings.hooks.validation.command_required"
                    ));
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
                    return Err(dbflux_i18n::t!(
                        "settings.hooks.validation.script_path_required"
                    ));
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
                    return Err(dbflux_i18n::t!(
                        "settings.hooks.validation.lua_path_required"
                    ));
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

        let env_denylist = Self::parse_env_denylist(&env_denylist_text);

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
            env_denylist,
            timeout_ms,
            execution_mode: selected.execution_mode,
            ready_signal: if selected_kind == HookKindSelection::Lua || ready_signal.is_empty() {
                None
            } else {
                Some(ready_signal)
            },
            on_failure,
        };

        Ok(Some((hook_id, hook)))
    }

    fn persist_hooks(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> bool {
        let app_state = self.app_state.read(cx);
        let runtime = app_state.storage_runtime();
        let protected_ids = app_state.protected_hook_row_ids();
        let desired = self
            .hook_definitions
            .iter()
            .map(|(name, definition)| HookDefinitionSave {
                id: definition.id.clone(),
                name: name.clone(),
                hook: definition.hook.clone(),
            })
            .collect::<Vec<_>>();
        let hooks = match commit_saved_hook_definitions(
            &mut self.hook_definitions,
            dbflux_app::config_loader::save_hook_definitions(runtime, &desired, &protected_ids),
        ) {
            Ok(hooks) => hooks,
            Err(e) => {
                report_error(
                    UserFacingError::new(
                        ErrorKind::Storage,
                        dbflux_i18n::t!("settings.hooks.error.save"),
                    )
                    .with_cause(e.to_string()),
                    cx,
                );
                return false;
            }
        };

        self.app_state.update(cx, move |state, _cx| {
            state.set_hook_definitions(hooks);
        });
        true
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
        self.hook_form_field = HookFormField::HookId;
        self.hook_editing_field = false;
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
        self.input_hook_env_denylist
            .update(cx, |input, cx| input.set_value("", window, cx));
        self.input_hook_timeout
            .update(cx, |input, cx| input.set_value("", window, cx));

        self.hook_failure_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_selected_index(Some(0), cx);
        });

        cx.notify();
    }

    pub(super) fn load_hook_values_without_focus(
        &mut self,
        hook_id: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(hook) = self
            .hook_definitions
            .get(hook_id)
            .map(|definition| definition.hook.clone())
        else {
            return;
        };

        self.editing_hook_id = Some(hook_id.to_string());
        self.hook_selected_id = Some(hook_id.to_string());
        let ids = self.hook_sorted_ids();
        self.hook_list_idx = ids.iter().position(|id| id == hook_id);
        self.hook_enabled = hook.enabled;
        self.hook_inherit_env = hook.inherit_env;
        self.apply_hook_editor_selection(
            super::hooks_section::HookEditorSelectionState::from_saved_hook(
                &hook.kind,
                hook.execution_mode,
            ),
            cx,
        );

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
        self.input_hook_env_denylist.update(cx, |input, cx| {
            input.set_value(hook.env_denylist.join(", "), window, cx)
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

    pub(super) fn load_hook_into_form(
        &mut self,
        hook_id: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(hook) = self
            .hook_definitions
            .get(hook_id)
            .map(|definition| definition.hook.clone())
        else {
            return;
        };

        self.editing_hook_id = Some(hook_id.to_string());
        self.hook_selected_id = Some(hook_id.to_string());
        let ids = self.hook_sorted_ids();
        self.hook_list_idx = ids.iter().position(|id| id == hook_id);
        self.hook_enabled = hook.enabled;
        self.hook_inherit_env = hook.inherit_env;
        self.apply_hook_editor_selection(
            super::hooks_section::HookEditorSelectionState::from_saved_hook(
                &hook.kind,
                hook.execution_mode,
            ),
            cx,
        );

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
        self.input_hook_env_denylist.update(cx, |input, cx| {
            input.set_value(hook.env_denylist.join(", "), window, cx)
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
                let toast_msg = error.to_string();
                Toast::error(toast_msg.clone())
                    .meta_right(now_hms())
                    .action(copy_action(toast_msg))
                    .push(cx);
                return;
            }
        };

        let duplicate = self.hook_definitions.contains_key(&hook_id)
            && self.editing_hook_id.as_deref() != Some(hook_id.as_str());

        if duplicate {
            let msg = hooks_duplicate_id(&hook_id);
            Toast::error(msg.clone())
                .meta_right(now_hms())
                .action(copy_action(msg))
                .push(cx);
            return;
        }

        let saved_definitions = self.hook_definitions.clone();
        update_hook_definition(
            &mut self.hook_definitions,
            self.editing_hook_id.as_deref(),
            hook_id.clone(),
            hook,
        );
        if !self.persist_hooks(window, cx) {
            self.hook_definitions = saved_definitions;
            return;
        }

        self.load_hook_into_form(&hook_id, window, cx);
        self.hook_focus = HookFocus::Form;
        Toast::success(dbflux_i18n::t!("settings.hooks.toast.saved"))
            .meta_right(now_hms())
            .push(cx);
    }

    pub(super) fn request_delete_hook(&mut self, hook_id: String, cx: &mut Context<Self>) {
        self.pending_delete_hook_id = Some(hook_id);
        cx.notify();
    }

    pub(super) fn confirm_delete_hook(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(hook_id) = self.pending_delete_hook_id.take() else {
            return;
        };

        let saved_definitions = self.hook_definitions.clone();
        self.hook_definitions.remove(&hook_id);

        if self.editing_hook_id.as_deref() == Some(hook_id.as_str()) {
            self.clear_hook_form(window, cx);
        }

        if self.hook_selected_id.as_deref() == Some(hook_id.as_str()) {
            self.hook_selected_id = None;
            self.hook_list_idx = None;
        }

        if !self.persist_hooks(window, cx) {
            self.hook_definitions = saved_definitions;
            return;
        }
        Toast::success(dbflux_i18n::t!("settings.hooks.toast.deleted"))
            .meta_right(now_hms())
            .push(cx);
        cx.notify();
    }

    pub(super) fn cancel_delete_hook(&mut self, cx: &mut Context<Self>) {
        self.pending_delete_hook_id = None;
        cx.notify();
    }

    pub(super) fn request_delete_protected_row(&mut self, row_id: String, cx: &mut Context<Self>) {
        self.pending_delete_protected_row_id = Some(row_id);
        cx.notify();
    }

    pub(super) fn confirm_delete_protected_row(&mut self, cx: &mut Context<Self>) {
        let Some(row_id) = self.pending_delete_protected_row_id.take() else {
            return;
        };

        let result = self
            .app_state
            .update(cx, |state, _cx| state.delete_protected_hook_row(&row_id));

        if let Err(error) = result {
            report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!("settings.hooks.error.delete_unreadable"),
                )
                .with_cause(error.to_string()),
                cx,
            );
            return;
        }

        Toast::success(dbflux_i18n::t!("settings.hooks.toast.unreadable_deleted"))
            .meta_right(now_hms())
            .push(cx);
        cx.notify();
    }

    pub(super) fn cancel_delete_protected_row(&mut self, cx: &mut Context<Self>) {
        self.pending_delete_protected_row_id = None;
        cx.notify();
    }

    fn parse_env_denylist(text: &str) -> Vec<String> {
        text.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
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
                return Err(hooks_env_pair_invalid(pair));
            };

            let key = key.trim();
            if key.is_empty() {
                return Err(dbflux_i18n::t!("settings.hooks.validation.env_key_empty"));
            }

            env.insert(key.to_string(), value.to_string());
        }

        Ok(env)
    }

    pub(super) fn render_hooks_section(&mut self, cx: &mut Context<Self>) -> impl IntoElement {
        layout::split_section_shell(
            cx.theme().border,
            dbflux_components::composites::page_header(
                dbflux_i18n::t!("settings.hooks.header.title"),
                dbflux_i18n::t!("settings.hooks.header.subtitle"),
                cx,
            ),
            self.render_hooks_list(cx),
            self.render_hook_form(cx),
        )
    }

    fn render_hooks_list(&mut self, cx: &mut Context<Self>) -> impl IntoElement + use<> {
        let hook_ids = self.hook_sorted_ids();
        let list_focused = self.content_focused && self.hook_focus == HookFocus::List;
        let is_new_button_focused = list_focused && self.hook_list_idx.is_none();

        if let Some(scroll_idx) = self.hook_pending_scroll_idx.take() {
            self.hook_list_scroll_handle.scroll_to_item(scroll_idx);
        }

        let toolbar = layout::master_list_toolbar(vec![
            Button::new("new-hook", dbflux_i18n::t!("settings.hooks.list.new"))
                .small()
                .primary()
                .icon(AppIcon::Plus)
                .focused(is_new_button_focused)
                .on_click(cx.listener(|this, _, window, cx| {
                    this.hook_focus = HookFocus::Form;
                    this.clear_hook_form(window, cx);
                }))
                .into_any_element(),
        ]);

        let rows: Vec<AnyElement> = hook_ids
            .into_iter()
            .enumerate()
            .map(|(idx, hook_id)| {
                let selected = self.editing_hook_id.as_deref() == Some(hook_id.as_str());
                let focused = list_focused && self.hook_list_idx == Some(idx);
                let hook = self.hook_definitions.get(&hook_id);
                let icon = match hook.map(|hook| &hook.kind) {
                    Some(HookKind::Script { .. }) => AppIcon::FileCode,
                    Some(HookKind::Lua { .. }) => AppIcon::Code,
                    _ => AppIcon::SquareTerminal,
                };
                let detail = hook.map(|hook| SharedString::from(hook.summary()));
                let hook_id_for_click = hook_id.clone();

                layout::master_list_row(
                    SharedString::from(format!("hook-item-{}", hook_id)),
                    layout::MasterRow {
                        icon: Some(icon),
                        title: hook_id.clone().into(),
                        detail,
                        trailing: None,
                    },
                    selected,
                    focused,
                    cx,
                )
                .on_click(cx.listener(move |this, _, window, cx| {
                    this.hook_focus = HookFocus::Form;
                    this.load_hook_into_form(&hook_id_for_click, window, cx);
                }))
                .into_any_element()
            })
            .collect();

        let empty = rows.is_empty();
        let theme = cx.theme();

        div()
            .w(SettingsMetrics::LIST_WIDTH)
            .h_full()
            .min_h_0()
            .flex_shrink_0()
            .border_r_1()
            .border_color(theme.border)
            .flex()
            .flex_col()
            .child(toolbar)
            .child(
                div()
                    .id("hooks-list-scroll")
                    .flex_1()
                    .min_h_0()
                    .overflow_scroll()
                    .track_scroll(&self.hook_list_scroll_handle)
                    .flex()
                    .flex_col()
                    .when(empty, |list| {
                        list.child(layout::master_list_empty(dbflux_i18n::t!(
                            "settings.hooks.list.empty"
                        )))
                    })
                    .children(rows)
                    .child(self.render_protected_hook_rows(cx)),
            )
    }

    fn render_protected_hook_rows(&self, cx: &mut Context<Self>) -> impl IntoElement + use<> {
        let protected: Vec<(String, String)> = self
            .app_state
            .read(cx)
            .protected_hook_rows()
            .iter()
            .map(|row| {
                let label = row.row_name.clone().unwrap_or_else(|| row.row_id.clone());
                (row.row_id.clone(), label)
            })
            .collect();

        div()
            .flex()
            .flex_col()
            .when(!protected.is_empty(), |container| {
                container
                    .child(
                        div()
                            .px(SettingsMetrics::LIST_ROW_PADDING_X)
                            .pt(SettingsMetrics::LIST_ROW_PADDING_Y)
                            .child(Text::label(dbflux_i18n::t!(
                                "settings.hooks.list.unreadable.title"
                            ))),
                    )
                    .children(protected.into_iter().map(|(row_id, label)| {
                        let row_id_for_click = row_id.clone();

                        layout::master_list_row(
                            SharedString::from(format!("protected-hook-item-{}", row_id)),
                            layout::MasterRow {
                                icon: Some(AppIcon::TriangleAlert),
                                title: label.into(),
                                detail: Some(
                                    dbflux_i18n::t!("settings.hooks.list.unreadable.hint").into(),
                                ),
                                trailing: Some(
                                    Button::new(
                                        SharedString::from(format!("delete-protected-{}", row_id)),
                                        dbflux_i18n::t!("hooks.action.delete"),
                                    )
                                    .small()
                                    .danger()
                                    .icon(AppIcon::Delete)
                                    .on_click(cx.listener(move |this, _, _, cx| {
                                        this.request_delete_protected_row(
                                            row_id_for_click.clone(),
                                            cx,
                                        );
                                    }))
                                    .into_any_element(),
                                ),
                            },
                            false,
                            false,
                            cx,
                        )
                    }))
            })
    }

    fn is_cursor_on(&self, field: HookFormField) -> bool {
        self.content_focused
            && self.hook_focus == HookFocus::Form
            && self.hook_form_field == field
            && !self.hook_editing_field
    }

    /// Keyboard cursor frame around a form control; a press moves the cursor
    /// to `field` and focuses its input when it has one.
    fn hook_field_frame(
        &self,
        field: HookFormField,
        _tint: Hsla,
        child: impl IntoElement,
        cx: &mut Context<Self>,
    ) -> Div {
        layout::cursor_ring(self.is_cursor_on(field), child, cx).on_mouse_down(
            MouseButton::Left,
            cx.listener(move |this, _, window, cx| {
                this.switching_input = true;
                this.hook_focus = HookFocus::Form;
                this.hook_form_field = field;
                this.hook_focus_current_field(window, cx);
                cx.notify();
            }),
        )
    }

    /// Text input row of the hook form.
    fn hook_input_row(
        &self,
        label: String,
        input: &Entity<InputState>,
        field: HookFormField,
        width: Option<Pixels>,
        help: Option<String>,
        cx: &mut Context<Self>,
    ) -> Div {
        let control = Input::new(input)
            .id(SharedString::from(format!("hook-{}", hook_field_id(field))))
            .aria_label(label.clone());

        layout::form_row(
            label,
            layout::field_frame(self.is_cursor_on(field), width, true, control, cx).on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, _, window, cx| {
                    this.switching_input = true;
                    this.hook_focus = HookFocus::Form;
                    this.hook_form_field = field;
                    this.hook_focus_current_field(window, cx);
                    cx.notify();
                }),
            ),
            help.map(SharedString::from),
        )
    }

    /// Checkbox row of the hook form, framed for the keyboard cursor.
    fn hook_checkbox_row(
        &self,
        id: &'static str,
        label: String,
        checked: bool,
        field: HookFormField,
        setter: fn(&mut Self, bool),
        cx: &mut Context<Self>,
    ) -> Div {
        layout::check_row(
            layout::cursor_ring(
                self.is_cursor_on(field),
                Checkbox::new(id)
                    .checked(checked)
                    .label(label)
                    .on_click(cx.listener(move |this, value: &bool, _, cx| {
                        this.hook_focus = HookFocus::Form;
                        this.hook_form_field = field;
                        setter(this, *value);
                        cx.notify();
                    })),
                cx,
            ),
            None,
        )
    }

    fn render_hook_lua_capability_rows(&self, cx: &mut Context<Self>) -> Div {
        div()
            .flex()
            .flex_col()
            .child(self.hook_checkbox_row(
                "hook-lua-logging",
                dbflux_i18n::t!("settings.hooks.form.capability.logging"),
                self.hook_lua_logging,
                lua_capability_field(LuaCapabilityRow::Logging),
                |this, value| this.hook_lua_logging = value,
                cx,
            ))
            .child(self.hook_checkbox_row(
                "hook-lua-env-read",
                dbflux_i18n::t!("settings.hooks.form.capability.env_read"),
                self.hook_lua_env_read,
                lua_capability_field(LuaCapabilityRow::EnvRead),
                |this, value| this.hook_lua_env_read = value,
                cx,
            ))
            .child(self.hook_checkbox_row(
                "hook-lua-connection-metadata",
                dbflux_i18n::t!("settings.hooks.form.capability.connection_metadata"),
                self.hook_lua_connection_metadata,
                lua_capability_field(LuaCapabilityRow::ConnectionMetadata),
                |this, value| this.hook_lua_connection_metadata = value,
                cx,
            ))
            .child(self.hook_checkbox_row(
                "hook-lua-process-run",
                dbflux_i18n::t!("settings.hooks.form.capability.process_run"),
                self.hook_lua_process_run,
                lua_capability_field(LuaCapabilityRow::ProcessRun),
                |this, value| this.hook_lua_process_run = value,
                cx,
            ))
    }

    /// Current failure policy, read from the failure selector's value.
    fn selected_failure_mode(&self, cx: &App) -> HookFailureMode {
        match self
            .hook_failure_dropdown
            .read(cx)
            .selected_value()
            .map(|value| value.to_string())
            .as_deref()
        {
            Some("warn") => HookFailureMode::Warn,
            Some("ignore") => HookFailureMode::Ignore,
            _ => HookFailureMode::Disconnect,
        }
    }

    fn set_failure_mode(&mut self, mode: HookFailureMode, cx: &mut Context<Self>) {
        let index = match mode {
            HookFailureMode::Disconnect => 0,
            HookFailureMode::Warn => 1,
            HookFailureMode::Ignore => 2,
        };

        self.hook_failure_dropdown.update(cx, |dropdown, cx| {
            dropdown.set_selected_index(Some(index), cx);
        });
    }

    fn render_hook_kind_selector(&self, cx: &mut Context<Self>) -> Div {
        let entity = cx.entity();
        let active = match self.hook_kind_selection {
            HookKindSelection::Command => "command",
            HookKindSelection::Script => "script",
            HookKindSelection::Lua => "lua",
        };

        let mut items = vec![
            SegmentedItem::new("command", dbflux_i18n::t!("hooks.kind.command"))
                .icon(AppIcon::SquareTerminal),
            SegmentedItem::new("script", dbflux_i18n::t!("hooks.kind.script"))
                .icon(AppIcon::FileCode),
        ];

        if cfg!(feature = "lua") {
            items.push(
                SegmentedItem::new("lua", dbflux_i18n::t!("hooks.kind.lua")).icon(AppIcon::Code),
            );
        }

        let control = SegmentedControl::new(items, active, move |selected, _window, cx| {
            let kind = match selected.as_ref() {
                "script" => HookKindSelection::Script,
                "lua" => HookKindSelection::Lua,
                _ => HookKindSelection::Command,
            };

            entity.update(cx, |this, cx| {
                this.hook_focus = HookFocus::Form;
                this.set_hook_kind_dropdown(kind, cx);
                this.validate_form_field();
                cx.notify();
            });
        });

        let cursor = self.content_focused
            && self.hook_focus == HookFocus::Form
            && is_kind_form_field(self.hook_form_field);

        layout::form_row(
            dbflux_i18n::t!("settings.hooks.form.kind"),
            div().flex().child(layout::cursor_ring(cursor, control, cx)),
            None,
        )
    }

    fn render_hook_execution_mode_selector(&self, cx: &mut Context<Self>) -> Div {
        let entity = cx.entity();
        let active = match self.hook_execution_mode {
            HookExecutionMode::Blocking => "blocking",
            HookExecutionMode::Detached => "detached",
        };

        let control = SegmentedControl::new(
            vec![
                SegmentedItem::new("blocking", dbflux_i18n::t!("hooks.execution.blocking")),
                SegmentedItem::new("detached", dbflux_i18n::t!("hooks.execution.detached")),
            ],
            active,
            move |selected, _window, cx| {
                let mode = if selected.as_ref() == "detached" {
                    HookExecutionMode::Detached
                } else {
                    HookExecutionMode::Blocking
                };

                entity.update(cx, |this, cx| {
                    this.hook_focus = HookFocus::Form;
                    this.hook_form_field = HookFormField::ExecutionMode;
                    this.set_hook_execution_mode_dropdown(mode, cx);
                    this.validate_form_field();
                    cx.notify();
                });
            },
        );

        layout::form_row(
            dbflux_i18n::t!("settings.hooks.form.execution_mode"),
            div().flex().child(layout::cursor_ring(
                self.is_cursor_on(HookFormField::ExecutionMode),
                control,
                cx,
            )),
            Some(dbflux_i18n::t!("settings.hooks.form.execution_mode_hint").into()),
        )
    }

    fn render_hook_failure_selector(&self, cx: &mut Context<Self>) -> Div {
        let entity = cx.entity();
        let active = match self.selected_failure_mode(cx) {
            HookFailureMode::Disconnect => "disconnect",
            HookFailureMode::Warn => "warn",
            HookFailureMode::Ignore => "ignore",
        };

        let control = SegmentedControl::new(
            vec![
                SegmentedItem::new("disconnect", dbflux_i18n::t!("hooks.failure.disconnect"))
                    .icon(AppIcon::Unplug),
                SegmentedItem::new("warn", dbflux_i18n::t!("hooks.failure.warn"))
                    .icon(AppIcon::TriangleAlert),
                SegmentedItem::new("ignore", dbflux_i18n::t!("hooks.failure.ignore"))
                    .icon(AppIcon::CircleX),
            ],
            active,
            move |selected, _window, cx| {
                let mode = match selected.as_ref() {
                    "warn" => HookFailureMode::Warn,
                    "ignore" => HookFailureMode::Ignore,
                    _ => HookFailureMode::Disconnect,
                };

                entity.update(cx, |this, cx| {
                    this.hook_focus = HookFocus::Form;
                    this.hook_form_field = HookFormField::OnFailure;
                    this.set_failure_mode(mode, cx);
                    cx.notify();
                });
            },
        );

        layout::form_row(
            dbflux_i18n::t!("settings.hooks.form.on_failure"),
            div().flex().child(layout::cursor_ring(
                self.is_cursor_on(HookFormField::OnFailure),
                control,
                cx,
            )),
            None,
        )
    }

    fn render_hook_form(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme().clone();

        let hook_kind = self.selected_hook_kind(cx);
        let is_script = hook_kind == HookKindSelection::Script;
        let is_lua = hook_kind == HookKindSelection::Lua;
        let uses_script_source = is_script || is_lua;
        let warnings = self.hook_form_warnings(cx);
        let preview = self.hook_form_preview(cx);
        let default_interpreter = self.default_script_interpreter_label(cx);
        let tint = ChromeColors::tint(&theme);
        let detached =
            !is_lua && self.selected_hook_execution_mode(cx) == HookExecutionMode::Detached;

        let command_rows = (hook_kind == HookKindSelection::Command).then(|| {
            div()
                .flex()
                .flex_col()
                .child(self.hook_input_row(
                    dbflux_i18n::t!("settings.hooks.form.command"),
                    &self.input_hook_command,
                    HookFormField::Command,
                    Some(HOOK_FIELD_WIDTH),
                    None,
                    cx,
                ))
                .child(self.hook_input_row(
                    dbflux_i18n::t!("settings.hooks.form.args"),
                    &self.input_hook_args,
                    HookFormField::Arguments,
                    None,
                    Some(dbflux_i18n::t!("settings.hooks.form.args_hint")),
                    cx,
                ))
        });

        let script_rows = uses_script_source.then(|| {
            let open_buttons = layout::inline_controls()
                .child(
                    Button::new(
                        "open-script-app",
                        dbflux_i18n::t!("settings.hooks.form.open_in_app"),
                    )
                    .small()
                    .secondary()
                    .icon(AppIcon::FileCode)
                    .focused(self.is_cursor_on(HookFormField::OpenInApp))
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.open_script_in_app(window, cx);
                    })),
                )
                .child(
                    Button::new(
                        "open-script-editor",
                        dbflux_i18n::t!("settings.hooks.form.open_in_editor"),
                    )
                    .small()
                    .secondary()
                    .icon(AppIcon::ExternalLink)
                    .focused(self.is_cursor_on(HookFormField::OpenInEditor))
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.open_script_in_default_editor(window, cx);
                    })),
                );

            div()
                .flex()
                .flex_col()
                .when(is_script, |rows| {
                    rows.child(layout::form_row(
                        dbflux_i18n::t!("settings.hooks.form.language"),
                        self.hook_field_frame(
                            HookFormField::ScriptLanguage,
                            tint,
                            div()
                                .w(SettingsMetrics::SELECT_WIDTH)
                                .child(self.script_language_dropdown.clone()),
                            cx,
                        )
                        .w(SettingsMetrics::SELECT_WIDTH),
                        None,
                    ))
                })
                .child(self.hook_input_row(
                    dbflux_i18n::t!("settings.hooks.form.file_path"),
                    &self.input_hook_script_file_path,
                    HookFormField::FilePath,
                    None,
                    Some(dbflux_i18n::t!("settings.hooks.form.file_path_hint")),
                    cx,
                ))
                .child(layout::form_row(String::new(), open_buttons, None))
                .when(is_script, |rows| {
                    rows.child(self.hook_input_row(
                        dbflux_i18n::t!("settings.hooks.form.interpreter"),
                        &self.input_hook_interpreter,
                        HookFormField::Interpreter,
                        Some(HOOK_FIELD_WIDTH),
                        Some(hooks_form_interpreter_hint(&default_interpreter)),
                        cx,
                    ))
                })
                .when(is_lua, |rows| {
                    rows.child(layout::form_row(
                        dbflux_i18n::t!("settings.hooks.form.capabilities"),
                        self.render_hook_lua_capability_rows(cx),
                        Some(
                            dbflux_i18n::t!("settings.hooks.form.capability.process_run_hint")
                                .into(),
                        ),
                    ))
                })
        });

        let process_rows = (!is_lua).then(|| {
            div()
                .flex()
                .flex_col()
                .child(self.render_hook_execution_mode_selector(cx))
                .when(detached, |rows| {
                    rows.child(self.hook_input_row(
                        dbflux_i18n::t!("settings.hooks.form.ready_signal"),
                        &self.input_hook_ready_signal,
                        HookFormField::ReadySignal,
                        Some(HOOK_FIELD_WIDTH),
                        Some(dbflux_i18n::t!("settings.hooks.form.ready_signal_hint")),
                        cx,
                    ))
                })
                .child(self.hook_input_row(
                    dbflux_i18n::t!("settings.hooks.form.cwd"),
                    &self.input_hook_cwd,
                    HookFormField::WorkingDirectory,
                    None,
                    None,
                    cx,
                ))
                .child(self.hook_input_row(
                    dbflux_i18n::t!("settings.hooks.form.env"),
                    &self.input_hook_env,
                    HookFormField::Environment,
                    None,
                    Some(dbflux_i18n::t!("settings.hooks.form.env_hint")),
                    cx,
                ))
                .child(self.hook_input_row(
                    dbflux_i18n::t!("settings.hooks.form.env_denylist"),
                    &self.input_hook_env_denylist,
                    HookFormField::EnvDenylist,
                    None,
                    Some(dbflux_i18n::t!("settings.hooks.form.env_denylist_hint")),
                    cx,
                ))
        });

        let timeout = layout::form_row(
            dbflux_i18n::t!("settings.hooks.form.timeout"),
            layout::field_frame(
                self.is_cursor_on(HookFormField::Timeout),
                Some(SettingsMetrics::NUMBER_FIELD_WIDTH),
                true,
                Input::new(&self.input_hook_timeout)
                    .id("hook-timeout")
                    .aria_label(dbflux_i18n::t!("settings.hooks.form.timeout"))
                    .suffix(
                        Text::code(dbflux_i18n::t!("settings.general.unit.milliseconds"))
                            .muted_foreground(),
                    ),
                cx,
            )
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, window, cx| {
                    this.switching_input = true;
                    this.hook_focus = HookFocus::Form;
                    this.hook_form_field = HookFormField::Timeout;
                    this.hook_focus_current_field(window, cx);
                    cx.notify();
                }),
            ),
            None,
        );

        let resolved_command = div()
            .relative()
            .px(SettingsMetrics::LIST_ROW_PADDING_Y)
            .py(SettingsMetrics::LIST_ROW_PADDING_Y)
            .font_family(AppFonts::MONO)
            .text_color(ChromeColors::strong(&theme))
            .child(
                Chamfer::new(ChamferCut::CONTROL)
                    .fill(theme.background)
                    .border(theme.border),
            )
            .child(format!("$ {preview}"));

        layout::sticky_form_shell(
            dbflux_components::composites::section_header(
                dbflux_i18n::t!("settings.hooks.form.group.hook"),
                Some(AppIcon::SquareTerminal.into()),
                cx,
            ),
            div()
                .flex()
                .flex_col()
                .on_mouse_down(
                    MouseButton::Left,
                    cx.listener(|this, _, _, _| {
                        this.switching_input = true;
                    }),
                )
                .child(self.hook_input_row(
                    dbflux_i18n::t!("settings.hooks.form.id"),
                    &self.input_hook_id,
                    HookFormField::HookId,
                    Some(HOOK_FIELD_WIDTH),
                    None,
                    cx,
                ))
                .child(self.render_hook_kind_selector(cx))
                .children(command_rows)
                .children(script_rows)
                .children(process_rows)
                .child(timeout)
                .child(self.render_hook_failure_selector(cx))
                .child(self.hook_checkbox_row(
                    "hook-enabled",
                    dbflux_i18n::t!("settings.hooks.form.enabled"),
                    self.hook_enabled,
                    HookFormField::Enabled,
                    |this, value| this.hook_enabled = value,
                    cx,
                ))
                .when(!is_lua, |form| {
                    form.child(self.hook_checkbox_row(
                        "hook-inherit-env",
                        dbflux_i18n::t!("settings.hooks.form.inherit_env"),
                        self.hook_inherit_env,
                        HookFormField::InheritEnv,
                        |this, value| this.hook_inherit_env = value,
                        cx,
                    ))
                })
                .child(dbflux_components::composites::section_header(
                    dbflux_i18n::t!("settings.hooks.form.resolved_command"),
                    Some(AppIcon::Code.into()),
                    cx,
                ))
                .child(resolved_command)
                .children(warnings.into_iter().map(|warning| {
                    div()
                        .mt(FormMetrics::INLINE_GAP)
                        .child(BannerBlock::new(BannerVariant::Warning, warning))
                })),
            None,
            &theme,
        )
    }

    /// Delete, on the left of the footer, for a saved hook.
    pub(super) fn render_hook_footer_leading_actions(
        &self,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        let hook_id = self.editing_hook_id.clone()?;

        Some(
            Button::new("delete-hook", dbflux_i18n::t!("hooks.action.delete"))
                .small()
                .danger()
                .icon(AppIcon::Delete)
                .focused(self.is_cursor_on(HookFormField::DeleteButton))
                .on_click(cx.listener(move |this, _, _, cx| {
                    this.request_delete_hook(hook_id.clone(), cx);
                }))
                .into_any_element(),
        )
    }

    pub(super) fn render_hook_footer_actions(&self, cx: &mut Context<Self>) -> AnyElement {
        let editing = self.editing_hook_id.is_some();

        Button::new(
            "save-hook",
            if editing {
                dbflux_i18n::t!("hooks.action.update")
            } else {
                dbflux_i18n::t!("hooks.action.create")
            },
        )
        .small()
        .primary()
        .icon(AppIcon::Check)
        .kbd("Ctrl S")
        .focused(self.is_cursor_on(HookFormField::SaveButton))
        .on_click(cx.listener(|this, _, window, cx| {
            this.save_hook(window, cx);
        }))
        .into_any_element()
    }

    pub(super) fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.pending_delete_hook_id.is_some() {
            return;
        }

        if !self.content_focused() && !self.editing_field() {
            return;
        }

        if self.handle_editing_keys(event, window, cx) {
            return;
        }

        let chord = key_chord_from_gpui(&event.keystroke);
        let ids = self.hook_sorted_ids();
        self.hook_sync_selection_from_ids(&ids);

        match self.hook_focus {
            HookFocus::List => match (chord.key.as_str(), chord.modifiers) {
                ("j", modifiers) | ("down", modifiers)
                    if modifiers == Modifiers::none() && !ids.is_empty() =>
                {
                    let next = self
                        .hook_list_idx
                        .unwrap_or(0)
                        .saturating_add(1)
                        .min(ids.len() - 1);
                    self.hook_select_index(next, window, cx);
                    cx.notify();
                }
                ("k", modifiers) | ("up", modifiers)
                    if modifiers == Modifiers::none() && !ids.is_empty() =>
                {
                    let prev = self.hook_list_idx.unwrap_or(0).saturating_sub(1);
                    self.hook_select_index(prev, window, cx);
                    cx.notify();
                }
                ("g", modifiers) if modifiers == Modifiers::none() && !ids.is_empty() => {
                    self.hook_select_index(0, window, cx);
                    cx.notify();
                }
                ("g", modifiers) if modifiers == Modifiers::shift() && !ids.is_empty() => {
                    self.hook_select_index(ids.len() - 1, window, cx);
                    cx.notify();
                }
                ("n", modifiers) if modifiers == Modifiers::none() => {
                    self.hook_focus = HookFocus::Form;
                    self.clear_hook_form(window, cx);
                    self.hook_form_field = HookFormField::HookId;
                    self.hook_editing_field = false;
                    cx.notify();
                }
                ("d", modifiers) if modifiers == Modifiers::none() => {
                    if let Some(hook_id) = self.hook_selected_id.clone() {
                        self.request_delete_hook(hook_id, cx);
                    }
                }
                ("l", modifiers) | ("right", modifiers) | ("enter", modifiers)
                    if modifiers == Modifiers::none() =>
                {
                    self.enter_form(window, cx);
                    cx.notify();
                }
                _ => {}
            },
            HookFocus::Form => match (chord.key.as_str(), chord.modifiers) {
                ("escape", modifiers) if modifiers == Modifiers::none() => {
                    self.exit_form(window, cx);
                    cx.notify();
                }
                ("j", modifiers) | ("down", modifiers) if modifiers == Modifiers::none() => {
                    self.move_down();
                    cx.notify();
                }
                ("k", modifiers) | ("up", modifiers) if modifiers == Modifiers::none() => {
                    self.move_up();
                    cx.notify();
                }
                ("h", modifiers) if modifiers == Modifiers::none() => {
                    self.exit_form(window, cx);
                    cx.notify();
                }
                ("left", modifiers) if modifiers == Modifiers::none() => {
                    self.move_left();
                    cx.notify();
                }
                ("l", modifiers) | ("right", modifiers) if modifiers == Modifiers::none() => {
                    self.move_right();
                    cx.notify();
                }
                ("enter", modifiers) if modifiers == Modifiers::none() => {
                    self.activate_current_field(window, cx);
                    cx.notify();
                }
                ("tab", modifiers) if modifiers == Modifiers::none() => {
                    self.tab_next();
                    cx.notify();
                }
                ("tab", modifiers) if modifiers == Modifiers::shift() => {
                    self.tab_prev();
                    cx.notify();
                }
                ("g", modifiers) if modifiers == Modifiers::none() => {
                    self.move_first();
                    cx.notify();
                }
                ("g", modifiers) if modifiers == Modifiers::shift() => {
                    self.move_last();
                    cx.notify();
                }
                _ => {}
            },
        }
    }

    pub(super) fn hook_focus_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.hook_editing_field = true;

        match self.hook_form_field {
            HookFormField::HookId => {
                self.input_hook_id
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            HookFormField::Command => {
                self.input_hook_command
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            HookFormField::Arguments => {
                self.input_hook_args
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            HookFormField::FilePath => {
                self.input_hook_script_file_path
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            HookFormField::Interpreter => {
                self.input_hook_interpreter
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            HookFormField::ReadySignal => {
                self.input_hook_ready_signal
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            HookFormField::WorkingDirectory => {
                self.input_hook_cwd
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            HookFormField::Environment => {
                self.input_hook_env
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            HookFormField::EnvDenylist => {
                self.input_hook_env_denylist
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            HookFormField::Timeout => {
                self.input_hook_timeout
                    .update(cx, |state, cx| state.focus(window, cx));
            }
            _ => {
                self.hook_editing_field = false;
            }
        }
    }

    pub(super) fn hook_activate_current_field(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match self.hook_form_field {
            HookFormField::KindCommand => {
                self.set_hook_kind_dropdown(HookKindSelection::Command, cx);
                self.validate_form_field();
            }
            HookFormField::KindScript => {
                self.set_hook_kind_dropdown(HookKindSelection::Script, cx);
                self.validate_form_field();
            }
            #[cfg(feature = "lua")]
            HookFormField::KindLua => {
                self.set_hook_kind_dropdown(HookKindSelection::Lua, cx);
                self.validate_form_field();
            }
            HookFormField::ExecutionMode => {
                let new_mode = match self.hook_execution_mode {
                    HookExecutionMode::Blocking => HookExecutionMode::Detached,
                    HookExecutionMode::Detached => HookExecutionMode::Blocking,
                };
                self.set_hook_execution_mode_dropdown(new_mode, cx);
                self.validate_form_field();
            }
            HookFormField::OnFailure => {
                let next = match self.selected_failure_mode(cx) {
                    HookFailureMode::Disconnect => HookFailureMode::Warn,
                    HookFailureMode::Warn => HookFailureMode::Ignore,
                    HookFailureMode::Ignore => HookFailureMode::Disconnect,
                };
                self.set_failure_mode(next, cx);
            }
            HookFormField::Enabled => {
                self.hook_enabled = !self.hook_enabled;
            }
            HookFormField::InheritEnv => {
                self.hook_inherit_env = !self.hook_inherit_env;
            }
            #[cfg(feature = "lua")]
            HookFormField::LuaLogging => {
                self.hook_lua_logging = !self.hook_lua_logging;
            }
            #[cfg(feature = "lua")]
            HookFormField::LuaEnvRead => {
                self.hook_lua_env_read = !self.hook_lua_env_read;
            }
            #[cfg(feature = "lua")]
            HookFormField::LuaConnectionMetadata => {
                self.hook_lua_connection_metadata = !self.hook_lua_connection_metadata;
            }
            #[cfg(feature = "lua")]
            HookFormField::LuaProcessRun => {
                self.hook_lua_process_run = !self.hook_lua_process_run;
            }
            HookFormField::OpenInApp => {
                self.open_script_in_app(window, cx);
            }
            HookFormField::OpenInEditor => {
                self.open_script_in_default_editor(window, cx);
            }
            HookFormField::SaveButton => {
                self.save_hook(window, cx);
            }
            HookFormField::DeleteButton => {
                if let Some(hook_id) = self.editing_hook_id.clone() {
                    self.request_delete_hook(hook_id, cx);
                }
            }
            field if Self::is_input_field(field) => {
                self.hook_focus_current_field(window, cx);
            }
            _ => {}
        }
    }
}

#[cfg(feature = "lua")]
fn is_kind_form_field(field: HookFormField) -> bool {
    matches!(
        field,
        HookFormField::KindCommand | HookFormField::KindScript | HookFormField::KindLua
    )
}

#[cfg(not(feature = "lua"))]
fn is_kind_form_field(field: HookFormField) -> bool {
    matches!(
        field,
        HookFormField::KindCommand | HookFormField::KindScript
    )
}

/// Width of the short text fields of the hook form (id, command). (300 px)
const HOOK_FIELD_WIDTH: Pixels = px(300.0);

/// Element-id suffix of a hook form field.
fn hook_field_id(field: HookFormField) -> &'static str {
    match field {
        HookFormField::HookId => "id",
        HookFormField::Command => "command",
        HookFormField::Arguments => "arguments",
        HookFormField::FilePath => "file-path",
        HookFormField::Interpreter => "interpreter",
        HookFormField::ReadySignal => "ready-signal",
        HookFormField::WorkingDirectory => "cwd",
        HookFormField::Environment => "env",
        HookFormField::EnvDenylist => "env-denylist",
        HookFormField::Timeout => "timeout",
        _ => "control",
    }
}

/// Lua capability checkboxes of the hook form.
#[derive(Clone, Copy)]
enum LuaCapabilityRow {
    Logging,
    EnvRead,
    ConnectionMetadata,
    ProcessRun,
}

/// Form field of a Lua capability checkbox. Builds without Lua have no
/// keyboard stops for them, so they map to the Enabled row.
#[cfg(feature = "lua")]
fn lua_capability_field(row: LuaCapabilityRow) -> HookFormField {
    match row {
        LuaCapabilityRow::Logging => HookFormField::LuaLogging,
        LuaCapabilityRow::EnvRead => HookFormField::LuaEnvRead,
        LuaCapabilityRow::ConnectionMetadata => HookFormField::LuaConnectionMetadata,
        LuaCapabilityRow::ProcessRun => HookFormField::LuaProcessRun,
    }
}

#[cfg(not(feature = "lua"))]
fn lua_capability_field(_row: LuaCapabilityRow) -> HookFormField {
    HookFormField::Enabled
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

#[cfg(test)]
mod tests {
    use super::{commit_saved_hook_definitions, update_hook_definition};
    use dbflux_app::config_loader::EditableGlobalHook;
    use dbflux_core::{ConnectionHook, HookExecutionMode, HookFailureMode, HookKind};
    use dbflux_storage::error::StorageError;
    use std::collections::HashMap;

    fn command_hook(command: &str) -> ConnectionHook {
        ConnectionHook {
            enabled: true,
            kind: HookKind::Command {
                command: command.to_string(),
                args: Vec::new(),
            },
            cwd: None,
            env: HashMap::new(),
            inherit_env: true,
            env_denylist: Vec::new(),
            timeout_ms: None,
            execution_mode: HookExecutionMode::Blocking,
            ready_signal: None,
            on_failure: HookFailureMode::Disconnect,
        }
    }

    #[test]
    fn failed_hook_save_keeps_editable_definitions() {
        let mut current = HashMap::from([(
            "edited".to_string(),
            EditableGlobalHook {
                id: Some("durable-id".to_string()),
                hook: command_hook("edited-command"),
            },
        )]);
        let before = current.clone();

        let committed = commit_saved_hook_definitions(
            &mut current,
            Err(StorageError::Data("injected save failure".to_string())),
        );

        assert!(committed.is_err());
        assert_eq!(current, before);
    }

    #[test]
    fn successful_hook_save_replaces_editable_definitions() {
        let mut current = HashMap::from([(
            "before".to_string(),
            EditableGlobalHook {
                id: Some("old-id".to_string()),
                hook: command_hook("before-command"),
            },
        )]);
        let saved = HashMap::from([(
            "after".to_string(),
            EditableGlobalHook {
                id: Some("new-id".to_string()),
                hook: command_hook("after-command"),
            },
        )]);

        commit_saved_hook_definitions(&mut current, Ok(saved.clone()))
            .expect("commit successful save");

        assert_eq!(current, saved);
    }

    #[test]
    fn rename_preserves_existing_durable_hook_id() {
        let mut definitions = HashMap::from([(
            "before".to_string(),
            EditableGlobalHook {
                id: Some("durable-id".to_string()),
                hook: command_hook("before-command"),
            },
        )]);

        update_hook_definition(
            &mut definitions,
            Some("before"),
            "after".to_string(),
            command_hook("after-command"),
        );

        assert!(!definitions.contains_key("before"));
        assert_eq!(
            definitions
                .get("after")
                .and_then(|definition| definition.id.as_deref()),
            Some("durable-id")
        );
    }

    #[test]
    fn new_hook_remains_without_durable_id() {
        let mut definitions = HashMap::new();

        update_hook_definition(
            &mut definitions,
            None,
            "new-hook".to_string(),
            command_hook("new-command"),
        );

        assert_eq!(
            definitions
                .get("new-hook")
                .and_then(|definition| definition.id.as_deref()),
            None
        );
    }

    const SETTINGS_HOOKS_CATALOG_KEYS: &[&str] = &[
        "settings.hooks.header.title",
        "settings.hooks.header.subtitle",
        "settings.hooks.list.new",
        "settings.hooks.list.empty",
        "settings.hooks.list.unreadable.title",
        "settings.hooks.list.unreadable.hint",
        "settings.hooks.form.title.edit",
        "settings.hooks.form.title.new",
        "settings.hooks.form.id",
        "settings.hooks.form.kind",
        "settings.hooks.form.command",
        "settings.hooks.form.args",
        "settings.hooks.form.args_hint",
        "settings.hooks.form.language",
        "settings.hooks.form.file_path",
        "settings.hooks.form.file_path_hint",
        "settings.hooks.form.open_in_app",
        "settings.hooks.form.open_in_editor",
        "settings.hooks.form.interpreter",
        "settings.hooks.form.interpreter_unsupported",
        "settings.hooks.form.capabilities",
        "settings.hooks.form.capability.logging",
        "settings.hooks.form.capability.env_read",
        "settings.hooks.form.capability.connection_metadata",
        "settings.hooks.form.capability.process_run",
        "settings.hooks.form.capability.process_run_hint",
        "settings.hooks.form.execution_mode",
        "settings.hooks.form.execution_mode_hint",
        "settings.hooks.form.ready_signal",
        "settings.hooks.form.ready_signal_hint",
        "settings.hooks.form.cwd",
        "settings.hooks.form.env",
        "settings.hooks.form.env_hint",
        "settings.hooks.form.env_denylist",
        "settings.hooks.form.env_denylist_hint",
        "settings.hooks.form.timeout",
        "settings.hooks.form.resolved_command",
        "settings.hooks.form.enabled",
        "settings.hooks.form.inherit_env",
        "settings.hooks.form.on_failure",
        "settings.hooks.form.preview_placeholder",
        "settings.hooks.status.unsupported_platform",
        "settings.hooks.status.script_missing",
        "settings.hooks.status.interpreter_missing",
        "settings.hooks.status.language_unsupported",
        "settings.hooks.status.lua_process_run_warning",
        "settings.hooks.error.open_script",
        "settings.hooks.error.command_not_editable",
        "settings.hooks.error.write_script",
        "settings.hooks.error.no_scripts_dir",
        "settings.hooks.error.create_dir",
        "settings.hooks.error.save",
        "settings.hooks.error.delete_unreadable",
        "settings.hooks.validation.id_required",
        "settings.hooks.validation.timeout",
        "settings.hooks.validation.command_required",
        "settings.hooks.validation.script_path_required",
        "settings.hooks.validation.lua_path_required",
        "settings.hooks.validation.duplicate_id",
        "settings.hooks.validation.env_pair",
        "settings.hooks.validation.env_key_empty",
        "settings.hooks.toast.saved",
        "settings.hooks.toast.deleted",
        "settings.hooks.toast.unreadable_deleted",
    ];

    #[test]
    fn settings_hooks_list_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for key in SETTINGS_HOOKS_CATALOG_KEYS {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(
                    !value.is_empty(),
                    "key {key} resolved empty for locale {locale}"
                );
                assert_ne!(value, *key, "key {key} did not resolve for locale {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "key {key} fell back to the raw locale-qualified form for locale {locale}"
                );
            }
        }
    }

    #[test]
    fn settings_hooks_saved_toast_differs_between_locales() {
        let english = dbflux_i18n::t!("settings.hooks.toast.saved", locale = "en");
        let spanish = dbflux_i18n::t!("settings.hooks.toast.saved", locale = "es");

        assert_eq!(english, "Hook saved");
        assert_ne!(english, spanish);
    }

    #[test]
    fn settings_hooks_form_title_differs_between_locales() {
        let english = dbflux_i18n::t!("settings.hooks.form.title.edit", locale = "en");
        let spanish = dbflux_i18n::t!("settings.hooks.form.title.edit", locale = "es");

        assert_eq!(english, "Edit Hook");
        assert_ne!(english, spanish);
    }
}
