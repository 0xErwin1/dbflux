use super::SettingsEvent;
use super::SettingsSection;
use super::SettingsSectionId;
use super::form_section::FormSection;
use super::section_trait::SectionFocusEvent;
use crate::app::{AppStateChanged, AppStateEntity};
use crate::keymap::{Modifiers, key_chord_from_gpui};
use crate::ui::components::dropdown::{Dropdown, DropdownItem, DropdownSelectionChanged};
use dbflux_components::controls::{InputEvent, InputState};
use dbflux_core::{ConnectionHook, HookExecutionMode, ScriptLanguage};
use gpui::prelude::*;
use gpui::*;
use gpui_component::dialog::Dialog;
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum HookKindSelection {
    Command,
    Script,
    Lua,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ScriptSourceSelection {
    File,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum HookFocus {
    List,
    Form,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum HookFormField {
    HookId,
    KindCommand,
    KindScript,
    #[cfg(feature = "lua")]
    KindLua,
    Command,
    Arguments,
    ScriptLanguage,
    FilePath,
    OpenInApp,
    OpenInEditor,
    Interpreter,
    #[cfg(feature = "lua")]
    LuaLogging,
    #[cfg(feature = "lua")]
    LuaEnvRead,
    #[cfg(feature = "lua")]
    LuaConnectionMetadata,
    #[cfg(feature = "lua")]
    LuaProcessRun,
    ExecutionMode,
    ReadySignal,
    WorkingDirectory,
    Environment,
    Timeout,
    Enabled,
    InheritEnv,
    OnFailure,
    DeleteButton,
    SaveButton,
}

pub(super) struct HooksSection {
    pub(super) app_state: Entity<AppStateEntity>,
    pub(super) hook_definitions: HashMap<String, ConnectionHook>,
    pub(super) hook_selected_id: Option<String>,
    pub(super) editing_hook_id: Option<String>,
    pub(super) pending_delete_hook_id: Option<String>,
    pub(super) input_hook_id: Entity<InputState>,
    pub(super) hook_kind_dropdown: Entity<Dropdown>,
    pub(super) hook_kind_selection: HookKindSelection,
    pub(super) input_hook_command: Entity<InputState>,
    pub(super) input_hook_args: Entity<InputState>,
    pub(super) script_language_dropdown: Entity<Dropdown>,
    pub(super) script_source_dropdown: Entity<Dropdown>,
    pub(super) input_hook_script_file_path: Entity<InputState>,
    pub(super) input_hook_script_content: Entity<InputState>,
    pub(super) hook_script_content_subscription: Option<Subscription>,
    pub(super) input_hook_interpreter: Entity<InputState>,
    pub(super) hook_execution_mode_dropdown: Entity<Dropdown>,
    pub(super) hook_execution_mode: HookExecutionMode,
    pub(super) input_hook_ready_signal: Entity<InputState>,
    pub(super) input_hook_cwd: Entity<InputState>,
    pub(super) input_hook_env: Entity<InputState>,
    pub(super) input_hook_timeout: Entity<InputState>,
    pub(super) hook_enabled: bool,
    pub(super) hook_inherit_env: bool,
    pub(super) hook_lua_logging: bool,
    pub(super) hook_lua_env_read: bool,
    pub(super) hook_lua_connection_metadata: bool,
    pub(super) hook_lua_process_run: bool,
    pub(super) hook_failure_dropdown: Entity<Dropdown>,
    pub(super) hook_focus: HookFocus,
    pub(super) hook_list_idx: Option<usize>,
    pub(super) hook_list_scroll_handle: ScrollHandle,
    pub(super) hook_pending_scroll_idx: Option<usize>,
    pub(super) hook_form_field: HookFormField,
    pub(super) hook_editing_field: bool,
    pub(super) content_focused: bool,
    pub(super) switching_input: bool,
    _subscriptions: Vec<Subscription>,
}

fn language_label_value(language: ScriptLanguage) -> &'static str {
    match language {
        ScriptLanguage::Bash => "bash",
        ScriptLanguage::Python => "python",
    }
}

fn notify_on_input_change(
    input: &Entity<InputState>,
    window: &mut Window,
    cx: &mut Context<HooksSection>,
) -> Subscription {
    cx.subscribe_in(input, window, |this, _, event: &InputEvent, _window, cx| {
        if matches!(event, InputEvent::Change) {
            cx.notify();
        }

        if matches!(event, InputEvent::Blur) {
            if this.switching_input {
                this.switching_input = false;
                return;
            }

            cx.emit(SectionFocusEvent::RequestFocusReturn);
        }
    })
}

impl HooksSection {
    pub(super) fn new(
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let hook_definitions = app_state.read(cx).hook_definitions().clone();

        let input_hook_id = cx.new(|cx| InputState::new(window, cx).placeholder("hook-id"));
        let hook_kind_dropdown = cx.new(|_cx| {
            #[cfg(feature = "lua")]
            let items = vec![
                DropdownItem::with_value("Command", "command"),
                DropdownItem::with_value("Script", "script"),
                DropdownItem::with_value("Lua", "lua"),
            ];

            #[cfg(not(feature = "lua"))]
            let items = vec![
                DropdownItem::with_value("Command", "command"),
                DropdownItem::with_value("Script", "script"),
            ];

            Dropdown::new("hook-kind")
                .items(items)
                .selected_index(Some(0))
        });
        let input_hook_command = cx.new(|cx| InputState::new(window, cx).placeholder("command"));
        let input_hook_args = cx.new(|cx| InputState::new(window, cx).placeholder("arg1 arg2 ..."));
        let script_language_dropdown = cx.new(|_cx| {
            let items = ScriptLanguage::available()
                .into_iter()
                .map(|language| {
                    DropdownItem::with_value(language.label(), language_label_value(language))
                })
                .collect();

            Dropdown::new("hook-script-language")
                .items(items)
                .selected_index(Some(0))
        });
        let script_source_dropdown = cx.new(|_cx| {
            Dropdown::new("hook-script-source")
                .items(vec![DropdownItem::with_value("File", "file")])
                .selected_index(Some(0))
        });
        let input_hook_script_file_path =
            cx.new(|cx| InputState::new(window, cx).placeholder("/path/to/script.py"));
        let input_hook_script_content = cx.new(|cx| {
            InputState::new(window, cx)
                .code_editor("python")
                .line_number(true)
                .soft_wrap(true)
                .placeholder("Enter script content...")
        });
        let input_hook_interpreter = cx.new(|cx| InputState::new(window, cx).placeholder("auto"));
        let hook_execution_mode_dropdown = cx.new(|_cx| {
            Dropdown::new("hook-execution-mode")
                .items(vec![
                    DropdownItem::with_value("Blocking", "blocking"),
                    DropdownItem::with_value("Detached", "detached"),
                ])
                .selected_index(Some(0))
        });
        let input_hook_ready_signal =
            cx.new(|cx| InputState::new(window, cx).placeholder("DBFLUX_READY"));
        let input_hook_cwd =
            cx.new(|cx| InputState::new(window, cx).placeholder("/path/to/working-dir"));
        let input_hook_env =
            cx.new(|cx| InputState::new(window, cx).placeholder("KEY=value, OTHER=value"));
        let input_hook_timeout = cx.new(|cx| InputState::new(window, cx).placeholder("30000"));
        let hook_failure_dropdown = cx.new(|_cx| {
            Dropdown::new("hook-failure-mode")
                .items(vec![
                    DropdownItem::with_value("Disconnect", "disconnect"),
                    DropdownItem::with_value("Warn", "warn"),
                    DropdownItem::with_value("Ignore", "ignore"),
                ])
                .selected_index(Some(0))
        });

        let app_state_subscription =
            cx.subscribe(&app_state, |this, _, _: &AppStateChanged, cx| {
                this.hook_definitions = this.app_state.read(cx).hook_definitions().clone();
                cx.notify();
            });
        let hook_kind_sub = cx.subscribe_in(
            &hook_kind_dropdown,
            window,
            |this, _, _: &DropdownSelectionChanged, window, cx| {
                this.refresh_hook_script_content_editor(window, cx);
            },
        );
        let hook_script_language_sub = cx.subscribe_in(
            &script_language_dropdown,
            window,
            |this, _, _: &DropdownSelectionChanged, window, cx| {
                this.refresh_hook_script_content_editor(window, cx);
            },
        );
        let hook_execution_mode_sub = cx.subscribe_in(
            &hook_execution_mode_dropdown,
            window,
            |_, _, _: &DropdownSelectionChanged, _window, cx| {
                cx.notify();
            },
        );
        let hook_script_source_sub = cx.subscribe_in(
            &script_source_dropdown,
            window,
            |this, _, _: &DropdownSelectionChanged, window, cx| {
                this.on_script_source_changed(window, cx);
            },
        );

        let hook_id_sub = notify_on_input_change(&input_hook_id, window, cx);
        let hook_command_sub = notify_on_input_change(&input_hook_command, window, cx);
        let hook_args_sub = notify_on_input_change(&input_hook_args, window, cx);
        let hook_script_file_sub = notify_on_input_change(&input_hook_script_file_path, window, cx);
        let hook_interpreter_sub = notify_on_input_change(&input_hook_interpreter, window, cx);
        let hook_ready_signal_sub = notify_on_input_change(&input_hook_ready_signal, window, cx);
        let hook_cwd_sub = notify_on_input_change(&input_hook_cwd, window, cx);
        let hook_env_sub = notify_on_input_change(&input_hook_env, window, cx);
        let hook_timeout_sub = notify_on_input_change(&input_hook_timeout, window, cx);

        let mut section = Self {
            app_state,
            hook_definitions,
            hook_selected_id: None,
            editing_hook_id: None,
            pending_delete_hook_id: None,
            input_hook_id,
            hook_kind_dropdown,
            hook_kind_selection: HookKindSelection::Command,
            input_hook_command,
            input_hook_args,
            script_language_dropdown,
            script_source_dropdown,
            input_hook_script_file_path,
            input_hook_script_content,
            hook_script_content_subscription: None,
            input_hook_interpreter,
            hook_execution_mode_dropdown,
            hook_execution_mode: HookExecutionMode::Blocking,
            input_hook_ready_signal,
            input_hook_cwd,
            input_hook_env,
            input_hook_timeout,
            hook_enabled: true,
            hook_inherit_env: true,
            hook_lua_logging: true,
            hook_lua_env_read: true,
            hook_lua_connection_metadata: true,
            hook_lua_process_run: false,
            hook_failure_dropdown,
            hook_focus: HookFocus::List,
            hook_list_idx: None,
            hook_list_scroll_handle: ScrollHandle::new(),
            hook_pending_scroll_idx: None,
            hook_form_field: HookFormField::HookId,
            hook_editing_field: false,
            content_focused: false,
            switching_input: false,
            _subscriptions: vec![
                app_state_subscription,
                hook_kind_sub,
                hook_script_language_sub,
                hook_execution_mode_sub,
                hook_script_source_sub,
                hook_id_sub,
                hook_command_sub,
                hook_args_sub,
                hook_script_file_sub,
                hook_interpreter_sub,
                hook_ready_signal_sub,
                hook_cwd_sub,
                hook_env_sub,
                hook_timeout_sub,
            ],
        };

        section.refresh_hook_script_content_editor(window, cx);

        let ids = section.hook_sorted_ids();
        if let Some(first_id) = ids.first() {
            section.hook_selected_id = Some(first_id.clone());
            section.hook_list_idx = Some(0);
            section.load_hook_values_without_focus(first_id, window, cx);
            section.hook_focus = HookFocus::List;
        }

        section
    }

    pub(crate) fn hook_sync_selection_from_ids(&mut self, ids: &[String]) {
        self.hook_list_idx = self
            .hook_selected_id
            .as_ref()
            .and_then(|selected| ids.iter().position(|id| id == selected));
    }

    pub(crate) fn hook_select_index(
        &mut self,
        index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let ids = self.hook_sorted_ids();
        let Some(hook_id) = ids.get(index).cloned() else {
            return;
        };

        self.hook_list_idx = Some(index);
        self.hook_selected_id = Some(hook_id.clone());
        self.hook_pending_scroll_idx = Some(index);
        self.load_hook_into_form(&hook_id, window, cx);
    }
}

impl FormSection for HooksSection {
    type Focus = HookFocus;
    type FormField = HookFormField;

    fn focus_area(&self) -> Self::Focus {
        self.hook_focus
    }

    fn set_focus_area(&mut self, focus: Self::Focus) {
        self.hook_focus = focus;
    }

    fn form_field(&self) -> Self::FormField {
        self.hook_form_field
    }

    fn set_form_field(&mut self, field: Self::FormField) {
        self.hook_form_field = field;
    }

    fn editing_field(&self) -> bool {
        self.hook_editing_field
    }

    fn set_editing_field(&mut self, editing: bool) {
        self.hook_editing_field = editing;
    }

    fn switching_input(&self) -> bool {
        self.switching_input
    }

    fn set_switching_input(&mut self, switching: bool) {
        self.switching_input = switching;
    }

    fn content_focused(&self) -> bool {
        self.content_focused
    }

    fn list_focus() -> Self::Focus {
        HookFocus::List
    }

    fn form_focus() -> Self::Focus {
        HookFocus::Form
    }

    fn first_form_field() -> Self::FormField {
        HookFormField::HookId
    }

    fn form_rows(&self) -> Vec<Vec<Self::FormField>> {
        let hook_kind = self.hook_kind_selection;
        let is_command = hook_kind == HookKindSelection::Command;
        let is_script = hook_kind == HookKindSelection::Script;
        let is_lua = hook_kind == HookKindSelection::Lua;
        let is_detached = !is_lua && self.hook_execution_mode == HookExecutionMode::Detached;

        let mut rows = vec![vec![HookFormField::HookId]];

        #[cfg(feature = "lua")]
        {
            rows.push(vec![
                HookFormField::KindCommand,
                HookFormField::KindScript,
                HookFormField::KindLua,
            ]);
        }

        #[cfg(not(feature = "lua"))]
        {
            rows.push(vec![HookFormField::KindCommand, HookFormField::KindScript]);
        }

        if is_command {
            rows.push(vec![HookFormField::Command]);
            rows.push(vec![HookFormField::Arguments]);
        }

        if is_script {
            rows.push(vec![HookFormField::ScriptLanguage]);
        }

        if is_script || is_lua {
            rows.push(vec![HookFormField::FilePath]);
            rows.push(vec![HookFormField::OpenInApp, HookFormField::OpenInEditor]);
        }

        if is_script {
            rows.push(vec![HookFormField::Interpreter]);
        }

        #[cfg(feature = "lua")]
        if is_lua {
            rows.push(vec![HookFormField::LuaLogging]);
            rows.push(vec![HookFormField::LuaEnvRead]);
            rows.push(vec![HookFormField::LuaConnectionMetadata]);
            rows.push(vec![HookFormField::LuaProcessRun]);
        }

        if !is_lua {
            rows.push(vec![HookFormField::ExecutionMode]);
            if is_detached {
                rows.push(vec![HookFormField::ReadySignal]);
            }
            rows.push(vec![HookFormField::WorkingDirectory]);
            rows.push(vec![HookFormField::Environment]);
        }

        rows.push(vec![HookFormField::Timeout]);
        rows.push(vec![HookFormField::Enabled]);
        if !is_lua {
            rows.push(vec![HookFormField::InheritEnv]);
        }
        rows.push(vec![HookFormField::OnFailure]);

        if self.editing_hook_id.is_some() {
            rows.push(vec![HookFormField::DeleteButton, HookFormField::SaveButton]);
        } else {
            rows.push(vec![HookFormField::SaveButton]);
        }

        rows
    }

    fn is_input_field(field: Self::FormField) -> bool {
        matches!(
            field,
            HookFormField::HookId
                | HookFormField::Command
                | HookFormField::Arguments
                | HookFormField::FilePath
                | HookFormField::Interpreter
                | HookFormField::ReadySignal
                | HookFormField::WorkingDirectory
                | HookFormField::Environment
                | HookFormField::Timeout
        )
    }

    fn focus_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        HooksSection::hook_focus_current_field(self, window, cx);
    }

    fn activate_current_field(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        HooksSection::hook_activate_current_field(self, window, cx);
    }

    fn validate_form_field(&mut self) {
        let rows = self.form_rows();
        let current = self.hook_form_field;

        for row in &rows {
            if row.contains(&current) {
                return;
            }
        }

        self.hook_form_field = HookFormField::HookId;
    }
}

impl SettingsSection for HooksSection {
    fn section_id(&self) -> SettingsSectionId {
        SettingsSectionId::Hooks
    }

    fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.content_focused {
            return;
        }

        if self.handle_editing_keys(event, window, cx) {
            return;
        }

        let chord = key_chord_from_gpui(&event.keystroke);

        match self.hook_focus {
            HookFocus::List => match (chord.key.as_str(), chord.modifiers) {
                ("j", modifiers) | ("down", modifiers) if modifiers == Modifiers::none() => {
                    self.hook_move_next(cx);
                    cx.notify();
                }
                ("k", modifiers) | ("up", modifiers) if modifiers == Modifiers::none() => {
                    self.hook_move_prev(cx);
                    cx.notify();
                }
                ("l", modifiers) | ("right", modifiers) | ("enter", modifiers)
                    if modifiers == Modifiers::none() =>
                {
                    if let Some(hook_id) = self.hook_selected_id.clone() {
                        self.load_hook_values_without_focus(&hook_id, window, cx);
                    }
                    self.enter_form(window, cx);
                    cx.notify();
                }
                ("d", modifiers) if modifiers == Modifiers::none() => {
                    if let Some(hook_id) = self.hook_selected_id() {
                        self.request_delete_hook(hook_id, cx);
                    }
                }
                ("g", modifiers) if modifiers == Modifiers::none() => {
                    self.hook_list_idx = None;
                    self.hook_selected_id = None;
                    cx.notify();
                }
                ("G", modifiers) if modifiers == Modifiers::none() => {
                    let count = self.hook_count(cx);
                    if count > 0 {
                        self.hook_list_idx = Some(count - 1);
                        let ids = self.hook_sorted_ids();
                        if let Some(last_id) = ids.last() {
                            self.hook_selected_id = Some(last_id.clone());
                        }
                    }
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
                ("h", modifiers) if modifiers == Modifiers::none() && !self.hook_editing_field => {
                    self.exit_form(window, cx);
                    cx.notify();
                }
                ("h", modifiers) | ("left", modifiers) if modifiers == Modifiers::none() => {
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
                ("G", modifiers) if modifiers == Modifiers::none() => {
                    self.move_last();
                    cx.notify();
                }
                _ => {}
            },
        }
    }

    fn focus_in(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = true;
        cx.notify();
    }

    fn focus_out(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = false;
        self.hook_editing_field = false;
        cx.notify();
    }

    fn is_dirty(&self, cx: &App) -> bool {
        self.has_unsaved_hook_changes(cx)
    }
}

impl EventEmitter<SectionFocusEvent> for HooksSection {}

impl EventEmitter<SettingsEvent> for HooksSection {}

impl Render for HooksSection {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let show_hook_delete = self.pending_delete_hook_id.is_some();
        let hook_delete_name = self.pending_delete_hook_id.clone().unwrap_or_default();

        div()
            .size_full()
            .min_h_0()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(self.render_hooks_section(cx))
            .when(show_hook_delete, |element| {
                let entity = cx.entity().clone();
                let entity_cancel = entity.clone();

                element.child(
                    Dialog::new(window, cx)
                        .title("Delete Hook")
                        .confirm()
                        .on_ok(move |_, window, cx| {
                            entity.update(cx, |section, cx| {
                                section.confirm_delete_hook(window, cx);
                            });
                            true
                        })
                        .on_cancel(move |_, _, cx| {
                            entity_cancel.update(cx, |section, cx| {
                                section.cancel_delete_hook(cx);
                            });
                            true
                        })
                        .child(div().text_sm().child(format!(
                            "Are you sure you want to delete hook \"{}\"?",
                            hook_delete_name
                        ))),
                )
            })
    }
}
