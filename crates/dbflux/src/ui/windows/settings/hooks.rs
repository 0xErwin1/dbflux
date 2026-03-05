use crate::ui::components::toast::ToastExt;
use crate::ui::icons::AppIcon;
use dbflux_core::{AppConfig, AppConfigStore, ConnectionHook, HookFailureMode};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::checkbox::Checkbox;
use gpui_component::input::Input;

use super::SettingsWindow;

impl SettingsWindow {
    fn hook_sorted_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.hook_definitions.keys().cloned().collect();
        ids.sort();
        ids
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
        let cwd_text = self.input_hook_cwd.read(cx).value().trim().to_string();
        let env_text = self.input_hook_env.read(cx).value().trim().to_string();
        let timeout_text = self.input_hook_timeout.read(cx).value().trim().to_string();

        if !strict
            && hook_id.is_empty()
            && command.is_empty()
            && args_text.is_empty()
            && cwd_text.is_empty()
            && env_text.is_empty()
        {
            return Ok(None);
        }

        if hook_id.is_empty() {
            return Err("Hook ID is required".to_string());
        }

        if command.is_empty() {
            return Err("Command is required".to_string());
        }

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

        let cwd = if cwd_text.is_empty() {
            None
        } else {
            Some(std::path::PathBuf::from(cwd_text))
        };

        let env = Self::parse_hook_env_pairs(&env_text)?;

        let hook = ConnectionHook {
            enabled: self.hook_enabled,
            command,
            args: args_text
                .split_whitespace()
                .map(ToString::to_string)
                .collect(),
            cwd,
            env,
            inherit_env: self.hook_inherit_env,
            timeout_ms,
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
        self.hook_enabled = true;
        self.hook_inherit_env = true;

        self.input_hook_id
            .update(cx, |input, cx| input.set_value("", window, cx));
        self.input_hook_command
            .update(cx, |input, cx| input.set_value("", window, cx));
        self.input_hook_args
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
        self.hook_enabled = hook.enabled;
        self.hook_inherit_env = hook.inherit_env;

        self.input_hook_id.update(cx, |input, cx| {
            input.set_value(hook_id.to_string(), window, cx)
        });
        self.input_hook_command
            .update(cx, |input, cx| input.set_value(&hook.command, window, cx));
        self.input_hook_args.update(cx, |input, cx| {
            input.set_value(hook.args.join(" "), window, cx)
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

    pub(super) fn render_hooks_section(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        div()
            .flex_1()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(
                div()
                    .p_4()
                    .border_b_1()
                    .border_color(theme.border)
                    .child(
                        div()
                            .text_lg()
                            .font_weight(FontWeight::SEMIBOLD)
                            .child("Hooks"),
                    )
                    .child(div().text_sm().text_color(theme.muted_foreground).child(
                        "Create reusable hooks and associate them from connection settings",
                    )),
            )
            .child(
                div()
                    .flex_1()
                    .flex()
                    .overflow_hidden()
                    .child(self.render_hooks_list(cx))
                    .child(self.render_hook_form(cx)),
            )
    }

    fn render_hooks_list(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let hook_ids = self.hook_sorted_ids();

        div()
            .w(px(280.0))
            .h_full()
            .border_r_1()
            .border_color(theme.border)
            .flex()
            .flex_col()
            .child(
                div().p_2().border_b_1().border_color(theme.border).child(
                    Button::new("new-hook")
                        .label("New Hook")
                        .small()
                        .w_full()
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.clear_hook_form(window, cx);
                        })),
                ),
            )
            .child(
                div()
                    .flex_1()
                    .overflow_hidden()
                    .p_2()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .when(hook_ids.is_empty(), |container: Div| {
                        container.child(
                            div()
                                .p_4()
                                .text_sm()
                                .text_color(theme.muted_foreground)
                                .child("No hooks defined"),
                        )
                    })
                    .children(hook_ids.into_iter().map(|hook_id| {
                        let selected = self.editing_hook_id.as_deref() == Some(hook_id.as_str());
                        let hook = self.hook_definitions.get(&hook_id).cloned();
                        let hook_id_for_click = hook_id.clone();

                        div()
                            .id(SharedString::from(format!("hook-item-{}", hook_id)))
                            .px_3()
                            .py_2()
                            .rounded(px(4.0))
                            .cursor_pointer()
                            .border_1()
                            .border_color(gpui::transparent_black())
                            .when(selected, |div| div.bg(theme.secondary))
                            .hover(|div| div.bg(theme.secondary))
                            .on_click(cx.listener(move |this, _, window, cx| {
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
                                                        .child(hook.command),
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

        div()
            .flex_1()
            .h_full()
            .flex()
            .flex_col()
            .overflow_hidden()
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
                    .overflow_hidden()
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
                                    .child("Working Directory"),
                            )
                            .child(Input::new(&self.input_hook_cwd).small()),
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
                    .child(
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
                    .p_4()
                    .border_t_1()
                    .border_color(theme.border)
                    .flex()
                    .gap_2()
                    .justify_end()
                    .when(editing, |div| {
                        let hook_id = self.editing_hook_id.clone().unwrap_or_default();
                        div.child(
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
