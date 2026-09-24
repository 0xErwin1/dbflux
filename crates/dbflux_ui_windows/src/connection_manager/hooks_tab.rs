use super::*;
use dbflux_components::controls::Input;
use dbflux_components::primitives::Text;
use dbflux_components::tokens::{Radii, Widths};
use gpui::prelude::FluentBuilder;
use gpui_component::ActiveTheme;

impl ConnectionManagerWindow {
    fn selected_hook_ids(&self, cx: &Context<Self>) -> Vec<String> {
        let (name_to_id, known_ids) = self.hook_id_lookup(cx);

        let pre_connect = Self::merge_hook_ids(
            self.settings_tab
                .conn_pre_hook_dropdown
                .read(cx)
                .selected_value()
                .map(|value| value.to_string()),
            &self.settings_tab.conn_pre_hook_extra_input.read(cx).value(),
            &name_to_id,
            &known_ids,
        );

        let post_connect = Self::merge_hook_ids(
            self.settings_tab
                .conn_post_hook_dropdown
                .read(cx)
                .selected_value()
                .map(|value| value.to_string()),
            &self
                .settings_tab
                .conn_post_hook_extra_input
                .read(cx)
                .value(),
            &name_to_id,
            &known_ids,
        );

        let pre_disconnect = Self::merge_hook_ids(
            self.settings_tab
                .conn_pre_disconnect_hook_dropdown
                .read(cx)
                .selected_value()
                .map(|value| value.to_string()),
            &self
                .settings_tab
                .conn_pre_disconnect_hook_extra_input
                .read(cx)
                .value(),
            &name_to_id,
            &known_ids,
        );

        let post_disconnect = Self::merge_hook_ids(
            self.settings_tab
                .conn_post_disconnect_hook_dropdown
                .read(cx)
                .selected_value()
                .map(|value| value.to_string()),
            &self
                .settings_tab
                .conn_post_disconnect_hook_extra_input
                .read(cx)
                .value(),
            &name_to_id,
            &known_ids,
        );

        let mut selected = Vec::new();

        for hook_id in pre_connect
            .into_iter()
            .chain(post_connect)
            .chain(pre_disconnect)
            .chain(post_disconnect)
        {
            if !selected.iter().any(|existing| existing == &hook_id) {
                selected.push(hook_id);
            }
        }

        selected
    }

    fn has_process_run_hook_selected(&self, cx: &Context<Self>) -> bool {
        let selected = self.selected_hook_ids(cx);
        if selected.is_empty() {
            return false;
        }

        let hook_definitions = self.app_state.read(cx).hook_definitions().clone();

        selected.into_iter().any(|hook_id| {
            hook_definitions.values().any(|definition| {
                definition.id.as_deref() == Some(hook_id.as_str())
                    && matches!(
                        &definition.kind,
                        dbflux_core::HookKind::Lua {
                            capabilities: dbflux_core::LuaCapabilities {
                                process_run: true,
                                ..
                            },
                            ..
                        }
                    )
            })
        })
    }

    /// Maps a Settings tab focus target to its "extra hooks" field: the label
    /// key, the snake_case field id and the input it edits.
    fn hook_extra_field(
        &self,
        focus: FormFocus,
    ) -> Option<(&'static str, &'static str, &Entity<InputState>)> {
        let settings = &self.settings_tab;

        match focus {
            FormFocus::SettingsPreConnectHookExtra => Some((
                "hooks.phase.extra_pre_connect",
                "pre_connect_hook_extra",
                &settings.conn_pre_hook_extra_input,
            )),
            FormFocus::SettingsPostConnectHookExtra => Some((
                "hooks.phase.extra_post_connect",
                "post_connect_hook_extra",
                &settings.conn_post_hook_extra_input,
            )),
            FormFocus::SettingsPreDisconnectHookExtra => Some((
                "hooks.phase.extra_pre_disconnect",
                "pre_disconnect_hook_extra",
                &settings.conn_pre_disconnect_hook_extra_input,
            )),
            FormFocus::SettingsPostDisconnectHookExtra => Some((
                "hooks.phase.extra_post_disconnect",
                "post_disconnect_hook_extra",
                &settings.conn_post_disconnect_hook_extra_input,
            )),
            _ => None,
        }
    }

    /// Returns the "extra hooks" input a Settings tab focus target edits.
    pub(super) fn settings_hook_extra_input(
        &self,
        focus: FormFocus,
    ) -> Option<&Entity<InputState>> {
        self.hook_extra_field(focus).map(|(_, _, input)| input)
    }

    /// Renders one "Extra <phase>" row: the label and the input holding the
    /// comma-separated hook IDs that run after the phase's dropdown hook.
    fn render_hook_extra_row(&self, focus_target: FormFocus, cx: &Context<Self>) -> Option<Div> {
        let (label_key, field_id, input) = self.hook_extra_field(focus_target)?;
        let label = dbflux_i18n::t!(label_key);

        let focused = self.edit_state == EditState::Navigating
            && self.active_tab == ActiveTab::Settings
            && self.form_focus == focus_target;
        let ring_color = cx.theme().ring;

        let row = div()
            .flex()
            .items_center()
            .gap_3()
            .child(div().w(px(160.0)).child(Text::caption(label.clone())))
            .child(
                div()
                    .w(Widths::CM_FORM_DROPDOWN)
                    .rounded(Radii::SM)
                    .border_2()
                    .when(focused, |d| d.border_color(ring_color))
                    .when(!focused, |d| d.border_color(gpui::transparent_black()))
                    .p(px(2.0))
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(move |this, _, window, cx| {
                            this.enter_edit_mode_for_field(focus_target, window, cx);
                        }),
                    )
                    .child(
                        Input::new(input)
                            .id(cm_setting_id(field_id))
                            .aria_label(label)
                            .small(),
                    ),
            );

        Some(row)
    }

    pub(super) fn render_hooks_rows(&self, _muted: Hsla, cx: &Context<Self>) -> Div {
        let show_process_run_warning = self.has_process_run_hook_selected(cx);

        div()
            .flex()
            .flex_col()
            .gap_3()
            .child(Text::caption(dbflux_i18n::t!("hooks.tab.intro")))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .child(
                        div()
                            .w(px(160.0))
                            .text_sm()
                            .child(dbflux_i18n::t!("hooks.phase.pre_connect_hook")),
                    )
                    .child(
                        div()
                            .w(Widths::CM_FORM_DROPDOWN)
                            .child(self.settings_tab.conn_pre_hook_dropdown.clone()),
                    ),
            )
            .children(self.render_hook_extra_row(FormFocus::SettingsPreConnectHookExtra, cx))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .child(
                        div()
                            .w(px(160.0))
                            .text_sm()
                            .child(dbflux_i18n::t!("hooks.phase.post_connect_hook")),
                    )
                    .child(
                        div()
                            .w(Widths::CM_FORM_DROPDOWN)
                            .child(self.settings_tab.conn_post_hook_dropdown.clone()),
                    ),
            )
            .children(self.render_hook_extra_row(FormFocus::SettingsPostConnectHookExtra, cx))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .child(
                        div()
                            .w(px(160.0))
                            .text_sm()
                            .child(dbflux_i18n::t!("hooks.phase.pre_disconnect_hook")),
                    )
                    .child(
                        div()
                            .w(Widths::CM_FORM_DROPDOWN)
                            .child(self.settings_tab.conn_pre_disconnect_hook_dropdown.clone()),
                    ),
            )
            .children(self.render_hook_extra_row(FormFocus::SettingsPreDisconnectHookExtra, cx))
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_3()
                    .child(
                        div()
                            .w(px(160.0))
                            .text_sm()
                            .child(dbflux_i18n::t!("hooks.phase.post_disconnect_hook")),
                    )
                    .child(
                        div()
                            .w(Widths::CM_FORM_DROPDOWN)
                            .child(self.settings_tab.conn_post_disconnect_hook_dropdown.clone()),
                    ),
            )
            .children(self.render_hook_extra_row(FormFocus::SettingsPostDisconnectHookExtra, cx))
            .when(show_process_run_warning, |this| {
                let theme = cx.theme();
                this.child(
                    div()
                        .rounded(Radii::SM)
                        .border_1()
                        .border_color(theme.warning.opacity(0.3))
                        .bg(theme.warning.opacity(0.1))
                        .p_2()
                        .child(
                            Text::caption(dbflux_i18n::t!("hooks.tab.lua_process_run_note"))
                                .warning(),
                        ),
                )
            })
    }
}

#[cfg(test)]
mod tests {
    const HOOKS_TAB_KEYS: &[&str] = &[
        "hooks.tab.intro",
        "hooks.tab.lua_process_run_note",
        "hooks.phase.pre_connect_hook",
        "hooks.phase.extra_pre_connect",
        "hooks.phase.post_connect_hook",
        "hooks.phase.extra_post_connect",
        "hooks.phase.pre_disconnect_hook",
        "hooks.phase.extra_pre_disconnect",
        "hooks.phase.post_disconnect_hook",
        "hooks.phase.extra_post_disconnect",
    ];

    #[test]
    fn hooks_tab_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for key in HOOKS_TAB_KEYS {
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
    fn hooks_tab_intro_differs_between_locales() {
        let en = dbflux_i18n::t!("hooks.tab.intro", locale = "en");
        let es = dbflux_i18n::t!("hooks.tab.intro", locale = "es");

        assert_ne!(en, es, "hooks.tab.intro should differ between en and es");
    }

    #[test]
    fn hooks_tab_reuses_settings_phase_keys() {
        let value = dbflux_i18n::t!("hooks.phase.pre_connect_hook", locale = "en");

        assert_eq!(
            value, "Pre-connect hook",
            "the connection manager hooks tab must resolve the same hooks.phase.* \
             keys the settings hooks section already defines, not a duplicate key set"
        );
    }
}
