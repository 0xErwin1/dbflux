use crate::components::json_editor_view;
use crate::controls::Button;
use crate::controls::{GpuiInput as Input, InputState};
use crate::icons::AppIcon;
use crate::modals::modal::{Modal, ModalFocus, ModalVariant};
use crate::primitives::{Icon, Text};
use crate::tokens::{FontSizes, Heights, Spacing};
use crate::typography::AppFonts;
use crate::vim::{VimBinding, VimHost};
use dbflux_core::LogErr;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::input::EditorState;

/// Event emitted when the user clicks "Import" with valid JSON.
#[derive(Clone)]
pub struct ImportDashboardConfirmed {
    /// The raw dashboard JSON supplied by the user.
    pub json: String,
    /// The dashboard name entered by the user. Defaults to "Imported Dashboard"
    /// when the pasted JSON has no top-level `"name"` field.
    pub name: String,
}

/// Event emitted when the user cancels the modal.
#[derive(Clone)]
pub struct ImportDashboardCancelled;

/// Default name used when the pasted JSON has no top-level `"name"` field.
pub const DEFAULT_IMPORT_NAME: &str = "Imported Dashboard";

/// Modal for pasting dashboard JSON and triggering an import.
///
/// Uses the standard `Modal` chrome (header / scrollable body / footer
/// with top divider) so it matches the rest of the modal surfaces in the app.
pub struct ModalImportDashboard {
    visible: bool,
    /// JSON editor for the raw dashboard payload.
    input: gpui::Entity<EditorState>,
    /// Text input for the dashboard name, pre-filled from pasted JSON.
    name_input: gpui::Entity<InputState>,
    focus: ModalFocus,
    validation_error: Option<String>,
    name_error: Option<String>,
    vim: VimBinding,
}

impl VimHost for ModalImportDashboard {
    fn vim(&self, input: EntityId) -> Option<&VimBinding> {
        self.vim.for_input(input)
    }

    fn vim_mut(&mut self, input: EntityId) -> Option<&mut VimBinding> {
        self.vim.for_input_mut(input)
    }
}

impl ModalImportDashboard {
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        // soft_wrap defaults to true in 0.6.1, so the old explicit builder is gone.
        let input = cx.new(|cx| {
            EditorState::new(window, cx)
                .language("json")
                .line_number(true)
                .placeholder(dbflux_i18n::t!("modals.import_dashboard.json_placeholder"))
        });

        let name_input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("modals.import_dashboard.name_placeholder"))
        });

        let vim = VimBinding::new(input.clone(), window, cx);

        let mut modal = Self {
            visible: false,
            input,
            name_input,
            focus: ModalFocus::new(cx),
            validation_error: None,
            name_error: None,
            vim,
        };

        let input = modal.vim.input_id();
        VimBinding::follow_setting(&mut modal, input, cx);
        modal
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    /// Open the modal and focus the JSON editor.
    ///
    /// The name field is pre-filled with `DEFAULT_IMPORT_NAME`; once the user
    /// formats the pasted JSON containing a top-level `"name"` key the field
    /// is updated.
    pub fn open(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.visible = true;
        self.validation_error = None;
        self.name_error = None;

        self.input.update(cx, |state, cx| {
            state.set_value("", window, cx);
        });

        self.name_input.update(cx, |state, cx| {
            state.set_value(DEFAULT_IMPORT_NAME, window, cx);
        });

        let editor_focus = self.input.read(cx).focus_handle(cx);
        self.focus.focus(Some(&editor_focus), window, cx);

        cx.notify();
    }

    /// Close the modal without emitting a confirmation.
    pub fn close(&mut self, cx: &mut Context<Self>) {
        if self.visible {
            self.visible = false;
            cx.emit(ImportDashboardCancelled);
        }

        self.focus.restore(cx);
        cx.notify();
    }

    /// Extract the top-level `"name"` string from JSON text, if present.
    fn extract_json_name(json: &str) -> Option<String> {
        let name_key = "\"name\"";
        let pos = json.find(name_key)?;
        let after_key = json[pos + name_key.len()..].trim_start();
        let after_colon = after_key.strip_prefix(':')?.trim_start();
        let after_quote = after_colon.strip_prefix('"')?;
        let end = after_quote.find('"')?;
        let name = &after_quote[..end];
        if name.is_empty() {
            None
        } else {
            Some(name.to_string())
        }
    }

    fn confirm(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        let json_value = self.input.read(cx).value().to_string();

        if let Err(e) = json_editor_view::validate_json(&json_value, false) {
            self.validation_error = Some(e);
            cx.notify();
            return;
        }

        let name = self.name_input.read(cx).value().trim().to_string();
        if name.is_empty() {
            self.name_error = Some(dbflux_i18n::t!("modals.import_dashboard.name_error"));
            cx.notify();
            return;
        }

        let current_name = self.name_input.read(cx).value().to_string();
        let derived_name =
            Self::extract_json_name(&json_value).unwrap_or_else(|| DEFAULT_IMPORT_NAME.to_string());

        let final_name = if current_name == DEFAULT_IMPORT_NAME {
            derived_name
        } else {
            current_name
        };

        if final_name.trim().is_empty() {
            self.name_error = Some(dbflux_i18n::t!("modals.import_dashboard.name_error"));
            cx.notify();
            return;
        }

        self.visible = false;
        cx.emit(ImportDashboardConfirmed {
            json: json_value,
            name: final_name,
        });

        self.focus.restore(cx);
        cx.notify();
    }

    fn format_json(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let value = self.input.read(cx).value().to_string();
        if let Some(formatted) = json_editor_view::format_json(&value) {
            self.input.update(cx, |state, cx| {
                state.set_value(&formatted, window, cx);
            });
            self.validation_error = None;

            let current_name = self.name_input.read(cx).value().to_string();
            if current_name == DEFAULT_IMPORT_NAME
                && let Some(name) = Self::extract_json_name(&formatted)
            {
                self.name_input
                    .update(cx, |state, cx| state.set_value(&name, window, cx));
            }
        }
        cx.notify();
    }

    fn compact_json(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let value = self.input.read(cx).value().to_string();
        if let Some(compact) = json_editor_view::compact_json(&value) {
            self.input.update(cx, |state, cx| {
                state.set_value(&compact, window, cx);
            });
            self.validation_error = None;
        }
        cx.notify();
    }
}

impl EventEmitter<ImportDashboardConfirmed> for ModalImportDashboard {}
impl EventEmitter<ImportDashboardCancelled> for ModalImportDashboard {}

impl Render for ModalImportDashboard {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        // Vim's listeners on the JSON editor's container. In Insert mode
        // Escape leaves Insert mode instead of cancelling, and in Normal mode
        // Enter moves down instead of confirming.
        let input = self.vim.input_id();
        let vim_editor_container = VimBinding::wire(self.vim.leader_scope(div(), cx), input, cx);
        let vim_editor_container = VimBinding::capture_action::<crate::actions::Cancel, _>(
            vim_editor_container,
            input,
            cx,
        );
        let vim_editor_container = VimBinding::capture_action::<crate::actions::Execute, _>(
            vim_editor_container,
            input,
            cx,
        );

        let theme = cx.theme();
        let entity = cx.entity().downgrade();
        let close = move |_window: &mut Window, cx: &mut App| {
            entity.update(cx, |this, cx| this.close(cx)).log_err();
        };

        let name_error = self.name_error.clone();
        let validation_error = self.validation_error.clone();

        // ---- Body --------------------------------------------------------
        // Name row
        let name_row = div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(Text::body(dbflux_i18n::t!(
                "modals.import_dashboard.name_label"
            )))
            .child(Input::new(&self.name_input))
            .when_some(name_error, |el, err| {
                el.child(
                    div()
                        .text_size(FontSizes::XS)
                        .text_color(theme.danger)
                        .child(err),
                )
            });

        // JSON editor — bordered container that fills the remaining space.
        let editor = vim_editor_container
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .child(Text::body(dbflux_i18n::t!(
                "modals.import_dashboard.json_label"
            )))
            .child(
                div()
                    .border_1()
                    .border_color(theme.border)
                    .rounded(px(4.0)) // guardrail-allow: border radius
                    .bg(theme.background)
                    .h(px(360.0))
                    .p(Spacing::SM)
                    .overflow_hidden()
                    .child(
                        self.vim
                            .editor(false)
                            .w_full()
                            .h_full()
                            .font_family(AppFonts::MONO)
                            .font_weight(FontWeight::MEDIUM)
                            .text_size(FontSizes::BASE),
                    ),
            )
            .children(self.vim.render_indicator(cx));

        // Validation banner (only when there is an error).
        let validation_banner = validation_error.map(|err| {
            div()
                .flex()
                .items_center()
                .gap(Spacing::SM)
                .px(Spacing::SM)
                .py(Spacing::XS)
                .bg(theme.danger.opacity(0.1))
                .border_1()
                .border_color(theme.danger.opacity(0.3))
                .rounded(px(4.0)) // guardrail-allow: border radius
                .child(
                    Icon::new(AppIcon::CircleAlert)
                        .size(Heights::ICON_SM)
                        .danger(),
                )
                .child(
                    div()
                        .text_size(FontSizes::XS)
                        .text_color(theme.danger)
                        .child(err),
                )
        });

        let body = div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .child(name_row)
            .child(editor)
            .when_some(validation_banner, |el, banner| el.child(banner));

        // ---- Footer ------------------------------------------------------
        let on_format = cx.listener(|this, _: &gpui::ClickEvent, window, cx| {
            this.format_json(window, cx);
        });
        let on_compact = cx.listener(|this, _: &gpui::ClickEvent, window, cx| {
            this.compact_json(window, cx);
        });
        let on_cancel = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            this.close(cx);
        });
        let on_save = cx.listener(|this, _: &gpui::ClickEvent, window, cx| {
            this.confirm(window, cx);
        });

        let footer = div()
            .w_full()
            .flex()
            .items_center()
            .justify_between()
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(
                        Button::new(
                            "import-dashboard-format",
                            dbflux_i18n::t!("modals.import_dashboard.format"),
                        )
                        .ghost()
                        .on_click(on_format),
                    )
                    .child(
                        Button::new(
                            "import-dashboard-compact",
                            dbflux_i18n::t!("modals.import_dashboard.compact"),
                        )
                        .ghost()
                        .on_click(on_compact),
                    ),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(
                        Button::new(
                            "import-dashboard-cancel",
                            dbflux_i18n::t!("modals.import_dashboard.cancel"),
                        )
                        .on_click(on_cancel),
                    )
                    .child(
                        Button::new(
                            "import-dashboard-save",
                            dbflux_i18n::t!("modals.import_dashboard.confirm"),
                        )
                        .primary()
                        .on_click(on_save),
                    ),
            );

        Modal::new(dbflux_i18n::t!("modals.import_dashboard.title"))
            .body(body)
            .footer(footer)
            .icon(AppIcon::Download)
            .variant(ModalVariant::Default)
            .width(px(720.0))
            .focus_handle(self.focus.handle())
            .on_close(close)
            .on_confirm({
                let entity = cx.entity().downgrade();
                move |window, cx| {
                    entity
                        .update(cx, |this, cx| this.confirm(window, cx))
                        .log_err();
                }
            })
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_IMPORT_NAME, ModalImportDashboard};

    #[test]
    fn extract_json_name_returns_name_when_present() {
        let json = r#"{"name": "Production Overview", "widgets": []}"#;
        let name = ModalImportDashboard::extract_json_name(json);
        assert_eq!(name, Some("Production Overview".to_string()));
    }

    #[test]
    fn extract_json_name_returns_none_when_absent() {
        let json = r#"{"widgets": []}"#;
        let name = ModalImportDashboard::extract_json_name(json);
        assert_eq!(name, None);
    }

    #[test]
    fn extract_json_name_returns_none_for_empty_name() {
        let json = r#"{"name": "", "widgets": []}"#;
        let name = ModalImportDashboard::extract_json_name(json);
        assert_eq!(name, None);
    }

    #[test]
    fn extract_json_name_returns_none_for_empty_json() {
        let name = ModalImportDashboard::extract_json_name("{}");
        assert_eq!(name, None);
    }

    #[test]
    fn default_import_name_constant_is_correct() {
        assert_eq!(DEFAULT_IMPORT_NAME, "Imported Dashboard");
    }

    #[test]
    fn modal_title_matches_translated_catalog_and_diverges_by_locale() {
        let en = dbflux_i18n::t!("modals.import_dashboard.title", locale = "en");
        let es = dbflux_i18n::t!("modals.import_dashboard.title", locale = "es");
        assert!(!en.contains("CloudWatch"));
        assert_eq!(en, "Import Dashboard from JSON");
        assert_ne!(en, es);
    }

    #[test]
    fn import_dashboard_keys_resolve_in_both_locales() {
        let keys = [
            "modals.import_dashboard.title",
            "modals.import_dashboard.name_label",
            "modals.import_dashboard.name_placeholder",
            "modals.import_dashboard.json_label",
            "modals.import_dashboard.json_placeholder",
            "modals.import_dashboard.name_error",
            "modals.import_dashboard.format",
            "modals.import_dashboard.compact",
            "modals.import_dashboard.cancel",
            "modals.import_dashboard.confirm",
        ];

        for key in keys {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");
            assert!(!en.is_empty() && en != key, "en missing for {key}");
            assert!(!es.is_empty() && es != key, "es missing for {key}");
        }
    }
}

#[cfg(test)]
mod keyboard_tests {
    // Explicit imports rather than the parent glob: combining `use super::*`
    // with `#[gpui::test]` sends the gpui_macros expansion into unbounded
    // recursion.
    use super::{ImportDashboardConfirmed, ModalImportDashboard};
    use gpui::{
        AppContext as _, Context, Entity, IntoElement, ParentElement as _, Render, Styled as _,
        TestAppContext, VisualTestContext, Window, div,
    };
    use std::cell::RefCell;
    use std::rc::Rc;

    struct Host {
        modal: Entity<ModalImportDashboard>,
    }

    impl Render for Host {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div().size_full().child(self.modal.clone())
        }
    }

    /// Opens the modal with the app's `Modal` bindings for Escape and Enter
    /// and Vim mode on, and records every confirmation.
    fn open(
        cx: &mut TestAppContext,
    ) -> (
        Entity<ModalImportDashboard>,
        Rc<RefCell<usize>>,
        &mut VisualTestContext,
    ) {
        cx.update(|cx| {
            gpui_component::init(cx);
            crate::actions::record_last_keystroke(cx);
            crate::vim::set_vim_enabled(cx, true);
            cx.bind_keys([
                gpui::KeyBinding::new("escape", crate::actions::Cancel, Some("Modal")),
                gpui::KeyBinding::new("enter", crate::actions::Execute, Some("Modal")),
            ]);
        });

        let confirmed = Rc::new(RefCell::new(0));
        let (host, window) = cx.add_window_view({
            let confirmed = confirmed.clone();
            move |window, cx| {
                let modal = cx.new(|cx| ModalImportDashboard::new(window, cx));
                cx.subscribe(&modal, move |_, _, _: &ImportDashboardConfirmed, _| {
                    *confirmed.borrow_mut() += 1;
                })
                .detach();
                Host { modal }
            }
        });
        let modal = window.update(|_, cx| host.read(cx).modal.clone());

        window.update(|window, cx| modal.update(cx, |modal, cx| modal.open(window, cx)));
        window.run_until_parked();

        (modal, confirmed, window)
    }

    /// Enter in Normal mode is a motion: it never confirms the import, even
    /// with valid JSON in the editor. Escape leaves Insert mode first and
    /// cancels from Normal mode.
    #[gpui::test]
    fn enter_in_vim_normal_mode_does_not_confirm(cx: &mut TestAppContext) {
        let (modal, confirmed, window) = open(cx);
        window.update(|window, cx| {
            let input = modal.read(cx).input.clone();
            input.update(cx, |state, cx| {
                state.set_value("{\n\"widgets\": []\n}", window, cx);
                state.set_selected_range(0..0, cx);
            });
        });
        window.run_until_parked();

        window.simulate_keystrokes("enter");
        window.run_until_parked();
        assert_eq!(*confirmed.borrow(), 0, "Enter in Normal mode confirmed");
        assert!(window.update(|_, cx| modal.read(cx).is_visible()));
        assert_eq!(
            window.update(|_, cx| modal.read(cx).input.read(cx).cursor()),
            2,
            "Enter moved down a line"
        );

        window.simulate_keystrokes("i");
        window.simulate_input(" ");
        window.simulate_keystrokes("escape");
        window.run_until_parked();
        assert!(
            window.update(|_, cx| modal.read(cx).is_visible()),
            "the first Escape only leaves Insert mode"
        );

        window.simulate_keystrokes("escape");
        window.run_until_parked();
        assert!(!window.update(|_, cx| modal.read(cx).is_visible()));
        assert_eq!(*confirmed.borrow(), 0);
    }
}
