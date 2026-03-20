use super::layout;
use super::section_trait::SectionFocusEvent;
use super::{SettingsSection, SettingsSectionId};
use crate::app::{AppState, AppStateChanged, McpRuntimeEventRaised};
use dbflux_mcp::TrustedClientDto;
use gpui::prelude::*;
use gpui::*;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::checkbox::Checkbox;
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::scroll::ScrollableElement;
use gpui_component::ActiveTheme;
use gpui_component::Disableable;
use gpui_component::Sizable;

pub(super) struct McpSection {
    app_state: Entity<AppState>,
    input_client_id: Entity<InputState>,
    input_client_name: Entity<InputState>,
    input_client_issuer: Entity<InputState>,
    selected_client_id: Option<String>,
    draft_active: bool,
    content_focused: bool,
    switching_input: bool,
    pending_sync_from_state: bool,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<SectionFocusEvent> for McpSection {}

impl McpSection {
    pub(super) fn new(
        app_state: Entity<AppState>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let input_client_id = cx.new(|cx| InputState::new(window, cx).placeholder("client-id"));
        let input_client_name =
            cx.new(|cx| InputState::new(window, cx).placeholder("Agent / integration name"));
        let input_client_issuer =
            cx.new(|cx| InputState::new(window, cx).placeholder("Issuer (optional)"));

        let state_sub = cx.subscribe(&app_state, |this, _, _: &AppStateChanged, cx| {
            this.pending_sync_from_state = true;
            cx.notify();
        });

        let blur_id = cx.subscribe(&input_client_id, |this, _, event: &InputEvent, cx| {
            if matches!(event, InputEvent::Blur) {
                if this.switching_input {
                    this.switching_input = false;
                    return;
                }
                cx.emit(SectionFocusEvent::RequestFocusReturn);
            }
        });

        let blur_name = cx.subscribe(&input_client_name, |this, _, event: &InputEvent, cx| {
            if matches!(event, InputEvent::Blur) {
                if this.switching_input {
                    this.switching_input = false;
                    return;
                }
                cx.emit(SectionFocusEvent::RequestFocusReturn);
            }
        });

        let blur_issuer = cx.subscribe(&input_client_issuer, |this, _, event: &InputEvent, cx| {
            if matches!(event, InputEvent::Blur) {
                if this.switching_input {
                    this.switching_input = false;
                    return;
                }
                cx.emit(SectionFocusEvent::RequestFocusReturn);
            }
        });

        Self {
            app_state,
            input_client_id,
            input_client_name,
            input_client_issuer,
            selected_client_id: None,
            draft_active: true,
            content_focused: false,
            switching_input: false,
            pending_sync_from_state: false,
            _subscriptions: vec![state_sub, blur_id, blur_name, blur_issuer],
        }
    }

    fn trusted_clients(&self, cx: &App) -> Vec<TrustedClientDto> {
        self.app_state
            .read(cx)
            .list_mcp_trusted_clients()
            .unwrap_or_default()
    }

    fn select_client(&mut self, client_id: &str, window: &mut Window, cx: &mut Context<Self>) {
        let Some(client) = self
            .trusted_clients(cx)
            .into_iter()
            .find(|item| item.id == client_id)
        else {
            return;
        };

        self.selected_client_id = Some(client.id.clone());
        self.draft_active = client.active;

        self.input_client_id.update(cx, |input, cx| {
            input.set_value(client.id, window, cx);
        });

        self.input_client_name.update(cx, |input, cx| {
            input.set_value(client.name, window, cx);
        });

        self.input_client_issuer.update(cx, |input, cx| {
            input.set_value(client.issuer.unwrap_or_default(), window, cx);
        });
    }

    fn clear_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.selected_client_id = None;
        self.draft_active = true;

        self.input_client_id.update(cx, |input, cx| {
            input.set_value("", window, cx);
        });

        self.input_client_name.update(cx, |input, cx| {
            input.set_value("", window, cx);
        });

        self.input_client_issuer.update(cx, |input, cx| {
            input.set_value("", window, cx);
        });
    }

    fn draft_client(&self, cx: &App) -> TrustedClientDto {
        let id = self.input_client_id.read(cx).value().trim().to_string();
        let name = self.input_client_name.read(cx).value().trim().to_string();
        let issuer = self.input_client_issuer.read(cx).value().trim().to_string();

        TrustedClientDto {
            id,
            name,
            issuer: (!issuer.is_empty()).then_some(issuer),
            active: self.draft_active,
        }
    }

    fn selected_client(&self, cx: &App) -> Option<TrustedClientDto> {
        let selected_id = self.selected_client_id.as_ref()?;
        self.trusted_clients(cx)
            .into_iter()
            .find(|item| &item.id == selected_id)
    }

    fn has_unsaved_changes(&self, cx: &App) -> bool {
        let draft = self.draft_client(cx);

        if draft.id.is_empty() && draft.name.is_empty() && draft.issuer.is_none() && draft.active {
            return false;
        }

        let Some(selected) = self.selected_client(cx) else {
            return true;
        };

        selected != draft
    }

    fn save_client(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::components::toast::ToastExt;

        let draft = self.draft_client(cx);

        if draft.id.is_empty() || draft.name.is_empty() {
            cx.toast_error("Client ID and name are required", window);
            return;
        }

        self.app_state.update(cx, |state, cx| {
            if let Err(error) = state.upsert_mcp_trusted_client(draft.clone()) {
                log::warn!("failed to upsert trusted client '{}': {}", draft.id, error);
                return;
            }

            for event in state.drain_mcp_runtime_events() {
                cx.emit(McpRuntimeEventRaised { event });
            }

            cx.emit(AppStateChanged);
        });

        self.selected_client_id = Some(draft.id);
        cx.toast_info("Trusted client saved", window);
    }

    fn delete_selected_client(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::components::toast::ToastExt;

        let Some(client_id) = self.selected_client_id.clone() else {
            cx.toast_warning("Select a trusted client first", window);
            return;
        };

        self.app_state.update(cx, |state, cx| {
            if let Err(error) = state.delete_mcp_trusted_client(&client_id) {
                log::warn!("failed to delete trusted client '{}': {}", client_id, error);
                return;
            }

            for event in state.drain_mcp_runtime_events() {
                cx.emit(McpRuntimeEventRaised { event });
            }

            cx.emit(AppStateChanged);
        });

        self.clear_form(window, cx);
        cx.toast_info("Trusted client deleted", window);
    }

    fn toggle_selected_client_active(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        use crate::ui::components::toast::ToastExt;

        let Some(mut selected) = self.selected_client(cx) else {
            cx.toast_warning("Select a trusted client first", window);
            return;
        };

        selected.active = !selected.active;
        self.draft_active = selected.active;

        self.app_state.update(cx, |state, cx| {
            if let Err(error) = state.upsert_mcp_trusted_client(selected.clone()) {
                log::warn!(
                    "failed to toggle trusted client '{}' active state: {}",
                    selected.id,
                    error
                );
                return;
            }

            for event in state.drain_mcp_runtime_events() {
                cx.emit(McpRuntimeEventRaised { event });
            }

            cx.emit(AppStateChanged);
        });

        let message = if selected.active {
            "Trusted client activated"
        } else {
            "Trusted client deactivated"
        };
        cx.toast_info(message, window);
    }
}

impl SettingsSection for McpSection {
    fn section_id(&self) -> SettingsSectionId {
        SettingsSectionId::Mcp
    }

    fn focus_in(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = true;
        cx.notify();
    }

    fn focus_out(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = false;
        cx.notify();
    }

    fn is_dirty(&self, cx: &App) -> bool {
        self.has_unsaved_changes(cx)
    }
}

impl Render for McpSection {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if self.pending_sync_from_state {
            self.pending_sync_from_state = false;

            if let Some(selected_id) = self.selected_client_id.clone() {
                self.select_client(&selected_id, window, cx);
            }
        }

        let theme = cx.theme();
        let clients = self.trusted_clients(cx);
        let selected = self.selected_client_id.clone();

        let list = div()
            .w(px(300.0))
            .h_full()
            .border_r_1()
            .border_color(theme.border)
            .p_3()
            .flex()
            .flex_col()
            .gap_2()
            .child(
                Button::new("mcp-client-new")
                    .label("New Trusted Client")
                    .small()
                    .ghost()
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.clear_form(window, cx);
                    })),
            )
            .child(
                div()
                    .flex_1()
                    .overflow_y_scrollbar()
                    .flex()
                    .flex_col()
                    .gap_1()
                    .when(clients.is_empty(), |root| {
                        root.child(
                            div()
                                .text_sm()
                                .text_color(theme.muted_foreground)
                                .child("No trusted clients configured."),
                        )
                    })
                    .children(clients.iter().map(|client| {
                        let id = client.id.clone();
                        let is_selected = selected.as_deref() == Some(client.id.as_str());

                        div()
                            .id(SharedString::from(format!("trusted-client-{}", client.id)))
                            .p_2()
                            .rounded(px(4.0))
                            .border_1()
                            .border_color(if is_selected {
                                theme.primary
                            } else {
                                transparent_black()
                            })
                            .bg(if is_selected {
                                theme.secondary
                            } else {
                                transparent_black()
                            })
                            .cursor_pointer()
                            .hover({
                                let secondary = theme.secondary;
                                move |div| div.bg(secondary)
                            })
                            .on_click(cx.listener(move |this, _, window, cx| {
                                this.select_client(&id, window, cx);
                            }))
                            .child(
                                div()
                                    .flex()
                                    .justify_between()
                                    .items_center()
                                    .child(
                                        div()
                                            .text_sm()
                                            .font_weight(FontWeight::MEDIUM)
                                            .child(client.name.clone()),
                                    )
                                    .child(
                                        div()
                                            .text_xs()
                                            .text_color(if client.active {
                                                theme.success
                                            } else {
                                                theme.muted_foreground
                                            })
                                            .child(if client.active {
                                                "active"
                                            } else {
                                                "inactive"
                                            }),
                                    ),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .text_color(theme.muted_foreground)
                                    .child(client.id.clone()),
                            )
                    })),
            );

        let save_label = if self.selected_client(cx).is_some() {
            "Update Client"
        } else {
            "Create Client"
        };

        let active_label = if self.draft_active {
            "Deactivate"
        } else {
            "Activate"
        };

        let form = div()
            .flex_1()
            .h_full()
            .flex()
            .flex_col()
            .child(layout::section_header(
                "MCP Governance",
                "Manage trusted clients used by MCP request identity gating",
                theme,
            ))
            .child(
                div()
                    .flex_1()
                    .overflow_y_scrollbar()
                    .p_4()
                    .flex()
                    .flex_col()
                    .gap_3()
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .child("Client ID"),
                    )
                    .child(Input::new(&self.input_client_id).small())
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .child("Name"),
                    )
                    .child(Input::new(&self.input_client_name).small())
                    .child(
                        div()
                            .text_sm()
                            .font_weight(FontWeight::MEDIUM)
                            .child("Issuer (optional)"),
                    )
                    .child(Input::new(&self.input_client_issuer).small())
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_2()
                            .child(
                                Checkbox::new("mcp-client-active")
                                    .checked(self.draft_active)
                                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                                        this.draft_active = *checked;
                                        cx.notify();
                                    })),
                            )
                            .child(div().text_sm().child("Active")),
                    ),
            )
            .child(
                div()
                    .p_4()
                    .border_t_1()
                    .border_color(theme.border)
                    .flex()
                    .items_center()
                    .justify_between()
                    .gap_2()
                    .child(div().text_xs().text_color(theme.muted_foreground).child(
                        if self.has_unsaved_changes(cx) {
                            "Unsaved form changes"
                        } else {
                            "All changes applied"
                        },
                    ))
                    .child(
                        div()
                            .flex()
                            .gap_2()
                            .child(
                                Button::new("mcp-client-toggle-active")
                                    .label(active_label)
                                    .small()
                                    .ghost()
                                    .disabled(self.selected_client(cx).is_none())
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.toggle_selected_client_active(window, cx);
                                    })),
                            )
                            .child(
                                Button::new("mcp-client-delete")
                                    .label("Delete")
                                    .small()
                                    .danger()
                                    .disabled(self.selected_client(cx).is_none())
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.delete_selected_client(window, cx);
                                    })),
                            )
                            .child(
                                Button::new("mcp-client-save")
                                    .label(save_label)
                                    .small()
                                    .primary()
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.save_client(window, cx);
                                    })),
                            ),
                    ),
            );

        div()
            .size_full()
            .flex()
            .overflow_hidden()
            .child(list)
            .child(form)
    }
}
