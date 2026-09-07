use super::context_menu::KvMenuTarget;
use super::decode::{self, KvEncodingChoice};
use super::parsing::{key_type_icon, key_type_label};
use super::view::{icon_button_base, render_delete_confirm_modal, render_kv_context_menu};
use super::{KeyValueFocusMode, KvValueViewMode, TtlState};
use crate::buckets_table::format_bytes;
use crate::handle::DocumentEvent;
use dbflux_components::controls::{Dropdown, Input};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text};
use dbflux_components::tokens::{FontSizes, Heights, Radii, Spacing};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::scroll::ScrollableElement;

/// Theme colors used by the size-gate placeholder and truncation notice,
/// copied out of `cx.theme()` up front since those renderers also need a
/// mutable `cx` for their `cx.listener(...)` handlers.
#[derive(Clone, Copy)]
struct KvGateColors {
    border: Hsla,
    warning: Hsla,
    muted_foreground: Hsla,
    list_active: Hsla,
}

impl super::KeyValueDocument {
    fn render_kv_gate_too_large(
        &self,
        size_bytes: u64,
        limit_bytes: u64,
        colors: KvGateColors,
        cx: &Context<Self>,
    ) -> impl IntoElement + use<> {
        div()
            .flex_1()
            .flex()
            .flex_col()
            .items_center()
            .justify_center()
            .gap(Spacing::SM)
            .p(Spacing::LG)
            .border_l_1()
            .border_color(colors.border)
            .child(Icon::new(AppIcon::TriangleAlert).color(colors.warning))
            .child(
                Text::body(dbflux_i18n::t!(
                    "document.key_value.render.gate.too_large",
                    size = format_bytes(size_bytes),
                    limit = format_bytes(limit_bytes)
                ))
                .color(colors.muted_foreground),
            )
            .child(
                div()
                    .id("kv-load-anyway")
                    .cursor_pointer()
                    .px(Spacing::MD)
                    .py(Spacing::XS)
                    .rounded(Radii::SM)
                    .border_1()
                    .border_color(colors.border)
                    .hover(|d| d.bg(colors.list_active))
                    .on_mouse_down(
                        MouseButton::Left,
                        cx.listener(|this, _, _, cx| {
                            this.load_selected_value_without_limit(cx);
                        }),
                    )
                    .child(Text::body(dbflux_i18n::t!(
                        "document.key_value.render.gate.load_anyway"
                    ))),
            )
    }

    fn render_kv_truncated_notice(
        &self,
        returned_bytes: u64,
        total_bytes: Option<u64>,
        colors: &KvGateColors,
    ) -> impl IntoElement + use<> {
        let message = match total_bytes {
            Some(total) => dbflux_i18n::t!(
                "document.key_value.render.gate.truncated",
                returned = format_bytes(returned_bytes),
                total = format_bytes(total)
            ),
            None => dbflux_i18n::t!(
                "document.key_value.render.gate.truncated_unknown_total",
                returned = format_bytes(returned_bytes)
            ),
        };

        div()
            .flex()
            .items_center()
            .gap(Spacing::XS)
            .px(Spacing::MD)
            .py(Spacing::XS)
            .border_b_1()
            .border_l_1()
            .border_color(colors.border)
            .bg(colors.warning.opacity(0.1))
            .child(
                Icon::new(AppIcon::TriangleAlert)
                    .small()
                    .color(colors.warning),
            )
            .child(Text::caption(message).color(colors.muted_foreground))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn key_value_refresh_label_differs_between_locales() {
        let english = dbflux_i18n::t!("document.data.grid.toolbar.refresh", locale = "en");
        let spanish = dbflux_i18n::t!("document.data.grid.toolbar.refresh", locale = "es");

        assert_eq!(english, "Refresh");
        assert_eq!(spanish, "Actualizar");
        assert_ne!(english, spanish);
    }

    #[test]
    fn key_value_render_keys_resolve_in_both_locales() {
        let keys = [
            "document.key_value.render.delete_key.title",
            "document.key_value.render.delete_key.message",
            "document.key_value.render.delete_member.title",
            "document.key_value.render.delete_member.message",
            "document.key_value.render.unknown_type",
            "document.key_value.render.value_header",
            "document.key_value.render.id_header",
            "document.key_value.render.fields_header",
            "document.key_value.render.field_score_header",
            "document.key_value.render.click_to_edit",
            "document.key_value.render.select_key_prompt",
            "document.key_value.render.prev",
            "document.key_value.render.next",
            "document.key_value.render.page",
            "document.key_value.render.keys_count.one",
            "document.key_value.render.keys_count.many",
            "document.key_value.render.filter.keys_placeholder",
            "document.key_value.render.filter.members_placeholder",
            "document.key_value.render.ttl.no_limit",
            "document.key_value.render.ttl.missing",
            "document.key_value.render.ttl.expired",
        ];

        for key in keys {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty in {locale}");
                assert_ne!(value, key, "{key} resolved to its own key in {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing from {locale} catalog"
                );
            }
        }
    }

    #[test]
    fn key_value_delete_key_message_interpolates_key_name() {
        let message = dbflux_i18n::t!(
            "document.key_value.render.delete_key.message",
            locale = "en",
            key = "session:42"
        );

        assert!(message.contains("session:42"));
    }

    #[test]
    fn key_value_page_label_interpolates_page_number() {
        let page = dbflux_i18n::t!("document.key_value.render.page", locale = "en", page = 3);

        assert!(page.contains('3'));
    }
}

impl Render for super::KeyValueDocument {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // Handle deferred modal opens before borrowing theme
        if self.pending_open_new_key_modal {
            self.pending_open_new_key_modal = false;
            self.new_key_modal
                .update(cx, |modal, cx| modal.open(window, cx));
        }

        if let Some(key_type) = self.pending_open_add_member_modal.take() {
            self.add_member_modal
                .update(cx, |modal, cx| modal.open(key_type, window, cx));
        }

        let theme = cx.theme();

        let error_message = self.last_error.clone();
        let is_structured = self.is_structured_type();
        let needs_value_col = self.needs_value_column();

        // Delete confirmation state (capture before building UI)
        let has_pending_delete =
            self.pending_key_delete.is_some() || self.pending_member_delete.is_some();
        let (delete_title, delete_message) = if let Some(pending) = &self.pending_key_delete {
            (
                dbflux_i18n::t!("document.key_value.render.delete_key.title"),
                dbflux_i18n::t!(
                    "document.key_value.render.delete_key.message",
                    key = pending.key
                ),
            )
        } else if let Some(pending) = &self.pending_member_delete {
            (
                dbflux_i18n::t!("document.key_value.render.delete_member.title"),
                dbflux_i18n::t!(
                    "document.key_value.render.delete_member.message",
                    member = pending.member_display
                ),
            )
        } else {
            (String::new(), String::new())
        };

        let filter_text = self
            .members_filter_input
            .read(cx)
            .value()
            .trim()
            .to_ascii_lowercase();
        let filtered_members: Vec<(usize, &super::parsing::MemberEntry)> = self
            .cached_members
            .iter()
            .enumerate()
            .filter(|(_, m)| {
                filter_text.is_empty() || m.display.to_ascii_lowercase().contains(&filter_text)
            })
            .collect();

        // -- Right panel --
        let right_panel = if let Some(value) = &self.selected_value {
            let key_name = value.entry.key.clone();
            let type_label = value
                .entry
                .key_type
                .map(key_type_label)
                .unwrap_or_else(|| dbflux_i18n::t!("document.key_value.render.unknown_type"));
            let ttl_color = match self.ttl_state {
                TtlState::Expired => theme.danger,
                TtlState::Missing => theme.warning,
                _ => theme.muted_foreground,
            };

            let size_label = value
                .entry
                .size_bytes
                .map(|s| format!("{} B", s))
                .unwrap_or_default();

            let mut panel = div().flex_1().flex().flex_col().overflow_hidden();

            // Header bar
            panel = panel.child(
                div()
                    .h(Heights::TOOLBAR)
                    .flex()
                    .items_center()
                    .justify_between()
                    .px(Spacing::MD)
                    .border_b_1()
                    .border_l_1()
                    .border_color(theme.border)
                    .bg(theme.tab_bar)
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::SM)
                            .child(Icon::new(AppIcon::KeyRound).small().muted())
                            .child(Text::body(key_name)),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::XS)
                            .when(is_structured, |d| {
                                d.child(
                                    icon_button_base("kv-add-member", AppIcon::Plus, theme)
                                        .on_mouse_down(
                                            MouseButton::Left,
                                            cx.listener(|this, _, _, cx| {
                                                if let Some(key_type) = this.selected_key_type() {
                                                    this.pending_open_add_member_modal =
                                                        Some(key_type);
                                                    cx.notify();
                                                }
                                            }),
                                        ),
                                )
                            })
                            .when(self.supports_document_view(), |d| {
                                let toggle_icon = match self.value_view_mode {
                                    KvValueViewMode::Table => AppIcon::Braces,
                                    KvValueViewMode::Document => AppIcon::Table,
                                };
                                d.child(
                                    icon_button_base("kv-toggle-view", toggle_icon, theme)
                                        .on_mouse_down(
                                            MouseButton::Left,
                                            cx.listener(|this, _, _, cx| {
                                                this.toggle_value_view_mode(cx);
                                            }),
                                        ),
                                )
                            })
                            .child(
                                icon_button_base("kv-refresh-val", AppIcon::RefreshCcw, theme)
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(|this, _, _, cx| {
                                            this.reload_selected_value(cx);
                                        }),
                                    ),
                            )
                            .child(
                                icon_button_base("kv-delete-key", AppIcon::Delete, theme)
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(|this, _, _, cx| {
                                            this.request_delete_key(cx);
                                        }),
                                    ),
                            ),
                    ),
            );

            // Metadata row
            panel = panel.child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::LG)
                    .px(Spacing::MD)
                    .py(Spacing::XS)
                    .border_b_1()
                    .border_l_1()
                    .border_color(theme.border)
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::XS)
                            .child(Text::caption(type_label)),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::XS)
                            .child(Icon::new(AppIcon::Clock).small().color(ttl_color))
                            .child(Text::body(self.ttl_display.clone()).color(ttl_color)),
                    )
                    .child(Text::caption(size_label)),
            );

            let load_state = value.load_state;
            let is_too_large = matches!(load_state, dbflux_core::KeyLoadState::TooLarge { .. });
            let gate_colors = KvGateColors {
                border: theme.border,
                warning: theme.warning,
                muted_foreground: theme.muted_foreground,
                list_active: theme.list_active,
            };

            if let dbflux_core::KeyLoadState::TooLarge {
                size_bytes,
                limit_bytes,
            } = load_state
            {
                panel = panel.child(self.render_kv_gate_too_large(
                    size_bytes,
                    limit_bytes,
                    gate_colors,
                    cx,
                ));
            }

            if let dbflux_core::KeyLoadState::Truncated {
                returned_bytes,
                total_bytes,
            } = load_state
            {
                panel = panel.child(self.render_kv_truncated_notice(
                    returned_bytes,
                    total_bytes,
                    &gate_colors,
                ));
            }

            if is_too_large {
                // The gate placeholder above is the entire content: `value`
                // is empty for `TooLarge`, so there is nothing to preview.
            } else if is_structured
                && self.value_view_mode == KvValueViewMode::Document
                && self.supports_document_view()
            {
                // -- Document tree view for Hash / Stream --
                if let Some(tree) = &self.document_tree {
                    panel = panel.child(
                        div()
                            .flex_1()
                            .overflow_hidden()
                            .border_l_1()
                            .border_color(theme.border)
                            .child(tree.clone()),
                    );
                } else {
                    panel = panel.child(
                        div()
                            .flex_1()
                            .flex()
                            .items_center()
                            .justify_center()
                            .border_l_1()
                            .border_color(theme.border)
                            .child(Text::muted(dbflux_i18n::t!("document.data.grid.empty"))),
                    );
                }
            } else if is_structured {
                // -- Table view for structured types --

                // Members filter
                panel = panel.child(
                    div()
                        .w_full()
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .px(Spacing::MD)
                        .py(Spacing::XS)
                        .border_b_1()
                        .border_l_1()
                        .border_color(theme.border)
                        .child(Icon::new(AppIcon::Search).small().muted())
                        .child(
                            div()
                                .flex_1()
                                .on_mouse_down(
                                    MouseButton::Left,
                                    cx.listener(|this, _, _, cx| {
                                        this.focus_mode = KeyValueFocusMode::TextInput;
                                        cx.stop_propagation();
                                        cx.notify();
                                    }),
                                )
                                .child(
                                    Input::new(&self.members_filter_input)
                                        .small()
                                        .cleanable(true)
                                        .w_full(),
                                ),
                        ),
                );

                // Members list header
                let mut header = div()
                    .flex()
                    .items_center()
                    .px(Spacing::MD)
                    .h(Heights::ROW_COMPACT)
                    .border_b_1()
                    .border_l_1()
                    .border_color(theme.border)
                    .bg(theme.secondary);

                let is_stream = self.is_stream_type();
                header = header.child(div().w(px(30.0)).child(Text::caption("#")));
                header = header.child(div().flex_1().child(Text::caption(if is_stream {
                    dbflux_i18n::t!("document.key_value.render.id_header")
                } else {
                    dbflux_i18n::t!("document.key_value.render.value_header")
                })));
                if needs_value_col {
                    header = header.child(div().w(px(200.0)).child(Text::caption(if is_stream {
                        dbflux_i18n::t!("document.key_value.render.fields_header")
                    } else {
                        dbflux_i18n::t!("document.key_value.render.field_score_header")
                    })));
                }
                header = header.child(div().w(Heights::ICON_MD));

                panel = panel.child(header);

                // Members list
                let mut members_list = div()
                    .flex_1()
                    .overflow_y_scrollbar()
                    .border_l_1()
                    .border_color(theme.border);

                for (original_index, member) in &filtered_members {
                    let idx = *original_index;
                    let is_editing = self.editing_member_index == Some(idx);
                    let is_selected = self.focus_mode == KeyValueFocusMode::ValuePanel
                        && self.selected_member_index == Some(idx);

                    let mut row = div()
                        .flex()
                        .items_center()
                        .px(Spacing::MD)
                        .h(Heights::ROW)
                        .border_b_1()
                        .border_color(theme.border)
                        .text_size(FontSizes::SM)
                        .when(is_selected, |d| d.bg(theme.list_active))
                        .when(!is_selected, |d| d.hover(|d| d.bg(theme.list_active)))
                        .on_mouse_down(
                            MouseButton::Right,
                            cx.listener(move |this, event: &MouseDownEvent, window, cx| {
                                cx.stop_propagation();
                                this.focus_mode = KeyValueFocusMode::ValuePanel;
                                this.selected_member_index = Some(idx);
                                cx.emit(DocumentEvent::RequestFocus);
                                this.open_context_menu(
                                    KvMenuTarget::Value,
                                    event.position,
                                    window,
                                    cx,
                                );
                            }),
                        );

                    row = row.child(div().w(px(30.0)).child(Text::caption(format!("{}", idx))));

                    if is_editing {
                        if let Some(input) = &self.member_edit_input {
                            row =
                                row.child(div().flex_1().child(Input::new(input).small().w_full()));
                            if let Some(score_input) = &self.member_edit_score_input {
                                row = row.child(
                                    div()
                                        .w(px(200.0))
                                        .child(Input::new(score_input).small().w_full()),
                                );
                            }
                        }
                    } else {
                        let value_cell = div().flex_1().child(member.display.clone());

                        row = row.child(if is_stream {
                            value_cell
                        } else {
                            value_cell.cursor_pointer().on_mouse_down(
                                MouseButton::Left,
                                cx.listener(move |this, _, window, cx| {
                                    cx.stop_propagation();
                                    this.start_member_edit(idx, window, cx);
                                }),
                            )
                        });

                        if needs_value_col {
                            row = row.child(
                                div().w(px(200.0)).child(Text::caption(
                                    member
                                        .field
                                        .clone()
                                        .or(member.score.map(|s| s.to_string()))
                                        .unwrap_or_default(),
                                )),
                            );
                        }

                        row = row.child(
                            icon_button_base(
                                ElementId::Name(format!("del-member-{}", idx).into()),
                                AppIcon::Delete,
                                theme,
                            )
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(move |this, _, _, cx| {
                                    cx.stop_propagation();
                                    this.request_delete_member(idx, cx);
                                }),
                            ),
                        );
                    }

                    members_list = members_list.child(row);
                }

                panel = panel.child(members_list);
            } else if let Some(input) = &self.string_edit_input {
                // Inline editing for String/JSON values
                panel = panel.child(
                    div()
                        .flex_1()
                        .overflow_y_scrollbar()
                        .p(Spacing::MD)
                        .border_l_1()
                        .border_color(theme.border)
                        .child(Input::new(input).small().w_full()),
                );
            } else {
                // Read-only value preview for String/JSON/Binary
                let key_type = value
                    .entry
                    .key_type
                    .unwrap_or(dbflux_core::KeyType::Unknown);
                let is_editable = decode::may_edit_value(
                    key_type,
                    value.repr,
                    self.kv_encoding_choice,
                    self.kv_decode_outcome.as_ref(),
                );
                let is_binary = value.repr == dbflux_core::ValueRepr::Binary;
                let value_preview = decode::render_value_preview_with_decode(
                    value,
                    self.kv_encoding_choice,
                    self.kv_decode_outcome.as_ref(),
                );
                let could_edit_if_raw = !is_editable
                    && matches!(
                        key_type,
                        dbflux_core::KeyType::String | dbflux_core::KeyType::Json
                    )
                    && is_binary
                    && !matches!(self.kv_encoding_choice, KvEncodingChoice::Raw);

                if is_binary {
                    let summary = self
                        .kv_decode_outcome
                        .as_ref()
                        .and_then(decode::encoding_summary_label);

                    panel = panel.child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::SM)
                            .px(Spacing::MD)
                            .py(Spacing::XS)
                            .border_b_1()
                            .border_l_1()
                            .border_color(theme.border)
                            .child(self.kv_encoding_dropdown.clone())
                            .when_some(summary, |d, summary| {
                                d.child(Text::caption(summary).color(theme.muted_foreground))
                            }),
                    );
                }

                panel = panel.child(
                    div()
                        .flex_1()
                        .overflow_y_scrollbar()
                        .p(Spacing::MD)
                        .border_l_1()
                        .border_color(theme.border)
                        .text_size(FontSizes::SM)
                        .text_color(theme.muted_foreground)
                        .when(is_editable, |d| {
                            d.cursor_pointer().on_mouse_down(
                                MouseButton::Left,
                                cx.listener(|this, _, window, cx| {
                                    this.start_string_edit(window, cx);
                                }),
                            )
                        })
                        .on_mouse_down(
                            MouseButton::Right,
                            cx.listener(|this, event: &MouseDownEvent, window, cx| {
                                cx.stop_propagation();
                                this.focus_mode = KeyValueFocusMode::ValuePanel;
                                cx.emit(DocumentEvent::RequestFocus);
                                this.open_context_menu(
                                    KvMenuTarget::Value,
                                    event.position,
                                    window,
                                    cx,
                                );
                            }),
                        )
                        .child(value_preview)
                        .when(is_editable, |d| {
                            d.child(div().pt(Spacing::SM).child(Text::caption(dbflux_i18n::t!(
                                "document.key_value.render.click_to_edit"
                            ))))
                        })
                        .when(could_edit_if_raw, |d| {
                            d.child(div().pt(Spacing::SM).child(Text::caption(dbflux_i18n::t!(
                                "document.key_value.render.decode.edit_locked"
                            ))))
                        }),
                );
            }

            panel.into_any_element()
        } else {
            div()
                .flex_1()
                .flex()
                .items_center()
                .justify_center()
                .border_l_1()
                .border_color(theme.border)
                .child(if self.runner.is_primary_active() {
                    div()
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .child(
                            Icon::new(AppIcon::Loader)
                                .size(px(12.0)) // guardrail-allow: 12px icon size, no ICON_XS token
                                .color(theme.muted_foreground),
                        )
                        .child(Text::muted(dbflux_i18n::t!("document.data.grid.loading")))
                        .into_any_element()
                } else {
                    Text::muted(dbflux_i18n::t!(
                        "document.key_value.render.select_key_prompt"
                    ))
                    .into_any_element()
                })
                .into_any_element()
        };

        let refresh_label = if self.refresh_policy.is_auto() {
            crate::labels::refresh_policy_label(self.refresh_policy)
        } else {
            dbflux_i18n::t!("document.data.grid.toolbar.refresh")
        };

        // -- Left panel --
        let left_panel = div()
            .w_1_3()
            .min_w(px(240.0))
            .flex()
            .flex_col()
            .overflow_hidden()
            // Toolbar
            .child(
                div()
                    .h(Heights::TOOLBAR)
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .px(Spacing::SM)
                    .border_b_1()
                    .border_color(theme.border)
                    .bg(theme.tab_bar)
                    .child(Icon::new(AppIcon::Search).small().muted())
                    .child(
                        div()
                            .flex_1()
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(|this, _, _, cx| {
                                    this.focus_mode = KeyValueFocusMode::TextInput;
                                    cx.stop_propagation();
                                    cx.notify();
                                }),
                            )
                            .child(
                                Input::new(&self.filter_input)
                                    .small()
                                    .cleanable(true)
                                    .w_full(),
                            ),
                    )
                    .child(
                        icon_button_base("kv-add", AppIcon::Plus, theme).on_mouse_down(
                            MouseButton::Left,
                            cx.listener(|this, _, _, cx| {
                                this.pending_open_new_key_modal = true;
                                cx.notify();
                            }),
                        ),
                    )
                    .child(
                        div()
                            .id("kv-refresh-control")
                            .h(Heights::BUTTON)
                            .flex()
                            .items_center()
                            .gap_0()
                            .rounded(Radii::SM)
                            .bg(theme.background)
                            .border_1()
                            .border_color(theme.input)
                            .child(
                                div()
                                    .id("kv-refresh-action")
                                    .h_full()
                                    .px(Spacing::SM)
                                    .flex()
                                    .items_center()
                                    .gap_1()
                                    .cursor_pointer()
                                    .hover(|d| d.bg(theme.accent.opacity(0.08)))
                                    .on_mouse_down(
                                        MouseButton::Left,
                                        cx.listener(|this, _, _, cx| {
                                            if this.runner.is_primary_active() {
                                                this.runner.cancel_primary(cx);
                                                this.last_error = None;
                                                cx.notify();
                                            } else {
                                                this.reload_keys(cx);
                                            }
                                        }),
                                    )
                                    .child(
                                        Icon::new(if self.runner.is_primary_active() {
                                            AppIcon::Loader
                                        } else if self.refresh_policy.is_auto() {
                                            AppIcon::Clock
                                        } else {
                                            AppIcon::RefreshCcw
                                        })
                                        .small()
                                        .color(theme.foreground),
                                    )
                                    .child(Text::body(refresh_label)),
                            )
                            .child(div().w(px(1.0)).h_full().bg(theme.input)) // guardrail-allow: vertical separator div, not a border-width token
                            .child(
                                div()
                                    .w(px(28.0)) // guardrail-allow: dropdown control width, not a height token
                                    .h_full()
                                    .child(self.refresh_dropdown.clone()),
                            ),
                    ),
            )
            // Pagination bar
            .child({
                let can_prev = self.can_go_prev();
                let can_next = self.can_go_next();
                let current_page = self.current_page;
                let key_count = self.keys.len();

                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .h(Heights::ROW_COMPACT)
                    .px(Spacing::SM)
                    .border_b_1()
                    .border_color(theme.border)
                    .bg(theme.tab_bar)
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap_1()
                            .child(if self.runner.is_primary_active() {
                                Icon::new(AppIcon::Loader)
                                    .size(px(12.0)) // guardrail-allow: 12px icon size, no ICON_XS token
                                    .color(theme.muted_foreground)
                                    .into_any_element()
                            } else {
                                Icon::new(AppIcon::Rows3)
                                    .size(px(12.0)) // guardrail-allow: 12px icon size, no ICON_XS token
                                    .muted()
                                    .into_any_element()
                            })
                            .child(Text::caption(if self.runner.is_primary_active() {
                                dbflux_i18n::t!("document.data.grid.loading")
                            } else if key_count == 1 {
                                dbflux_i18n::t!(
                                    "document.key_value.render.keys_count.one",
                                    count = key_count
                                )
                            } else {
                                dbflux_i18n::t!(
                                    "document.key_value.render.keys_count.many",
                                    count = key_count
                                )
                            })),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::SM)
                            .child(
                                div()
                                    .id("kv-prev-page")
                                    .flex()
                                    .items_center()
                                    .gap_1()
                                    .px(Spacing::XS)
                                    .rounded(Radii::SM)
                                    .text_size(FontSizes::XS)
                                    .when(can_prev, |d| {
                                        d.cursor_pointer()
                                            .hover(|d| d.bg(theme.secondary))
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.go_prev_page(cx);
                                            }))
                                    })
                                    .when(!can_prev, |d| d.opacity(0.5))
                                    .child({
                                        let icon_color = if can_prev {
                                            theme.foreground
                                        } else {
                                            theme.muted_foreground
                                        };
                                        Icon::new(AppIcon::ChevronLeft)
                                            .size(px(12.0)) // guardrail-allow: 12px icon size, no ICON_XS token
                                            .color(icon_color)
                                    })
                                    .child(
                                        Text::caption(dbflux_i18n::t!(
                                            "document.key_value.render.prev"
                                        ))
                                        .color(
                                            if can_prev {
                                                theme.foreground
                                            } else {
                                                theme.muted_foreground
                                            },
                                        ),
                                    ),
                            )
                            .child(Text::caption(dbflux_i18n::t!(
                                "document.key_value.render.page",
                                page = current_page
                            )))
                            .child(
                                div()
                                    .id("kv-next-page")
                                    .flex()
                                    .items_center()
                                    .gap_1()
                                    .px(Spacing::XS)
                                    .rounded(Radii::SM)
                                    .text_size(FontSizes::XS)
                                    .when(can_next, |d| {
                                        d.cursor_pointer()
                                            .hover(|d| d.bg(theme.secondary))
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.go_next_page(cx);
                                            }))
                                    })
                                    .when(!can_next, |d| d.opacity(0.5))
                                    .child(
                                        Text::caption(dbflux_i18n::t!(
                                            "document.key_value.render.next"
                                        ))
                                        .color(
                                            if can_next {
                                                theme.foreground
                                            } else {
                                                theme.muted_foreground
                                            },
                                        ),
                                    )
                                    .child({
                                        let icon_color = if can_next {
                                            theme.foreground
                                        } else {
                                            theme.muted_foreground
                                        };
                                        Icon::new(AppIcon::ChevronRight)
                                            .size(px(12.0)) // guardrail-allow: 12px icon size, no ICON_XS token
                                            .color(icon_color)
                                    }),
                            ),
                    )
            })
            .when_some(error_message, |this, message| {
                this.child(Text::caption(crate::labels::shared_error_prefix(&message)))
            })
            // Keys list
            .child(div().flex_1().overflow_y_scrollbar().children(
                self.keys.iter().enumerate().map(|(index, key)| {
                    let selected = self.selected_index == Some(index);
                    let is_renaming = self.renaming_index == Some(index);
                    let row_bg = if selected {
                        theme.list_active
                    } else {
                        theme.transparent
                    };

                    let (icon, icon_color) = key_type_icon(key.key_type);

                    let mut row = div()
                        .h(Heights::ROW)
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .px(Spacing::SM)
                        .bg(row_bg)
                        .border_b_1()
                        .border_color(theme.border)
                        .cursor_pointer()
                        .hover(|d| d.bg(theme.list_active))
                        .on_mouse_down(
                            MouseButton::Left,
                            cx.listener(move |this, _, _, cx| {
                                this.focus_mode = KeyValueFocusMode::List;
                                this.select_index(index, cx);
                            }),
                        )
                        .on_mouse_down(
                            MouseButton::Right,
                            cx.listener(move |this, event: &MouseDownEvent, window, cx| {
                                cx.stop_propagation();
                                this.focus_mode = KeyValueFocusMode::List;
                                this.select_index(index, cx);
                                cx.emit(DocumentEvent::RequestFocus);
                                this.open_context_menu(
                                    KvMenuTarget::Key,
                                    event.position,
                                    window,
                                    cx,
                                );
                            }),
                        );

                    row = row.child(Icon::new(icon).small().color(icon_color));

                    if is_renaming {
                        if let Some(input) = &self.rename_input {
                            row =
                                row.child(div().flex_1().child(Input::new(input).small().w_full()));
                        }
                    } else {
                        row = row.child(
                            div()
                                .flex_1()
                                .overflow_hidden()
                                .text_ellipsis()
                                .whitespace_nowrap()
                                .child(Text::caption(key.key.clone())),
                        );

                        row = row.child(Text::caption(
                            key.key_type.map(key_type_label).unwrap_or_else(|| {
                                dbflux_i18n::t!("document.key_value.parsing.type.unknown")
                            }),
                        ));
                    }

                    row
                }),
            ));

        // -- Compose --
        let this_entity = cx.entity().clone();

        div()
            .size_full()
            .track_focus(&self.focus_handle)
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, _, cx| {
                    this.focus_mode = KeyValueFocusMode::List;
                    cx.emit(DocumentEvent::RequestFocus);
                    cx.notify();
                }),
            )
            .flex()
            .child(
                canvas(
                    move |bounds, _, cx| {
                        this_entity.update(cx, |this, _| {
                            this.panel_origin = bounds.origin;
                        });
                    },
                    |_, _, _, _| {},
                )
                .absolute()
                .size_full(),
            )
            .child(left_panel)
            .child(right_panel)
            .when(self.new_key_modal.read(cx).is_visible(), |d| {
                d.child(self.new_key_modal.clone())
            })
            .when(self.add_member_modal.read(cx).is_visible(), |d| {
                d.child(self.add_member_modal.clone())
            })
            .when(has_pending_delete, |d| {
                d.child(render_delete_confirm_modal(
                    &delete_title,
                    &delete_message,
                    cx,
                ))
            })
            .when_some(self.context_menu.as_ref(), |d, menu| {
                d.child(render_kv_context_menu(
                    menu,
                    &self.context_menu_focus,
                    self.panel_origin,
                    cx,
                ))
            })
    }
}
