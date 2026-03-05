use super::context_menu::{KvContextMenu, KvMenuAction, KvMenuTarget};
use super::parsing::{key_type_icon, key_type_label, render_value_preview};
use super::{KeyValueDocumentEvent, KeyValueFocusMode, KvValueViewMode, TtlState};
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use gpui::prelude::FluentBuilder;
use gpui::*;
use gpui_component::input::Input;
use gpui_component::scroll::ScrollableElement;
use gpui_component::{ActiveTheme, Sizable};

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
                "Delete key?".to_string(),
                format!("Delete \"{}\"? This action cannot be undone.", pending.key),
            )
        } else if let Some(pending) = &self.pending_member_delete {
            (
                "Delete member?".to_string(),
                format!(
                    "Delete \"{}\"? This action cannot be undone.",
                    pending.member_display
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
                .map(|t| key_type_label(t).to_string())
                .unwrap_or_else(|| "Unknown".to_string());
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
                            .child(
                                svg()
                                    .path(AppIcon::KeyRound.path())
                                    .size(Heights::ICON_SM)
                                    .text_color(theme.muted_foreground),
                            )
                            .child(div().text_size(FontSizes::BASE).child(key_name)),
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
                    .text_size(FontSizes::XS)
                    .text_color(theme.muted_foreground)
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::XS)
                            .child(type_label),
                    )
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::XS)
                            .child(
                                svg()
                                    .path(AppIcon::Clock.path())
                                    .size(Heights::ICON_SM)
                                    .text_color(ttl_color),
                            )
                            .text_color(ttl_color)
                            .child(self.ttl_display.clone()),
                    )
                    .child(size_label),
            );

            if is_structured
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
                            .text_color(theme.muted_foreground)
                            .text_size(FontSizes::SM)
                            .child("No data"),
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
                        .child(
                            svg()
                                .path(AppIcon::Search.path())
                                .size(Heights::ICON_SM)
                                .text_color(theme.muted_foreground),
                        )
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
                    .bg(theme.secondary)
                    .text_size(FontSizes::XS)
                    .text_color(theme.muted_foreground);

                let is_stream = self.is_stream_type();
                header = header.child(div().w(px(30.0)).child("#"));
                header = header.child(div().flex_1().child(if is_stream { "ID" } else { "Value" }));
                if needs_value_col {
                    header = header.child(div().w(px(200.0)).child(if is_stream {
                        "Fields"
                    } else {
                        "Field/Score"
                    }));
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
                                cx.emit(KeyValueDocumentEvent::RequestFocus);
                                this.open_context_menu(
                                    KvMenuTarget::Value,
                                    event.position,
                                    window,
                                    cx,
                                );
                            }),
                        );

                    row = row.child(
                        div()
                            .w(px(30.0))
                            .text_size(FontSizes::XS)
                            .text_color(theme.muted_foreground)
                            .child(format!("{}", idx)),
                    );

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
                                div().w(px(200.0)).text_color(theme.muted_foreground).child(
                                    member
                                        .field
                                        .clone()
                                        .or(member.score.map(|s| s.to_string()))
                                        .unwrap_or_default(),
                                ),
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
                let is_editable = matches!(
                    value.entry.key_type,
                    Some(dbflux_core::KeyType::String) | Some(dbflux_core::KeyType::Json)
                );
                let value_preview = render_value_preview(value);

                panel = panel.child(
                    div()
                        .flex_1()
                        .overflow_y_scrollbar()
                        .p(Spacing::MD)
                        .border_l_1()
                        .border_color(theme.border)
                        .text_size(FontSizes::SM)
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
                                cx.emit(KeyValueDocumentEvent::RequestFocus);
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
                            d.child(
                                div()
                                    .pt(Spacing::SM)
                                    .text_size(FontSizes::XS)
                                    .text_color(theme.muted_foreground)
                                    .child("Click or press Enter to edit"),
                            )
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
                .text_color(theme.muted_foreground)
                .text_size(FontSizes::SM)
                .child(if self.runner.is_primary_active() {
                    "Loading..."
                } else {
                    "Select a key to inspect"
                })
                .into_any_element()
        };

        let refresh_label = if self.refresh_policy.is_auto() {
            self.refresh_policy.label()
        } else {
            "Refresh"
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
                    .child(
                        svg()
                            .path(AppIcon::Search.path())
                            .size(Heights::ICON_SM)
                            .text_color(theme.muted_foreground),
                    )
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
                                    .text_size(FontSizes::SM)
                                    .text_color(theme.foreground)
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
                                        svg()
                                            .path(if self.runner.is_primary_active() {
                                                AppIcon::Loader.path()
                                            } else if self.refresh_policy.is_auto() {
                                                AppIcon::Clock.path()
                                            } else {
                                                AppIcon::RefreshCcw.path()
                                            })
                                            .size(Heights::ICON_SM)
                                            .text_color(theme.foreground),
                                    )
                                    .child(refresh_label),
                            )
                            .child(div().w(px(1.0)).h_full().bg(theme.input))
                            .child(
                                div()
                                    .w(px(28.0))
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
                            .text_size(FontSizes::XS)
                            .text_color(theme.muted_foreground)
                            .child(
                                svg()
                                    .path(AppIcon::Rows3.path())
                                    .size_3()
                                    .text_color(theme.muted_foreground),
                            )
                            .child(if self.runner.is_primary_active() {
                                "Loading...".to_string()
                            } else {
                                format!("{} keys", key_count)
                            }),
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
                                            .text_color(theme.foreground)
                                            .hover(|d| d.bg(theme.secondary))
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.go_prev_page(cx);
                                            }))
                                    })
                                    .when(!can_prev, |d| {
                                        d.text_color(theme.muted_foreground).opacity(0.5)
                                    })
                                    .child(
                                        svg()
                                            .path(AppIcon::ChevronLeft.path())
                                            .size_3()
                                            .text_color(if can_prev {
                                                theme.foreground
                                            } else {
                                                theme.muted_foreground
                                            }),
                                    )
                                    .child("Prev"),
                            )
                            .child(
                                div()
                                    .text_size(FontSizes::XS)
                                    .text_color(theme.muted_foreground)
                                    .child(format!("Page {}", current_page)),
                            )
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
                                            .text_color(theme.foreground)
                                            .hover(|d| d.bg(theme.secondary))
                                            .on_click(cx.listener(|this, _, _, cx| {
                                                this.go_next_page(cx);
                                            }))
                                    })
                                    .when(!can_next, |d| {
                                        d.text_color(theme.muted_foreground).opacity(0.5)
                                    })
                                    .child("Next")
                                    .child(
                                        svg()
                                            .path(AppIcon::ChevronRight.path())
                                            .size_3()
                                            .text_color(if can_next {
                                                theme.foreground
                                            } else {
                                                theme.muted_foreground
                                            }),
                                    ),
                            ),
                    )
            })
            .when_some(error_message, |this, message| {
                this.child(
                    div()
                        .px(Spacing::SM)
                        .py(Spacing::XS)
                        .text_size(FontSizes::XS)
                        .text_color(theme.muted_foreground)
                        .child(format!("Error: {}", message)),
                )
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
                                cx.emit(KeyValueDocumentEvent::RequestFocus);
                                this.open_context_menu(
                                    KvMenuTarget::Key,
                                    event.position,
                                    window,
                                    cx,
                                );
                            }),
                        );

                    row = row.child(
                        svg()
                            .path(icon.path())
                            .size(Heights::ICON_SM)
                            .text_color(icon_color),
                    );

                    if is_renaming {
                        if let Some(input) = &self.rename_input {
                            row =
                                row.child(div().flex_1().child(Input::new(input).small().w_full()));
                        }
                    } else {
                        row = row.child(
                            div()
                                .flex_1()
                                .text_size(FontSizes::SM)
                                .overflow_hidden()
                                .text_ellipsis()
                                .whitespace_nowrap()
                                .child(key.key.clone()),
                        );

                        row = row.child(
                            div()
                                .text_size(FontSizes::XS)
                                .text_color(theme.muted_foreground)
                                .child(
                                    key.key_type
                                        .map(|t| key_type_label(t).to_string())
                                        .unwrap_or_else(|| "?".to_string()),
                                ),
                        );
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
                    cx.emit(KeyValueDocumentEvent::RequestFocus);
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

fn render_delete_confirm_modal(
    title: &str,
    message: &str,
    cx: &mut Context<super::KeyValueDocument>,
) -> impl IntoElement {
    let theme = cx.theme();
    let btn_hover = theme.muted;

    div()
        .id("kv-delete-modal-overlay")
        .absolute()
        .inset_0()
        .bg(gpui::hsla(0.0, 0.0, 0.0, 0.5))
        .flex()
        .items_center()
        .justify_center()
        .on_mouse_down(MouseButton::Left, |_, _, cx| {
            cx.stop_propagation();
        })
        .child(
            div()
                .bg(theme.background)
                .border_1()
                .border_color(theme.border)
                .rounded(Radii::MD)
                .p(Spacing::MD)
                .min_w(px(300.0))
                .flex()
                .flex_col()
                .gap(Spacing::MD)
                .child(
                    div()
                        .flex()
                        .items_center()
                        .gap_2()
                        .child(
                            svg()
                                .path(AppIcon::TriangleAlert.path())
                                .size_5()
                                .text_color(theme.warning),
                        )
                        .child(
                            div()
                                .text_size(FontSizes::SM)
                                .font_weight(FontWeight::SEMIBOLD)
                                .text_color(theme.foreground)
                                .child(title.to_string()),
                        ),
                )
                .child(
                    div()
                        .text_size(FontSizes::SM)
                        .text_color(theme.muted_foreground)
                        .child(message.to_string()),
                )
                .child(
                    div()
                        .flex()
                        .justify_end()
                        .gap(Spacing::SM)
                        .child(
                            div()
                                .id("kv-delete-cancel-btn")
                                .flex()
                                .items_center()
                                .gap_1()
                                .px(Spacing::SM)
                                .py(Spacing::XS)
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .text_size(FontSizes::SM)
                                .text_color(theme.muted_foreground)
                                .bg(theme.secondary)
                                .hover(move |d| d.bg(btn_hover))
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.pending_key_delete = None;
                                    this.pending_member_delete = None;
                                    cx.notify();
                                }))
                                .child(
                                    svg()
                                        .path(AppIcon::X.path())
                                        .size_4()
                                        .text_color(theme.muted_foreground),
                                )
                                .child("Cancel"),
                        )
                        .child(
                            div()
                                .id("kv-delete-confirm-btn")
                                .flex()
                                .items_center()
                                .gap_1()
                                .px(Spacing::SM)
                                .py(Spacing::XS)
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .text_size(FontSizes::SM)
                                .text_color(theme.background)
                                .bg(theme.danger)
                                .hover(|d| d.opacity(0.9))
                                .on_click(cx.listener(|this, _, _, cx| {
                                    if this.pending_key_delete.is_some() {
                                        this.confirm_delete_key(cx);
                                    } else if this.pending_member_delete.is_some() {
                                        this.confirm_delete_member(cx);
                                    }
                                }))
                                .child(
                                    svg()
                                        .path(AppIcon::Delete.path())
                                        .size_4()
                                        .text_color(theme.background),
                                )
                                .child("Delete"),
                        ),
                ),
        )
}

fn render_kv_context_menu(
    menu: &KvContextMenu,
    menu_focus: &FocusHandle,
    panel_origin: Point<Pixels>,
    cx: &mut Context<super::KeyValueDocument>,
) -> impl IntoElement {
    let theme = cx.theme();
    let menu_width = px(180.0);
    let menu_x = menu.position.x - panel_origin.x;
    let menu_y = menu.position.y - panel_origin.y;
    let selected_index = menu.selected_index;

    let menu_items: Vec<AnyElement> = menu
        .items
        .iter()
        .enumerate()
        .map(|(idx, item)| {
            let is_selected = idx == selected_index;
            let is_danger = item.is_danger;
            let label = item.label;
            let icon = item.icon;
            let action = item.action;

            div()
                .id(SharedString::from(label))
                .flex()
                .items_center()
                .gap(Spacing::SM)
                .h(Heights::ROW_COMPACT)
                .px(Spacing::SM)
                .mx(Spacing::XS)
                .rounded(Radii::SM)
                .cursor_pointer()
                .text_size(FontSizes::SM)
                .text_color(if is_danger {
                    theme.danger
                } else {
                    theme.foreground
                })
                .when(is_selected, |d| {
                    d.bg(if is_danger {
                        theme.danger.opacity(0.1)
                    } else {
                        theme.accent
                    })
                    .text_color(if is_danger {
                        theme.danger
                    } else {
                        theme.accent_foreground
                    })
                })
                .when(!is_selected, |d| {
                    d.hover(|d| {
                        d.bg(if is_danger {
                            theme.danger.opacity(0.1)
                        } else {
                            theme.secondary
                        })
                    })
                })
                .on_mouse_move(cx.listener(move |this, _, _, cx| {
                    if let Some(ref mut m) = this.context_menu
                        && m.selected_index != idx
                    {
                        m.selected_index = idx;
                        cx.notify();
                    }
                }))
                .on_click(cx.listener(move |this, _, window, cx| {
                    if let Some(m) = this.context_menu.take() {
                        let target = m.target;
                        this.execute_menu_action(action, target, window, cx);
                    }
                }))
                .child(svg().path(icon.path()).size_4().text_color(if is_danger {
                    theme.danger
                } else if is_selected {
                    theme.accent_foreground
                } else {
                    theme.muted_foreground
                }))
                .child(label)
                .into_any_element()
        })
        .collect();

    deferred(
        div()
            .id("kv-context-menu-overlay")
            .absolute()
            .top_0()
            .left_0()
            .size_full()
            .track_focus(menu_focus)
            .on_key_down(cx.listener(|this, event: &KeyDownEvent, window, cx| {
                use crate::keymap::{KeyChord, default_keymap};

                let chord = KeyChord::from_gpui(&event.keystroke);
                let keymap = default_keymap();

                if let Some(cmd) = keymap.resolve(crate::keymap::ContextId::ContextMenu, &chord)
                    && this.dispatch_menu_command(cmd, window, cx)
                {
                    cx.stop_propagation();
                }
            }))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(|this, _, window, cx| {
                    this.close_context_menu(window, cx);
                }),
            )
            .on_mouse_down(
                MouseButton::Right,
                cx.listener(|this, _, window, cx| {
                    this.close_context_menu(window, cx);
                }),
            )
            .child(
                div()
                    .id("kv-context-menu")
                    .absolute()
                    .left(menu_x)
                    .top(menu_y)
                    .w(menu_width)
                    .bg(theme.popover)
                    .border_1()
                    .border_color(theme.border)
                    .rounded(Radii::MD)
                    .shadow_lg()
                    .py(Spacing::XS)
                    .occlude()
                    .on_mouse_down(MouseButton::Left, |_, _, cx| {
                        cx.stop_propagation();
                    })
                    .children(menu_items),
            ),
    )
    .with_priority(1)
}

pub(super) fn icon_button_base(
    id: impl Into<ElementId>,
    icon: AppIcon,
    theme: &gpui_component::Theme,
) -> Stateful<Div> {
    let foreground = theme.muted_foreground;
    let hover_bg = theme.secondary;

    div()
        .id(id.into())
        .w(Heights::ICON_MD)
        .h(Heights::ICON_MD)
        .flex()
        .items_center()
        .justify_center()
        .rounded(Radii::SM)
        .cursor_pointer()
        .hover(move |d| d.bg(hover_bg))
        .child(
            svg()
                .path(icon.path())
                .size(Heights::ICON_SM)
                .text_color(foreground),
        )
}
