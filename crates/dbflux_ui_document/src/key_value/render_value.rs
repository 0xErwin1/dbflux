//! The value pane: key header, metadata row, and the body for the open key
//! (string lines with "View as", member tables, the sorted-set and stream
//! panes), plus the expiry popover and the large-value gate.

use super::collection_panes::{
    busiest_group, entry_time_label, format_score, idle_label, local_now, relative_bar_fraction,
    short_entry_id, stream_field_columns,
};
use super::context_menu::KvMenuTarget;
use super::decode::{Compression, SpanKind, ViewAs, may_edit_value, parses_as_json_document};
use super::expiry::{EXPIRY_PRESETS, ExpiryMode};
use super::key_tree::group_thousands;
use super::metadata::format_size;
use super::render::{type_badge_element, update_document};
use super::{KeyValueDocument, KeyValueFocusMode, KvValueViewMode, TtlState};
use crate::handle::DocumentEvent;
use crate::pane::DocumentSidePanel;
use dbflux_components::controls::{Button, Input, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{
    Badge, BadgeTone, BannerBlock, BannerVariant, Chamfer, Icon, Kbd, SegmentedControl,
    SegmentedItem, Text,
};
use dbflux_components::tokens::{
    ChamferCut, ChromeColors, FontSizes, KeyValueMetrics, Shadows, Spacing, SyntaxColors,
};
use dbflux_components::typography::AppFonts;
use dbflux_core::{KeyLoadState, KeyType, KeyValueFeatures, RangeOrder, ValueRepr};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use std::rc::Rc;

/// Hint shown under the value for the open key's type.
fn value_hint(key_type: Option<KeyType>) -> Option<String> {
    match key_type {
        Some(KeyType::Hash) => Some(dbflux_i18n::t!("document.key_value.hint.hash")),
        Some(KeyType::SortedSet) => Some(dbflux_i18n::t!("document.key_value.hint.sorted_set")),
        Some(KeyType::List) | Some(KeyType::Set) => {
            Some(dbflux_i18n::t!("document.key_value.hint.collection"))
        }
        Some(KeyType::Stream) => Some(dbflux_i18n::t!("document.key_value.hint.stream")),
        _ => None,
    }
}

/// Count shown in the metadata row: `8 fields`, `200 members`, `150 entries`.
fn member_count_label(key_type: KeyType, count: u64) -> Option<String> {
    let count_text = group_thousands(count);

    let key = match (key_type, count == 1) {
        (KeyType::Hash, true) => "document.key_value.meta.fields.one",
        (KeyType::Hash, false) => "document.key_value.meta.fields.many",
        (KeyType::Stream, true) => "document.key_value.meta.entries.one",
        (KeyType::Stream, false) => "document.key_value.meta.entries.many",
        (KeyType::List | KeyType::Set | KeyType::SortedSet, true) => {
            "document.key_value.meta.members.one"
        }
        (KeyType::List | KeyType::Set | KeyType::SortedSet, false) => {
            "document.key_value.meta.members.many"
        }
        _ => return None,
    };

    Some(dbflux_i18n::t!(key, count = count_text))
}

impl KeyValueDocument {
    pub(super) fn render_value_section(&mut self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();

        let Some(value) = self.selected_value.as_ref() else {
            let loading = self.runner.is_primary_active() && self.selected_index.is_some();

            return div()
                .flex_1()
                .min_w_0()
                .flex()
                .items_center()
                .justify_center()
                .gap(Spacing::SM)
                .text_size(FontSizes::XS)
                .text_color(theme.muted_foreground)
                .when(loading, |empty| {
                    empty.child(
                        Icon::new(AppIcon::Loader)
                            .size(KeyValueMetrics::META_ICON)
                            .color(theme.muted_foreground),
                    )
                })
                .child(if loading {
                    dbflux_i18n::t!("document.data.grid.loading")
                } else {
                    dbflux_i18n::t!("document.key_value.render.select_key_prompt")
                })
                .into_any_element();
        };

        let key_type = value.entry.key_type;
        let load_state = value.load_state;

        let header = self.render_value_header(cx);
        let meta_row = self.render_meta_row(cx);

        let body = if let KeyLoadState::TooLarge {
            size_bytes,
            limit_bytes,
        } = load_state
        {
            self.render_large_value_gate(size_bytes, limit_bytes, cx)
        } else if self.zset_pane.is_some() {
            self.render_zset_body(cx)
        } else if self.stream_pane.is_some() {
            self.render_stream_body(cx)
        } else if self.is_structured_type() {
            self.render_members_body(cx)
        } else {
            self.render_string_body(cx)
        };

        let hint = value_hint(key_type);

        div()
            .id("kv-value-pane")
            .relative()
            .flex_1()
            .min_w_0()
            .min_h_0()
            .flex()
            .flex_col()
            .child(header)
            .child(meta_row)
            .when(
                matches!(load_state, KeyLoadState::Truncated { .. })
                    && self.zset_pane.is_none()
                    && self.stream_pane.is_none(),
                |pane| pane.child(self.render_truncated_notice(load_state, cx)),
            )
            .child(body)
            .when_some(hint, |pane, hint| {
                pane.child(
                    div()
                        .flex()
                        .flex_none()
                        .items_center()
                        .gap(KeyValueMetrics::FOOTER_GAP)
                        .h(KeyValueMetrics::HINT_ROW_HEIGHT)
                        .px(KeyValueMetrics::VALUE_PADDING_X)
                        .border_t_1()
                        .border_color(theme.border)
                        .text_size(KeyValueMetrics::FOOTER_FONT)
                        .text_color(theme.muted_foreground)
                        .child(
                            Icon::new(AppIcon::Info)
                                .size(KeyValueMetrics::FOOTER_ICON)
                                .color(theme.muted_foreground),
                        )
                        .child(hint),
                )
            })
            .when(self.expiry_editor.is_some(), |pane| {
                pane.child(self.render_expiry_popover(cx))
            })
            .into_any_element()
    }

    fn render_value_header(&self, cx: &mut Context<Self>) -> impl IntoElement + use<> {
        let theme = cx.theme();
        let strong = ChromeColors::strong(theme);
        let entity = cx.entity().downgrade();

        let key_name = self
            .selected_value
            .as_ref()
            .map(|value| value.entry.key.clone())
            .unwrap_or_default();
        let key_type = self.selected_key_type();
        let is_structured = self.is_structured_type();
        let is_stream = key_type == Some(KeyType::Stream);

        let view_toggle = self.supports_document_view().then(|| {
            let (table_label, document_label) = if is_stream {
                (
                    dbflux_i18n::t!("document.key_value.value.view_entries"),
                    "JSON".to_string(),
                )
            } else {
                (
                    dbflux_i18n::t!("document.key_value.value.view_table"),
                    dbflux_i18n::t!("document.key_value.value.view_document"),
                )
            };

            let active = match self.value_view_mode {
                KvValueViewMode::Table => "table",
                KvValueViewMode::Document => "document",
            };

            let entity = entity.clone();
            SegmentedControl::new(
                vec![
                    SegmentedItem::new("table", table_label).icon(AppIcon::Table),
                    SegmentedItem::new("document", document_label).icon(AppIcon::Braces),
                ],
                active,
                move |id, _, cx| {
                    let wants_document = id.as_ref() == "document";
                    update_document(&entity, cx, |this, cx| {
                        let is_document = this.value_view_mode == KvValueViewMode::Document;
                        if wants_document != is_document {
                            this.toggle_value_view_mode(cx);
                        }
                    });
                },
            )
        });

        let add_label = match key_type {
            Some(KeyType::Hash) => dbflux_i18n::t!("document.key_value.value.add_field"),
            Some(KeyType::Stream) => dbflux_i18n::t!("document.key_value.value.add_entry"),
            _ => dbflux_i18n::t!("document.key_value.value.add_member"),
        };

        // Wraps onto a second row when the value pane is too narrow for every
        // action, so the last ones are never clipped.
        div()
            .id("kv-value-header")
            .flex()
            .flex_none()
            .flex_wrap()
            .items_center()
            .gap(KeyValueMetrics::VALUE_HEADER_GAP)
            .min_h(KeyValueMetrics::VALUE_HEADER_HEIGHT)
            .px(KeyValueMetrics::VALUE_PADDING_X)
            .py(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .child(type_badge_element(key_type, cx))
            .child(
                div()
                    .min_w_0()
                    .whitespace_nowrap()
                    .overflow_hidden()
                    .text_ellipsis()
                    .font_family(AppFonts::MONO)
                    .text_size(KeyValueMetrics::KEY_NAME_FONT)
                    .font_weight(FontWeight::BOLD)
                    .text_color(strong)
                    .child(key_name.clone()),
            )
            .child(
                Button::new(
                    "kv-copy-key",
                    dbflux_i18n::t!("document.key_value.context_menu.copy_key"),
                )
                .icon(AppIcon::Copy)
                .icon_only()
                .on_click(move |_, _, cx| {
                    cx.write_to_clipboard(ClipboardItem::new_string(key_name.clone()));
                }),
            )
            .child(div().flex_1())
            .when_some(view_toggle, |header, toggle| header.child(toggle))
            .when(is_structured, |header| {
                header.child(
                    Button::new("kv-add-member", add_label)
                        .icon(AppIcon::Plus)
                        .on_click(cx.listener(|this, _, _, cx| {
                            if let Some(key_type) = this.selected_key_type() {
                                this.pending_open_add_member_modal = Some(key_type);
                                cx.notify();
                            }
                        })),
                )
            })
            .child(
                Button::new(
                    "kv-copy-command",
                    dbflux_i18n::t!("document.key_value.context_menu.copy_as_command"),
                )
                .icon(AppIcon::Code)
                .icon_only()
                .on_click(cx.listener(|this, _, _, cx| {
                    this.handle_copy_as_command(KvMenuTarget::Key, cx);
                })),
            )
            .child(
                Button::new(
                    "kv-rename-key",
                    dbflux_i18n::t!("document.key_value.context_menu.rename"),
                )
                .icon(AppIcon::Pencil)
                .icon_only()
                .on_click(cx.listener(|this, _, window, cx| {
                    this.start_rename(window, cx);
                })),
            )
            .child(
                Button::new(
                    "kv-reload-value",
                    dbflux_i18n::t!("document.key_value.value.reload"),
                )
                .icon(AppIcon::RefreshCcw)
                .icon_only()
                .on_click(cx.listener(|this, _, _, cx| {
                    this.reload_selected_value(cx);
                })),
            )
            .child(
                Button::new(
                    "kv-delete-key",
                    dbflux_i18n::t!("document.key_value.render.delete_confirm.delete"),
                )
                .danger()
                .icon(AppIcon::Delete)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.request_delete_key(cx);
                })),
            )
    }

    fn render_meta_row(&self, cx: &mut Context<Self>) -> impl IntoElement + use<> {
        let theme = cx.theme();
        let strong = ChromeColors::strong(theme);
        let muted = theme.muted_foreground;
        let can_edit_expiry = self.supports_expiry(cx);

        let meta_item = |icon: AppIcon, color: Hsla| {
            div()
                .flex()
                .items_center()
                .gap(KeyValueMetrics::META_ITEM_GAP)
                .text_color(color)
                .child(
                    Icon::new(icon)
                        .size(KeyValueMetrics::META_ICON)
                        .color(color),
                )
        };

        let ttl_item = {
            let item = match self.ttl_state {
                TtlState::Remaining { .. } => meta_item(AppIcon::Clock, muted)
                    .child(dbflux_i18n::t!("document.key_value.meta.ttl"))
                    .child(
                        div()
                            .text_color(strong)
                            .child(self.selected_ttl_label().unwrap_or_default()),
                    ),
                TtlState::Expired => meta_item(AppIcon::Clock, theme.danger)
                    .child(dbflux_i18n::t!("document.key_value.render.ttl.expired")),
                TtlState::Missing => meta_item(AppIcon::Clock, theme.warning)
                    .child(dbflux_i18n::t!("document.key_value.render.ttl.missing")),
                TtlState::NoLimit => {
                    meta_item(AppIcon::Clock, if can_edit_expiry { strong } else { muted })
                        .child(dbflux_i18n::t!("document.key_value.meta.no_expiry"))
                }
            };

            item.id("kv-meta-ttl").when(can_edit_expiry, |item| {
                item.cursor_pointer()
                    .child(
                        Icon::new(AppIcon::Pencil)
                            .size(KeyValueMetrics::META_EDIT_ICON)
                            .color(muted),
                    )
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.open_expiry_editor(window, cx);
                    }))
            })
        };

        let size_bytes = self
            .value_metadata
            .as_ref()
            .and_then(|metadata| metadata.size_bytes)
            .or_else(|| {
                self.selected_value
                    .as_ref()
                    .and_then(|value| value.entry.size_bytes)
            });

        let encoding = self
            .value_metadata
            .as_ref()
            .and_then(|metadata| metadata.encoding.clone());

        let count = self.member_total().and_then(|count| {
            self.selected_key_type()
                .and_then(|key_type| member_count_label(key_type, count))
        });

        let groups = self
            .stream_pane
            .as_ref()
            .filter(|pane| pane.groups_loaded)
            .map(|pane| {
                let count = pane.groups.len() as u64;
                if count == 1 {
                    dbflux_i18n::t!("document.key_value.meta.groups.one")
                } else {
                    dbflux_i18n::t!(
                        "document.key_value.meta.groups.many",
                        count = group_thousands(count)
                    )
                }
            });

        let detected = if self.is_structured_type() {
            None
        } else {
            self.value_detection.format.detected_label()
        };

        div()
            .flex()
            .flex_none()
            .items_center()
            .gap(KeyValueMetrics::META_GAP)
            .h(KeyValueMetrics::META_ROW_HEIGHT)
            .px(KeyValueMetrics::VALUE_PADDING_X)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.background)
            .text_size(KeyValueMetrics::META_FONT)
            .child(ttl_item)
            .when_some(size_bytes, |row, size| {
                row.child(meta_item(AppIcon::HardDrive, muted).child(format_size(size)))
            })
            .when_some(encoding, |row, encoding| {
                row.child(meta_item(AppIcon::Layers, muted).child(encoding))
            })
            .when_some(count, |row, count| {
                row.child(meta_item(AppIcon::Hash, muted).child(count))
            })
            .when_some(groups, |row, groups| {
                row.child(meta_item(AppIcon::Layers, muted).child(groups))
            })
            .when_some(detected, |row, detected| {
                row.child(meta_item(AppIcon::Braces, theme.success).child(detected))
            })
    }

    /// Total members of the open collection: the server's count for ranged
    /// panes, the loaded rows otherwise.
    fn member_total(&self) -> Option<u64> {
        if let Some(pane) = &self.zset_pane {
            return Some(pane.total);
        }

        if let Some(pane) = &self.stream_pane {
            return Some(pane.total);
        }

        self.is_structured_type()
            .then_some(self.cached_members.len() as u64)
    }

    fn render_truncated_notice(
        &self,
        load_state: KeyLoadState,
        cx: &mut Context<Self>,
    ) -> impl IntoElement + use<> {
        let KeyLoadState::Truncated {
            returned_bytes,
            total_bytes,
        } = load_state
        else {
            return div();
        };

        let message = match total_bytes {
            Some(total) => dbflux_i18n::t!(
                "document.key_value.render.gate.truncated",
                returned = format_size(returned_bytes),
                total = format_size(total)
            ),
            None => dbflux_i18n::t!(
                "document.key_value.render.gate.truncated_unknown_total",
                returned = format_size(returned_bytes)
            ),
        };

        let theme = cx.theme();

        div()
            .flex()
            .flex_none()
            .items_center()
            .gap(Spacing::XS)
            .px(KeyValueMetrics::VALUE_PADDING_X)
            .py(Spacing::XS)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.warning.opacity(KeyValueMetrics::CALLOUT_FILL_ALPHA))
            .child(
                Icon::new(AppIcon::TriangleAlert)
                    .size(KeyValueMetrics::META_ICON)
                    .color(theme.warning),
            )
            .child(Text::caption(message).color(theme.muted_foreground))
    }

    fn render_large_value_gate(
        &self,
        size_bytes: u64,
        limit_bytes: u64,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let key_name = self.selected_key().unwrap_or_default();
        let is_string = matches!(
            self.selected_key_type(),
            Some(KeyType::String | KeyType::Bytes | KeyType::Json) | None
        );
        let can_preview = is_string && self.key_features.contains(KeyValueFeatures::VALUE_PREFIX);

        let actions = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .when(can_preview, |actions| {
                actions.child(
                    Button::new(
                        "kv-preview-prefix",
                        dbflux_i18n::t!(
                            "document.key_value.gate.preview",
                            size = format_size(super::decode::VALUE_PREVIEW_BYTES)
                        ),
                    )
                    .icon(AppIcon::Eye)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.preview_value_prefix(cx);
                    })),
                )
            })
            .child(
                Button::new(
                    "kv-load-anyway",
                    dbflux_i18n::t!("document.key_value.render.gate.load_anyway"),
                )
                .primary()
                .on_click(cx.listener(|this, _, _, cx| {
                    this.load_selected_value_without_limit(cx);
                })),
            );

        div()
            .flex_1()
            .min_h_0()
            .p(KeyValueMetrics::GATE_MARGIN)
            .child(
                BannerBlock::new(
                    BannerVariant::Warning,
                    dbflux_i18n::t!(
                        "document.key_value.gate.title",
                        key = key_name,
                        size = format_size(size_bytes),
                        limit = format_size(limit_bytes)
                    ),
                )
                .with_body(dbflux_i18n::t!("document.key_value.gate.body"))
                .with_actions(actions),
            )
            .into_any_element()
    }

    /// A value toolbar row. It wraps onto more rows instead of clipping its
    /// controls when the value pane is narrow.
    fn render_value_toolbar(&self) -> Div {
        div()
            .flex()
            .flex_none()
            .flex_wrap()
            .items_center()
            .gap(KeyValueMetrics::TOOLBAR_GAP)
            .min_h(KeyValueMetrics::VALUE_TOOLBAR_HEIGHT)
            .px(KeyValueMetrics::VALUE_PADDING_X)
            .py(Spacing::XS)
    }

    fn render_member_filter(&self, cx: &mut Context<Self>) -> impl IntoElement + use<> {
        let theme = cx.theme();

        div()
            .w(KeyValueMetrics::MEMBER_FILTER_WIDTH)
            .flex_none()
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
                    .id("kv-member-filter")
                    .w_full()
                    .cleanable(true)
                    .prefix(
                        Icon::new(AppIcon::ListFilter)
                            .size(KeyValueMetrics::FOLDER_ICON)
                            .color(theme.muted_foreground),
                    ),
            )
    }

    /// Indices of the cached members that pass the member filter.
    fn filtered_member_indices(&self, cx: &App) -> Vec<usize> {
        let filter = self
            .members_filter_input
            .read(cx)
            .value()
            .trim()
            .to_lowercase();

        self.cached_members
            .iter()
            .enumerate()
            .filter(|(_, member)| {
                filter.is_empty()
                    || member.display.to_lowercase().contains(&filter)
                    || member
                        .field
                        .as_ref()
                        .is_some_and(|field| field.to_lowercase().contains(&filter))
            })
            .map(|(index, _)| index)
            .collect()
    }

    fn render_members_body(&mut self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();

        if self.value_view_mode == KvValueViewMode::Document && self.supports_document_view() {
            let content = match &self.document_tree {
                Some(tree) => div().size_full().child(tree.clone()).into_any_element(),
                None => div()
                    .size_full()
                    .flex()
                    .items_center()
                    .justify_center()
                    .child(Text::caption(dbflux_i18n::t!("document.data.grid.empty")))
                    .into_any_element(),
            };

            return div()
                .flex_1()
                .min_h_0()
                .overflow_hidden()
                .child(content)
                .into_any_element();
        }

        let key_type = self.selected_key_type();
        let is_hash = key_type == Some(KeyType::Hash);
        let visible = Rc::new(self.filtered_member_indices(cx));
        let loaded = self.cached_members.len();

        let source_label = match key_type {
            Some(KeyType::Hash) => "HGETALL",
            Some(KeyType::List) => "LRANGE",
            Some(KeyType::Set) => "SMEMBERS",
            _ => "",
        };

        let toolbar = self
            .render_value_toolbar()
            .border_b_1()
            .border_color(theme.border)
            .child(self.render_member_filter(cx))
            .child(div().flex_1())
            .child(
                div()
                    .text_size(KeyValueMetrics::FOOTER_FONT)
                    .text_color(theme.muted_foreground)
                    .child(dbflux_i18n::t!(
                        "document.key_value.value.loaded_summary",
                        command = source_label,
                        shown = group_thousands(visible.len() as u64),
                        loaded = group_thousands(loaded as u64)
                    )),
            );

        let header = div()
            .flex()
            .flex_none()
            .items_center()
            .h(KeyValueMetrics::MEMBER_HEADER_HEIGHT)
            .px(KeyValueMetrics::MEMBER_PADDING_X)
            .border_b_1()
            .border_color(theme.input)
            .bg(theme.background)
            .text_size(KeyValueMetrics::LIST_HEADER_FONT)
            .text_color(theme.muted_foreground)
            .child(div().w(KeyValueMetrics::INDEX_COLUMN).child("#"))
            .when(is_hash, |header| {
                header.child(
                    div()
                        .w(KeyValueMetrics::FIELD_COLUMN)
                        .child(dbflux_i18n::t!("document.key_value.value.column_field")),
                )
            })
            .child(
                div()
                    .flex_1()
                    .child(dbflux_i18n::t!("document.key_value.render.value_header")),
            )
            .child(
                div()
                    .w(KeyValueMetrics::FORMAT_COLUMN)
                    .child(dbflux_i18n::t!("document.key_value.value.column_format")),
            )
            .child(div().w(KeyValueMetrics::ACTION_COLUMN));

        let entity = cx.entity().clone();
        let list_visible = visible.clone();

        let list = uniform_list("kv-member-list", visible.len(), move |range, _, cx| {
            entity.update(cx, |this, cx| {
                range
                    .filter_map(|position| list_visible.get(position).copied())
                    .map(|member_index| this.render_member_row(member_index, is_hash, cx))
                    .collect()
            })
        })
        .flex_1()
        .min_h_0();

        div()
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .child(toolbar)
            .child(header)
            .child(list)
            .into_any_element()
    }

    fn render_member_row(
        &mut self,
        member_index: usize,
        is_hash: bool,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme().clone();
        let strong = ChromeColors::strong(&theme);
        let tint = ChromeColors::tint(&theme);

        let Some(member) = self.cached_members.get(member_index).cloned() else {
            return div().into_any_element();
        };

        let is_selected = self.selected_member_index == Some(member_index)
            && self.focus_mode == KeyValueFocusMode::ValuePanel;
        let is_editing = self.editing_member_index == Some(member_index);
        let is_json = parses_as_json_document(&member.display);

        let value_cell = if is_editing {
            match &self.member_edit_input {
                Some(input) => div()
                    .flex_1()
                    .min_w_0()
                    .child(Input::new(input).small().w_full())
                    .into_any_element(),
                None => div().flex_1().into_any_element(),
            }
        } else {
            let content = if is_json {
                self.render_inline_json(&member.display, cx)
                    .into_any_element()
            } else {
                div().child(member.display.clone()).into_any_element()
            };

            div()
                .flex_1()
                .min_w_0()
                .whitespace_nowrap()
                .overflow_hidden()
                .text_ellipsis()
                .child(content)
                .into_any_element()
        };

        div()
            .id(("kv-member-row", member_index))
            .flex()
            .items_center()
            .h(KeyValueMetrics::MEMBER_ROW_HEIGHT)
            .px(KeyValueMetrics::MEMBER_PADDING_X)
            .border_b_1()
            .border_color(theme.table_row_border)
            .font_family(AppFonts::MONO)
            .text_size(KeyValueMetrics::LIST_ROW_FONT)
            .text_color(theme.foreground)
            .when(is_selected, |row| {
                row.bg(tint.opacity(KeyValueMetrics::SELECTED_MEMBER_ALPHA))
            })
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, _, _, cx| {
                    cx.stop_propagation();
                    this.focus_mode = KeyValueFocusMode::ValuePanel;
                    this.selected_member_index = Some(member_index);
                    cx.emit(DocumentEvent::RequestFocus);
                    cx.notify();
                }),
            )
            .on_click(cx.listener(move |this, event: &ClickEvent, window, cx| {
                if event.click_count() == 2 {
                    this.start_member_edit(member_index, window, cx);
                }
            }))
            .on_mouse_down(
                MouseButton::Right,
                cx.listener(move |this, event: &MouseDownEvent, window, cx| {
                    cx.stop_propagation();
                    this.focus_mode = KeyValueFocusMode::ValuePanel;
                    this.selected_member_index = Some(member_index);
                    cx.emit(DocumentEvent::RequestFocus);
                    this.open_context_menu(KvMenuTarget::Value, event.position, window, cx);
                }),
            )
            .child(
                div()
                    .w(KeyValueMetrics::INDEX_COLUMN)
                    .text_color(theme.input)
                    .child((member_index + 1).to_string()),
            )
            .when(is_hash, |row| {
                row.child(
                    div()
                        .w(KeyValueMetrics::FIELD_COLUMN)
                        .pr(Spacing::SM)
                        .whitespace_nowrap()
                        .overflow_hidden()
                        .text_ellipsis()
                        .text_color(strong)
                        .child(member.field.clone().unwrap_or_default()),
                )
            })
            .child(value_cell)
            .child(
                div()
                    .w(KeyValueMetrics::FORMAT_COLUMN)
                    .pl(Spacing::SM)
                    .font_family(AppFonts::INTERFACE)
                    .child(if is_json {
                        Badge::new("JSON", BadgeTone::Accent)
                    } else {
                        Badge::new(
                            dbflux_i18n::t!("document.key_value.view_as.auto"),
                            BadgeTone::Neutral,
                        )
                    }),
            )
            .child(self.render_member_delete_cell(member_index, cx))
            .into_any_element()
    }

    fn render_member_delete_cell(
        &self,
        member_index: usize,
        cx: &mut Context<Self>,
    ) -> impl IntoElement + use<> {
        let muted = cx.theme().muted_foreground;
        let danger = cx.theme().danger;

        div()
            .id(("kv-member-delete", member_index))
            .w(KeyValueMetrics::ACTION_COLUMN)
            .flex()
            .justify_center()
            .cursor_pointer()
            .child(
                Icon::new(AppIcon::Delete)
                    .size(KeyValueMetrics::ACTION_ICON)
                    .color(muted),
            )
            .hover(move |cell| cell.text_color(danger))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, _, _, cx| {
                    cx.stop_propagation();
                    this.request_delete_member(member_index, cx);
                }),
            )
    }

    fn render_inline_json(&self, text: &str, cx: &App) -> impl IntoElement + use<> {
        let theme = cx.theme();
        let syntax = SyntaxColors::for_current(cx);

        div().flex().children(
            super::decode::inline_json_spans(text)
                .into_iter()
                .map(|span| {
                    let color = match span.kind {
                        SpanKind::Key => syntax.type_name,
                        SpanKind::Literal => syntax.number,
                        SpanKind::Dim => theme.muted_foreground,
                        SpanKind::Plain => theme.foreground,
                    };

                    div().text_color(color).child(span.text)
                }),
        )
    }

    fn render_string_body(&mut self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();
        let entity = cx.entity().downgrade();

        let Some(value) = self.selected_value.as_ref() else {
            return div().into_any_element();
        };

        let key_type = value.entry.key_type.unwrap_or(KeyType::Unknown);
        let editable = may_edit_value(key_type, value, self.value_view_as, self.value_compression);
        let is_binary = value.repr == ValueRepr::Binary;

        let view_control = {
            let entity = entity.clone();
            SegmentedControl::new(
                ViewAs::ALL
                    .iter()
                    .map(|view| SegmentedItem::new(view.id(), view.label()))
                    .collect(),
                self.value_view_as.id(),
                move |id, _, cx| {
                    if let Some(view) = ViewAs::from_id(id.as_ref()) {
                        update_document(&entity, cx, |this, cx| this.set_value_view_as(view, cx));
                    }
                },
            )
        };

        let toolbar = self
            .render_value_toolbar()
            .border_b_1()
            .border_color(theme.border)
            .child(
                div()
                    .text_size(KeyValueMetrics::FOOTER_FONT)
                    .text_color(theme.muted_foreground)
                    .child(dbflux_i18n::t!("document.key_value.view_as.label")),
            )
            .child(view_control)
            .child(
                div()
                    .ml(Spacing::SM)
                    .text_size(KeyValueMetrics::FOOTER_FONT)
                    .text_color(theme.muted_foreground)
                    .child(dbflux_i18n::t!("document.key_value.view_as.compression")),
            )
            .child(
                div()
                    .w(KeyValueMetrics::COMPRESSION_WIDTH)
                    .child(self.compression_dropdown.clone()),
            )
            .child(div().flex_1())
            .when(editable && self.string_edit_input.is_none(), |toolbar| {
                toolbar.child(
                    Button::new(
                        "kv-edit-value",
                        dbflux_i18n::t!("document.key_value.value.edit"),
                    )
                    .primary()
                    .icon(AppIcon::Pencil)
                    .kbd("Enter")
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.start_string_edit(window, cx);
                    })),
                )
            })
            .when(
                !editable && is_binary && self.value_compression != Compression::None,
                |toolbar| {
                    toolbar.child(
                        Text::caption(dbflux_i18n::t!(
                            "document.key_value.render.decode.edit_locked"
                        ))
                        .color(theme.muted_foreground),
                    )
                },
            );

        if let Some(input) = &self.string_edit_input {
            return div()
                .flex_1()
                .min_h_0()
                .flex()
                .flex_col()
                .child(toolbar)
                .child(
                    div()
                        .p(KeyValueMetrics::VALUE_PADDING_X)
                        .font_family(AppFonts::MONO)
                        .child(Input::new(input).w_full()),
                )
                .into_any_element();
        }

        let notice = self
            .rendered_value
            .as_ref()
            .and_then(|rendered| rendered.notice.clone());
        let line_count = self
            .rendered_value
            .as_ref()
            .map(|rendered| rendered.lines.len())
            .unwrap_or(0);

        let lines = if self.rendered_value.is_none() {
            div()
                .flex_1()
                .flex()
                .items_center()
                .justify_center()
                .child(
                    Icon::new(AppIcon::Loader)
                        .size(KeyValueMetrics::META_ICON)
                        .color(theme.muted_foreground),
                )
                .into_any_element()
        } else {
            let entity = cx.entity().clone();

            uniform_list("kv-value-lines", line_count, move |range, _, cx| {
                entity.update(cx, |this, cx| {
                    range
                        .map(|line_index| this.render_value_line(line_index, cx))
                        .collect()
                })
            })
            .flex_1()
            .min_h_0()
            .into_any_element()
        };

        div()
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .child(toolbar)
            .when_some(notice, |body, notice| {
                body.child(
                    div()
                        .flex()
                        .items_center()
                        .gap(Spacing::XS)
                        .px(KeyValueMetrics::VALUE_PADDING_X)
                        .py(Spacing::XS)
                        .bg(theme.warning.opacity(KeyValueMetrics::CALLOUT_FILL_ALPHA))
                        .child(
                            Icon::new(AppIcon::TriangleAlert)
                                .size(KeyValueMetrics::META_ICON)
                                .color(theme.warning),
                        )
                        .child(Text::caption(notice).color(theme.muted_foreground)),
                )
            })
            .child(
                div()
                    .id("kv-value-text")
                    .flex_1()
                    .min_h_0()
                    .flex()
                    .flex_col()
                    .pt(KeyValueMetrics::VALUE_PADDING_TOP)
                    .bg(theme.background)
                    .font_family(AppFonts::MONO)
                    .text_size(FontSizes::SM)
                    .when(editable, |text| {
                        text.cursor_pointer().on_click(cx.listener(
                            |this, event: &ClickEvent, window, cx| {
                                if event.click_count() == 2 {
                                    this.start_string_edit(window, cx);
                                }
                            },
                        ))
                    })
                    .on_mouse_down(
                        MouseButton::Right,
                        cx.listener(|this, event: &MouseDownEvent, window, cx| {
                            cx.stop_propagation();
                            this.focus_mode = KeyValueFocusMode::ValuePanel;
                            cx.emit(DocumentEvent::RequestFocus);
                            this.open_context_menu(KvMenuTarget::Value, event.position, window, cx);
                        }),
                    )
                    .child(lines),
            )
            .into_any_element()
    }

    fn render_value_line(&self, line_index: usize, cx: &App) -> AnyElement {
        let theme = cx.theme();
        let syntax = SyntaxColors::for_current(cx);
        let strong = ChromeColors::strong(theme);

        let Some(line) = self
            .rendered_value
            .as_ref()
            .and_then(|rendered| rendered.lines.get(line_index))
        else {
            return div().into_any_element();
        };

        div()
            .flex()
            .h(KeyValueMetrics::VALUE_LINE_HEIGHT)
            .items_center()
            .child(
                div()
                    .flex_none()
                    .w(KeyValueMetrics::LINE_NUMBER_WIDTH)
                    .pr(KeyValueMetrics::LINE_NUMBER_PADDING_RIGHT)
                    .flex()
                    .justify_end()
                    .text_color(theme.input)
                    .child((line_index + 1).to_string()),
            )
            .child(
                div()
                    .flex()
                    .whitespace_nowrap()
                    .pl(KeyValueMetrics::JSON_INDENT * line.indent as f32)
                    .children(line.spans.iter().map(|span| {
                        let color = match span.kind {
                            SpanKind::Key => syntax.type_name,
                            SpanKind::Literal => syntax.number,
                            SpanKind::Dim => theme.muted_foreground,
                            SpanKind::Plain => strong,
                        };

                        div().text_color(color).child(span.text.clone())
                    })),
            )
            .into_any_element()
    }

    fn render_zset_body(&mut self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();
        let entity = cx.entity().downgrade();

        let Some(pane) = self.zset_pane.as_ref() else {
            return div().into_any_element();
        };

        let order = pane.order;
        let loaded = pane.members.len() as u64;
        let total = pane.total;
        let loading = pane.loading;

        let order_control = SegmentedControl::new(
            vec![
                SegmentedItem::new(
                    "desc",
                    dbflux_i18n::t!("document.key_value.zset.highest_first"),
                ),
                SegmentedItem::new(
                    "asc",
                    dbflux_i18n::t!("document.key_value.zset.lowest_first"),
                ),
            ],
            if order == RangeOrder::Descending {
                "desc"
            } else {
                "asc"
            },
            move |id, _, cx| {
                let order = if id.as_ref() == "desc" {
                    RangeOrder::Descending
                } else {
                    RangeOrder::Ascending
                };
                update_document(&entity, cx, |this, cx| this.set_zset_order(order, cx));
            },
        );

        let toolbar = self
            .render_value_toolbar()
            .border_b_1()
            .border_color(theme.border)
            .child(self.render_member_filter(cx))
            .child(order_control)
            .child(div().flex_1())
            .child(
                div()
                    .text_size(KeyValueMetrics::FOOTER_FONT)
                    .text_color(theme.muted_foreground)
                    .child(dbflux_i18n::t!(
                        "document.key_value.zset.loaded_summary",
                        loaded = group_thousands(loaded),
                        total = group_thousands(total)
                    )),
            )
            .when(loaded < total, |toolbar| {
                toolbar.child(
                    Button::new(
                        "kv-zset-load-more",
                        dbflux_i18n::t!(
                            "document.key_value.zset.load_more",
                            count = super::collection_panes::ZSET_PAGE_SIZE
                        ),
                    )
                    .icon(if loading {
                        AppIcon::Loader
                    } else {
                        AppIcon::ChevronDown
                    })
                    .disabled(loading)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.load_more_ranged(cx);
                    })),
                )
            });

        let header = div()
            .flex()
            .flex_none()
            .items_center()
            .h(KeyValueMetrics::MEMBER_HEADER_HEIGHT)
            .px(KeyValueMetrics::MEMBER_PADDING_X)
            .border_b_1()
            .border_color(theme.input)
            .bg(theme.background)
            .text_size(KeyValueMetrics::LIST_HEADER_FONT)
            .text_color(theme.muted_foreground)
            .child(
                div()
                    .w(KeyValueMetrics::RANK_COLUMN)
                    .child(dbflux_i18n::t!("document.key_value.zset.column_rank")),
            )
            .child(
                div()
                    .flex_1()
                    .child(dbflux_i18n::t!("document.key_value.zset.column_member")),
            )
            .child(
                div()
                    .w(KeyValueMetrics::SCORE_COLUMN)
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .child(dbflux_i18n::t!("document.key_value.zset.column_score"))
                    .child(
                        Icon::new(AppIcon::ArrowUpDown)
                            .size(KeyValueMetrics::META_EDIT_ICON)
                            .color(theme.muted_foreground),
                    ),
            )
            .child(
                div()
                    .w(KeyValueMetrics::BAR_COLUMN)
                    .child(dbflux_i18n::t!("document.key_value.zset.column_relative")),
            )
            .child(div().w(KeyValueMetrics::ACTION_COLUMN));

        let visible = Rc::new(self.filtered_member_indices(cx));
        let entity = cx.entity().clone();
        let list_visible = visible.clone();

        let list = uniform_list("kv-zset-list", visible.len(), move |range, _, cx| {
            entity.update(cx, |this, cx| {
                range
                    .filter_map(|position| list_visible.get(position).copied())
                    .map(|member_index| this.render_zset_row(member_index, cx))
                    .collect()
            })
        })
        .flex_1()
        .min_h_0();

        div()
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .child(toolbar)
            .child(header)
            .child(list)
            .into_any_element()
    }

    fn render_zset_row(&mut self, member_index: usize, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();
        let strong = ChromeColors::strong(&theme);
        let tint = ChromeColors::tint(&theme);

        let Some(member) = self.cached_members.get(member_index).cloned() else {
            return div().into_any_element();
        };
        let (top, bottom) = self
            .zset_pane
            .as_ref()
            .map(|pane| pane.reference_scores())
            .unwrap_or((0.0, 0.0));

        let score = member.score.unwrap_or(0.0);
        let rank = member_index + 1;
        let is_selected = self.selected_member_index == Some(member_index)
            && self.focus_mode == KeyValueFocusMode::ValuePanel;
        let is_editing = self.editing_member_index == Some(member_index);
        let fraction = relative_bar_fraction(score, top, bottom);

        let member_cell = match (is_editing, &self.member_edit_input) {
            (true, Some(input)) => div()
                .flex_1()
                .pr(Spacing::SM)
                .child(Input::new(input).small().w_full())
                .into_any_element(),
            _ => div()
                .flex_1()
                .min_w_0()
                .whitespace_nowrap()
                .overflow_hidden()
                .text_ellipsis()
                .text_color(strong)
                .child(member.display.clone())
                .into_any_element(),
        };

        let score_cell = match (is_editing, &self.member_edit_score_input) {
            (true, Some(input)) => div()
                .w(KeyValueMetrics::SCORE_COLUMN)
                .pr(Spacing::SM)
                .child(Input::new(input).small().w_full())
                .into_any_element(),
            _ => div()
                .w(KeyValueMetrics::SCORE_COLUMN)
                .text_color(theme.foreground)
                .child(format_score(score))
                .into_any_element(),
        };

        div()
            .id(("kv-zset-row", member_index))
            .flex()
            .items_center()
            .h(KeyValueMetrics::RANKED_ROW_HEIGHT)
            .px(KeyValueMetrics::MEMBER_PADDING_X)
            .border_b_1()
            .border_color(theme.table_row_border)
            .font_family(AppFonts::MONO)
            .text_size(KeyValueMetrics::LIST_ROW_FONT)
            .when(is_selected, |row| {
                row.bg(tint.opacity(KeyValueMetrics::SELECTED_MEMBER_ALPHA))
            })
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, _, _, cx| {
                    cx.stop_propagation();
                    this.focus_mode = KeyValueFocusMode::ValuePanel;
                    this.selected_member_index = Some(member_index);
                    cx.emit(DocumentEvent::RequestFocus);
                    cx.notify();
                }),
            )
            .on_click(cx.listener(move |this, event: &ClickEvent, window, cx| {
                if event.click_count() == 2 {
                    this.start_member_edit(member_index, window, cx);
                }
            }))
            .on_mouse_down(
                MouseButton::Right,
                cx.listener(move |this, event: &MouseDownEvent, window, cx| {
                    cx.stop_propagation();
                    this.focus_mode = KeyValueFocusMode::ValuePanel;
                    this.selected_member_index = Some(member_index);
                    cx.emit(DocumentEvent::RequestFocus);
                    this.open_context_menu(KvMenuTarget::Value, event.position, window, cx);
                }),
            )
            .child(
                div()
                    .w(KeyValueMetrics::RANK_COLUMN)
                    .text_color(if rank <= 3 { tint } else { theme.input })
                    .child(rank.to_string()),
            )
            .child(member_cell)
            .child(score_cell)
            .child(
                div()
                    .w(KeyValueMetrics::BAR_COLUMN)
                    .h(KeyValueMetrics::BAR_HEIGHT)
                    .bg(theme.secondary)
                    .child(div().h_full().w(relative(fraction)).bg(theme.warning)),
            )
            .child(self.render_member_delete_cell(member_index, cx))
            .into_any_element()
    }

    fn render_stream_body(&mut self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();
        let entity = cx.entity().downgrade();

        if self.value_view_mode == KvValueViewMode::Document {
            return self.render_members_body(cx);
        }

        let Some(pane) = self.stream_pane.as_ref() else {
            return div().into_any_element();
        };

        let order = pane.order;
        let loaded = pane.entries.len() as u64;
        let total = pane.total;
        let loading = pane.loading;
        let start_input = pane.start_input.clone();
        let end_input = pane.end_input.clone();
        let columns = Rc::new(stream_field_columns(&pane.entries));

        let order_control = SegmentedControl::new(
            vec![
                SegmentedItem::new(
                    "desc",
                    dbflux_i18n::t!("document.key_value.stream.newest_first"),
                ),
                SegmentedItem::new(
                    "asc",
                    dbflux_i18n::t!("document.key_value.stream.oldest_first"),
                ),
            ],
            if order == RangeOrder::Descending {
                "desc"
            } else {
                "asc"
            },
            move |id, _, cx| {
                let order = if id.as_ref() == "desc" {
                    RangeOrder::Descending
                } else {
                    RangeOrder::Ascending
                };
                update_document(&entity, cx, |this, cx| this.set_stream_order(order, cx));
            },
        );

        let range_field = |id: &'static str, input: Entity<InputState>, icon: AppIcon| {
            div()
                .w(KeyValueMetrics::RANGE_INPUT_WIDTH)
                .flex_none()
                .font_family(AppFonts::MONO)
                .child(
                    Input::new(&input).id(id).w_full().prefix(
                        Icon::new(icon)
                            .size(KeyValueMetrics::FOLDER_ICON)
                            .color(theme.muted_foreground),
                    ),
                )
        };

        let command = if order == RangeOrder::Descending {
            "XREVRANGE"
        } else {
            "XRANGE"
        };

        let toolbar = self
            .render_value_toolbar()
            .border_b_1()
            .border_color(theme.border)
            .child(range_field(
                "kv-stream-start",
                start_input,
                AppIcon::ChevronLeft,
            ))
            .child(range_field(
                "kv-stream-end",
                end_input,
                AppIcon::ChevronRight,
            ))
            .child(order_control)
            .child(div().flex_1())
            .child(
                div()
                    .id("kv-stream-loaded-summary")
                    .flex_none()
                    .whitespace_nowrap()
                    .text_size(KeyValueMetrics::FOOTER_FONT)
                    .text_color(theme.muted_foreground)
                    .child(dbflux_i18n::t!(
                        "document.key_value.stream.loaded_summary",
                        command = command,
                        loaded = group_thousands(loaded),
                        total = group_thousands(total)
                    )),
            )
            .when(loaded < total, |toolbar| {
                toolbar.child(
                    Button::new(
                        "kv-stream-load-more",
                        dbflux_i18n::t!("document.key_value.stream.load_more"),
                    )
                    .inline()
                    .icon(if loading {
                        AppIcon::Loader
                    } else {
                        AppIcon::ChevronDown
                    })
                    .disabled(loading)
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.load_more_ranged(cx);
                    })),
                )
            });

        let column_count = columns.len();
        let header = div()
            .flex()
            .flex_none()
            .items_center()
            .h(KeyValueMetrics::MEMBER_HEADER_HEIGHT)
            .px(KeyValueMetrics::MEMBER_PADDING_X)
            .border_b_1()
            .border_color(theme.input)
            .bg(theme.background)
            .text_size(KeyValueMetrics::LIST_HEADER_FONT)
            .text_color(theme.muted_foreground)
            .child(
                div()
                    .w(KeyValueMetrics::ENTRY_ID_COLUMN)
                    .child(dbflux_i18n::t!("document.key_value.stream.column_id")),
            )
            .child(
                div()
                    .w(KeyValueMetrics::ENTRY_TIME_COLUMN)
                    .child(dbflux_i18n::t!("document.key_value.stream.column_time")),
            )
            .children(columns.iter().enumerate().map(|(index, column)| {
                let cell = div()
                    .whitespace_nowrap()
                    .overflow_hidden()
                    .text_ellipsis()
                    .child(column.clone());

                if index + 1 == column_count {
                    cell.flex_1()
                } else {
                    cell.w(KeyValueMetrics::ENTRY_FIELD_COLUMN)
                }
            }));

        let entity = cx.entity().clone();
        let list_columns = columns.clone();
        let entry_count = loaded as usize;
        let now = local_now();

        let list = uniform_list("kv-stream-list", entry_count, move |range, _, cx| {
            entity.update(cx, |this, cx| {
                range
                    .map(|entry_index| this.render_stream_row(entry_index, &list_columns, &now, cx))
                    .collect()
            })
        })
        .flex_1()
        .min_h_0();

        div()
            .flex_1()
            .min_h_0()
            .min_w_0()
            .flex()
            .flex_col()
            .child(toolbar)
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .min_w_0()
                    .flex()
                    .flex_col()
                    .child(header)
                    .child(list),
            )
            .into_any_element()
    }

    /// Whether the consumer groups of the selected stream are on screen: a
    /// loaded stream in the entries view, on a connection that reads groups.
    fn shows_stream_groups(&self) -> bool {
        let Some(value) = self.selected_value.as_ref() else {
            return false;
        };

        !matches!(value.load_state, KeyLoadState::TooLarge { .. })
            && self.zset_pane.is_none()
            && self.stream_pane.is_some()
            && self.value_view_mode != KvValueViewMode::Document
            && self.key_features.contains(KeyValueFeatures::STREAM_GROUPS)
    }

    /// The consumer groups panel, for the workspace to draw as a full-height
    /// island at the far right (IslKvStream).
    pub(super) fn side_panels(&self, cx: &mut Context<Self>) -> Vec<DocumentSidePanel> {
        if !self.shows_stream_groups() {
            return Vec::new();
        }

        vec![DocumentSidePanel {
            id: "kv-stream-groups".into(),
            width: KeyValueMetrics::GROUPS_WIDTH,
            content: self.render_consumer_groups(cx),
        }]
    }

    fn render_stream_row(
        &mut self,
        entry_index: usize,
        columns: &[String],
        now: &chrono::DateTime<chrono::Local>,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme().clone();
        let strong = ChromeColors::strong(&theme);
        let tint = ChromeColors::tint(&theme);

        let Some(entry) = self
            .stream_pane
            .as_ref()
            .and_then(|pane| pane.entries.get(entry_index))
            .cloned()
        else {
            return div().into_any_element();
        };

        let is_selected = self.selected_member_index == Some(entry_index)
            && self.focus_mode == KeyValueFocusMode::ValuePanel;
        let column_count = columns.len();

        div()
            .id(("kv-stream-row", entry_index))
            .flex()
            .items_center()
            .h(KeyValueMetrics::RANKED_ROW_HEIGHT)
            .px(KeyValueMetrics::MEMBER_PADDING_X)
            .border_b_1()
            .border_color(theme.table_row_border)
            .font_family(AppFonts::MONO)
            .text_size(KeyValueMetrics::LIST_ROW_FONT)
            .when(is_selected, |row| {
                row.bg(tint.opacity(KeyValueMetrics::SELECTED_MEMBER_ALPHA))
            })
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, _, _, cx| {
                    cx.stop_propagation();
                    this.focus_mode = KeyValueFocusMode::ValuePanel;
                    this.selected_member_index = Some(entry_index);
                    cx.emit(DocumentEvent::RequestFocus);
                    cx.notify();
                }),
            )
            .on_mouse_down(
                MouseButton::Right,
                cx.listener(move |this, event: &MouseDownEvent, window, cx| {
                    cx.stop_propagation();
                    this.focus_mode = KeyValueFocusMode::ValuePanel;
                    this.selected_member_index = Some(entry_index);
                    cx.emit(DocumentEvent::RequestFocus);
                    this.open_context_menu(KvMenuTarget::Value, event.position, window, cx);
                }),
            )
            .child(
                div()
                    .w(KeyValueMetrics::ENTRY_ID_COLUMN)
                    .text_color(theme.muted_foreground)
                    .child(entry.id.clone()),
            )
            .child(
                div()
                    .w(KeyValueMetrics::ENTRY_TIME_COLUMN)
                    .text_color(theme.foreground)
                    .child(entry_time_label(&entry.id, now).unwrap_or_default()),
            )
            .children(columns.iter().enumerate().map(|(index, column)| {
                let value = entry
                    .fields
                    .iter()
                    .find(|(field, _)| field == column)
                    .map(|(_, value)| value.clone())
                    .unwrap_or_default();

                let cell = div()
                    .whitespace_nowrap()
                    .overflow_hidden()
                    .text_ellipsis()
                    .pr(Spacing::SM)
                    .text_color(if index == 0 { strong } else { theme.foreground })
                    .child(value);

                if index + 1 == column_count {
                    cell.flex_1()
                } else {
                    cell.w(KeyValueMetrics::ENTRY_FIELD_COLUMN)
                }
            }))
            .into_any_element()
    }

    fn render_consumer_groups(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();
        let strong = ChromeColors::strong(&theme);
        let tint = ChromeColors::tint(&theme);
        let muted = theme.muted_foreground;

        let Some(pane) = self.stream_pane.as_ref() else {
            return div().into_any_element();
        };

        let groups_header = div()
            .flex()
            .flex_none()
            .items_center()
            .gap(Spacing::SM)
            .h(KeyValueMetrics::GROUPS_HEADER_HEIGHT)
            .px(KeyValueMetrics::GROUPS_PADDING_X)
            .border_b_1()
            .border_color(theme.border)
            .child(
                Icon::new(AppIcon::Layers)
                    .size(KeyValueMetrics::FOLDER_ICON)
                    .color(tint),
            )
            .child(
                div()
                    .font_weight(FontWeight::BOLD)
                    .text_color(strong)
                    .child(dbflux_i18n::t!("document.key_value.stream.groups_title")),
            );

        let table_header = div()
            .flex()
            .flex_none()
            .items_center()
            .h(KeyValueMetrics::GROUPS_TABLE_HEADER_HEIGHT)
            .px(KeyValueMetrics::GROUPS_PADDING_X)
            .border_b_1()
            .border_color(theme.input)
            .text_size(KeyValueMetrics::LIST_HEADER_FONT)
            .text_color(muted)
            .child(
                div()
                    .flex_1()
                    .child(dbflux_i18n::t!("document.key_value.stream.column_group")),
            )
            .child(
                div()
                    .w(KeyValueMetrics::GROUPS_COUNT_COLUMN)
                    .child(dbflux_i18n::t!("document.key_value.stream.column_readers")),
            )
            .child(
                div()
                    .w(KeyValueMetrics::GROUPS_COUNT_COLUMN)
                    .child(dbflux_i18n::t!("document.key_value.stream.column_pending")),
            )
            .child(
                div()
                    .w(KeyValueMetrics::GROUPS_ID_COLUMN)
                    .child(dbflux_i18n::t!(
                        "document.key_value.stream.column_last_read"
                    )),
            );

        let rows = pane.groups.iter().enumerate().map(|(index, group)| {
            div()
                .id(("kv-stream-group", index))
                .flex()
                .items_center()
                .h(KeyValueMetrics::GROUPS_ROW_HEIGHT)
                .px(KeyValueMetrics::GROUPS_PADDING_X)
                .border_b_1()
                .border_color(theme.table_row_border)
                .font_family(AppFonts::MONO)
                .text_size(KeyValueMetrics::LIST_ROW_FONT)
                .child(div().flex_1().text_color(strong).child(group.name.clone()))
                .child(
                    div()
                        .w(KeyValueMetrics::GROUPS_COUNT_COLUMN)
                        .child(group.consumers.len().to_string()),
                )
                .child(
                    div()
                        .w(KeyValueMetrics::GROUPS_COUNT_COLUMN)
                        .text_color(if group.pending > 0 {
                            theme.warning
                        } else {
                            muted
                        })
                        .child(group.pending.to_string()),
                )
                .child(
                    div()
                        .w(KeyValueMetrics::GROUPS_ID_COLUMN)
                        .text_size(KeyValueMetrics::LIST_META_FONT)
                        .text_color(muted)
                        .child(short_entry_id(&group.last_delivered_id)),
                )
        });

        let callout = busiest_group(&pane.groups).map(|group| {
            let name = group.name.clone();
            let claim_open = pane
                .claim
                .as_ref()
                .is_some_and(|claim| claim.group == group.name);
            let claim_input = pane
                .claim
                .as_ref()
                .filter(|claim| claim.group == group.name)
                .map(|claim| claim.consumer_input.clone());

            let idle = group
                .oldest_pending_idle_ms
                .map(|idle| {
                    dbflux_i18n::t!(
                        "document.key_value.stream.callout_idle",
                        idle = idle_label(idle)
                    )
                })
                .unwrap_or_default();

            let view_name = name.clone();
            let claim_name = name.clone();

            div()
                .relative()
                .flex()
                .flex_col()
                .gap(KeyValueMetrics::CALLOUT_GAP)
                .m(KeyValueMetrics::CALLOUT_MARGIN)
                .p(KeyValueMetrics::CALLOUT_PADDING)
                .child(
                    Chamfer::new(ChamferCut::INPUT)
                        .fill(theme.warning.opacity(KeyValueMetrics::CALLOUT_FILL_ALPHA))
                        .left_edge(theme.warning, KeyValueMetrics::CALLOUT_STRIPE),
                )
                .child(
                    div()
                        .font_weight(FontWeight::SEMIBOLD)
                        .text_color(strong)
                        .child(dbflux_i18n::t!(
                            "document.key_value.stream.callout_title",
                            group = name.as_str(),
                            count = group.pending
                        )),
                )
                .child(
                    div()
                        .text_size(FontSizes::XS)
                        .text_color(muted)
                        .child(format!(
                            "{idle}{}",
                            dbflux_i18n::t!("document.key_value.stream.callout_body")
                        )),
                )
                .child(
                    div()
                        .flex()
                        .gap(Spacing::SM)
                        .child(
                            Button::new(
                                "kv-stream-view-pending",
                                dbflux_i18n::t!("document.key_value.stream.view_pending"),
                            )
                            .icon(AppIcon::Eye)
                            .on_click(cx.listener(
                                move |this, _, _, cx| {
                                    this.view_pending_entries(view_name.clone(), cx);
                                },
                            )),
                        )
                        .child(
                            Button::new(
                                "kv-stream-claim",
                                dbflux_i18n::t!("document.key_value.stream.claim"),
                            )
                            .icon(AppIcon::ArrowLeftRight)
                            .selected(claim_open)
                            .on_click(cx.listener(
                                move |this, _, window, cx| {
                                    this.open_claim_form(claim_name.clone(), window, cx);
                                },
                            )),
                        ),
                )
                .when_some(claim_input, |callout, input| {
                    callout.child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::SM)
                            .child(
                                div()
                                    .flex_1()
                                    .font_family(AppFonts::MONO)
                                    .child(Input::new(&input).small().w_full()),
                            )
                            .child(
                                Button::new(
                                    "kv-stream-claim-confirm",
                                    dbflux_i18n::t!("document.key_value.stream.claim_confirm"),
                                )
                                .primary()
                                .on_click(cx.listener(
                                    |this, _, _, cx| {
                                        this.claim_pending_entries(cx);
                                    },
                                )),
                            )
                            .child(
                                Button::new(
                                    "kv-stream-claim-cancel",
                                    dbflux_i18n::t!("document.key_value.console.cancel"),
                                )
                                .ghost()
                                .on_click(cx.listener(
                                    |this, _, _, cx| {
                                        this.close_claim_form(cx);
                                    },
                                )),
                            ),
                    )
                })
        });

        let pending_list = pane.pending.as_ref().map(|(group, entries)| {
            div()
                .flex()
                .flex_col()
                .mx(KeyValueMetrics::CALLOUT_MARGIN)
                .child(Text::label(dbflux_i18n::t!(
                    "document.key_value.stream.pending_title",
                    group = group.as_str()
                )))
                .children(entries.iter().enumerate().map(|(index, entry)| {
                    div()
                        .id(("kv-stream-pending", index))
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .h(KeyValueMetrics::RANKED_ROW_HEIGHT)
                        .border_b_1()
                        .border_color(theme.table_row_border)
                        .font_family(AppFonts::MONO)
                        .text_size(KeyValueMetrics::LIST_META_FONT)
                        .child(div().flex_1().text_color(strong).child(entry.id.clone()))
                        .child(
                            div()
                                .text_color(theme.foreground)
                                .child(entry.consumer.clone()),
                        )
                        .child(div().text_color(muted).child(idle_label(entry.idle_ms)))
                }))
        });

        div()
            .id("kv-stream-groups")
            .size_full()
            .flex()
            .flex_col()
            .overflow_y_scroll()
            .child(groups_header)
            .child(table_header)
            .children(rows)
            .when(pane.groups_loaded && pane.groups.is_empty(), |panel| {
                panel.child(
                    div()
                        .p(KeyValueMetrics::GROUPS_PADDING_X)
                        .text_size(FontSizes::XS)
                        .text_color(muted)
                        .child(dbflux_i18n::t!("document.key_value.stream.no_groups")),
                )
            })
            .when_some(callout, |panel, callout| panel.child(callout))
            .when_some(pending_list, |panel, pending| panel.child(pending))
            .into_any_element()
    }

    fn render_expiry_popover(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();
        let strong = ChromeColors::strong(&theme);
        let tint = ChromeColors::tint(&theme);
        let muted = theme.muted_foreground;
        let entity = cx.entity().downgrade();

        let Some(editor) = self.expiry_editor.as_ref() else {
            return div().into_any_element();
        };

        let mode = editor.mode;
        let resolved = editor.resolve(cx);
        let error = editor.error.clone();

        let mode_control = SegmentedControl::new(
            ExpiryMode::ALL
                .iter()
                .map(|mode| SegmentedItem::new(mode.id(), mode.label()))
                .collect(),
            mode.id(),
            move |id, window, cx| {
                if let Some(mode) = ExpiryMode::from_id(id.as_ref()) {
                    update_document(&entity, cx, |this, cx| {
                        this.set_expiry_mode(mode, window, cx)
                    });
                }
            },
        );

        let presets = EXPIRY_PRESETS.iter().map(|preset| {
            let preset: &'static str = preset;

            div()
                .id(SharedString::from(format!("kv-expiry-preset-{preset}")))
                .relative()
                .flex()
                .items_center()
                .h(KeyValueMetrics::FORMAT_BADGE_HEIGHT)
                .px(KeyValueMetrics::FORMAT_BADGE_PADDING_X)
                .cursor_pointer()
                .text_size(KeyValueMetrics::FORMAT_BADGE_FONT)
                .font_weight(FontWeight::SEMIBOLD)
                .text_color(theme.foreground)
                .child(
                    Chamfer::new(ChamferCut::KEYCAP)
                        .fill(theme.secondary)
                        .fill_hover(theme.secondary_hover)
                        .interactive(SharedString::from(format!(
                            "kv-expiry-preset-fill-{preset}"
                        ))),
                )
                .child(preset)
                .on_click(cx.listener(move |this, _, window, cx| {
                    this.apply_expiry_preset(preset, window, cx);
                }))
        });

        let fields = match mode {
            ExpiryMode::Never => div()
                .text_size(FontSizes::XS)
                .text_color(muted)
                .child(dbflux_i18n::t!("document.key_value.expiry.never_body"))
                .into_any_element(),
            ExpiryMode::In => div()
                .flex()
                .items_center()
                .gap(Spacing::SM)
                .child(
                    div()
                        .w(KeyValueMetrics::EXPIRY_DURATION_WIDTH)
                        .font_family(AppFonts::MONO)
                        .child(
                            Input::new(&editor.duration_input)
                                .id("kv-expiry-duration")
                                .w_full(),
                        ),
                )
                .children(presets)
                .into_any_element(),
            ExpiryMode::At => div()
                .w(KeyValueMetrics::EXPIRY_AT_WIDTH)
                .font_family(AppFonts::MONO)
                .child(Input::new(&editor.at_input).id("kv-expiry-at").w_full())
                .into_any_element(),
        };

        let preview = match (&error, &resolved) {
            (Some(message), _) => div()
                .text_size(FontSizes::XS)
                .text_color(theme.danger)
                .child(message.clone())
                .into_any_element(),
            (None, Ok((_, preview))) => div()
                .flex()
                .items_center()
                .gap(Spacing::XS)
                .text_size(FontSizes::XS)
                .text_color(muted)
                .map(|line| match &preview.when {
                    Some(when) => line
                        .child(dbflux_i18n::t!("document.key_value.expiry.expires"))
                        .child(div().text_color(strong).child(when.clone()))
                        .child(dbflux_i18n::t!(
                            "document.key_value.expiry.local_command",
                            command = preview.command.as_str()
                        )),
                    None => line.child(dbflux_i18n::t!(
                        "document.key_value.expiry.never_command",
                        command = preview.command.as_str()
                    )),
                })
                .into_any_element(),
            (None, Err(_)) => div()
                .text_size(FontSizes::XS)
                .text_color(muted)
                .child(dbflux_i18n::t!("document.key_value.expiry.preview_pending"))
                .into_any_element(),
        };

        div()
            .id("kv-expiry-popover")
            .absolute()
            .left(KeyValueMetrics::EXPIRY_OFFSET_LEFT)
            .top(KeyValueMetrics::EXPIRY_OFFSET_TOP)
            .w(KeyValueMetrics::EXPIRY_WIDTH)
            .flex()
            .flex_col()
            .gap(KeyValueMetrics::EXPIRY_GAP)
            .p(KeyValueMetrics::EXPIRY_PADDING)
            .occlude()
            .shadow(vec![Shadows::lg()])
            .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
            .child(
                Chamfer::new(ChamferCut::OVERLAY)
                    .fill(theme.secondary)
                    .border(theme.input),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(
                        Icon::new(AppIcon::Clock)
                            .size(KeyValueMetrics::SEARCH_CARD_ICON)
                            .color(tint),
                    )
                    .child(
                        div()
                            .font_weight(FontWeight::BOLD)
                            .text_color(strong)
                            .child(dbflux_i18n::t!("document.key_value.expiry.title")),
                    )
                    .child(div().flex_1())
                    .when_some(
                        dbflux_ui_base::keymap::shortcut_label(
                            dbflux_app::keymap::ContextId::KeyValue,
                            dbflux_app::keymap::Command::EditExpiry,
                        ),
                        |header, label| header.child(Kbd::new(label)),
                    ),
            )
            .child(mode_control)
            .child(fields)
            .child(preview)
            .child(
                div()
                    .flex()
                    .justify_end()
                    .gap(Spacing::SM)
                    .child(
                        Button::new(
                            "kv-expiry-cancel",
                            dbflux_i18n::t!("document.key_value.console.cancel"),
                        )
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.close_expiry_editor(window, cx);
                        })),
                    )
                    .child(
                        Button::new(
                            "kv-expiry-apply",
                            dbflux_i18n::t!("document.key_value.expiry.apply"),
                        )
                        .primary()
                        .icon(AppIcon::Check)
                        .kbd("Enter")
                        .disabled(resolved.is_err())
                        .on_click(cx.listener(|this, _, window, cx| {
                            this.apply_expiry_editor(window, cx);
                        })),
                    ),
            )
            .into_any_element()
    }
}

#[cfg(test)]
mod tests {
    use super::{member_count_label, value_hint};
    use dbflux_core::KeyType;

    #[test]
    fn member_counts_name_the_collection_kind() {
        assert_eq!(
            member_count_label(KeyType::Hash, 8).as_deref(),
            Some("8 fields")
        );
        assert_eq!(
            member_count_label(KeyType::Hash, 1).as_deref(),
            Some("1 field")
        );
        assert_eq!(
            member_count_label(KeyType::SortedSet, 200).as_deref(),
            Some("200 members")
        );
        assert_eq!(
            member_count_label(KeyType::Stream, 1500).as_deref(),
            Some("1,500 entries")
        );
        assert_eq!(member_count_label(KeyType::String, 3), None);
    }

    #[test]
    fn hints_exist_for_collections_only() {
        assert!(value_hint(Some(KeyType::Hash)).is_some());
        assert!(value_hint(Some(KeyType::SortedSet)).is_some());
        assert!(value_hint(Some(KeyType::String)).is_none());
    }
}
