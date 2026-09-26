//! The key list column: header, virtualized rows (flat or namespace tree)
//! with TTL and size, the search-in-progress card and the scan footer.

use super::KeyValueDocument;
use super::context_menu::KvMenuTarget;
use super::key_tree::{KeyListLayout, KeyListRow, folder_count_label, group_thousands};
use super::metadata::{KeyExpiry, TtlTone, format_size, format_ttl, ttl_tone};
use super::render::type_badge_element;
use crate::handle::DocumentEvent;
use dbflux_components::controls::{Button, Input};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text};
use dbflux_components::tokens::{Borders, ChromeColors, KeyValueMetrics, Spacing};
use dbflux_components::typography::AppFonts;
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use std::time::Instant;

/// Share of the keyspace already loaded or scanned, for the footer bar.
pub(super) fn progress_fraction(done: u64, total: Option<u64>) -> f32 {
    match total {
        Some(total) if total > 0 => (done as f64 / total as f64).clamp(0.0, 1.0) as f32,
        _ => 0.0,
    }
}

impl KeyValueDocument {
    pub(super) fn render_key_list_section(&mut self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme().clone();
        let entity = cx.entity().clone();
        let row_count = self.key_rows.len();
        let show_search_card =
            self.is_filtered_scan(cx) && self.is_scanning() && !self.scan_complete();

        let header = div()
            .flex()
            .flex_none()
            .items_center()
            .h(KeyValueMetrics::LIST_HEADER_HEIGHT)
            .pl(KeyValueMetrics::LIST_PADDING_LEFT)
            .pr(KeyValueMetrics::LIST_PADDING_RIGHT)
            .border_b_1()
            .border_color(theme.input)
            .bg(theme.background)
            .text_size(KeyValueMetrics::LIST_HEADER_FONT)
            .text_color(theme.muted_foreground)
            .child(
                div()
                    .flex_1()
                    .flex()
                    .items_center()
                    .gap(Spacing::XS)
                    .child(dbflux_i18n::t!("document.key_value.list.column_key"))
                    .child(
                        Icon::new(AppIcon::ArrowUpDown)
                            .size(KeyValueMetrics::META_EDIT_ICON)
                            .color(theme.muted_foreground),
                    ),
            )
            .child(
                div()
                    .w(KeyValueMetrics::TTL_COLUMN)
                    .child(dbflux_i18n::t!("document.key_value.list.column_ttl")),
            )
            .child(
                div()
                    .w(KeyValueMetrics::SIZE_COLUMN)
                    .child(dbflux_i18n::t!("document.key_value.list.column_size")),
            );

        let rows = if row_count == 0 {
            self.render_key_list_empty(cx).into_any_element()
        } else {
            uniform_list("kv-key-list", row_count, move |range, _window, cx| {
                entity.update(cx, |this, cx| {
                    this.request_visible_metadata(range.clone(), cx);

                    range
                        .filter_map(|row_index| {
                            this.key_rows
                                .get(row_index)
                                .cloned()
                                .map(|row| this.render_key_row(row_index, &row, cx))
                        })
                        .collect()
                })
            })
            .flex_1()
            .min_h_0()
            .track_scroll(&self.key_list_scroll)
            .into_any_element()
        };

        div()
            .id("kv-key-list-section")
            .w(KeyValueMetrics::KEY_LIST_WIDTH)
            .flex_none()
            .flex()
            .flex_col()
            .min_h_0()
            .border_r_1()
            .border_color(theme.border)
            .child(header)
            .when_some(self.last_error.clone(), |section, message| {
                section.child(
                    div()
                        .px(KeyValueMetrics::LIST_PADDING_LEFT)
                        .py(Spacing::XS)
                        .child(
                            Text::caption(crate::labels::shared_error_prefix(&message)).danger(),
                        ),
                )
            })
            .child(
                div()
                    .flex_1()
                    .min_h_0()
                    .flex()
                    .flex_col()
                    .child(rows)
                    .when(show_search_card, |list| {
                        list.child(self.render_search_card(cx))
                    }),
            )
            .child(self.render_key_list_footer(cx))
            .into_any_element()
    }

    fn render_key_list_empty(&self, cx: &mut Context<Self>) -> impl IntoElement + use<> {
        let theme = cx.theme();
        let loading = self.runner.is_primary_active() && !self.scan_started;

        let message = if loading {
            dbflux_i18n::t!("document.data.grid.loading")
        } else if self.is_filtered_scan(cx) {
            dbflux_i18n::t!("document.key_value.list.no_matches")
        } else {
            dbflux_i18n::t!("document.key_value.list.empty")
        };

        div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .px(KeyValueMetrics::LIST_PADDING_LEFT)
            .py(Spacing::MD)
            .text_size(KeyValueMetrics::FOOTER_FONT)
            .text_color(theme.muted_foreground)
            .when(loading, |row| {
                row.child(
                    Icon::new(AppIcon::Loader)
                        .size(KeyValueMetrics::FOOTER_ICON)
                        .color(theme.muted_foreground),
                )
            })
            .child(message)
    }

    fn render_key_row(
        &mut self,
        row_index: usize,
        row: &KeyListRow,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let indent = KeyValueMetrics::LIST_INDENT * row.depth() as f32;

        match row {
            KeyListRow::Folder {
                prefix,
                key_count,
                expanded,
                ..
            } => self.render_folder_row(row_index, prefix, *key_count, *expanded, indent, cx),
            KeyListRow::Key { key_index, .. } => {
                self.render_key_entry_row(row_index, *key_index, indent, cx)
            }
        }
    }

    fn render_folder_row(
        &self,
        row_index: usize,
        prefix: &str,
        key_count: usize,
        expanded: bool,
        indent: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme().clone();
        let strong = ChromeColors::strong(&theme);
        let is_cursor = self.list_cursor == Some(row_index)
            && self.selected_index.is_none_or(|index| {
                self.key_rows.get(row_index).and_then(KeyListRow::key_index) != Some(index)
            })
            && self.focus_mode == super::KeyValueFocusMode::List;
        let prefix_for_click = prefix.to_string();

        dbflux_components::composites::ListRow::new(("kv-folder-row", row_index))
            .selected(is_cursor)
            .build(cx)
            .flex()
            .items_center()
            .h(KeyValueMetrics::LIST_ROW_HEIGHT)
            .pl(KeyValueMetrics::LIST_PADDING_LEFT + indent)
            .pr(KeyValueMetrics::LIST_PADDING_RIGHT)
            .border_b_1()
            .border_color(theme.table_row_border)
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, _, _, cx| {
                    cx.stop_propagation();
                    this.focus_mode = super::KeyValueFocusMode::List;
                    this.list_cursor = Some(row_index);
                    this.toggle_folder(&prefix_for_click, cx);
                    cx.emit(DocumentEvent::RequestFocus);
                }),
            )
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .flex()
                    .items_center()
                    .gap(KeyValueMetrics::FOLDER_GAP)
                    .child(
                        Icon::new(if expanded {
                            AppIcon::ChevronDown
                        } else {
                            AppIcon::ChevronRight
                        })
                        .size(KeyValueMetrics::FOLDER_CHEVRON)
                        .color(theme.muted_foreground),
                    )
                    .child(
                        Icon::new(AppIcon::Folder)
                            .size(KeyValueMetrics::FOLDER_ICON)
                            .color(theme.muted_foreground),
                    )
                    .child(
                        div()
                            .font_family(AppFonts::MONO)
                            .text_color(strong)
                            .whitespace_nowrap()
                            .overflow_hidden()
                            .text_ellipsis()
                            .child(prefix.to_string()),
                    ),
            )
            .child(
                div()
                    .w(KeyValueMetrics::FOLDER_COUNT_COLUMN)
                    .flex()
                    .justify_end()
                    .font_family(AppFonts::MONO)
                    .text_size(KeyValueMetrics::FOLDER_COUNT_FONT)
                    .text_color(theme.muted_foreground)
                    .child(folder_count_label(key_count, self.scan_complete())),
            )
            .into_any_element()
    }

    fn render_key_entry_row(
        &self,
        row_index: usize,
        key_index: usize,
        indent: Pixels,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme().clone();
        let strong = ChromeColors::strong(&theme);
        let tint = ChromeColors::tint(&theme);

        let Some(entry) = self.keys.get(key_index) else {
            return div().into_any_element();
        };

        let selected = self.selected_index == Some(key_index);
        let is_renaming = self.renaming_index == Some(key_index);
        let metadata = self.key_metadata.get(&entry.key).copied();
        let now = Instant::now();

        let (ttl_text, ttl_color) = match metadata.map(|metadata| metadata.expiry) {
            Some(KeyExpiry::Unknown) | None => (String::new(), theme.muted_foreground),
            Some(KeyExpiry::Missing) => (
                dbflux_i18n::t!("document.key_value.render.ttl.missing"),
                theme.warning,
            ),
            Some(expiry) => {
                let remaining = expiry.remaining_seconds(now);
                let color = match ttl_tone(remaining) {
                    TtlTone::Normal => theme.foreground,
                    TtlTone::Urgent => theme.danger,
                    TtlTone::Muted => theme.muted_foreground,
                };
                (format_ttl(remaining), color)
            }
        };

        let size_text = metadata
            .and_then(|metadata| metadata.size_bytes)
            .map(format_size)
            .unwrap_or_default();

        let name_cell = if is_renaming {
            match &self.rename_input {
                Some(input) => div()
                    .flex_1()
                    .child(Input::new(input).small().w_full())
                    .into_any_element(),
                None => div().flex_1().into_any_element(),
            }
        } else {
            div()
                .flex_1()
                .min_w_0()
                .whitespace_nowrap()
                .overflow_hidden()
                .text_ellipsis()
                .text_color(if selected { strong } else { theme.foreground })
                .child(entry.key.clone())
                .into_any_element()
        };

        div()
            .id(("kv-key-row", row_index))
            .relative()
            .flex()
            .items_center()
            .h(KeyValueMetrics::LIST_ROW_HEIGHT)
            .pl(KeyValueMetrics::LIST_PADDING_LEFT + indent)
            .pr(KeyValueMetrics::LIST_PADDING_RIGHT)
            .border_b_1()
            .border_color(theme.table_row_border)
            .font_family(AppFonts::MONO)
            .text_size(KeyValueMetrics::LIST_ROW_FONT)
            .cursor_pointer()
            .when(selected, |row| {
                row.bg(tint.opacity(KeyValueMetrics::SELECTED_ROW_ALPHA))
                    .child(
                        div()
                            .absolute()
                            .left_0()
                            .top_0()
                            .bottom_0()
                            .w(Borders::MEDIUM)
                            .bg(tint),
                    )
            })
            .when(!selected, |row| row.hover(|row| row.bg(theme.list_hover)))
            .on_mouse_down(
                MouseButton::Left,
                cx.listener(move |this, _, _, cx| {
                    cx.stop_propagation();
                    this.focus_mode = super::KeyValueFocusMode::List;
                    this.list_cursor = Some(row_index);
                    this.select_index(key_index, cx);
                    cx.emit(DocumentEvent::RequestFocus);
                }),
            )
            .on_mouse_down(
                MouseButton::Right,
                cx.listener(move |this, event: &MouseDownEvent, window, cx| {
                    cx.stop_propagation();
                    this.focus_mode = super::KeyValueFocusMode::List;
                    this.list_cursor = Some(row_index);
                    this.select_index(key_index, cx);
                    cx.emit(DocumentEvent::RequestFocus);
                    this.open_context_menu(KvMenuTarget::Key, event.position, window, cx);
                }),
            )
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .flex()
                    .items_center()
                    .gap(KeyValueMetrics::LIST_ROW_GAP)
                    .child(div().flex_none().w(KeyValueMetrics::CHEVRON_SLOT))
                    .child(type_badge_element(entry.key_type, cx))
                    .child(name_cell),
            )
            .child(
                div()
                    .w(KeyValueMetrics::TTL_COLUMN)
                    .text_size(KeyValueMetrics::LIST_META_FONT)
                    .text_color(ttl_color)
                    .child(ttl_text),
            )
            .child(
                div()
                    .w(KeyValueMetrics::SIZE_COLUMN)
                    .text_size(KeyValueMetrics::LIST_META_FONT)
                    .text_color(theme.muted_foreground)
                    .child(size_text),
            )
            .into_any_element()
    }

    fn render_search_card(&self, cx: &mut Context<Self>) -> impl IntoElement + use<> {
        let theme = cx.theme();
        let tint = ChromeColors::tint(theme);
        let strong = ChromeColors::strong(theme);
        let match_count = self.keys.len();

        let title = if match_count == 1 {
            dbflux_i18n::t!("document.key_value.search.title.one")
        } else {
            dbflux_i18n::t!(
                "document.key_value.search.title.many",
                count = group_thousands(match_count as u64)
            )
        };

        div()
            .flex()
            .flex_none()
            .flex_col()
            .items_start()
            .gap(KeyValueMetrics::SEARCH_CARD_GAP)
            .mx(KeyValueMetrics::SEARCH_CARD_MARGIN_X)
            .my(KeyValueMetrics::SEARCH_CARD_MARGIN_Y)
            .p(KeyValueMetrics::SEARCH_CARD_PADDING)
            .border_1()
            .border_dashed()
            .border_color(theme.input)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .text_color(strong)
                    .font_weight(FontWeight::SEMIBOLD)
                    .child(
                        Icon::new(AppIcon::Loader)
                            .size(KeyValueMetrics::SEARCH_CARD_ICON)
                            .color(tint),
                    )
                    .child(title),
            )
            .child(
                div()
                    .text_size(KeyValueMetrics::SEARCH_CARD_FONT)
                    .line_height(relative(1.5))
                    .text_color(theme.muted_foreground)
                    .child(dbflux_i18n::t!("document.key_value.search.body")),
            )
            .child(
                div()
                    .flex()
                    .gap(Spacing::SM)
                    .child(
                        Button::new(
                            "kv-search-stop",
                            dbflux_i18n::t!("document.key_value.search.stop"),
                        )
                        .icon(AppIcon::CircleX)
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.stop_scan(cx);
                        })),
                    )
                    .child(
                        Button::new(
                            "kv-search-whole-keyspace",
                            dbflux_i18n::t!("document.key_value.search.whole_keyspace"),
                        )
                        .icon(AppIcon::Layers)
                        .disabled(self.scan_mode == super::pagination::ScanMode::WholeKeyspace)
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.search_whole_keyspace(cx);
                        })),
                    ),
            )
    }

    fn render_key_list_footer(&self, cx: &mut Context<Self>) -> impl IntoElement + use<> {
        let theme = cx.theme();
        let strong = ChromeColors::strong(theme);
        let tint = ChromeColors::tint(theme);
        let filtered = self.is_filtered_scan(cx);
        let scanning = self.runner.is_primary_active();

        let (counted, verb_key, bar_width) = if filtered {
            (
                self.scanned_keys.max(self.keys.len() as u64),
                "document.key_value.footer.scanned",
                KeyValueMetrics::PROGRESS_WIDTH_SEARCHING,
            )
        } else {
            (
                self.keys.len() as u64,
                "document.key_value.footer.loaded",
                KeyValueMetrics::PROGRESS_WIDTH,
            )
        };

        let counted = match self.key_total {
            Some(total) if self.scan_complete() => total.max(counted).min(total),
            Some(total) => counted.min(total),
            None => counted,
        };

        let fraction = if self.scan_complete() {
            1.0
        } else {
            progress_fraction(counted, self.key_total)
        };

        let summary = div()
            .flex()
            .items_center()
            .gap(Spacing::XS)
            .whitespace_nowrap()
            .child(dbflux_i18n::t!(verb_key))
            .child(div().text_color(strong).child(group_thousands(counted)))
            .when_some(self.key_total, |summary, total| {
                summary.child(dbflux_i18n::t!(
                    "document.key_value.footer.of_total",
                    total = group_thousands(total)
                ))
            });

        let hint = if filtered {
            dbflux_i18n::t!("document.key_value.footer.hint_pattern")
        } else if self.list_layout == KeyListLayout::Tree {
            dbflux_i18n::t!("document.key_value.footer.hint_tree")
        } else {
            dbflux_i18n::t!("document.key_value.footer.hint_list")
        };

        div()
            .flex()
            .flex_none()
            .items_center()
            .gap(KeyValueMetrics::FOOTER_GAP)
            .h(KeyValueMetrics::FOOTER_HEIGHT)
            .px(KeyValueMetrics::FOOTER_PADDING_X)
            .border_t_1()
            .border_color(theme.border)
            .bg(theme.background)
            .text_size(KeyValueMetrics::FOOTER_FONT)
            .text_color(theme.muted_foreground)
            .child(
                Icon::new(if scanning {
                    AppIcon::Loader
                } else {
                    AppIcon::ChartSpline
                })
                .size(KeyValueMetrics::FOOTER_ICON)
                .color(if scanning {
                    tint
                } else {
                    theme.muted_foreground
                }),
            )
            .child(summary)
            .when(self.key_total.is_some(), |footer| {
                footer.child(
                    div()
                        .flex_none()
                        .w(bar_width)
                        .h(KeyValueMetrics::PROGRESS_HEIGHT)
                        .bg(theme.secondary)
                        .child(div().h_full().w(relative(fraction)).bg(theme.primary)),
                )
            })
            .when(!(self.scan_complete() || filtered && scanning), |footer| {
                footer.child(
                    Button::new(
                        "kv-load-more",
                        dbflux_i18n::t!("document.key_value.footer.load_more"),
                    )
                    .inline()
                    .icon(AppIcon::ChevronDown)
                    .when_some(
                        dbflux_ui_base::keymap::shortcut_label(
                            dbflux_app::keymap::ContextId::KeyValue,
                            dbflux_app::keymap::Command::LoadMore,
                        ),
                        Button::kbd,
                    )
                    .disabled(!self.can_load_more_keys())
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.load_more_keys(cx);
                    })),
                )
            })
            .child(div().flex_1())
            .child(div().whitespace_nowrap().child(hint))
    }
}

#[cfg(test)]
mod tests {
    use super::progress_fraction;

    #[test]
    fn progress_is_the_share_of_the_keyspace() {
        assert_eq!(progress_fraction(0, Some(100)), 0.0);
        assert_eq!(progress_fraction(50, Some(200)), 0.25);
        assert_eq!(progress_fraction(300, Some(200)), 1.0);
        assert_eq!(progress_fraction(10, None), 0.0);
        assert_eq!(progress_fraction(10, Some(0)), 0.0);
    }
}
