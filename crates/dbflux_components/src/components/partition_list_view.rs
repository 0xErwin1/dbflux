//! `PartitionListView`: the Partitions view of a table, one row per
//! partition of a [`TablePartitions`] with its active parts, rows, size on
//! disk, raw size, compression ratio, key range and last change.
//!
//! The source line names where the list came from and when it was read, and
//! a refresh button asks the host to read it again through
//! [`PartitionListRefreshRequested`].

use std::ops::Range;
use std::sync::Arc;

use chrono::Local;
use dbflux_core::TablePartitions;
use dbflux_core::keymap_types::{Command, ContextId};
use gpui::prelude::*;
use gpui::{
    EventEmitter, FocusHandle, Hsla, IntoElement, ParentElement, Pixels, Render, ScrollStrategy,
    SharedString, Styled, UniformListScrollHandle, Window, div, px, uniform_list,
};
use gpui_component::ActiveTheme;

use crate::actions::RunCommand;
use crate::components::column_facts::{
    UNKNOWN_FACT, format_optional_bytes, format_range, format_ratio, group_thousands,
};
use crate::components::column_profile_view::{read_at_label, source_label};
use crate::composites::EmptyState;
use crate::controls::Button;
use crate::icons::AppIcon;
use crate::primitives::{Icon, LoadingState, Spinner};
use crate::tokens::{Fields, FontSizes, GridMetrics, Spacing};

/// Height of the toolbar and of one partition row.
const BAR_HEIGHT: Pixels = px(46.0);
const HEADER_HEIGHT: Pixels = px(34.0);
const ROW_HEIGHT: Pixels = px(34.0);

/// Fixed widths of the columns, in display order. The range takes the rest.
const NAME_WIDTH: Pixels = px(200.0);
const PARTS_WIDTH: Pixels = px(80.0);
const ROWS_WIDTH: Pixels = px(140.0);
const ON_DISK_WIDTH: Pixels = px(100.0);
const RAW_WIDTH: Pixels = px(100.0);
const RATIO_WIDTH: Pixels = px(70.0);
const MODIFIED_WIDTH: Pixels = px(160.0);

/// Emitted when the user asks to read the partitions again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionListRefreshRequested;

/// The text of one partition row, formatted once when the list is set.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PartitionRow {
    pub(crate) name: SharedString,
    pub(crate) parts: SharedString,
    pub(crate) rows: SharedString,
    pub(crate) on_disk: SharedString,
    pub(crate) raw: SharedString,
    pub(crate) ratio: SharedString,
    pub(crate) range: SharedString,
    pub(crate) modified: SharedString,
}

impl PartitionRow {
    fn new(partition: &dbflux_core::TablePartition) -> Self {
        let count =
            |value: Option<u64>| value.map_or_else(|| UNKNOWN_FACT.to_string(), group_thousands);

        Self {
            name: partition.name.to_string().into(),
            parts: count(partition.parts).into(),
            rows: count(partition.rows).into(),
            on_disk: format_optional_bytes(partition.compressed_bytes).into(),
            raw: format_optional_bytes(partition.uncompressed_bytes).into(),
            ratio: format_ratio(partition.compression_ratio()).into(),
            range: partition
                .range
                .as_ref()
                .map_or_else(
                    || UNKNOWN_FACT.to_string(),
                    |range| format_range(&range.min, &range.max, range.exact),
                )
                .into(),
            modified: partition
                .modified_at
                .map_or_else(
                    || UNKNOWN_FACT.to_string(),
                    |modified| {
                        modified
                            .with_timezone(&Local)
                            .format("%Y-%m-%d %H:%M")
                            .to_string()
                    },
                )
                .into(),
        }
    }
}

pub struct PartitionListView {
    content: LoadingState<Arc<TablePartitions>>,
    rows: Vec<PartitionRow>,
    source_line: SharedString,
    summary: SharedString,
    refreshing: bool,
    cursor: Option<usize>,
    focus: FocusHandle,
    scroll: UniformListScrollHandle,
}

impl PartitionListView {
    pub fn new(cx: &mut Context<Self>) -> Self {
        Self {
            content: LoadingState::Idle,
            rows: Vec::new(),
            source_line: SharedString::default(),
            summary: SharedString::default(),
            refreshing: false,
            cursor: None,
            focus: cx.focus_handle(),
            scroll: UniformListScrollHandle::new(),
        }
    }

    pub fn set_loading(&mut self, cx: &mut Context<Self>) {
        if self.content.loaded().is_some() {
            self.refreshing = true;
        } else {
            self.content = LoadingState::Loading;
        }

        cx.notify();
    }

    pub fn set_failed(&mut self, message: impl Into<SharedString>, cx: &mut Context<Self>) {
        self.content = LoadingState::Failed {
            message: message.into(),
        };
        self.rows.clear();
        self.refreshing = false;
        self.cursor = None;
        cx.notify();
    }

    pub fn set_partitions(&mut self, partitions: Arc<TablePartitions>, cx: &mut Context<Self>) {
        self.rows = partitions
            .partitions
            .iter()
            .map(PartitionRow::new)
            .collect();

        let lead = source_label(partitions.source);
        self.source_line = match partitions.captured_at {
            Some(captured_at) => format!("{lead} · {}", read_at_label(captured_at, Local::now())),
            None => lead,
        }
        .into();

        self.summary = match &partitions.key {
            Some(key) => dbflux_i18n::t!(
                "components.partition_list.summary_with_key",
                count = group_thousands(partitions.partitions.len() as u64),
                key = key
            ),
            None => dbflux_i18n::t!(
                "components.partition_list.summary",
                count = group_thousands(partitions.partitions.len() as u64)
            ),
        }
        .into();

        self.content = LoadingState::Loaded(partitions);
        self.refreshing = false;
        self.cursor = None;
        cx.notify();
    }

    pub fn is_loaded(&self) -> bool {
        self.content.loaded().is_some()
    }

    pub fn source_text(&self) -> &SharedString {
        &self.source_line
    }

    pub fn summary_text(&self) -> &SharedString {
        &self.summary
    }

    #[cfg(test)]
    pub(crate) fn rows(&self) -> &[PartitionRow] {
        &self.rows
    }

    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.cursor.is_none() && !self.rows.is_empty() {
            self.cursor = Some(0);
        }

        self.focus.focus(window, cx);
        cx.notify();
    }

    /// Asks the host to read the partitions again, unless a read is running.
    pub fn request_refresh(&mut self, cx: &mut Context<Self>) {
        if self.refreshing || self.content.is_loading() {
            return;
        }

        cx.emit(PartitionListRefreshRequested);
    }

    fn move_cursor(&mut self, forward: bool, cx: &mut Context<Self>) {
        let count = self.rows.len();

        if count == 0 {
            self.cursor = None;
            return;
        }

        let next = match (self.cursor, forward) {
            (Some(position), true) => (position + 1).min(count - 1),
            (Some(position), false) => position.saturating_sub(1),
            (None, _) => 0,
        };

        self.cursor = Some(next);
        self.scroll.scroll_to_item(next, ScrollStrategy::Nearest);
        cx.notify();
    }

    fn handle_run_command(
        &mut self,
        action: &RunCommand,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match Command::from_action_id(&action.command) {
            Some(Command::SelectNext) => self.move_cursor(true, cx),
            Some(Command::SelectPrev) => self.move_cursor(false, cx),
            _ => cx.propagate(),
        }
    }

    fn render_toolbar(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let muted = theme.muted_foreground;

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(Spacing::SM)
            .h(BAR_HEIGHT)
            .px(Spacing::MD)
            .child(
                div()
                    .debug_selector(|| "partition-list-summary".to_string())
                    .whitespace_nowrap()
                    .text_size(FontSizes::XS)
                    .text_color(theme.foreground)
                    .child(self.summary.clone()),
            )
            .child(div().flex_1())
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .min_w_0()
                    .text_size(FontSizes::XS)
                    .text_color(muted)
                    .child(
                        Icon::new(AppIcon::HardDrive)
                            .size(Fields::LEADING_ICON)
                            .color(muted),
                    )
                    .child(div().truncate().child(self.source_line.clone())),
            )
            .when(self.refreshing, |toolbar| toolbar.child(Spinner::new(2)))
            .child(
                div()
                    .debug_selector(|| "partition-list-refresh".to_string())
                    .child(
                        Button::new(
                            "partition-list-refresh",
                            dbflux_i18n::t!("components.partition_list.refresh"),
                        )
                        .icon(AppIcon::RefreshCcw)
                        .icon_only()
                        .ghost()
                        .disabled(self.refreshing)
                        .on_click(
                            cx.listener(|this, _event, _window, cx| this.request_refresh(cx)),
                        ),
                    ),
            )
    }

    fn render_header(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        let cell = |width: Option<Pixels>, key: &str, right: bool| {
            div()
                .px(GridMetrics::CELL_PADDING_X)
                .when_some(width, |cell, width| cell.w(width).flex_shrink_0())
                .when(width.is_none(), |cell| cell.flex_1().min_w_0())
                .when(right, |cell| cell.flex().justify_end())
                .whitespace_nowrap()
                .child(dbflux_i18n::t!(&format!(
                    "components.partition_list.header.{key}"
                )))
        };

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .h(HEADER_HEIGHT)
            .bg(theme.table_head)
            .text_size(GridMetrics::TYPE_FONT)
            .text_color(theme.muted_foreground)
            .child(cell(Some(NAME_WIDTH), "partition", false))
            .child(cell(Some(PARTS_WIDTH), "parts", true))
            .child(cell(Some(ROWS_WIDTH), "rows", true))
            .child(cell(Some(ON_DISK_WIDTH), "on_disk", true))
            .child(cell(Some(RAW_WIDTH), "raw", true))
            .child(cell(Some(RATIO_WIDTH), "ratio", true))
            .child(cell(None, "range", false))
            .child(cell(Some(MODIFIED_WIDTH), "modified", false))
    }

    fn render_rows(
        &mut self,
        range: Range<usize>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Vec<gpui::AnyElement> {
        let theme = cx.theme();
        let focused = self.focus.is_focused(window);
        let border = theme.border;
        let strong = theme.foreground;
        let muted = theme.muted_foreground;
        let wash =
            crate::tokens::ChromeColors::tint(theme).opacity(GridMetrics::CELL_SELECTED_ALPHA);

        range
            .filter_map(|position| {
                let row = self.rows.get(position)?;
                let at_cursor = focused && self.cursor == Some(position);

                Some(
                    div()
                        .id(("partition-list-row", position))
                        .debug_selector(move || format!("partition-list-row-{position}"))
                        .flex()
                        .items_center()
                        .w_full()
                        .h(ROW_HEIGHT)
                        .border_b_1()
                        .border_color(border)
                        .font_family(crate::fonts::editor_family(cx))
                        .text_size(GridMetrics::FONT)
                        .when(at_cursor, |row| row.bg(wash))
                        .child(text_cell(Some(NAME_WIDTH), row.name.clone(), strong, false))
                        .child(text_cell(
                            Some(PARTS_WIDTH),
                            row.parts.clone(),
                            strong,
                            true,
                        ))
                        .child(text_cell(Some(ROWS_WIDTH), row.rows.clone(), strong, true))
                        .child(text_cell(
                            Some(ON_DISK_WIDTH),
                            row.on_disk.clone(),
                            strong,
                            true,
                        ))
                        .child(text_cell(Some(RAW_WIDTH), row.raw.clone(), muted, true))
                        .child(text_cell(Some(RATIO_WIDTH), row.ratio.clone(), muted, true))
                        .child(text_cell(None, row.range.clone(), muted, false))
                        .child(text_cell(
                            Some(MODIFIED_WIDTH),
                            row.modified.clone(),
                            muted,
                            false,
                        ))
                        .into_any_element(),
                )
            })
            .collect()
    }

    fn render_body(&self, cx: &Context<Self>) -> gpui::AnyElement {
        match &self.content {
            LoadingState::Idle | LoadingState::Loading => div()
                .debug_selector(|| "partition-list-loading".to_string())
                .flex()
                .flex_1()
                .items_center()
                .justify_center()
                .gap(Spacing::SM)
                .text_size(FontSizes::XS)
                .text_color(cx.theme().muted_foreground)
                .child(Spinner::new(3))
                .child(dbflux_i18n::t!("components.partition_list.loading"))
                .into_any_element(),
            LoadingState::Failed { message } => div()
                .debug_selector(|| "partition-list-error".to_string())
                .flex()
                .flex_1()
                .items_center()
                .justify_center()
                .child(
                    EmptyState::new(AppIcon::TriangleAlert, message.clone())
                        .title(dbflux_i18n::t!("components.partition_list.error_title"))
                        .danger(),
                )
                .into_any_element(),
            LoadingState::Loaded(partitions) if partitions.partitions.is_empty() => div()
                .flex()
                .flex_col()
                .flex_1()
                .child(self.render_toolbar(cx))
                .child(
                    div()
                        .debug_selector(|| "partition-list-empty".to_string())
                        .flex()
                        .flex_1()
                        .items_center()
                        .justify_center()
                        .child(EmptyState::new(
                            AppIcon::Layers,
                            dbflux_i18n::t!("components.partition_list.empty"),
                        )),
                )
                .into_any_element(),
            LoadingState::Loaded(_) => div()
                .flex()
                .flex_col()
                .flex_1()
                .min_h_0()
                .child(self.render_toolbar(cx))
                .child(self.render_header(cx))
                .child(
                    div()
                        .id("partition-list-table")
                        .key_context(ContextId::Dropdown.as_gpui_context())
                        .track_focus(&self.focus)
                        .on_action(cx.listener(Self::handle_run_command))
                        .flex_1()
                        .min_h_0()
                        .child(
                            uniform_list(
                                "partition-list-rows",
                                self.rows.len(),
                                cx.processor(Self::render_rows),
                            )
                            .track_scroll(&self.scroll)
                            .size_full(),
                        ),
                )
                .into_any_element(),
        }
    }
}

fn text_cell(width: Option<Pixels>, text: SharedString, color: Hsla, right: bool) -> gpui::Div {
    div()
        .px(GridMetrics::CELL_PADDING_X)
        .when_some(width, |cell, width| cell.w(width).flex_shrink_0())
        .when(width.is_none(), |cell| cell.flex_1().min_w_0())
        .whitespace_nowrap()
        .truncate()
        .when(right, |cell| cell.flex().justify_end())
        .text_color(color)
        .child(text)
}

impl Render for PartitionListView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .id("partition-list-view")
            .flex()
            .flex_col()
            .size_full()
            .font_family(crate::fonts::ui_family(cx))
            .text_color(cx.theme().foreground)
            .child(self.render_body(cx))
    }
}

impl EventEmitter<PartitionListRefreshRequested> for PartitionListView {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{TimeZone, Utc};
    use dbflux_core::{ProfileSource, TablePartition, TablePartitions, ValueRange};
    use gpui::{AppContext as _, TestAppContext};

    use super::{PartitionListRefreshRequested, PartitionListView, PartitionRow};

    fn partitions() -> Arc<TablePartitions> {
        Arc::new(TablePartitions {
            key: Some("toYYYYMM(ts)".into()),
            partitions: vec![
                TablePartition {
                    parts: Some(412),
                    rows: Some(1_204_331),
                    compressed_bytes: Some(10),
                    uncompressed_bytes: Some(65),
                    range: Some(ValueRange {
                        min: "2026-09-01".into(),
                        max: "2026-09-30".into(),
                        exact: true,
                    }),
                    ..TablePartition::new("202609")
                },
                TablePartition::new("202608"),
            ],
            source: ProfileSource::Catalog("system.parts"),
            captured_at: Some(Utc.with_ymd_and_hms(2026, 9, 30, 12, 0, 0).unwrap()),
        })
    }

    #[gpui::test]
    fn a_partition_list_formats_rows_and_names_its_source(cx: &mut TestAppContext) {
        let view = cx.new(PartitionListView::new);

        view.update(cx, |view, cx| view.set_partitions(partitions(), cx));

        view.read_with(cx, |view, _| {
            assert_eq!(
                view.rows()[0],
                PartitionRow {
                    name: "202609".into(),
                    parts: "412".into(),
                    rows: "1\u{202F}204\u{202F}331".into(),
                    on_disk: "10 B".into(),
                    raw: "65 B".into(),
                    ratio: "6.5×".into(),
                    range: "2026-09-01 → 2026-09-30".into(),
                    modified: "—".into(),
                }
            );
            assert_eq!(
                view.rows()[1].rows.as_ref(),
                "—",
                "an unknown count is a dash"
            );
            assert_eq!(
                view.summary_text().as_ref(),
                "2 partitions · PARTITION BY toYYYYMM(ts)"
            );
            assert!(
                view.source_text()
                    .starts_with("Sizes from system.parts · read at "),
                "{}",
                view.source_text()
            );
        });
    }

    #[gpui::test]
    fn refresh_is_requested_once_until_the_list_arrives(cx: &mut TestAppContext) {
        let view = cx.new(PartitionListView::new);
        let requests = std::rc::Rc::new(std::cell::Cell::new(0));

        let _subscription = cx.update(|cx| {
            let requests = requests.clone();
            cx.subscribe(&view, move |_, _: &PartitionListRefreshRequested, _| {
                requests.set(requests.get() + 1);
            })
        });

        view.update(cx, |view, cx| {
            view.set_loading(cx);
            view.request_refresh(cx);
            view.set_partitions(partitions(), cx);
            view.request_refresh(cx);
            view.set_loading(cx);
            view.request_refresh(cx);
        });
        cx.run_until_parked();

        assert_eq!(
            requests.get(),
            1,
            "only the request between two reads is sent"
        );
    }
}
