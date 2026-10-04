//! `ColumnProfileView`: the Columns view of a table, one row per column of a
//! [`TableProfile`] with what the source knows about it (codec, size on
//! disk, compression ratio, share of the table, distinct values, nulls and
//! value range) and an eye that adds the column to the Data projection or
//! drops it.
//!
//! An eye toggle changes the applied projection at once and emits
//! [`ProjectionChanged`]; dropping the last shown column is refused, so the
//! Data view always has a column to read.

use std::ops::Range;
use std::sync::Arc;

use dbflux_core::keymap_types::{Command, ContextId};
use dbflux_core::{ColumnBadge, ColumnProfile, ColumnProjection, ProfileSource, TableProfile};
use gpui::prelude::*;
use gpui::{
    Entity, EventEmitter, FocusHandle, FontWeight, IntoElement, ParentElement, Pixels, Render,
    Role, ScrollStrategy, SharedString, Styled, Subscription, Toggled, UniformListScrollHandle,
    Window, div, px, uniform_list,
};
use gpui_component::ActiveTheme;

use crate::actions::RunCommand;
use crate::components::column_facts::{
    UNKNOWN_FACT, format_count, format_optional_bytes, format_percent, format_range, format_ratio,
};
use crate::components::column_projection::ProjectionChanged;
use crate::composites::EmptyState;
use crate::controls::{Button, Input, InputEvent, InputMoveDown, InputState};
use crate::icons::AppIcon;
use crate::primitives::{Badge, BadgeTone, Icon, LoadingState, Spinner};
use crate::tokens::{ChromeColors, Fields, FontSizes, GridMetrics, Spacing};
use crate::typography::AppFonts;

/// Height of the filter and sort row, and of the footer.
const BAR_HEIGHT: Pixels = px(46.0);

/// Height of the column header row.
const HEADER_HEIGHT: Pixels = px(34.0);

/// Height of one column row: name over type.
const ROW_HEIGHT: Pixels = px(44.0);

/// Width of the filter field.
const FILTER_WIDTH: Pixels = px(280.0);

/// Fixed widths of the table's columns, in display order. The distribution
/// column takes the rest of the row.
const EYE_WIDTH: Pixels = px(34.0);
const NAME_WIDTH: Pixels = px(260.0);
const CODEC_WIDTH: Pixels = px(108.0);
const ON_DISK_WIDTH: Pixels = px(96.0);
const RATIO_WIDTH: Pixels = px(70.0);
const SHARE_WIDTH: Pixels = px(150.0);
const DISTINCT_WIDTH: Pixels = px(104.0);
const NULLS_WIDTH: Pixels = px(64.0);

/// Share bar: 6 px tall, its label 30 px wide.
const SHARE_BAR_HEIGHT: Pixels = px(6.0); // guardrail-allow: share bar height from the artboard
const SHARE_LABEL_WIDTH: Pixels = px(30.0);

/// The order of the rows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ColumnSort {
    /// Schema order.
    #[default]
    TableOrder,
    /// Largest size on disk first; columns of unknown size last.
    Size,
    /// By name, ignoring case.
    Name,
}

impl ColumnSort {
    /// The next order of the sort button's cycle.
    pub fn next(self) -> Self {
        match self {
            Self::TableOrder => Self::Size,
            Self::Size => Self::Name,
            Self::Name => Self::TableOrder,
        }
    }

    fn label(self) -> String {
        match self {
            Self::TableOrder => dbflux_i18n::t!("components.column_profile.sort.table_order"),
            Self::Size => dbflux_i18n::t!("components.column_profile.sort.size"),
            Self::Name => dbflux_i18n::t!("components.column_profile.sort.name"),
        }
    }
}

/// The text of one column row, formatted once when the profile is set.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProfileRow {
    pub(crate) name: SharedString,
    pub(crate) type_name: SharedString,
    pub(crate) badges: Vec<SharedString>,
    pub(crate) codec: SharedString,
    pub(crate) on_disk: SharedString,
    pub(crate) ratio: SharedString,
    /// Share of the table's bytes, from 0.0 to 1.0, when known.
    pub(crate) share: Option<f32>,
    pub(crate) share_label: SharedString,
    pub(crate) distinct: SharedString,
    pub(crate) nulls: SharedString,
    pub(crate) distribution: SharedString,
    compressed_bytes: Option<u64>,
    sort_name: String,
}

impl ProfileRow {
    pub(crate) fn new(column: &ColumnProfile, table: &TableProfile) -> Self {
        let share = column.share_of(table.total_compressed_bytes);

        let distribution = column.range.as_ref().map_or_else(
            || UNKNOWN_FACT.to_string(),
            |range| format_range(&range.min, &range.max, range.exact),
        );

        Self {
            name: SharedString::from(column.name.to_string()),
            type_name: SharedString::from(column.type_name.to_string()),
            badges: column
                .badges
                .iter()
                .map(|badge| badge_label(*badge).into())
                .collect(),
            codec: column
                .codec
                .as_deref()
                .unwrap_or(UNKNOWN_FACT)
                .to_string()
                .into(),
            on_disk: format_optional_bytes(column.compressed_bytes).into(),
            ratio: format_ratio(column.compression_ratio()).into(),
            share: share.map(|share| share as f32),
            share_label: format_percent(share).into(),
            distinct: format_count(column.distinct_count).into(),
            nulls: format_percent(column.null_fraction(table.row_count)).into(),
            distribution: distribution.into(),
            compressed_bytes: column.compressed_bytes,
            sort_name: column.name.to_lowercase(),
        }
    }
}

fn badge_label(badge: ColumnBadge) -> String {
    match badge {
        ColumnBadge::SortKey => dbflux_i18n::t!("components.column_profile.badge.sort_key"),
        ColumnBadge::PartitionKey => {
            dbflux_i18n::t!("components.column_profile.badge.partition_key")
        }
    }
}

fn source_label(source: ProfileSource) -> String {
    match source {
        ProfileSource::FileFooter => {
            dbflux_i18n::t!("components.column_profile.source.file_footer")
        }
    }
}

pub struct ColumnProfileView {
    content: LoadingState<Arc<TableProfile>>,
    rows: Vec<ProfileRow>,
    applied: ColumnProjection,
    filter: Entity<InputState>,
    query: String,
    sort: ColumnSort,
    /// Column indices the filter shows, in the current sort order.
    visible: Vec<usize>,
    /// Position in `visible` the keyboard points at.
    cursor: Option<usize>,
    table_focus: FocusHandle,
    table_scroll: UniformListScrollHandle,
    sort_label: SharedString,
    source_label: SharedString,
    footer_counts: SharedString,
    _subscriptions: Vec<Subscription>,
}

impl ColumnProfileView {
    /// A view that waits for its profile.
    pub fn new(window: &mut Window, cx: &mut Context<Self>) -> Self {
        let filter = cx.new(|cx| {
            InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "components.column_profile.filter_placeholder"
            ))
        });

        let subscription = cx.subscribe(&filter, |this, _filter, event: &InputEvent, cx| {
            if matches!(event, InputEvent::Change) {
                this.query = this.filter.read(cx).value().to_string();
                this.refresh_visible(cx);
            }
        });

        Self {
            content: LoadingState::Loading,
            rows: Vec::new(),
            applied: ColumnProjection::none(0),
            filter,
            query: String::new(),
            sort: ColumnSort::default(),
            visible: Vec::new(),
            cursor: None,
            table_focus: cx.focus_handle(),
            table_scroll: UniformListScrollHandle::new(),
            sort_label: ColumnSort::default().label().into(),
            source_label: SharedString::default(),
            footer_counts: SharedString::default(),
            _subscriptions: vec![subscription],
        }
    }

    /// Shows the loading state until a profile or an error arrives.
    pub fn set_loading(&mut self, cx: &mut Context<Self>) {
        self.content = LoadingState::Loading;
        self.rows.clear();
        self.visible.clear();
        self.cursor = None;
        cx.notify();
    }

    /// Shows why the profile could not be read.
    pub fn set_failed(&mut self, message: impl Into<SharedString>, cx: &mut Context<Self>) {
        self.content = LoadingState::Failed {
            message: message.into(),
        };
        self.rows.clear();
        self.visible.clear();
        self.cursor = None;
        cx.notify();
    }

    /// Shows `profile` with the columns of `applied` marked as shown in Data.
    pub fn set_profile(
        &mut self,
        profile: Arc<TableProfile>,
        applied: ColumnProjection,
        cx: &mut Context<Self>,
    ) {
        self.rows = profile
            .columns
            .iter()
            .map(|column| ProfileRow::new(column, &profile))
            .collect();
        self.source_label = source_label(profile.source_label).into();
        self.content = LoadingState::Loaded(profile);
        self.applied = applied;
        self.cursor = None;
        self.refresh_footer();
        self.refresh_visible(cx);
    }

    /// Replaces the applied projection after the host changed it elsewhere,
    /// such as from the projection picker.
    pub fn set_applied(&mut self, applied: ColumnProjection, cx: &mut Context<Self>) {
        self.applied = applied;
        self.refresh_footer();
        cx.notify();
    }

    pub fn applied(&self) -> &ColumnProjection {
        &self.applied
    }

    pub fn sort(&self) -> ColumnSort {
        self.sort
    }

    pub fn set_sort(&mut self, sort: ColumnSort, cx: &mut Context<Self>) {
        self.sort = sort;
        self.sort_label = sort.label().into();
        self.refresh_visible(cx);
    }

    /// Moves to the next order of the sort button's cycle: table order,
    /// size, name.
    pub fn cycle_sort(&mut self, cx: &mut Context<Self>) {
        self.set_sort(self.sort.next(), cx);
    }

    /// Replaces the filter text and narrows the rows to match it.
    pub fn set_filter_query(&mut self, query: &str, window: &mut Window, cx: &mut Context<Self>) {
        self.filter.update(cx, |filter, cx| {
            filter.set_value(query.to_string(), window, cx)
        });
        self.query = query.to_string();
        self.refresh_visible(cx);
    }

    /// The column indices the filter shows, in the current sort order.
    pub fn visible_columns(&self) -> &[usize] {
        &self.visible
    }

    /// The footer's column counts: every column of the profile and how many
    /// are shown in Data, whatever the filter shows.
    pub fn footer_counts(&self) -> &SharedString {
        &self.footer_counts
    }

    /// Moves keyboard focus to the column rows.
    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.cursor.is_none() && !self.visible.is_empty() {
            self.cursor = Some(0);
        }

        self.table_focus.focus(window, cx);
        cx.notify();
    }

    /// Adds the column at `index` to the Data projection or drops it, and
    /// emits the new projection. Returns false, emitting nothing, when the
    /// toggle would leave the projection without columns.
    pub fn toggle_column(&mut self, index: usize, cx: &mut Context<Self>) -> bool {
        if index >= self.applied.total_count() {
            return false;
        }

        let mut next = self.applied.clone();
        next.toggle(index);

        if !next.is_applicable() {
            return false;
        }

        self.applied = next.clone();
        self.refresh_footer();
        cx.emit(ProjectionChanged(next));
        cx.notify();
        true
    }

    fn refresh_footer(&mut self) {
        self.footer_counts = dbflux_i18n::t!(
            "components.column_profile.footer.counts",
            total = self.rows.len(),
            shown = self.applied.selected_count()
        )
        .into();
    }

    /// Recomputes the rows the filter shows, in sort order, keeping the
    /// cursor on the same column when it is still shown.
    fn refresh_visible(&mut self, cx: &mut Context<Self>) {
        let cursor_column = self
            .cursor
            .and_then(|position| self.visible.get(position).copied());

        let mut visible = match self.content.loaded() {
            Some(profile) => self.applied.filter(profile, &self.query),
            None => Vec::new(),
        };

        match self.sort {
            ColumnSort::TableOrder => {}
            ColumnSort::Size => visible.sort_by_key(|index| {
                let bytes = self.rows.get(*index).and_then(|row| row.compressed_bytes);
                (bytes.is_none(), std::cmp::Reverse(bytes), *index)
            }),
            ColumnSort::Name => visible.sort_by(|left, right| {
                let name = |index: &usize| self.rows.get(*index).map(|row| row.sort_name.as_str());
                name(left).cmp(&name(right)).then(left.cmp(right))
            }),
        }

        self.visible = visible;

        self.cursor = match cursor_column {
            Some(column) => self.visible.iter().position(|index| *index == column),
            None => None,
        }
        .or_else(|| self.cursor.filter(|_| !self.visible.is_empty()).map(|_| 0));

        cx.notify();
    }

    fn move_cursor(&mut self, forward: bool, cx: &mut Context<Self>) {
        let count = self.visible.len();

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
        self.table_scroll
            .scroll_to_item(next, ScrollStrategy::Nearest);
        cx.notify();
    }

    /// Keys of the column rows: the `Dropdown` keys move the cursor and
    /// Space toggles the eye of the row under it. Other keys propagate.
    fn handle_run_command(
        &mut self,
        action: &RunCommand,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        match Command::from_action_id(&action.command) {
            Some(Command::SelectNext) => self.move_cursor(true, cx),
            Some(Command::SelectPrev) => self.move_cursor(false, cx),
            Some(Command::ExpandCollapse) => {
                if let Some(index) = self
                    .cursor
                    .and_then(|position| self.visible.get(position).copied())
                {
                    self.toggle_column(index, cx);
                }
            }
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
                    .id("column-profile-filter-row")
                    .w(FILTER_WIDTH)
                    .capture_action(cx.listener(|this, _: &InputMoveDown, window, cx| {
                        this.focus(window, cx);
                        cx.stop_propagation();
                    }))
                    .child(
                        Input::new(&self.filter)
                            .id("column-profile-filter")
                            .w_full()
                            .prefix(
                                Icon::new(AppIcon::Search)
                                    .size(Fields::LEADING_ICON)
                                    .color(muted),
                            ),
                    ),
            )
            .child(
                Button::new("column-profile-sort", self.sort_label.clone())
                    .icon(AppIcon::ArrowUpDown)
                    .on_click(cx.listener(|this, _event, _window, cx| this.cycle_sort(cx))),
            )
            .child(div().flex_1())
            .when(!self.source_label.is_empty(), |toolbar| {
                toolbar.child(
                    div()
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .whitespace_nowrap()
                        .text_size(FontSizes::XS)
                        .text_color(muted)
                        .child(
                            Icon::new(AppIcon::HardDrive)
                                .size(Fields::LEADING_ICON)
                                .color(muted),
                        )
                        .child(self.source_label.clone()),
                )
            })
    }

    fn render_header(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        let cell = |width: Option<Pixels>, label: String, right: bool| {
            div()
                .px(GridMetrics::CELL_PADDING_X)
                .when_some(width, |cell, width| cell.w(width).flex_shrink_0())
                .when(width.is_none(), |cell| cell.flex_1().min_w_0())
                .when(right, |cell| cell.flex().justify_end())
                .whitespace_nowrap()
                .child(label)
        };

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .h(HEADER_HEIGHT)
            .bg(theme.table_head)
            .text_size(GridMetrics::TYPE_FONT)
            .text_color(theme.muted_foreground)
            .child(div().w(EYE_WIDTH).flex_shrink_0())
            .child(cell(Some(NAME_WIDTH), header_label("column"), false))
            .child(cell(Some(CODEC_WIDTH), header_label("codec"), false))
            .child(cell(Some(ON_DISK_WIDTH), header_label("on_disk"), true))
            .child(cell(Some(RATIO_WIDTH), header_label("ratio"), true))
            .child(cell(Some(SHARE_WIDTH), header_label("share"), false))
            .child(cell(Some(DISTINCT_WIDTH), header_label("distinct"), true))
            .child(cell(Some(NULLS_WIDTH), header_label("nulls"), true))
            .child(cell(None, header_label("distribution"), false))
    }

    fn render_rows(
        &mut self,
        range: Range<usize>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Vec<gpui::AnyElement> {
        let theme = cx.theme();
        let strong = ChromeColors::strong(theme);
        let tint = ChromeColors::tint(theme);
        let muted = theme.muted_foreground;
        let secondary_text = theme.foreground;
        let border = theme.border;
        let track = theme.secondary;
        let info = theme.info;
        let focused = self.table_focus.is_focused(window);
        let cursor_wash = tint.opacity(GridMetrics::CELL_SELECTED_ALPHA);

        range
            .filter_map(|position| {
                let index = *self.visible.get(position)?;
                let row = self.rows.get(index)?;
                let shown = self.applied.is_selected(index);
                let at_cursor = focused && self.cursor == Some(position);

                let eye = div()
                    .id(("column-profile-eye", index))
                    .flex()
                    .flex_shrink_0()
                    .items_center()
                    .justify_center()
                    .w(EYE_WIDTH)
                    .h_full()
                    .cursor_pointer()
                    .role(Role::CheckBox)
                    .aria_toggled(if shown { Toggled::True } else { Toggled::False })
                    .aria_label(row.name.clone())
                    .on_click(cx.listener(move |this, _event, _window, cx| {
                        this.cursor = Some(position);
                        this.toggle_column(index, cx);
                    }))
                    .child(
                        Icon::new(if shown { AppIcon::Eye } else { AppIcon::EyeOff })
                            .size(Fields::LEADING_ICON)
                            .color(if shown { tint } else { muted }),
                    );

                let name_cell = div()
                    .flex()
                    .flex_col()
                    .flex_shrink_0()
                    .gap(px(2.0))
                    .w(NAME_WIDTH)
                    .min_w_0()
                    .px(GridMetrics::CELL_PADDING_X)
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::XXS)
                            .min_w_0()
                            .child(
                                div()
                                    .min_w_0()
                                    .truncate()
                                    .font_weight(FontWeight::SEMIBOLD)
                                    .text_color(if shown { strong } else { muted })
                                    .child(row.name.clone()),
                            )
                            .children(
                                row.badges
                                    .iter()
                                    .map(|badge| Badge::new(badge.clone(), BadgeTone::Accent)),
                            ),
                    )
                    .child(
                        div()
                            .truncate()
                            .text_size(GridMetrics::TYPE_FONT)
                            .text_color(muted)
                            .child(row.type_name.clone()),
                    );

                let fixed = |width: Pixels, text: SharedString, color, right: bool| {
                    div()
                        .flex_shrink_0()
                        .w(width)
                        .px(GridMetrics::CELL_PADDING_X)
                        .whitespace_nowrap()
                        .truncate()
                        .when(right, |cell| cell.flex().justify_end())
                        .text_color(color)
                        .child(text)
                };

                let share_cell = div()
                    .flex()
                    .flex_shrink_0()
                    .items_center()
                    .gap(Spacing::SM)
                    .w(SHARE_WIDTH)
                    .px(GridMetrics::CELL_PADDING_X)
                    .child(div().flex_1().h(SHARE_BAR_HEIGHT).bg(track).when_some(
                        row.share,
                        |bar, share| {
                            bar.child(
                                div()
                                    .h_full()
                                    .w(gpui::relative(share.clamp(0.0, 1.0)))
                                    .bg(info),
                            )
                        },
                    ))
                    .child(
                        div()
                            .flex()
                            .flex_shrink_0()
                            .justify_end()
                            .w(SHARE_LABEL_WIDTH)
                            .text_size(FontSizes::LABEL)
                            .text_color(muted)
                            .child(row.share_label.clone()),
                    );

                Some(
                    div()
                        .id(("column-profile-row", index))
                        .flex()
                        .items_center()
                        .h(ROW_HEIGHT)
                        .border_b_1()
                        .border_color(border)
                        .font_family(AppFonts::MONO)
                        .text_size(GridMetrics::FONT)
                        .when(at_cursor, |row| row.bg(cursor_wash))
                        .child(eye)
                        .child(name_cell)
                        .child(
                            fixed(CODEC_WIDTH, row.codec.clone(), muted, false)
                                .text_size(FontSizes::LABEL),
                        )
                        .child(fixed(ON_DISK_WIDTH, row.on_disk.clone(), strong, true))
                        .child(fixed(RATIO_WIDTH, row.ratio.clone(), secondary_text, true))
                        .child(share_cell)
                        .child(fixed(
                            DISTINCT_WIDTH,
                            row.distinct.clone(),
                            secondary_text,
                            true,
                        ))
                        .child(fixed(NULLS_WIDTH, row.nulls.clone(), secondary_text, true))
                        .child(
                            div()
                                .flex_1()
                                .min_w_0()
                                .px(GridMetrics::CELL_PADDING_X)
                                .truncate()
                                .text_size(FontSizes::LABEL)
                                .text_color(muted)
                                .child(row.distribution.clone()),
                        )
                        .into_any_element(),
                )
            })
            .collect()
    }

    fn render_table(&self, cx: &Context<Self>) -> gpui::AnyElement {
        let theme = cx.theme();

        let rows = if self.visible.is_empty() {
            div()
                .id("column-profile-no-match")
                .debug_selector(|| "column-profile-no-match".to_string())
                .flex()
                .flex_1()
                .items_center()
                .justify_center()
                .text_size(FontSizes::XS)
                .text_color(theme.muted_foreground)
                .child(dbflux_i18n::t!("components.column_profile.no_match"))
                .into_any_element()
        } else {
            uniform_list(
                "column-profile-rows",
                self.visible.len(),
                cx.processor(Self::render_rows),
            )
            .track_scroll(&self.table_scroll)
            .flex_1()
            .into_any_element()
        };

        div()
            .id("column-profile-table")
            .key_context(ContextId::Dropdown.as_gpui_context())
            .track_focus(&self.table_focus)
            .on_action(cx.listener(Self::handle_run_command))
            .flex()
            .flex_col()
            .flex_1()
            .min_h_0()
            .child(self.render_header(cx))
            .child(rows)
            .into_any_element()
    }

    fn render_footer(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let muted = theme.muted_foreground;

        div()
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(Spacing::LG)
            .h(BAR_HEIGHT)
            .px(Spacing::LG)
            .border_t_1()
            .border_color(theme.input)
            .text_size(FontSizes::XS)
            .text_color(muted)
            .child(
                div()
                    .whitespace_nowrap()
                    .text_color(theme.foreground)
                    .child(self.footer_counts.clone()),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::XXS)
                    .min_w_0()
                    .child(
                        Icon::new(AppIcon::Info)
                            .size(Fields::LEADING_ICON)
                            .color(muted),
                    )
                    .child(
                        div()
                            .truncate()
                            .child(dbflux_i18n::t!("components.column_profile.footer.hint")),
                    ),
            )
    }

    fn render_body(&self, cx: &Context<Self>) -> gpui::AnyElement {
        match &self.content {
            LoadingState::Idle | LoadingState::Loading => div()
                .id("column-profile-loading")
                .debug_selector(|| "column-profile-loading".to_string())
                .flex()
                .flex_1()
                .items_center()
                .justify_center()
                .gap(Spacing::SM)
                .text_size(FontSizes::XS)
                .text_color(cx.theme().muted_foreground)
                .child(Spinner::new(0))
                .child(dbflux_i18n::t!("components.column_profile.loading"))
                .into_any_element(),
            LoadingState::Failed { message } => div()
                .id("column-profile-error")
                .debug_selector(|| "column-profile-error".to_string())
                .flex()
                .flex_1()
                .items_center()
                .justify_center()
                .child(
                    EmptyState::new(AppIcon::TriangleAlert, message.clone())
                        .title(dbflux_i18n::t!("components.column_profile.error_title"))
                        .danger(),
                )
                .into_any_element(),
            LoadingState::Loaded(profile) if profile.columns.is_empty() => div()
                .id("column-profile-empty")
                .debug_selector(|| "column-profile-empty".to_string())
                .flex()
                .flex_1()
                .items_center()
                .justify_center()
                .child(EmptyState::new(
                    AppIcon::Columns,
                    dbflux_i18n::t!("components.column_profile.empty"),
                ))
                .into_any_element(),
            LoadingState::Loaded(_) => div()
                .flex()
                .flex_col()
                .flex_1()
                .min_h_0()
                .child(self.render_toolbar(cx))
                .child(self.render_table(cx))
                .child(self.render_footer(cx))
                .into_any_element(),
        }
    }
}

fn header_label(key: &str) -> String {
    dbflux_i18n::t!(&format!("components.column_profile.header.{key}"))
}

impl Render for ColumnProfileView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .id("column-profile-view")
            .flex()
            .flex_col()
            .size_full()
            .font_family(AppFonts::INTERFACE)
            .text_color(cx.theme().foreground)
            .child(self.render_body(cx))
    }
}

impl EventEmitter<ProjectionChanged> for ColumnProfileView {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use dbflux_core::{
        ColumnBadge, ColumnProfile, ColumnProjection, ProfileSource, TableProfile, ValueRange,
    };
    use gpui::{
        AppContext as _, Context, Entity, IntoElement, ParentElement as _, Render, Styled as _,
        TestAppContext, VisualTestContext, Window, div,
    };

    use super::{ColumnProfileView, ColumnSort, ProfileRow};
    use crate::components::column_facts::UNKNOWN_FACT;
    use crate::components::column_projection::ProjectionChanged;
    use crate::controls::bind_dropdown_keys_for_tests;

    struct Owner {
        view: Entity<ColumnProfileView>,
        changes: Vec<ColumnProjection>,
        _subscription: gpui::Subscription,
    }

    impl Render for Owner {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div().size_full().child(self.view.clone())
        }
    }

    fn unknown(name: &str, type_name: &str) -> ColumnProfile {
        ColumnProfile {
            name: name.into(),
            type_name: type_name.into(),
            badges: Vec::new(),
            codec: None,
            compressed_bytes: None,
            uncompressed_bytes: None,
            null_count: None,
            distinct_count: None,
            range: None,
        }
    }

    fn known(name: &str, type_name: &str, compressed_bytes: u64) -> ColumnProfile {
        ColumnProfile {
            codec: Some("ZSTD".into()),
            compressed_bytes: Some(compressed_bytes),
            uncompressed_bytes: Some(compressed_bytes * 4),
            null_count: Some(0),
            ..unknown(name, type_name)
        }
    }

    fn table(columns: Vec<ColumnProfile>) -> TableProfile {
        let total = columns
            .iter()
            .filter_map(|column| column.compressed_bytes)
            .sum::<u64>();

        TableProfile {
            columns,
            row_count: Some(1000),
            total_compressed_bytes: Some(total),
            total_uncompressed_bytes: None,
            source_label: ProfileSource::FileFooter,
        }
    }

    fn four_columns() -> Arc<TableProfile> {
        Arc::new(table(vec![
            known("zone_id", "INT64", 4096),
            known("name", "UTF8", 2048),
            unknown("note", "UTF8"),
            known("created_at", "TIMESTAMP(MICROS)", 8192),
        ]))
    }

    fn setup(
        profile: Option<Arc<TableProfile>>,
        cx: &mut TestAppContext,
    ) -> (Entity<Owner>, &mut VisualTestContext) {
        cx.update(gpui_component::init);
        bind_dropdown_keys_for_tests(cx);

        let (owner, window) = cx.add_window_view(|window, cx| {
            let view = cx.new(|cx| {
                let mut view = ColumnProfileView::new(window, cx);

                if let Some(profile) = profile {
                    let total = profile.columns.len();
                    view.set_profile(profile, ColumnProjection::from_indices(total, &[0, 1]), cx);
                }

                view
            });

            let subscription = cx.subscribe_in(
                &view,
                window,
                |this: &mut Owner, _, event: &ProjectionChanged, _window, _cx| {
                    this.changes.push(event.0.clone());
                },
            );

            Owner {
                view,
                changes: Vec::new(),
                _subscription: subscription,
            }
        });
        window.run_until_parked();

        (owner, window)
    }

    fn view(owner: &Entity<Owner>, window: &mut VisualTestContext) -> Entity<ColumnProfileView> {
        window.update(|_, cx| owner.read(cx).view.clone())
    }

    #[gpui::test]
    fn eye_toggle_emits_projection_change(cx: &mut TestAppContext) {
        let (owner, window) = setup(Some(four_columns()), cx);
        let view = view(&owner, window);

        let added = window.update(|_, cx| view.update(cx, |view, cx| view.toggle_column(3, cx)));
        assert!(added);

        // From the keyboard: the cursor starts on `zone_id`; Space drops it.
        window.update(|window, cx| view.update(cx, |view, cx| view.focus(window, cx)));
        window.simulate_keystrokes("space");
        window.run_until_parked();

        // Dropping `name` and then `created_at` would leave nothing shown.
        window.update(|_, cx| view.update(cx, |view, cx| view.toggle_column(1, cx)));
        let refused = window.update(|_, cx| view.update(cx, |view, cx| view.toggle_column(3, cx)));
        assert!(!refused, "the last shown column cannot be dropped");

        let (changes, applied) = window.update(|_, cx| {
            let owner = owner.read(cx);
            (
                owner
                    .changes
                    .iter()
                    .map(ColumnProjection::selected_indices)
                    .collect::<Vec<_>>(),
                owner.view.read(cx).applied().selected_indices(),
            )
        });
        assert_eq!(
            changes,
            vec![vec![0, 1, 3], vec![1, 3], vec![3]],
            "each accepted toggle emits the new projection, the refused one nothing"
        );
        assert_eq!(applied, vec![3]);
    }

    #[gpui::test]
    fn filter_keeps_footer_counts(cx: &mut TestAppContext) {
        let (owner, window) = setup(Some(four_columns()), cx);
        let view = view(&owner, window);

        let before = window.update(|_, cx| view.read(cx).footer_counts().clone());
        assert_eq!(before.as_ref(), "4 columns · 2 shown in Data");

        window.update(|window, cx| {
            view.update(cx, |view, cx| view.set_filter_query("utf", window, cx))
        });
        window.run_until_parked();

        let (visible, after) = window.update(|_, cx| {
            let view = view.read(cx);
            (
                view.visible_columns().to_vec(),
                view.footer_counts().clone(),
            )
        });
        assert_eq!(visible, vec![1, 2]);
        assert_eq!(
            after, before,
            "the footer counts the profile, not the filter"
        );

        window.update(|window, cx| {
            view.update(cx, |view, cx| view.set_filter_query("missing", window, cx))
        });
        window.run_until_parked();
        assert!(window.debug_bounds("column-profile-no-match").is_some());
    }

    #[gpui::test]
    fn empty_profile_shows_empty_state(cx: &mut TestAppContext) {
        let (owner, window) = setup(Some(Arc::new(table(Vec::new()))), cx);

        assert!(
            window.debug_bounds("column-profile-empty").is_some(),
            "a profile without columns shows the empty state"
        );
        assert!(window.debug_bounds("column-profile-no-match").is_none());

        let view = view(&owner, window);
        window.update(|_, cx| view.update(cx, |view, cx| view.set_loading(cx)));
        window.run_until_parked();
        assert!(window.debug_bounds("column-profile-loading").is_some());

        window
            .update(|_, cx| view.update(cx, |view, cx| view.set_failed("footer is truncated", cx)));
        window.run_until_parked();
        assert!(window.debug_bounds("column-profile-error").is_some());
    }

    #[test]
    fn unknown_values_show_a_dash() {
        let column = unknown("payload", "BYTE_ARRAY");
        let row = ProfileRow::new(&column, &table(vec![column.clone()]));

        for (field, text) in [
            ("codec", &row.codec),
            ("on disk", &row.on_disk),
            ("ratio", &row.ratio),
            ("share", &row.share_label),
            ("distinct", &row.distinct),
            ("nulls", &row.nulls),
            ("distribution", &row.distribution),
        ] {
            assert_eq!(text.as_ref(), UNKNOWN_FACT, "unknown {field} shows a dash");
        }
        assert_eq!(row.share, None);

        let zero = ColumnProfile {
            codec: Some("SNAPPY".into()),
            compressed_bytes: Some(0),
            uncompressed_bytes: Some(0),
            null_count: Some(0),
            distinct_count: Some(0),
            range: Some(ValueRange {
                min: "a".into(),
                max: "zz".into(),
                exact: false,
            }),
            badges: vec![ColumnBadge::SortKey],
            ..unknown("empty", "UTF8")
        };
        let row = ProfileRow::new(&zero, &table(vec![zero.clone()]));

        assert_eq!(row.codec.as_ref(), "SNAPPY");
        assert_eq!(row.on_disk.as_ref(), "0 B", "a known zero is not a dash");
        assert_eq!(
            row.ratio.as_ref(),
            UNKNOWN_FACT,
            "a ratio over zero bytes is unknown"
        );
        assert_eq!(row.distinct.as_ref(), "0");
        assert_eq!(row.nulls.as_ref(), "0%");
        assert_eq!(row.distribution.as_ref(), "≈ a → zz");
        assert_eq!(row.badges.len(), 1);
    }

    #[gpui::test]
    fn sort_orders_by_size_and_name(cx: &mut TestAppContext) {
        let (owner, window) = setup(Some(four_columns()), cx);
        let view = view(&owner, window);

        window.update(|_, cx| view.update(cx, |view, cx| view.set_sort(ColumnSort::Size, cx)));
        let by_size = window.update(|_, cx| view.read(cx).visible_columns().to_vec());
        assert_eq!(by_size, vec![3, 0, 1, 2], "unknown size sorts last");

        window.update(|_, cx| view.update(cx, |view, cx| view.cycle_sort(cx)));
        let by_name = window.update(|_, cx| view.read(cx).visible_columns().to_vec());
        assert_eq!(by_name, vec![3, 1, 2, 0]);

        window.update(|_, cx| view.update(cx, |view, cx| view.cycle_sort(cx)));
        let table_order = window.update(|_, cx| view.read(cx).visible_columns().to_vec());
        assert_eq!(table_order, vec![0, 1, 2, 3]);
    }

    #[test]
    fn profile_view_keys_resolve_in_every_locale() {
        let keys = [
            "components.column_profile.filter_placeholder",
            "components.column_profile.sort.table_order",
            "components.column_profile.sort.size",
            "components.column_profile.sort.name",
            "components.column_profile.source.file_footer",
            "components.column_profile.header.column",
            "components.column_profile.header.on_disk",
            "components.column_profile.header.share",
            "components.column_profile.header.distinct",
            "components.column_profile.header.nulls",
            "components.column_profile.header.distribution",
            "components.column_profile.footer.counts",
            "components.column_profile.footer.hint",
            "components.column_profile.loading",
            "components.column_profile.empty",
            "components.column_profile.no_match",
            "components.column_profile.error_title",
            "components.column_profile.badge.sort_key",
            "components.column_profile.badge.partition_key",
        ];

        for key in keys {
            let english = dbflux_i18n::t!(key, locale = "en");
            assert!(
                !english.is_empty() && !english.ends_with(key),
                "en misses {key}"
            );

            // A key missing from a catalog falls back to English. "Codec" and
            // "Ratio" read the same in some catalogs, so they are left out.
            for locale in ["es", "ko", "pt_BR", "zh_Hans"] {
                let text = dbflux_i18n::t!(key, locale = locale);
                assert_ne!(text, english, "{locale} misses {key}");
            }
        }
    }
}
