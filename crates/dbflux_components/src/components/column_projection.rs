//! `ColumnProjectionPicker`: the "N of M columns" control that chooses which
//! columns of a [`TableProfile`] a paged table reads.
//!
//! The trigger opens a popover over a draft copy of the applied
//! [`ColumnProjection`]. The draft changes freely, and only Apply turns it
//! into the applied projection and emits [`ProjectionChanged`]; Escape or a
//! press outside the popover throws the draft away. Search narrows the rows
//! shown without touching the selection of the rows it hides.

use std::ops::Range;
use std::sync::Arc;

use dbflux_core::keymap_types::{Command, ContextId};
use dbflux_core::{ColumnProjection, TableProfile};
use gpui::prelude::*;
use gpui::{
    Anchor, App, ClickEvent, ElementId, Entity, EventEmitter, FocusHandle, Focusable, FontWeight,
    IntoElement, MouseDownEvent, ParentElement, Pixels, Point, Render, ScrollStrategy,
    SharedString, Styled, Subscription, UniformListScrollHandle, Window, anchored, deferred, div,
    point, px, uniform_list,
};
use gpui_component::ActiveTheme;

use crate::actions::RunCommand;
use crate::components::column_facts::format_optional_bytes;
use crate::controls::{
    Button, ButtonSize, Checkbox, Input, InputEvent, InputMoveDown, InputMoveUp, InputState,
};
use crate::icons::AppIcon;
use crate::primitives::{Chamfer, Icon};
use crate::tokens::{ChamferCut, ChromeColors, Fields, FontSizes, Spacing};
use crate::typography::AppFonts;

/// Width of the open popover.
const POPOVER_WIDTH: Pixels = px(380.0);

/// Tallest the column list grows before it scrolls.
const LIST_MAX_HEIGHT: Pixels = px(320.0);

/// Emitted when Apply turns the draft into the applied projection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionChanged(pub ColumnProjection);

/// The text of one column row, formatted once when the profile is set.
struct PickerRow {
    name: SharedString,
    type_name: SharedString,
    size: SharedString,
}

/// Which control of the open popover holds keyboard focus, in Tab order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FocusStop {
    List,
    Search,
    SelectAll,
    SelectNone,
    Apply,
}

pub struct ColumnProjectionPicker {
    id: ElementId,
    profile: Arc<TableProfile>,
    rows: Vec<PickerRow>,
    applied: ColumnProjection,
    /// The selection being edited. `Some` exactly while the popover is open.
    draft: Option<ColumnProjection>,
    search: Entity<InputState>,
    query: String,
    /// Column indices the search shows, in schema order.
    visible: Vec<usize>,
    /// Position in `visible` the keyboard points at.
    highlighted: Option<usize>,
    list_focus: FocusHandle,
    select_all_focus: FocusHandle,
    select_none_focus: FocusHandle,
    apply_focus: FocusHandle,
    list_scroll: UniformListScrollHandle,
    /// Where focus was when the popover opened, given back when it closes.
    return_focus: Option<FocusHandle>,
    /// The press that last closed the popover from outside it. A click on
    /// the trigger that starts with this press must not open it again.
    outside_press: Option<Point<Pixels>>,
    trigger_label: SharedString,
    _subscriptions: Vec<Subscription>,
}

impl ColumnProjectionPicker {
    pub fn new(
        id: impl Into<ElementId>,
        profile: Arc<TableProfile>,
        applied: ColumnProjection,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let search = cx.new(|cx| {
            InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "components.column_projection.search_placeholder"
            ))
        });

        let subscription = cx.subscribe_in(
            &search,
            window,
            |this, _search, event: &InputEvent, window, cx| match event {
                InputEvent::Change => {
                    this.query = this.search.read(cx).value().to_string();
                    this.refilter(cx);
                }
                InputEvent::PressEnter { .. } => this.apply(window, cx),
                _ => {}
            },
        );

        let mut picker = Self {
            id: id.into(),
            rows: Vec::new(),
            profile,
            applied,
            draft: None,
            search,
            query: String::new(),
            visible: Vec::new(),
            highlighted: None,
            list_focus: cx.focus_handle(),
            select_all_focus: cx.focus_handle(),
            select_none_focus: cx.focus_handle(),
            apply_focus: cx.focus_handle(),
            list_scroll: UniformListScrollHandle::new(),
            return_focus: None,
            outside_press: None,
            trigger_label: SharedString::default(),
            _subscriptions: vec![subscription],
        };

        picker.rebuild_rows();
        picker.refresh_trigger_label();
        picker
    }

    /// Replaces the profile and the applied projection, closing the popover
    /// without emitting anything.
    pub fn set_profile(
        &mut self,
        profile: Arc<TableProfile>,
        applied: ColumnProjection,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.close(window, cx);
        self.profile = profile;
        self.applied = applied;
        self.rebuild_rows();
        self.refresh_trigger_label();
        cx.notify();
    }

    /// Replaces the applied projection after the host changed it elsewhere,
    /// such as from the Columns view. An open draft is left as it is.
    pub fn set_applied(&mut self, applied: ColumnProjection, cx: &mut Context<Self>) {
        self.applied = applied;
        self.refresh_trigger_label();
        cx.notify();
    }

    pub fn applied(&self) -> &ColumnProjection {
        &self.applied
    }

    /// The selection being edited, while the popover is open.
    pub fn draft(&self) -> Option<&ColumnProjection> {
        self.draft.as_ref()
    }

    pub fn is_open(&self) -> bool {
        self.draft.is_some()
    }

    /// The column indices the search shows, in schema order.
    pub fn visible_columns(&self) -> &[usize] {
        &self.visible
    }

    /// Whether Apply would change anything: the draft selects at least one
    /// column and differs from the applied projection.
    pub fn can_apply(&self) -> bool {
        self.draft
            .as_ref()
            .is_some_and(|draft| draft.is_applicable() && *draft != self.applied)
    }

    /// Opens the popover over a fresh draft of the applied projection and
    /// moves keyboard focus to its column list. Hosts bind this to a command
    /// so the picker opens from the keyboard.
    pub fn open(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.is_open() || self.rows.is_empty() {
            return;
        }

        self.draft = Some(self.applied.clone());
        self.outside_press = None;
        self.clear_search(window, cx);
        self.highlighted = Some(0);
        self.list_scroll.scroll_to_item(0, ScrollStrategy::Top);

        if !self.contains_focus(window, cx) {
            self.return_focus = window.focused(cx);
        }

        self.list_focus.focus(window, cx);
        cx.notify();
    }

    pub fn toggle_open(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.is_open() {
            self.discard(window, cx);
        } else {
            self.open(window, cx);
        }
    }

    /// Closes the popover and throws the draft away.
    pub fn discard(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.close(window, cx);
    }

    /// Makes the draft the applied projection, emits [`ProjectionChanged`]
    /// and closes the popover. Does nothing while [`Self::can_apply`] is
    /// false.
    pub fn apply(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.can_apply() {
            return;
        }

        let Some(draft) = self.draft.clone() else {
            return;
        };

        self.applied = draft.clone();
        self.refresh_trigger_label();
        self.close(window, cx);
        cx.emit(ProjectionChanged(draft));
    }

    /// Flips the draft selection of the column at `index`.
    pub fn toggle_column(&mut self, index: usize, cx: &mut Context<Self>) {
        if let Some(draft) = self.draft.as_mut() {
            draft.toggle(index);
            cx.notify();
        }
    }

    /// Selects every column the search shows. Hidden columns keep their
    /// selection.
    pub fn select_all(&mut self, cx: &mut Context<Self>) {
        self.select_visible(true, cx);
    }

    /// Clears every column the search shows. Hidden columns keep their
    /// selection.
    pub fn select_none(&mut self, cx: &mut Context<Self>) {
        self.select_visible(false, cx);
    }

    /// Replaces the search text and narrows the rows to match it.
    pub fn set_search_query(&mut self, query: &str, window: &mut Window, cx: &mut Context<Self>) {
        self.search.update(cx, |search, cx| {
            search.set_value(query.to_string(), window, cx)
        });
        self.query = query.to_string();
        self.refilter(cx);
    }

    fn select_visible(&mut self, selected: bool, cx: &mut Context<Self>) {
        let Some(draft) = self.draft.as_mut() else {
            return;
        };

        for &index in &self.visible {
            if draft.is_selected(index) != selected {
                draft.toggle(index);
            }
        }

        cx.notify();
    }

    /// Narrows the rows to the columns matching the search, keeping the
    /// highlight on the same column when it is still shown.
    fn refilter(&mut self, cx: &mut Context<Self>) {
        let highlighted_column = self
            .highlighted
            .and_then(|position| self.visible.get(position).copied());

        self.visible = self.applied.filter(&self.profile, &self.query);

        self.highlighted = if self.visible.is_empty() {
            None
        } else {
            Some(
                highlighted_column
                    .and_then(|column| self.visible.iter().position(|index| *index == column))
                    .unwrap_or(0),
            )
        };

        cx.notify();
    }

    fn clear_search(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.set_search_query("", window, cx);
    }

    fn close(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.draft.take().is_none() {
            return;
        }

        self.highlighted = None;

        let had_focus = self.contains_focus(window, cx);
        let previous = self.return_focus.take();

        if had_focus && let Some(previous) = previous {
            previous.focus(window, cx);
        }

        cx.notify();
    }

    fn rebuild_rows(&mut self) {
        self.rows = self
            .profile
            .columns
            .iter()
            .map(|column| PickerRow {
                name: SharedString::from(column.name.to_string()),
                type_name: SharedString::from(column.type_name.to_string()),
                size: format_optional_bytes(column.compressed_bytes).into(),
            })
            .collect();
        self.visible = (0..self.rows.len()).collect();
        self.highlighted = None;
    }

    fn refresh_trigger_label(&mut self) {
        self.trigger_label = dbflux_i18n::t!(
            "components.column_projection.trigger",
            selected = self.applied.selected_count(),
            total = self.applied.total_count()
        )
        .into();
    }

    fn search_focus(&self, cx: &App) -> FocusHandle {
        self.search.read(cx).focus_handle(cx)
    }

    fn contains_focus(&self, window: &Window, cx: &App) -> bool {
        [
            &self.list_focus,
            &self.select_all_focus,
            &self.select_none_focus,
            &self.apply_focus,
        ]
        .into_iter()
        .any(|handle| handle.contains_focused(window, cx))
            || self.search_focus(cx).contains_focused(window, cx)
    }

    fn focused_stop(&self, window: &Window, cx: &App) -> Option<FocusStop> {
        if self.list_focus.contains_focused(window, cx) {
            Some(FocusStop::List)
        } else if self.search_focus(cx).contains_focused(window, cx) {
            Some(FocusStop::Search)
        } else if self.select_all_focus.is_focused(window) {
            Some(FocusStop::SelectAll)
        } else if self.select_none_focus.is_focused(window) {
            Some(FocusStop::SelectNone)
        } else if self.apply_focus.is_focused(window) {
            Some(FocusStop::Apply)
        } else {
            None
        }
    }

    /// Moves focus to the next (or previous) control of the popover. Apply
    /// is skipped while it is disabled.
    fn cycle_focus(&mut self, forward: bool, window: &mut Window, cx: &mut Context<Self>) {
        let mut stops = vec![
            FocusStop::List,
            FocusStop::Search,
            FocusStop::SelectAll,
            FocusStop::SelectNone,
        ];

        if self.can_apply() {
            stops.push(FocusStop::Apply);
        }

        let current = self
            .focused_stop(window, cx)
            .and_then(|stop| stops.iter().position(|candidate| *candidate == stop))
            .unwrap_or(0);

        let next = if forward {
            (current + 1) % stops.len()
        } else {
            (current + stops.len() - 1) % stops.len()
        };

        let Some(stop) = stops.get(next) else {
            return;
        };

        let handle = match stop {
            FocusStop::List => self.list_focus.clone(),
            FocusStop::Search => self.search_focus(cx),
            FocusStop::SelectAll => self.select_all_focus.clone(),
            FocusStop::SelectNone => self.select_none_focus.clone(),
            FocusStop::Apply => self.apply_focus.clone(),
        };

        handle.focus(window, cx);
        cx.notify();
    }

    fn move_highlight(&mut self, forward: bool, cx: &mut Context<Self>) {
        let count = self.visible.len();

        if count == 0 {
            self.highlighted = None;
            return;
        }

        let next = match (self.highlighted, forward) {
            (Some(position), true) => (position + 1).min(count - 1),
            (Some(position), false) => position.saturating_sub(1),
            (None, _) => 0,
        };

        self.highlighted = Some(next);
        self.list_scroll
            .scroll_to_item(next, ScrollStrategy::Nearest);
        cx.notify();
    }

    fn toggle_highlighted(&mut self, cx: &mut Context<Self>) {
        let Some(index) = self
            .highlighted
            .and_then(|position| self.visible.get(position).copied())
        else {
            return;
        };

        self.toggle_column(index, cx);
    }

    /// Keys of the open popover. In the column list the `Dropdown` keys
    /// move, toggle (Space), apply (Enter) and discard (Escape); from any
    /// control Escape discards and Tab cycles through the controls. Keys it
    /// has no use for propagate.
    fn handle_run_command(
        &mut self,
        action: &RunCommand,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.is_open() {
            cx.propagate();
            return;
        }

        let in_list = self.list_focus.contains_focused(window, cx);

        match Command::from_action_id(&action.command) {
            Some(Command::SelectNext) if in_list => self.move_highlight(true, cx),
            Some(Command::SelectPrev) if in_list => self.move_highlight(false, cx),
            Some(Command::ExpandCollapse) if in_list => self.toggle_highlighted(cx),
            Some(Command::Execute) if in_list => self.apply(window, cx),
            Some(Command::Cancel) => self.discard(window, cx),
            Some(Command::CycleFocusForward) => self.cycle_focus(true, window, cx),
            Some(Command::CycleFocusBackward) => self.cycle_focus(false, window, cx),
            _ => cx.propagate(),
        }
    }

    fn handle_mouse_down_out(
        &mut self,
        event: &MouseDownEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.is_open() {
            return;
        }

        self.outside_press = Some(event.position);
        self.discard(window, cx);
    }

    fn handle_trigger_click(
        &mut self,
        event: &ClickEvent,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let ClickEvent::Mouse(click) = event
            && self.outside_press.take() == Some(click.down.position)
        {
            return;
        }

        self.toggle_open(window, cx);
    }

    fn render_rows(
        &mut self,
        range: Range<usize>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Vec<gpui::AnyElement> {
        let theme = cx.theme();
        let highlight = ChromeColors::tint(theme).opacity(Fields::MENU_HIGHLIGHT_ALPHA);
        let strong = ChromeColors::strong(theme);
        let muted = theme.muted_foreground;
        let hover = theme.list_hover;

        range
            .filter_map(|position| {
                let index = *self.visible.get(position)?;
                let row = self.rows.get(index)?;
                let checked = self
                    .draft
                    .as_ref()
                    .is_some_and(|draft| draft.is_selected(index));
                let highlighted = self.highlighted == Some(position);

                let shape = if highlighted {
                    Chamfer::new(ChamferCut::KEYCAP).fill(highlight)
                } else {
                    Chamfer::new(ChamferCut::KEYCAP)
                        .fill_hover(hover)
                        .interactive(("column-projection-row-shape", index))
                };

                Some(
                    div()
                        .id(("column-projection-row", index))
                        .relative()
                        .flex()
                        .items_center()
                        .gap(Fields::CHECKBOX_GAP)
                        .h(Fields::MENU_ROW_HEIGHT)
                        .mx(Fields::MENU_ROW_INSET)
                        .px(Fields::PADDING_X)
                        .cursor_pointer()
                        .whitespace_nowrap()
                        .child(shape)
                        .on_click(cx.listener(move |this, _event, _window, cx| {
                            this.highlighted = Some(position);
                            this.toggle_column(index, cx);
                        }))
                        .child(
                            Checkbox::new(("column-projection-check", index))
                                .checked(checked)
                                .aria_label(row.name.clone()),
                        )
                        .child(
                            div()
                                .flex_shrink_0()
                                .font_family(AppFonts::MONO)
                                .font_weight(FontWeight::SEMIBOLD)
                                .text_color(strong)
                                .child(row.name.clone()),
                        )
                        .child(
                            div()
                                .flex_1()
                                .min_w_0()
                                .truncate()
                                .font_family(AppFonts::MONO)
                                .text_size(FontSizes::LABEL)
                                .text_color(muted)
                                .child(row.type_name.clone()),
                        )
                        .child(
                            div()
                                .flex_shrink_0()
                                .font_family(AppFonts::MONO)
                                .text_size(FontSizes::LABEL)
                                .text_color(muted)
                                .child(row.size.clone()),
                        )
                        .into_any_element(),
                )
            })
            .collect()
    }

    fn render_search_row(&self, cx: &Context<Self>) -> impl IntoElement {
        let muted = cx.theme().muted_foreground;

        div()
            .id("column-projection-search-row")
            .px(Spacing::SM)
            .pt(Spacing::SM)
            .capture_action(cx.listener(|this, _: &InputMoveDown, window, cx| {
                this.list_focus.focus(window, cx);
                this.move_highlight(true, cx);
                cx.stop_propagation();
            }))
            .capture_action(cx.listener(|this, _: &InputMoveUp, window, cx| {
                this.list_focus.focus(window, cx);
                cx.stop_propagation();
            }))
            .child(
                Input::new(&self.search)
                    .id("column-projection-search")
                    .small()
                    .w_full()
                    .prefix(
                        Icon::new(AppIcon::Search)
                            .size(Fields::LEADING_ICON)
                            .color(muted),
                    ),
            )
    }

    fn render_bulk_actions(
        &self,
        draft: &ColumnProjection,
        cx: &Context<Self>,
    ) -> impl IntoElement {
        let muted = cx.theme().muted_foreground;

        div()
            .flex()
            .items_center()
            .gap(Spacing::XS)
            .px(Spacing::SM)
            .py(Spacing::XS)
            .child(
                Button::new(
                    "column-projection-select-all",
                    dbflux_i18n::t!("components.column_projection.select_all"),
                )
                .ghost()
                .size(ButtonSize::Inline)
                .focus_handle(&self.select_all_focus)
                .on_click(cx.listener(|this, _event, _window, cx| this.select_all(cx))),
            )
            .child(
                Button::new(
                    "column-projection-select-none",
                    dbflux_i18n::t!("components.column_projection.select_none"),
                )
                .ghost()
                .size(ButtonSize::Inline)
                .focus_handle(&self.select_none_focus)
                .on_click(cx.listener(|this, _event, _window, cx| this.select_none(cx))),
            )
            .child(div().flex_1())
            .child(
                div()
                    .text_size(FontSizes::XS)
                    .text_color(muted)
                    .child(dbflux_i18n::t!(
                        "components.column_projection.selected_count",
                        selected = draft.selected_count(),
                        total = draft.total_count()
                    )),
            )
    }

    fn render_list_region(&self, window: &Window, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let muted = theme.muted_foreground;
        let list_focused = self.list_focus.contains_focused(window, cx);

        let list = if self.visible.is_empty() {
            div()
                .id("column-projection-empty")
                .px(Spacing::LG)
                .py(Spacing::MD)
                .text_size(FontSizes::XS)
                .text_color(muted)
                .child(dbflux_i18n::t!("components.column_projection.no_match"))
                .into_any_element()
        } else {
            let row_count = self.visible.len();
            let list_height = (Fields::MENU_ROW_HEIGHT * row_count as f32).min(LIST_MAX_HEIGHT);

            uniform_list(
                "column-projection-rows",
                row_count,
                cx.processor(Self::render_rows),
            )
            .track_scroll(&self.list_scroll)
            .h(list_height)
            .into_any_element()
        };

        div()
            .id("column-projection-list")
            .key_context(ContextId::Dropdown.as_gpui_context())
            .track_focus(&self.list_focus)
            .relative()
            .py(Spacing::XS)
            .text_size(Fields::TEXT)
            .when(list_focused, |region| {
                region.child(Chamfer::new(ChamferCut::KEYCAP).ring(
                    crate::primitives::ChamferRing::focus(ChromeColors::tint(theme)),
                ))
            })
            .child(list)
    }

    fn render_footer(&self, cx: &Context<Self>) -> impl IntoElement {
        let theme = cx.theme();
        let muted = theme.muted_foreground;
        let can_apply = self.can_apply();

        div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .px(Spacing::SM)
            .pb(Spacing::SM)
            .pt(Spacing::XS)
            .border_t_1()
            .border_color(theme.border)
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .truncate()
                    .text_size(FontSizes::LABEL)
                    .text_color(muted)
                    .child(dbflux_i18n::t!("components.column_projection.hint")),
            )
            .child(
                Button::new(
                    "column-projection-apply",
                    dbflux_i18n::t!("components.column_projection.apply"),
                )
                .primary()
                .size(ButtonSize::Inline)
                .disabled(!can_apply)
                .focus_handle(&self.apply_focus)
                .on_click(cx.listener(|this, _event, window, cx| this.apply(window, cx))),
            )
    }

    fn render_popover(&self, window: &Window, cx: &Context<Self>) -> gpui::AnyElement {
        let Some(draft) = self.draft.as_ref() else {
            return div().into_any_element();
        };

        let theme = cx.theme();

        let popover = div()
            .id("column-projection-popover")
            .relative()
            .flex()
            .flex_col()
            .w(POPOVER_WIDTH)
            .font_family(AppFonts::INTERFACE)
            .text_size(FontSizes::BASE)
            .text_color(theme.foreground)
            .shadow_lg()
            .occlude()
            .on_action(cx.listener(Self::handle_run_command))
            .on_mouse_down_out(cx.listener(Self::handle_mouse_down_out))
            .child(
                Chamfer::new(ChamferCut::OVERLAY)
                    .fill(theme.popover)
                    .border(theme.border),
            )
            .child(self.render_search_row(cx))
            .child(self.render_bulk_actions(draft, cx))
            .child(self.render_list_region(window, cx))
            .child(self.render_footer(cx));

        deferred(
            anchored()
                .anchor(Anchor::TopLeft)
                .offset(point(px(0.0), Spacing::XS))
                .snap_to_window()
                .child(popover),
        )
        .with_priority(1)
        .into_any_element()
    }
}

impl Render for ColumnProjectionPicker {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let has_columns = !self.rows.is_empty();

        let trigger = Button::new("column-projection-trigger", self.trigger_label.clone())
            .icon(AppIcon::Columns)
            .selected(self.is_open())
            .disabled(!has_columns)
            .on_click(cx.listener(Self::handle_trigger_click));

        div()
            .id(self.id.clone())
            .flex()
            .flex_col()
            .child(trigger)
            .child(self.render_popover(window, cx))
    }
}

impl EventEmitter<ProjectionChanged> for ColumnProjectionPicker {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use dbflux_core::{ColumnProfile, ColumnProjection, ProfileSource, TableProfile};
    use gpui::{
        AppContext as _, Context, Entity, FocusHandle, InteractiveElement as _, IntoElement,
        ParentElement as _, Render, Styled as _, TestAppContext, VisualTestContext, Window, div,
    };

    use super::{ColumnProjectionPicker, ProjectionChanged};
    use crate::controls::bind_dropdown_keys_for_tests;

    struct Owner {
        focus: FocusHandle,
        picker: Entity<ColumnProjectionPicker>,
        changes: Vec<ColumnProjection>,
        _subscription: gpui::Subscription,
    }

    impl Render for Owner {
        fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
            div()
                .size_full()
                .track_focus(&self.focus)
                .child(self.picker.clone())
        }
    }

    fn column(name: &str, type_name: &str, compressed_bytes: Option<u64>) -> ColumnProfile {
        ColumnProfile {
            name: name.into(),
            type_name: type_name.into(),
            badges: Vec::new(),
            codec: None,
            compressed_bytes,
            uncompressed_bytes: None,
            null_count: None,
            distinct_count: None,
            range: None,
        }
    }

    fn profile() -> Arc<TableProfile> {
        Arc::new(TableProfile {
            columns: vec![
                column("id", "INT64", Some(4096)),
                column("name", "UTF8", Some(2048)),
                column("note", "UTF8", None),
                column("created_at", "TIMESTAMP(MICROS)", Some(8192)),
            ],
            row_count: Some(100),
            total_compressed_bytes: Some(14336),
            total_uncompressed_bytes: None,
            source_label: ProfileSource::FileFooter,
        })
    }

    fn setup(cx: &mut TestAppContext) -> (Entity<Owner>, &mut VisualTestContext) {
        cx.update(gpui_component::init);
        bind_dropdown_keys_for_tests(cx);

        let (owner, window) = cx.add_window_view(|window, cx| {
            let picker = cx.new(|cx| {
                ColumnProjectionPicker::new(
                    "test-projection-picker",
                    profile(),
                    ColumnProjection::from_indices(4, &[0, 1]),
                    window,
                    cx,
                )
            });

            let subscription = cx.subscribe_in(
                &picker,
                window,
                |this: &mut Owner, _, event: &ProjectionChanged, _window, _cx| {
                    this.changes.push(event.0.clone());
                },
            );

            Owner {
                focus: cx.focus_handle(),
                picker,
                changes: Vec::new(),
                _subscription: subscription,
            }
        });
        window.run_until_parked();

        window.update(|window, cx| {
            let focus = owner.read(cx).focus.clone();
            focus.focus(window, cx);

            let picker = owner.read(cx).picker.clone();
            picker.update(cx, |picker, cx| picker.open(window, cx));
        });
        window.run_until_parked();

        (owner, window)
    }

    fn picker(
        owner: &Entity<Owner>,
        window: &mut VisualTestContext,
    ) -> Entity<ColumnProjectionPicker> {
        window.update(|_, cx| owner.read(cx).picker.clone())
    }

    #[gpui::test]
    fn apply_emits_selected_indices(cx: &mut TestAppContext) {
        let (owner, window) = setup(cx);

        // Highlight starts on `id`: move to `note` and select it, then apply.
        window.simulate_keystrokes("down down space enter");
        window.run_until_parked();

        let (changes, open, applied, owner_focused) = window.update(|window, cx| {
            let owner = owner.read(cx);
            let picker = owner.picker.read(cx);
            (
                owner.changes.clone(),
                picker.is_open(),
                picker.applied().clone(),
                owner.focus.is_focused(window),
            )
        });

        assert_eq!(changes.len(), 1, "Apply emits exactly once");
        assert_eq!(changes[0].selected_indices(), vec![0, 1, 2]);
        assert_eq!(applied.selected_indices(), vec![0, 1, 2]);
        assert!(!open, "Apply closes the popover");
        assert!(owner_focused, "Apply gives focus back to the host");
    }

    #[gpui::test]
    fn select_none_disables_apply(cx: &mut TestAppContext) {
        let (owner, window) = setup(cx);
        let picker = picker(&owner, window);

        let unchanged = window.update(|_, cx| picker.read(cx).can_apply());
        assert!(
            !unchanged,
            "a draft equal to the applied projection cannot be applied"
        );

        window.update(|_, cx| picker.update(cx, |picker, cx| picker.select_none(cx)));
        window.run_until_parked();

        let (can_apply, selected) = window.update(|_, cx| {
            let picker = picker.read(cx);
            (
                picker.can_apply(),
                picker.draft().map(ColumnProjection::selected_count),
            )
        });
        assert_eq!(selected, Some(0));
        assert!(!can_apply, "an empty draft cannot be applied");

        window.simulate_keystrokes("enter");
        window.run_until_parked();

        let (changes, open) = window.update(|_, cx| {
            let owner = owner.read(cx);
            (owner.changes.len(), owner.picker.read(cx).is_open())
        });
        assert_eq!(changes, 0, "Enter on an empty draft emits nothing");
        assert!(open, "the popover stays open");
    }

    #[gpui::test]
    fn search_keeps_selection_of_hidden_rows(cx: &mut TestAppContext) {
        let (owner, window) = setup(cx);
        let picker = picker(&owner, window);

        window.update(|window, cx| {
            picker.update(cx, |picker, cx| picker.set_search_query("utf", window, cx))
        });
        window.run_until_parked();

        let visible = window.update(|_, cx| picker.read(cx).visible_columns().to_vec());
        assert_eq!(
            visible,
            vec![1, 2],
            "the search shows only the UTF8 columns"
        );

        window.update(|_, cx| picker.update(cx, |picker, cx| picker.select_none(cx)));
        window.update(|_, cx| picker.update(cx, |picker, cx| picker.toggle_column(2, cx)));
        window.run_until_parked();

        window.update(|window, cx| {
            picker.update(cx, |picker, cx| picker.set_search_query("", window, cx))
        });
        window.run_until_parked();

        let (visible, draft) = window.update(|_, cx| {
            let picker = picker.read(cx);
            (
                picker.visible_columns().to_vec(),
                picker.draft().map(ColumnProjection::selected_indices),
            )
        });
        assert_eq!(visible, vec![0, 1, 2, 3]);
        assert_eq!(
            draft,
            Some(vec![0, 2]),
            "`id` was hidden by the search and stays selected"
        );
    }

    #[gpui::test]
    fn escape_discards_the_draft(cx: &mut TestAppContext) {
        let (owner, window) = setup(cx);

        window.simulate_keystrokes("down down space down space");
        window.run_until_parked();

        let draft = window.update(|_, cx| {
            owner
                .read(cx)
                .picker
                .read(cx)
                .draft()
                .map(ColumnProjection::selected_indices)
        });
        assert_eq!(draft, Some(vec![0, 1, 2, 3]));

        window.simulate_keystrokes("escape");
        window.run_until_parked();

        let (changes, open, applied, owner_focused) = window.update(|window, cx| {
            let owner = owner.read(cx);
            let picker = owner.picker.read(cx);
            (
                owner.changes.len(),
                picker.is_open(),
                picker.applied().selected_indices(),
                owner.focus.is_focused(window),
            )
        });
        assert_eq!(changes, 0, "Escape emits nothing");
        assert!(!open, "Escape closes the popover");
        assert_eq!(applied, vec![0, 1], "the applied projection is unchanged");
        assert!(owner_focused, "Escape gives focus back to the host");

        window.update(|window, cx| {
            let picker = owner.read(cx).picker.clone();
            picker.update(cx, |picker, cx| picker.open(window, cx));
        });
        let reopened = window.update(|_, cx| {
            owner
                .read(cx)
                .picker
                .read(cx)
                .draft()
                .map(ColumnProjection::selected_indices)
        });
        assert_eq!(
            reopened,
            Some(vec![0, 1]),
            "reopening starts from the applied projection"
        );
    }

    #[test]
    fn picker_keys_resolve_in_every_locale() {
        let keys = [
            "components.column_projection.trigger",
            "components.column_projection.search_placeholder",
            "components.column_projection.select_all",
            "components.column_projection.select_none",
            "components.column_projection.selected_count",
            "components.column_projection.apply",
            "components.column_projection.no_match",
            "components.column_projection.hint",
        ];

        for key in keys {
            let english = dbflux_i18n::t!(key, locale = "en");
            assert!(
                !english.is_empty() && !english.ends_with(key),
                "en misses {key}"
            );

            // A key missing from a catalog falls back to English, so a
            // translated catalog must give a different text.
            for locale in ["es", "ko", "pt_BR", "zh_Hans"] {
                let text = dbflux_i18n::t!(key, locale = locale);
                assert_ne!(text, english, "{locale} misses {key}");
            }
        }

        for locale in ["en", "es", "ko", "pt_BR", "zh_Hans"] {
            for key in [
                "components.column_projection.trigger",
                "components.column_projection.selected_count",
            ] {
                let text = dbflux_i18n::t!(key, locale = locale);
                assert!(
                    text.contains("%{selected}") && text.contains("%{total}"),
                    "{locale} {key} names both counts: {text}"
                );
            }
        }
    }
}
