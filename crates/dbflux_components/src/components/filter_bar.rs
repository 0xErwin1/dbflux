//! Reusable filter/toolbar bar with keyboard focus-ring navigation.
//!
//! Mirrors the `GridFocusMode::Toolbar` state machine in `DataGridPanel` so
//! every document gets the same UX:
//!
//! - `FocusToolbar` / `FocusSearch` → enter toolbar mode (focus ring, no text input focus yet)
//! - `h` / `l` or `←` / `→`         → move ring between items
//! - `Enter`                          → activate focused item (focus the input or fire the action)
//! - `Escape` / `FocusUp`            → return focus to the caller (handled by the document)
//!
//! ## State machine
//!
//! ```text
//!  Inactive ──FocusToolbar──► Navigating ──Enter──► Editing
//!                                 ▲                     │
//!                                 └────Escape/blur──────┘
//!             Inactive ◄──Escape──┘
//! ```
//!
//! ## Usage
//!
//! 1. Create a `FilterBarState` with your items.
//! 2. Store it in your document's struct.
//! 3. Call `state.dispatch()` from `dispatch_command` when toolbar is active.
//! 4. In render, build the element with `FilterBar::new(&state).render(cx)`.

use crate::controls::{Dropdown, InputState};
use crate::icons::AppIcon;
use crate::primitives::{Chamfer, ChamferRing, FOCUS_RING_SELECTOR, Icon, WhenFocusVisible};
use crate::tokens::{ChamferCut, ChromeColors, Fields, FontSizes, Heights, Radii, Spacing};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::Sizable;
use gpui_component::date_picker::DatePickerState;

// ── Item kinds ────────────────────────────────────────────────────────────────

/// A single navigable item in the bar.
pub enum FilterBarItem {
    /// A text input. Activated with Enter → enters Editing mode, input gets GPUI focus.
    Input {
        label: SharedString,
        input: Entity<InputState>,
    },
    /// A dropdown. Activated with Enter → opens the dropdown (stays in Navigating).
    Dropdown {
        label: SharedString,
        dropdown: Entity<Dropdown>,
    },
    DatePicker {
        label: SharedString,
        date_picker: Entity<DatePickerState>,
    },
    /// An action button. Activated with Enter → the document handles it externally.
    /// `activate_input` returns `false` for buttons so the caller can dispatch the action.
    Button {
        label: SharedString,
        icon: Option<AppIcon>,
    },
}

impl FilterBarItem {
    pub fn input(label: impl Into<SharedString>, input: Entity<InputState>) -> Self {
        Self::Input {
            label: label.into(),
            input,
        }
    }

    pub fn dropdown(label: impl Into<SharedString>, dropdown: Entity<Dropdown>) -> Self {
        Self::Dropdown {
            label: label.into(),
            dropdown,
        }
    }

    pub fn date_picker(
        label: impl Into<SharedString>,
        date_picker: Entity<DatePickerState>,
    ) -> Self {
        Self::DatePicker {
            label: label.into(),
            date_picker,
        }
    }

    pub fn button(label: impl Into<SharedString>) -> Self {
        Self::Button {
            label: label.into(),
            icon: None,
        }
    }

    pub fn button_with_icon(label: impl Into<SharedString>, icon: AppIcon) -> Self {
        Self::Button {
            label: label.into(),
            icon: Some(icon),
        }
    }
}

// ── Focus mode ────────────────────────────────────────────────────────────────

/// Toolbar-level focus state, analogous to `GridFocusMode` in DataGridPanel.
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub enum FilterBarMode {
    /// Toolbar is not focused; the caller's list/table has focus.
    #[default]
    Inactive,
    /// Keyboard focus ring is on the toolbar, navigating items.
    Navigating,
    /// One input item has keyboard focus and is receiving text.
    Editing,
}

// ── Dispatch result ───────────────────────────────────────────────────────────

/// Result of `FilterBarState::dispatch`.
pub enum FilterBarDispatch {
    /// The command was handled; call `cx.notify()`.
    Handled,
    /// The Escape/FocusUp command was issued — exit the toolbar and restore
    /// focus to the document's main content area.
    Exit,
    /// The command was not for the toolbar.
    Ignored,
}

// ── State ─────────────────────────────────────────────────────────────────────

/// State for a `FilterBar`. Owned by the parent document.
///
/// All mutating methods return without calling `cx.notify()` — the caller is
/// responsible for that, consistent with GPUI conventions for embedded state.
pub struct FilterBarState {
    items: Vec<FilterBarItem>,
    focused_index: usize,
    mode: FilterBarMode,
}

impl FilterBarState {
    pub fn new(items: Vec<FilterBarItem>) -> Self {
        Self {
            items,
            focused_index: 0,
            mode: FilterBarMode::Inactive,
        }
    }

    // ── Queries ────────────────────────────────────────────────────────────

    pub fn is_active(&self) -> bool {
        self.mode != FilterBarMode::Inactive
    }

    pub fn is_editing(&self) -> bool {
        self.mode == FilterBarMode::Editing
    }

    pub fn mode(&self) -> FilterBarMode {
        self.mode
    }

    pub fn focused_index(&self) -> usize {
        self.focused_index
    }

    pub fn item_count(&self) -> usize {
        self.items.len()
    }

    pub fn set_items(&mut self, items: Vec<FilterBarItem>) {
        self.items = items;

        if self.items.is_empty() {
            self.focused_index = 0;
            self.mode = FilterBarMode::Inactive;
            return;
        }

        self.focused_index = self.focused_index.min(self.items.len().saturating_sub(1));
    }

    // ── Activation ────────────────────────────────────────────────────────

    /// Enter toolbar navigation mode with the ring on `index` (clamped).
    pub fn enter(&mut self, index: usize) {
        self.mode = FilterBarMode::Navigating;
        self.focused_index = index.min(self.items.len().saturating_sub(1));
    }

    /// Leave toolbar mode entirely. The caller restores focus to the main area.
    pub fn deactivate(&mut self) {
        self.mode = FilterBarMode::Inactive;
        self.focused_index = 0;
    }

    /// Transition from Editing back to Navigating (e.g. on input blur).
    /// Call this from the input's `InputEvent::Blur` subscription.
    pub fn exit_editing(&mut self) {
        if self.mode == FilterBarMode::Editing {
            self.mode = FilterBarMode::Navigating;
        }
    }

    /// Returns the `Entity<Dropdown>` for the currently focused item, if it is
    /// a `Dropdown` variant. Used by the document to route keyboard commands
    /// (j/k, Enter, Escape) into the open dropdown.
    pub fn focused_dropdown_entity(&self) -> Option<Entity<crate::controls::Dropdown>> {
        match self.items.get(self.focused_index) {
            Some(FilterBarItem::Dropdown { dropdown, .. }) => Some(dropdown.clone()),
            _ => None,
        }
    }

    // ── Navigation ────────────────────────────────────────────────────────

    pub fn move_left(&mut self) {
        if self.focused_index > 0 {
            self.focused_index -= 1;
        }
        self.mode = FilterBarMode::Navigating;
    }

    pub fn move_right(&mut self) {
        if self.focused_index + 1 < self.items.len() {
            self.focused_index += 1;
        }
        self.mode = FilterBarMode::Navigating;
    }

    /// Activate the currently focused item.
    ///
    /// - `Input` → enters Editing mode and gives GPUI focus to the text input.
    ///   Returns `true`.
    /// - `Dropdown` → opens the dropdown, stays in Navigating mode.
    ///   Returns `true`.
    /// - `Button` → the document is responsible for executing the action.
    ///   Returns `false` so the caller can check `focused_index()` and dispatch.
    pub fn activate_input(&mut self, window: &mut Window, cx: &mut App) -> bool {
        let Some(item) = self.items.get(self.focused_index) else {
            return false;
        };

        match item {
            FilterBarItem::Input { input, .. } => {
                self.mode = FilterBarMode::Editing;
                let input = input.clone();
                input.update(cx, |state, cx| {
                    state.focus(window, cx);
                });
                true
            }
            FilterBarItem::Dropdown { dropdown, .. } => {
                // Open the dropdown. Mode stays Navigating so h/l still work
                // after closing the dropdown.
                let dropdown = dropdown.clone();
                dropdown.update(cx, |d, cx| {
                    d.open(cx);
                });
                true
            }
            FilterBarItem::DatePicker { date_picker, .. } => {
                self.mode = FilterBarMode::Editing;
                let date_picker = date_picker.clone();
                date_picker.update(cx, |state, cx| {
                    state.focus_handle(cx).focus(window, cx);
                });
                true
            }
            FilterBarItem::Button { .. } => {
                // Caller handles the action based on focused_index().
                false
            }
        }
    }

    // ── Keyboard dispatch ─────────────────────────────────────────────────

    /// Handle a `Command` while the toolbar is active.
    ///
    /// The caller must call `cx.notify()` after `Handled` and restore focus
    /// after `Exit`.
    pub fn dispatch(
        &mut self,
        cmd: dbflux_core::keymap_types::Command,
        window: &mut Window,
        cx: &mut App,
    ) -> FilterBarDispatch {
        use dbflux_core::keymap_types::Command;

        match cmd {
            Command::ColumnLeft | Command::FocusLeft => {
                self.move_left();
                FilterBarDispatch::Handled
            }
            Command::ColumnRight | Command::FocusRight => {
                self.move_right();
                FilterBarDispatch::Handled
            }
            Command::Execute => {
                self.activate_input(window, cx);
                FilterBarDispatch::Handled
            }
            Command::Cancel | Command::FocusUp => FilterBarDispatch::Exit,
            _ => FilterBarDispatch::Ignored,
        }
    }
}

// ── Render element ────────────────────────────────────────────────────────────

/// Renders a `FilterBarState` as a toolbar row.
///
/// Embed with `.child(FilterBar::new(&state).render(cx))` in the parent's
/// `render` method.
pub struct FilterBar<'a> {
    state: &'a FilterBarState,
    /// Extra items appended after the bar's own items (e.g. a spacer + export button).
    extra: Vec<AnyElement>,
}

impl<'a> FilterBar<'a> {
    pub fn new(state: &'a FilterBarState) -> Self {
        Self {
            state,
            extra: Vec::new(),
        }
    }

    /// Append an extra element at the right end of the toolbar row.
    pub fn with_extra(mut self, element: AnyElement) -> Self {
        self.extra.push(element);
        self
    }

    pub fn render(self, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme().clone();
        let show_ring = self.state.mode == FilterBarMode::Navigating;
        let focused_index = self.state.focused_index;

        let items: Vec<AnyElement> = self
            .state
            .items
            .iter()
            .enumerate()
            .map(|(idx, item)| {
                let ring_active = show_ring && idx == focused_index;
                render_item(item, ring_active, &theme, cx)
            })
            .collect();

        div()
            .flex()
            .flex_wrap()
            .items_center()
            .gap(Spacing::SM)
            .min_h(Heights::TOOLBAR)
            .px(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.secondary)
            .children(items)
            .children(self.extra)
    }
}

// ── Filter field ──────────────────────────────────────────────────────────────

/// The filter field of a table toolbar: one chamfered field holding the
/// filter keyword and its editor, and on its right, past a divider, the limit
/// keyword and its value.
///
/// Layout from the table screen: `Fields::FILTER_HEIGHT` tall, cut
/// `ChamferCut::INPUT`, ground fill with a 1 px `theme.input` line, the data
/// face at `FontSizes::BASE`, keywords bold in the tint. The editor and the
/// limit value are frameless children supplied by the host, which owns their
/// state and events.
///
/// Focus: the field's ring (inset tint) marks the filter part; a ring around
/// the limit value marks the limit part; an error replaces the field's ring
/// with a danger one.
#[derive(IntoElement)]
pub struct FilterField {
    id: ElementId,
    filter: Option<(SharedString, AnyElement)>,
    trailing: Vec<AnyElement>,
    limit: Option<(SharedString, AnyElement)>,
    filter_focused: bool,
    limit_focused: bool,
    error: bool,
}

impl FilterField {
    pub fn new(id: impl Into<ElementId>) -> Self {
        Self {
            id: id.into(),
            filter: None,
            trailing: Vec::new(),
            limit: None,
            filter_focused: false,
            limit_focused: false,
            error: false,
        }
    }

    /// The filter part: its keyword (for example `WHERE`) and the frameless
    /// editor that follows it.
    pub fn filter(mut self, keyword: impl Into<SharedString>, editor: impl IntoElement) -> Self {
        self.filter = Some((keyword.into(), editor.into_any_element()));
        self
    }

    /// An element drawn after the filter editor, before the limit part
    /// (clear button, status chips).
    pub fn trailing(mut self, element: impl IntoElement) -> Self {
        self.trailing.push(element.into_any_element());
        self
    }

    /// The limit part: its keyword (for example `LIMIT`) and the frameless
    /// value input.
    pub fn limit(mut self, keyword: impl Into<SharedString>, value: impl IntoElement) -> Self {
        self.limit = Some((keyword.into(), value.into_any_element()));
        self
    }

    pub fn filter_focused(mut self, focused: bool) -> Self {
        self.filter_focused = focused;
        self
    }

    pub fn limit_focused(mut self, focused: bool) -> Self {
        self.limit_focused = focused;
        self
    }

    /// Marks the filter as invalid: the field's ring turns to the danger color.
    pub fn error(mut self, error: bool) -> Self {
        self.error = error;
        self
    }
}

impl RenderOnce for FilterField {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let tint = ChromeColors::tint(theme);
        let has_filter = self.filter.is_some();

        let mut shape = Chamfer::new(ChamferCut::INPUT)
            .fill(theme.background)
            .border(theme.input);

        if self.error {
            shape = shape.ring(ChamferRing::outline(theme.danger));
        } else if self.filter_focused {
            shape = shape.ring(ChamferRing::focus(tint));
        }

        let keyword = |text: SharedString| {
            div()
                .flex_none()
                .font_weight(FontWeight::BOLD)
                .text_color(tint)
                .child(text)
        };

        div()
            .id(self.id)
            .relative()
            .flex()
            .items_center()
            .when(has_filter, |this| this.flex_1().min_w(px(0.0)))
            .h(Fields::FILTER_HEIGHT)
            .px(Fields::FILTER_PADDING_X)
            .gap(Fields::FILTER_GAP)
            .font_family(crate::fonts::editor_family(cx))
            .text_size(FontSizes::BASE)
            .text_color(theme.accent_foreground)
            .child(shape)
            .when_some(self.filter, |this, (filter_keyword, editor)| {
                this.child(
                    Icon::new(AppIcon::ListFilter)
                        .size(Fields::FILTER_ICON)
                        .color(theme.muted_foreground),
                )
                .child(keyword(filter_keyword))
                .child(div().flex_1().min_w(px(0.0)).child(editor))
            })
            .children(self.trailing)
            .when_some(self.limit, |this, (limit_keyword, value)| {
                let value_box = div()
                    .relative()
                    .flex_none()
                    .w(Fields::FILTER_LIMIT_WIDTH)
                    .when(self.limit_focused, |this| {
                        this.child(Chamfer::new(ChamferCut::KEYCAP).ring(ChamferRing::focus(tint)))
                    })
                    .child(value);

                this.child(
                    div()
                        .flex()
                        .flex_none()
                        .items_center()
                        .when(has_filter, |this| {
                            this.pl(Fields::FILTER_PADDING_X)
                                .border_l_1()
                                .border_color(theme.border)
                        })
                        .child(keyword(limit_keyword))
                        .child(value_box),
                )
            })
    }
}

// ── Private render helpers ────────────────────────────────────────────────────

/// The navigation cursor's frame over a filter item, shown only while focus
/// is visible.
fn focus_frame(theme: &gpui_component::theme::Theme) -> WhenFocusVisible {
    WhenFocusVisible::new(
        div()
            .absolute()
            .inset_0()
            .rounded(Radii::SM)
            .border_1()
            .border_color(theme.ring)
            .debug_selector(|| FOCUS_RING_SELECTOR.to_string()),
    )
}

fn render_item(
    item: &FilterBarItem,
    ring_active: bool,
    theme: &gpui_component::theme::Theme,
    _cx: &App,
) -> AnyElement {
    use crate::controls::Input;
    use crate::primitives::Text;

    match item {
        FilterBarItem::Input { label, input } => div()
            .flex()
            .items_center()
            .gap(Spacing::XS)
            .child(Text::caption(label.clone()))
            .child(
                div()
                    .flex()
                    .items_center()
                    .h(Heights::CONTROL)
                    .min_w(px(180.0))
                    .rounded(Radii::SM)
                    .relative()
                    .child(div().flex_1().child(Input::new(input).small()))
                    .when(ring_active, |d| d.child(focus_frame(theme))),
            )
            .into_any_element(),

        FilterBarItem::Dropdown { label, dropdown } => div()
            .flex()
            .items_center()
            .gap(Spacing::XS)
            .child(Text::caption(label.clone()))
            .child(
                div()
                    .flex()
                    .items_center()
                    .h(Heights::CONTROL)
                    .rounded(Radii::SM)
                    .relative()
                    .child(dropdown.clone())
                    .when(ring_active, |d| d.child(focus_frame(theme))),
            )
            .into_any_element(),

        FilterBarItem::DatePicker { label, date_picker } => div()
            .flex()
            .items_center()
            .gap(Spacing::XS)
            .child(Text::caption(label.clone()))
            .child(
                div()
                    .flex()
                    .items_center()
                    .h(Heights::CONTROL)
                    .min_w(px(220.0))
                    .rounded(Radii::SM)
                    .relative()
                    .child(gpui_component::date_picker::DatePicker::new(date_picker).small())
                    .when(ring_active, |d| d.child(focus_frame(theme))),
            )
            .into_any_element(),

        FilterBarItem::Button { label, icon } => div()
            .flex()
            .items_center()
            .h(Heights::CONTROL)
            .px(Spacing::SM)
            .gap_1()
            .rounded(Radii::SM)
            .bg(theme.background)
            .relative()
            .border_1()
            .border_color(theme.input)
            .cursor_pointer()
            .hover(|d| d.bg(theme.accent.opacity(0.08)))
            .when_some(*icon, |d, icon| {
                d.child(Icon::new(icon).size(Heights::ICON_SM).muted())
            })
            .child(Text::body(label.clone()).font_size(FontSizes::SM))
            .when(ring_active, |d| d.child(focus_frame(theme)))
            .into_any_element(),
    }
}
