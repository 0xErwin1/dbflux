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

use crate::ui::components::dropdown::Dropdown;
use crate::ui::icons::AppIcon;
use crate::ui::tokens::{FontSizes, Heights, Radii, Spacing};
use gpui::prelude::*;
use gpui::*;
use gpui_component::input::InputState;
use gpui_component::ActiveTheme;

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
    pub fn focused_dropdown_entity(
        &self,
    ) -> Option<Entity<crate::ui::components::dropdown::Dropdown>> {
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
        cmd: crate::keymap::Command,
        window: &mut Window,
        cx: &mut App,
    ) -> FilterBarDispatch {
        use crate::keymap::Command;

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
            .items_center()
            .gap(Spacing::SM)
            .h(Heights::TOOLBAR)
            .px(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .bg(theme.secondary)
            .children(items)
            .children(self.extra)
    }
}

// ── Private render helpers ────────────────────────────────────────────────────

fn render_item(
    item: &FilterBarItem,
    ring_active: bool,
    theme: &gpui_component::theme::Theme,
    _cx: &App,
) -> AnyElement {
    use dbflux_components::controls::Input;
    use dbflux_components::primitives::Text;

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
                    .min_w(px(180.0))
                    .rounded(Radii::SM)
                    .when(ring_active, |d| d.border_1().border_color(theme.ring))
                    .child(div().flex_1().child(Input::new(input).small())),
            )
            .into_any_element(),

        FilterBarItem::Dropdown { label, dropdown } => div()
            .flex()
            .items_center()
            .gap(Spacing::XS)
            .child(Text::caption(label.clone()))
            .child(
                div()
                    .rounded(Radii::SM)
                    .when(ring_active, |d| d.border_1().border_color(theme.ring))
                    .child(dropdown.clone()),
            )
            .into_any_element(),

        FilterBarItem::Button { label, icon } => {
            let border_color = if ring_active { theme.ring } else { theme.input };

            div()
                .flex()
                .items_center()
                .h(Heights::BUTTON)
                .px(Spacing::SM)
                .gap_1()
                .rounded(Radii::SM)
                .bg(theme.background)
                .border_1()
                .border_color(border_color)
                .cursor_pointer()
                .text_size(FontSizes::SM)
                .text_color(theme.foreground)
                .hover(|d| d.bg(theme.accent.opacity(0.08)))
                .when_some(*icon, |d, icon| {
                    d.child(
                        svg()
                            .path(icon.path())
                            .size_4()
                            .text_color(theme.muted_foreground),
                    )
                })
                .child(label.clone())
                .into_any_element()
        }
    }
}
