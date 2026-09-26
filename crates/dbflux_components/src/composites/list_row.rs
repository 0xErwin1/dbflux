//! `ListRow` — the selected, hovered and focused states of a list row in one
//! place (DSApp "Tree", DSAppPlan "ListRow").

use gpui::prelude::*;
use gpui::{App, Div, ElementId, Stateful, div};
use gpui_component::ActiveTheme;

use crate::primitives::{FOCUS_MARKER_SELECTOR, WhenFocusVisible};
use crate::tokens::{ChromeColors, TreeMetrics};

/// A clickable row of a list, table-like panel or picker.
///
/// - Selected: tint wash, optionally with a 2 px tint bar on the left edge
///   (the tree and sidebar treatment).
/// - Hovered: the palette hover wash, only while not selected.
/// - Focused: the keyboard cursor of the list. The tint wash (when not
///   already selected) and the 2 px tint bar on the left edge, shown only
///   while focus is visible; a list never rings its rows.
///
/// [`ListRow::build`] returns the row as a `Stateful<Div>`; the caller lays
/// out its content and wires its handlers on it. Rows have no cut: they hold
/// data.
pub struct ListRow {
    id: ElementId,
    selected: bool,
    focused: bool,
    selection_bar: bool,
    interactive: bool,
}

impl ListRow {
    pub fn new(id: impl Into<ElementId>) -> Self {
        Self {
            id: id.into(),
            selected: false,
            focused: false,
            selection_bar: false,
            interactive: true,
        }
    }

    pub fn selected(mut self, selected: bool) -> Self {
        self.selected = selected;
        self
    }

    pub fn focused(mut self, focused: bool) -> Self {
        self.focused = focused;
        self
    }

    /// Adds the 2 px tint bar on the left edge of the selected row.
    pub fn selection_bar(mut self, selection_bar: bool) -> Self {
        self.selection_bar = selection_bar;
        self
    }

    /// A display-only row: no hover wash and no pointer cursor.
    pub fn interactive(mut self, interactive: bool) -> Self {
        self.interactive = interactive;
        self
    }

    pub fn build(self, cx: &App) -> Stateful<Div> {
        let theme = cx.theme();
        let tint = ChromeColors::tint(theme);
        let hover_wash = theme.list_hover;
        let selected_wash = theme.list_active;
        let show_bar = self.selected && self.selection_bar;

        div()
            .id(self.id)
            .relative()
            .when(self.selected, |row| row.bg(theme.list_active))
            .when(self.interactive, |row| row.cursor_pointer())
            .when(self.interactive && !self.selected, |row| {
                row.hover(move |row| row.bg(hover_wash))
            })
            .when(show_bar, |row| {
                row.child(
                    div()
                        .absolute()
                        .left_0()
                        .top_0()
                        .bottom_0()
                        .w(TreeMetrics::SELECTION_BAR)
                        .bg(tint),
                )
            })
            .when(self.focused, |row| {
                row.child(WhenFocusVisible::new(
                    div()
                        .absolute()
                        .inset_0()
                        .when(!self.selected, |marker| marker.bg(selected_wash))
                        .debug_selector(|| FOCUS_MARKER_SELECTOR.to_string())
                        .child(
                            div()
                                .absolute()
                                .left_0()
                                .top_0()
                                .bottom_0()
                                .w(TreeMetrics::SELECTION_BAR)
                                .bg(tint),
                        ),
                ))
            })
    }
}
