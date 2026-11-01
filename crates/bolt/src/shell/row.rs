//! The list row the sidebar's tree and the master lists render: one
//! indented row with an optional chevron, an icon, a label, a trailing slot,
//! the hover and selected states, and an inline-edit state.
//!
//! The row is a pure render: the host owns the tree state, the cursor and
//! the expansion set, lists its rows from that state every frame, and wires
//! the click handlers. Nothing here navigates, expands or edits on its own.
//! While a row is being edited (renamed in place), the edit field replaces
//! the label; [`inline_edit`] builds the frameless field the two products
//! draw, and the host commits or cancels it on Enter, Escape and clicks
//! outside the row.

use gpui::prelude::*;
use gpui::{
    App, ClickEvent, ElementId, Entity, Hsla, IntoElement, ParentElement, RenderOnce, SharedString,
    Styled, Window, div,
};
use gpui_component::theme::Theme;
use gpui_component::{ActiveTheme, input::InputState};

use crate::controls::Input;
use crate::fonts;
use crate::icon::IconSource;
use crate::primitives::{Icon, Text};
use crate::tokens::{Borders, ChromeColors, TreeMetrics};

/// Which way a row's expand chevron points.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Chevron {
    /// The row's children are shown.
    Expanded,
    /// The row's children are hidden.
    Collapsed,
}

impl Chevron {
    fn icon(self) -> gpui_component::IconName {
        match self {
            Self::Expanded => gpui_component::IconName::ChevronDown,
            Self::Collapsed => gpui_component::IconName::ChevronRight,
        }
    }
}

/// Fills and marks of a row, per its selected state (the states board).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct NavRowColors {
    /// The wash under a selected row; transparent for an unselected one.
    pub rest: Hsla,
    /// The wash while the pointer is over an unselected row.
    pub hover: Hsla,
    /// The selection bar at the row's left edge.
    pub selection_bar: Hsla,
    /// The indent guide lines of ancestor levels.
    pub guide: Hsla,
    /// The icon color of an unselected row.
    pub icon: Hsla,
    /// The icon color of a selected row.
    pub icon_selected: Hsla,
    /// The label color of an unselected row.
    pub label: Hsla,
    /// The label color of a selected row.
    pub label_selected: Hsla,
}

pub fn nav_row_colors(theme: &Theme, selected: bool) -> NavRowColors {
    NavRowColors {
        rest: if selected {
            theme.list_active
        } else {
            gpui::transparent_black()
        },
        hover: theme.list_hover,
        selection_bar: ChromeColors::tint(theme),
        guide: theme.border,
        icon: theme.muted_foreground,
        icon_selected: ChromeColors::tint(theme),
        label: theme.foreground,
        label_selected: ChromeColors::strong(theme),
    }
}

/// The frameless inline-edit field a renaming row shows in place of its
/// label. The host owns the `InputState` and the commit/cancel handling.
pub fn inline_edit(state: &Entity<InputState>) -> Input {
    Input::new(state).appearance(false).w_full()
}

type RowClickHandler = std::rc::Rc<dyn Fn(&ClickEvent, &mut Window, &mut App) + 'static>;

/// One indented list row: guides per ancestor level, an optional chevron, an
/// optional icon, the label (or the inline edit), and a trailing slot.
///
/// Element ids, when the row id is a name: `chevron` becomes `<id>-chevron`,
/// the label `<id>-label`, the selection bar `<id>-selection-bar` and each
/// indent guide `<id>-guide-<level>`.
#[derive(IntoElement)]
pub struct NavRow {
    id: ElementId,
    depth: usize,
    label: SharedString,
    icon: Option<IconSource>,
    chevron: Option<Chevron>,
    guides: bool,
    selected: bool,
    editing: Option<gpui::AnyElement>,
    label_color: Option<Hsla>,
    trailing: Vec<gpui::AnyElement>,
    on_click: Option<RowClickHandler>,
    on_chevron_click: Option<RowClickHandler>,
}

impl NavRow {
    pub fn new(id: impl Into<ElementId>, label: impl Into<SharedString>) -> Self {
        Self {
            id: id.into(),
            depth: 0,
            label: label.into(),
            icon: None,
            chevron: None,
            guides: false,
            selected: false,
            editing: None,
            label_color: None,
            trailing: Vec::new(),
            on_click: None,
            on_chevron_click: None,
        }
    }

    /// Nesting level: every level indents the row by `TreeMetrics::INDENT`.
    pub fn depth(mut self, depth: usize) -> Self {
        self.depth = depth;
        self
    }

    /// Icon before the label.
    pub fn icon(mut self, icon: impl Into<IconSource>) -> Self {
        self.icon = Some(icon.into());
        self
    }

    /// Draw the expand chevron, pointing the given way.
    pub fn chevron(mut self, chevron: Chevron) -> Self {
        self.chevron = Some(chevron);
        self
    }

    /// Draw one indent guide per ancestor level, running through the
    /// ancestor's chevron column.
    pub fn guides(mut self, guides: bool) -> Self {
        self.guides = guides;
        self
    }

    /// The row is the one open in the detail pane.
    pub fn selected(mut self, selected: bool) -> Self {
        self.selected = selected;
        self
    }

    /// The inline-edit field shown in place of the label while the row is
    /// being renamed. See [`inline_edit`].
    pub fn editing(mut self, editing: impl IntoElement) -> Self {
        self.editing = Some(editing.into_any_element());
        self
    }

    /// Overrides the label color (the selected state picks it otherwise).
    pub fn label_color(mut self, color: impl Into<Hsla>) -> Self {
        self.label_color = Some(color.into());
        self
    }

    /// Trailing content after the label: counts, latency, a menu button.
    pub fn trailing(mut self, element: impl IntoElement) -> Self {
        self.trailing.push(element.into_any_element());
        self
    }

    pub fn on_click(
        mut self,
        handler: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        self.on_click = Some(std::rc::Rc::new(handler));
        self
    }

    /// Clicks on the chevron run this instead of reaching the row.
    pub fn on_chevron_click(
        mut self,
        handler: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        self.on_chevron_click = Some(std::rc::Rc::new(handler));
        self
    }

    fn name(&self) -> Option<SharedString> {
        match &self.id {
            ElementId::Name(name) => Some(name.clone()),
            _ => None,
        }
    }

    fn render_chevron(&self, chevron: Chevron, color: Hsla) -> gpui::Stateful<gpui::Div> {
        let chevron_id = self
            .name()
            .map(|name| ElementId::Name(format!("{name}-chevron").into()))
            .unwrap_or_else(|| self.id.clone());
        let on_chevron_click = self.on_chevron_click.clone();
        let selector = self.name().map(|name| format!("{name}-chevron"));

        let mut chevron_el = div().id(chevron_id).flex_none().cursor_pointer();
        if let Some(selector) = selector {
            chevron_el = chevron_el.debug_selector(move || selector.clone());
        }

        chevron_el
            .child(
                Icon::new(chevron.icon())
                    .size(TreeMetrics::CHEVRON)
                    .color(color),
            )
            .on_click(move |event, window, cx| {
                // The chevron toggles the row; the click must not also land
                // on the row itself. Without a chevron handler the click
                // falls through to the row.
                if let Some(handler) = &on_chevron_click {
                    cx.stop_propagation();
                    handler(event, window, cx);
                }
            })
    }
}

impl RenderOnce for NavRow {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let colors = nav_row_colors(theme, self.selected);
        let label_color = self.label_color.unwrap_or(if self.selected {
            colors.label_selected
        } else {
            colors.label
        });
        let icon_color = if self.selected {
            colors.icon_selected
        } else {
            colors.icon
        };

        let indent_px = fonts::ui_px(cx, TreeMetrics::INDENT);
        let padding_left = TreeMetrics::PADDING_X + indent_px * self.depth as f32;

        let mut guides: Vec<gpui::AnyElement> = Vec::new();
        if self.guides && self.depth > 0 {
            let guide_base = fonts::ui_px(cx, TreeMetrics::GUIDE_OFFSET);
            let row_name = self.name();
            for level in 0..self.depth {
                let offset = guide_base + indent_px * level as f32;
                let mut guide = div()
                    .absolute()
                    .top_0()
                    .bottom_0()
                    .left(offset)
                    .w(Borders::THIN)
                    .bg(colors.guide);
                if let Some(name) = &row_name {
                    let selector = format!("{name}-guide-{level}");
                    guide = guide.debug_selector(move || selector.clone());
                }
                guides.push(guide.into_any_element());
            }
        }

        let row_name = self.name();

        let mut row = div()
            .id(self.id.clone())
            .when_some(row_name.clone(), |row, name| {
                row.debug_selector(move || name.to_string())
            })
            .aria_selected(self.selected)
            .when_some(self.chevron.as_ref(), |row, chevron| {
                row.aria_expanded(*chevron == Chevron::Expanded)
            })
            .relative()
            .flex_none()
            .flex()
            .items_center()
            .gap(TreeMetrics::GAP)
            .h(TreeMetrics::ROW_HEIGHT)
            .pl(padding_left)
            .pr(TreeMetrics::PADDING_X)
            .whitespace_nowrap()
            .bg(colors.rest)
            .children(guides)
            .when(self.selected, |row| {
                let mut bar = div()
                    .absolute()
                    .left_0()
                    .top_0()
                    .bottom_0()
                    .w(TreeMetrics::SELECTION_BAR)
                    .bg(colors.selection_bar);
                if let Some(name) = &row_name {
                    let selector = format!("{name}-selection-bar");
                    bar = bar.debug_selector(move || selector.clone());
                }
                row.child(bar)
            })
            .when(!self.selected, |row| {
                row.hover(move |style| style.bg(colors.hover))
            })
            .when_some(self.chevron, |row, chevron| {
                row.child(self.render_chevron(chevron, icon_color))
            })
            .when_some(self.icon, |row, icon| {
                row.child(Icon::new(icon).size(TreeMetrics::ICON).color(icon_color))
            });

        if let Some(editing) = self.editing {
            row = row.child(
                div()
                    .flex_1()
                    .min_w_0()
                    .h_full()
                    .flex()
                    .items_center()
                    .child(editing),
            );
        } else {
            let mut label = div()
                .flex_1()
                .min_w_0()
                .overflow_hidden()
                .child(Text::body(self.label).text_color(label_color));
            if let Some(name) = &row_name {
                let selector = format!("{name}-label");
                label = label.debug_selector(move || selector.clone());
            }
            row = row.child(label);
        }

        row = row.children(self.trailing);

        if self.on_click.is_some() {
            let handler = self.on_click;
            row = row
                .on_click(move |event, window, cx| {
                    if let Some(handler) = &handler {
                        handler(event, window, cx);
                    }
                })
                .cursor_pointer();
        }

        row
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokens::BASE_REM;
    use gpui::{Pixels, px};

    fn install_test_theme(cx: &mut gpui::TestAppContext) {
        cx.update(gpui_component::init);
        cx.update(|cx| {
            crate::theme::install(
                crate::brand::Brand::neutral(),
                crate::palette::PaletteMode::Dark,
                crate::density::Density::Default,
                cx,
            );
        });
    }

    #[test]
    fn selected_row_takes_the_selected_wash_and_the_unselected_one_stays_transparent() {
        let theme = gpui_component::Theme::default();
        let tint = ChromeColors::tint(&theme);

        let selected = nav_row_colors(&theme, true);
        assert_eq!(selected.rest, theme.list_active);
        assert_eq!(selected.selection_bar, tint);
        assert_eq!(selected.icon_selected, tint);
        assert_eq!(selected.label_selected, ChromeColors::strong(&theme));

        let plain = nav_row_colors(&theme, false);
        assert_eq!(plain.rest, gpui::transparent_black());
        assert_eq!(plain.hover, theme.list_hover);
        assert_eq!(plain.guide, theme.border);
        assert_eq!(plain.icon, theme.muted_foreground);
        assert_eq!(plain.label, theme.foreground);
    }

    #[test]
    fn chevron_icon_follows_the_expansion() {
        assert!(matches!(
            Chevron::Expanded.icon(),
            gpui_component::IconName::ChevronDown
        ));
        assert!(matches!(
            Chevron::Collapsed.icon(),
            gpui_component::IconName::ChevronRight
        ));
    }

    type Events = std::rc::Rc<std::cell::RefCell<Vec<&'static str>>>;

    /// A column of rows rebuilt every frame from the harness state, the way
    /// a host lists its rows.
    struct RowHarness {
        depth: usize,
        guides: bool,
        selected: bool,
        chevron: bool,
        trailing: bool,
        /// Whether the row is being renamed; the field state is built on the
        /// first frame the row renders with.
        rename: bool,
        editing: Option<Entity<InputState>>,
        events: Events,
    }

    impl Render for RowHarness {
        fn render(
            &mut self,
            window: &mut Window,
            cx: &mut gpui::Context<Self>,
        ) -> impl IntoElement {
            if self.rename && self.editing.is_none() {
                self.editing = Some(cx.new(|cx| InputState::new(window, cx)));
            }

            let events = self.events.clone();
            let chevron_events = self.events.clone();

            let mut row = NavRow::new("row-under-test", "Connections")
                .depth(self.depth)
                .guides(self.guides)
                .selected(self.selected)
                .on_click(move |_, _, _| events.borrow_mut().push("row"));

            if self.chevron {
                row = row
                    .chevron(Chevron::Collapsed)
                    .on_chevron_click(move |_, _, _| {
                        chevron_events.borrow_mut().push("chevron");
                    });
            }

            if self.trailing {
                row = row.trailing(
                    div()
                        .debug_selector(|| "row-trailing".to_string())
                        .child("3"),
                );
            }

            if let Some(state) = self.editing.clone() {
                row = row.editing(
                    div()
                        .debug_selector(|| "row-edit-slot".to_string())
                        .child(inline_edit(&state)),
                );
            }

            div().size_full().flex().flex_col().child(row)
        }
    }

    fn open_row(
        cx: &mut gpui::TestAppContext,
        build: impl FnOnce(&mut RowHarness) + 'static,
    ) -> (Events, &mut gpui::VisualTestContext) {
        install_test_theme(cx);

        let events: Events = std::rc::Rc::default();
        let events_for_harness = events.clone();
        let (_, window) = cx.add_window_view(move |window, cx| {
            let mut harness = RowHarness {
                depth: 0,
                guides: false,
                selected: false,
                chevron: false,
                trailing: false,
                rename: false,
                editing: None,
                events: events_for_harness,
            };
            build(&mut harness);
            let harness = cx.new(|_| harness);
            gpui_component::Root::new(harness, window, cx)
        });
        window.run_until_parked();
        (events, window)
    }

    fn indent_at_base_rem() -> Pixels {
        TreeMetrics::INDENT.to_pixels(px(BASE_REM))
    }

    #[gpui::test]
    fn row_renders_at_the_tree_row_metrics(cx: &mut gpui::TestAppContext) {
        let (_, window) = open_row(cx, |_| {});

        let row = window
            .debug_bounds("row-under-test")
            .expect("the row is laid out");

        assert_eq!(
            row.size.height,
            TreeMetrics::ROW_HEIGHT.to_pixels(px(BASE_REM)),
            "the row is one tree row tall"
        );
    }

    #[gpui::test]
    fn content_indents_one_level_per_depth(cx: &mut gpui::TestAppContext) {
        let (_, window) = open_row(cx, |harness| harness.depth = 2);

        let row = window
            .debug_bounds("row-under-test")
            .expect("the row is laid out");
        let label = window
            .debug_bounds("row-under-test-label")
            .expect("the label is laid out");

        let expected = TreeMetrics::PADDING_X + indent_at_base_rem() * 2.0;
        assert_eq!(
            label.left() - row.left(),
            expected,
            "the label starts after one indent per depth level"
        );
    }

    #[gpui::test]
    fn guides_draw_one_line_per_ancestor_level(cx: &mut gpui::TestAppContext) {
        let (_, window) = open_row(cx, |harness| {
            harness.depth = 2;
            harness.guides = true;
        });

        let row = window
            .debug_bounds("row-under-test")
            .expect("the row is laid out");
        let guide_base = TreeMetrics::GUIDE_OFFSET.to_pixels(px(BASE_REM));
        let indent = indent_at_base_rem();

        let selectors = ["row-under-test-guide-0", "row-under-test-guide-1"];
        for (level, selector) in selectors.iter().enumerate() {
            let guide = window
                .debug_bounds(selector)
                .unwrap_or_else(|| panic!("guide {level} is laid out"));
            assert_eq!(guide.size.width, Borders::THIN);
            assert_eq!(
                guide.left() - row.left(),
                guide_base + indent * level as f32,
                "guide {level} runs through its ancestor's chevron column"
            );
        }

        assert!(
            window.debug_bounds("row-under-test-guide-2").is_none(),
            "a depth-2 row draws no guide for its own level"
        );
    }

    #[gpui::test]
    fn a_selected_row_shows_the_selection_bar_and_an_unselected_one_shows_none(
        cx: &mut gpui::TestAppContext,
    ) {
        let (_, window) = open_row(cx, |harness| harness.selected = true);

        let row = window
            .debug_bounds("row-under-test")
            .expect("the row is laid out");
        let bar = window
            .debug_bounds("row-under-test-selection-bar")
            .expect("the selected row carries a selection bar");

        assert_eq!(bar.size.width, TreeMetrics::SELECTION_BAR);
        assert_eq!(bar.left(), row.left());
        assert_eq!(bar.size.height, row.size.height);

        let (_, window) = open_row(cx, |_| {});
        assert!(
            window
                .debug_bounds("row-under-test-selection-bar")
                .is_none(),
            "an unselected row draws no selection bar"
        );
    }

    #[gpui::test]
    fn a_row_with_a_chevron_reports_it_and_a_row_without_renders_none(
        cx: &mut gpui::TestAppContext,
    ) {
        let (_, window) = open_row(cx, |harness| harness.chevron = true);
        assert!(
            window.debug_bounds("row-under-test-chevron").is_some(),
            "the chevron renders"
        );

        let (_, window) = open_row(cx, |_| {});
        assert!(window.debug_bounds("row-under-test-chevron").is_none());
    }

    #[gpui::test]
    fn clicking_the_chevron_does_not_reach_the_row_but_clicking_the_row_does(
        cx: &mut gpui::TestAppContext,
    ) {
        let (events, window) = open_row(cx, |harness| harness.chevron = true);

        let chevron = window
            .debug_bounds("row-under-test-chevron")
            .expect("the chevron is laid out")
            .center();
        window.simulate_click(chevron, gpui::Modifiers::default());
        window.run_until_parked();
        assert_eq!(*events.borrow(), ["chevron"]);

        let label = window
            .debug_bounds("row-under-test-label")
            .expect("the label is laid out")
            .center();
        window.simulate_click(label, gpui::Modifiers::default());
        window.run_until_parked();
        assert_eq!(*events.borrow(), ["chevron", "row"]);
    }

    #[gpui::test]
    fn trailing_content_sits_inside_the_row(cx: &mut gpui::TestAppContext) {
        let (_, window) = open_row(cx, |harness| harness.trailing = true);

        let row = window
            .debug_bounds("row-under-test")
            .expect("the row is laid out");
        let trailing = window
            .debug_bounds("row-trailing")
            .expect("the trailing slot is laid out");

        assert!(trailing.right() <= row.right());
        assert!(trailing.left() > row.left());
    }

    #[gpui::test]
    fn the_inline_edit_replaces_the_label(cx: &mut gpui::TestAppContext) {
        let (_, window) = open_row(cx, |harness| harness.rename = true);
        let row = window
            .debug_bounds("row-under-test")
            .expect("the row is laid out");

        let field = window
            .debug_bounds("row-edit-slot")
            .expect("the inline edit is laid out in place of the label");
        assert!(field.left() >= row.left());
        assert!(field.right() <= row.right());

        assert!(
            window.debug_bounds("row-under-test-label").is_none(),
            "the label is gone while the row is being edited"
        );
    }
}
