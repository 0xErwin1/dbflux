//! `TreeNavRow` — one row of a navigation tree: indent guides, chevron,
//! icon, leading mark, name, count and an optional trailing element.

use gpui::prelude::*;
use gpui::{
    AnyElement, App, ClickEvent, Div, ElementId, Hsla, Role, SharedString, Stateful, Window, div,
};
use gpui_component::ActiveTheme;

use crate::composites::ListRow;
use crate::icon::IconSource;
use crate::primitives::{BadgeTone, Icon};
use crate::selector::id_selector;
use crate::tokens::{Borders, ChromeColors, TreeMetrics};

type ToggleHandler = Box<dyn Fn(&ClickEvent, &mut Window, &mut App)>;

/// The expand chevron of a [`TreeNavRow`]: the icon for the current state,
/// the name the chevron announces (the caller's words, `"Collapse Users"`)
/// and the handler a click on it runs instead of the row's.
pub struct TreeNavChevron {
    expanded: bool,
    icon: IconSource,
    label: SharedString,
    on_toggle: ToggleHandler,
}

impl TreeNavChevron {
    pub fn new(
        expanded: bool,
        icon: impl Into<IconSource>,
        label: impl Into<SharedString>,
        on_toggle: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
    ) -> Self {
        Self {
            expanded,
            icon: icon.into(),
            label: label.into(),
            on_toggle: Box::new(on_toggle),
        }
    }
}

/// One row of a navigation tree on [`ListRow`]: 26 px tall, indented by
/// 14 px per depth level with a 1 px guide per ancestor, the 2 px tint bar
/// and wash while selected.
///
/// From the start: the chevron (or, with [`TreeNavRow::chevron_spacer`],
/// its empty column so leaves line up with their siblings), the icon, the
/// leading mark (a short kind label), the name, the muted mono count and
/// the trailing element. The name takes the free width and truncates.
///
/// The row is a label node under its id, named by its label, announcing
/// its selected state and, when it has a chevron, its expanded state. The
/// parts carry `<id>-chevron`, `<id>-icon`, `<id>-name`, `<id>-count` and
/// `<id>-trailing`. A click on the chevron runs its toggle and a click on
/// the trailing slot stops at the slot: neither reaches the handler the
/// caller wires on the row.
///
/// ```ignore
/// TreeNavRow::new("tree-row-7", "Create user", depth)
///     .selected(selected)
///     .leading(method_mark)
///     .count("3")
///     .trailing(Badge::new("201", BadgeTone::Success).id("tree-status-7"))
///     .build(cx)
///     .on_click(open_handler)
/// ```
pub struct TreeNavRow {
    id: SharedString,
    label: SharedString,
    depth: usize,
    selected: bool,
    focused: bool,
    emphasized: bool,
    chevron: Option<TreeNavChevron>,
    chevron_spacer: bool,
    icon: Option<(IconSource, BadgeTone)>,
    leading: Option<AnyElement>,
    name: Option<AnyElement>,
    count: Option<SharedString>,
    trailing: Option<AnyElement>,
}

impl TreeNavRow {
    /// A row under `id` named `label`, at `depth` (0 for a root).
    pub fn new(id: impl Into<SharedString>, label: impl Into<SharedString>, depth: usize) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            depth,
            selected: false,
            focused: false,
            emphasized: false,
            chevron: None,
            chevron_spacer: false,
            icon: None,
            leading: None,
            name: None,
            count: None,
            trailing: None,
        }
    }

    pub fn selected(mut self, selected: bool) -> Self {
        self.selected = selected;
        self
    }

    /// The keyboard cursor of the tree, shown while focus is visible.
    pub fn focused(mut self, focused: bool) -> Self {
        self.focused = focused;
        self
    }

    /// Draws the name in the strong color while unselected, for a row that
    /// heads its subtree (a root, an open group).
    pub fn emphasized(mut self, emphasized: bool) -> Self {
        self.emphasized = emphasized;
        self
    }

    pub fn chevron(mut self, chevron: TreeNavChevron) -> Self {
        self.chevron = Some(chevron);
        self
    }

    /// Keeps the chevron's column empty on a row without one.
    pub fn chevron_spacer(mut self, spacer: bool) -> Self {
        self.chevron_spacer = spacer;
        self
    }

    /// The node icon in the color of a badge in `tone`.
    pub fn icon(mut self, icon: impl Into<IconSource>, tone: BadgeTone) -> Self {
        self.icon = Some((icon.into(), tone));
        self
    }

    /// An element between the icon and the name.
    pub fn leading(mut self, element: impl IntoElement) -> Self {
        self.leading = Some(element.into_any_element());
        self
    }

    /// An element in place of the drawn name (an inline rename field).
    pub fn name(mut self, element: impl IntoElement) -> Self {
        self.name = Some(element.into_any_element());
        self
    }

    /// The muted count after the name.
    pub fn count(mut self, count: impl Into<SharedString>) -> Self {
        self.count = Some(count.into());
        self
    }

    /// Draws `element` at the row's end. The element carries its own id, so
    /// tests and automation find and click it; a click on it never reaches
    /// the row.
    pub fn trailing(mut self, element: impl IntoElement) -> Self {
        self.trailing = Some(element.into_any_element());
        self
    }

    pub fn build(self, cx: &App) -> Stateful<Div> {
        let theme = cx.theme().clone();
        let id = self.id.clone();
        let part = |suffix: &str| part(&id, suffix);

        let mut row = self.frame(&theme, cx);
        if let Some(chevron) = self.chevron {
            row = row.aria_expanded(chevron.expanded).child(chevron_part(
                part("chevron"),
                chevron,
                theme.muted_foreground,
            ));
        } else if self.chevron_spacer {
            row = row.child(div().w(TreeMetrics::CHEVRON).flex_none());
        }

        let name_color = if self.selected || self.emphasized {
            ChromeColors::strong(&theme)
        } else {
            theme.foreground
        };
        let name = self
            .name
            .unwrap_or_else(|| name_part(part("name"), self.label, name_color));

        row.children(
            self.icon
                .map(|(icon, tone)| icon_part(part("icon"), icon, tone, &theme)),
        )
        .children(self.leading)
        .child(name)
        .children(
            self.count
                .map(|count| count_part(part("count"), count, theme.muted_foreground, cx)),
        )
        .children(
            self.trailing
                .map(|trailing| trailing_part(part("trailing"), trailing)),
        )
    }

    /// The row itself: the list row, its name and states, the indent and
    /// one guide per ancestor.
    fn frame(&self, theme: &gpui_component::Theme, cx: &App) -> Stateful<Div> {
        let indent = crate::fonts::ui_px(cx, TreeMetrics::INDENT);
        let guide = crate::fonts::ui_px(cx, TreeMetrics::GUIDE_OFFSET);

        ListRow::new(ElementId::Name(self.id.clone()))
            .selected(self.selected)
            .focused(self.focused)
            .selection_bar(true)
            .build(cx)
            .role(Role::Label)
            .aria_label(self.label.clone())
            .aria_selected(self.selected)
            .h(TreeMetrics::ROW_HEIGHT)
            .flex_none()
            .flex()
            .items_center()
            .gap(TreeMetrics::GAP)
            .pl(TreeMetrics::PADDING_X + indent * self.depth as f32)
            .pr(TreeMetrics::PADDING_X)
            .text_size(TreeMetrics::FONT)
            .whitespace_nowrap()
            .children((0..self.depth).map(|level| {
                div()
                    .absolute()
                    .top_0()
                    .bottom_0()
                    .left(guide + indent * level as f32)
                    .w(Borders::THIN)
                    .bg(theme.border)
            }))
    }
}

/// A part of the row under `<id>-<suffix>`.
fn part(id: &SharedString, suffix: &str) -> Stateful<Div> {
    let part_id = ElementId::Name(format!("{id}-{suffix}").into());
    div().debug_selector(id_selector(&part_id)).id(part_id)
}

fn chevron_part(part: Stateful<Div>, chevron: TreeNavChevron, color: Hsla) -> Stateful<Div> {
    let on_toggle = chevron.on_toggle;

    part.role(Role::Button)
        .aria_label(chevron.label)
        .flex_none()
        .cursor_pointer()
        .on_click(move |event, window, cx| {
            cx.stop_propagation();
            on_toggle(event, window, cx);
        })
        .child(
            Icon::new(chevron.icon)
                .size(TreeMetrics::CHEVRON)
                .color(color),
        )
}

fn icon_part(
    part: Stateful<Div>,
    icon: IconSource,
    tone: BadgeTone,
    theme: &gpui_component::Theme,
) -> Stateful<Div> {
    part.flex_none().child(
        Icon::new(icon)
            .size(TreeMetrics::ICON)
            .color(tone.text_color(theme)),
    )
}

fn name_part(part: Stateful<Div>, label: SharedString, color: Hsla) -> AnyElement {
    part.flex_1()
        .min_w_0()
        .overflow_hidden()
        .text_ellipsis()
        .text_color(color)
        .child(label)
        .into_any_element()
}

fn count_part(part: Stateful<Div>, count: SharedString, color: Hsla, cx: &App) -> Stateful<Div> {
    part.flex_none()
        .font_family(crate::fonts::editor_family(cx))
        .text_size(TreeMetrics::META_FONT)
        .text_color(color)
        .child(count)
}

fn trailing_part(part: Stateful<Div>, trailing: AnyElement) -> Stateful<Div> {
    // The slot catches its own clicks, so a trailing control never selects
    // or opens the row.
    part.flex_none()
        .on_click(|_, _, cx| cx.stop_propagation())
        .child(trailing)
}

#[cfg(test)]
mod tests;
