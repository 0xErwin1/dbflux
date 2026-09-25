//! `ActivityRail` — the column of view buttons at the left edge of the main
//! window (AppByzTable, DSAppPlan "ActivityRail").

use std::rc::Rc;

use gpui::prelude::*;
use gpui::{App, ElementId, Hsla, SharedString, Window, div};
use gpui_component::ActiveTheme;
use gpui_component::tooltip::Tooltip;

use crate::icons::AppIcon;
use crate::primitives::{Chamfer, Icon, Status, StatusIndicator};
use crate::tokens::{ButtonMetrics, ChamferCut, ChromeColors, ShellMetrics};

/// Where an entry sits in the rail: stacked from the top, or pinned to the
/// bottom edge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RailPlacement {
    Top,
    Bottom,
}

/// One button of the rail. The host builds the list from its own state; the
/// rail never knows which view an entry opens.
#[derive(Clone, Debug, PartialEq)]
pub struct RailEntry {
    /// Stable identifier, passed back to the selection handler and used as
    /// the element id (`rail-<id>`).
    pub id: SharedString,
    pub icon: AppIcon,
    /// Tooltip and accessible name.
    pub label: SharedString,
    /// The view the entry opens is the one on screen.
    pub active: bool,
    /// Draws the tint diamond in the top-right corner (work waiting).
    pub pending: bool,
    pub placement: RailPlacement,
}

impl RailEntry {
    pub fn new(id: impl Into<SharedString>, icon: AppIcon, label: impl Into<SharedString>) -> Self {
        Self {
            id: id.into(),
            icon,
            label: label.into(),
            active: false,
            pending: false,
            placement: RailPlacement::Top,
        }
    }

    pub fn active(mut self, active: bool) -> Self {
        self.active = active;
        self
    }

    pub fn pending(mut self, pending: bool) -> Self {
        self.pending = pending;
        self
    }

    pub fn bottom(mut self) -> Self {
        self.placement = RailPlacement::Bottom;
        self
    }

    /// Element id of the entry's button.
    pub fn element_id(&self) -> SharedString {
        format!("rail-{}", self.id).into()
    }
}

/// Fills and icon color of a rail button: the active entry takes a tint wash
/// and a tint icon, the others are transparent with a muted icon and the
/// ghost hover and press fills (DSStates).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RailButtonColors {
    pub rest: Hsla,
    pub hover: Hsla,
    pub pressed: Hsla,
    pub icon: Hsla,
}

pub fn rail_button_colors(theme: &gpui_component::Theme, active: bool) -> RailButtonColors {
    if active {
        let tint = ChromeColors::tint(theme);

        return RailButtonColors {
            rest: tint.opacity(ShellMetrics::RAIL_ACTIVE_ALPHA),
            hover: tint.opacity(ButtonMetrics::SOFT_FILL_HOVER),
            pressed: tint.opacity(ButtonMetrics::SOFT_FILL_PRESSED),
            icon: tint,
        };
    }

    RailButtonColors {
        rest: gpui::transparent_black(),
        hover: theme.secondary,
        pressed: theme.secondary_hover,
        icon: theme.muted_foreground,
    }
}

type RailSelectHandler = Rc<dyn Fn(&SharedString, &mut Window, &mut App)>;

/// The activity rail: `ShellMetrics::RAIL_WIDTH` wide, the window ground with
/// a line on its right, 38 px buttons stacked from the top and the bottom
/// entries pinned to the bottom edge.
#[derive(IntoElement)]
pub struct ActivityRail {
    id: ElementId,
    entries: Vec<RailEntry>,
    on_select: Option<RailSelectHandler>,
}

impl ActivityRail {
    pub fn new(id: impl Into<ElementId>, entries: Vec<RailEntry>) -> Self {
        Self {
            id: id.into(),
            entries,
            on_select: None,
        }
    }

    /// Called with the entry id when a button is clicked.
    pub fn on_select(
        mut self,
        handler: impl Fn(&SharedString, &mut Window, &mut App) + 'static,
    ) -> Self {
        self.on_select = Some(Rc::new(handler));
        self
    }

    /// Top entries in display order, then bottom entries in display order.
    pub fn partition(entries: &[RailEntry]) -> (Vec<&RailEntry>, Vec<&RailEntry>) {
        entries
            .iter()
            .partition(|entry| entry.placement == RailPlacement::Top)
    }

    fn render_button(
        entry: &RailEntry,
        on_select: Option<RailSelectHandler>,
        cx: &App,
    ) -> impl IntoElement {
        let colors = rail_button_colors(cx.theme(), entry.active);
        let element_id = entry.element_id();
        let entry_id = entry.id.clone();
        let label = entry.label.clone();

        div()
            .id(ElementId::Name(element_id))
            .aria_label(label.clone())
            .relative()
            .flex()
            .flex_shrink_0()
            .items_center()
            .justify_center()
            .size(ShellMetrics::RAIL_BUTTON)
            .cursor_pointer()
            .child(
                Chamfer::new(ChamferCut::CONTROL)
                    .fill(colors.rest)
                    .fill_hover(colors.hover)
                    .fill_active(colors.pressed)
                    .interactive("rail-button-chamfer"),
            )
            .child(
                Icon::new(entry.icon)
                    .size(ShellMetrics::RAIL_ICON)
                    .color(colors.icon),
            )
            .when(entry.pending, |button| {
                button.child(
                    div()
                        .absolute()
                        .top(ShellMetrics::RAIL_INDICATOR_TOP)
                        .right(ShellMetrics::RAIL_INDICATOR_RIGHT)
                        .child(StatusIndicator::new(Status::Busy)),
                )
            })
            .tooltip(move |window, cx| Tooltip::new(label.clone()).build(window, cx))
            .when_some(on_select, |button, handler| {
                button.on_click(move |_, window, cx| handler(&entry_id, window, cx))
            })
    }
}

impl RenderOnce for ActivityRail {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let (top, bottom) = Self::partition(&self.entries);

        let top_buttons: Vec<_> = top
            .into_iter()
            .map(|entry| Self::render_button(entry, self.on_select.clone(), cx))
            .collect();
        let bottom_buttons: Vec<_> = bottom
            .into_iter()
            .map(|entry| Self::render_button(entry, self.on_select.clone(), cx))
            .collect();

        div()
            .id(self.id)
            .flex()
            .flex_col()
            .flex_shrink_0()
            .items_center()
            .gap(ShellMetrics::RAIL_GAP)
            .w(ShellMetrics::RAIL_WIDTH)
            .h_full()
            .pt(ShellMetrics::RAIL_PADDING_Y)
            .pb(ShellMetrics::RAIL_PADDING_Y)
            .bg(theme.background)
            .border_r_1()
            .border_color(theme.border)
            .children(top_buttons)
            .child(div().flex_1())
            .children(bottom_buttons)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries() -> Vec<RailEntry> {
        vec![
            RailEntry::new("connections", AppIcon::Database, "Connections").active(true),
            RailEntry::new("settings", AppIcon::Settings, "Settings").bottom(),
            RailEntry::new("scripts", AppIcon::SquareTerminal, "Scripts"),
            RailEntry::new("approvals", AppIcon::Bot, "Approvals").pending(true),
        ]
    }

    #[test]
    fn partition_keeps_order_within_each_edge() {
        let entries = entries();
        let (top, bottom) = ActivityRail::partition(&entries);

        let top_ids: Vec<&str> = top.iter().map(|entry| entry.id.as_ref()).collect();
        let bottom_ids: Vec<&str> = bottom.iter().map(|entry| entry.id.as_ref()).collect();

        assert_eq!(top_ids, ["connections", "scripts", "approvals"]);
        assert_eq!(bottom_ids, ["settings"]);
    }

    #[test]
    fn builders_set_state_and_placement() {
        let entry = RailEntry::new("audit", AppIcon::FingerprintPattern, "Audit");
        assert!(!entry.active);
        assert!(!entry.pending);
        assert_eq!(entry.placement, RailPlacement::Top);
        assert_eq!(entry.element_id().as_ref(), "rail-audit");

        let entry = entry.active(true).pending(true).bottom();
        assert!(entry.active);
        assert!(entry.pending);
        assert_eq!(entry.placement, RailPlacement::Bottom);
    }

    #[test]
    fn active_button_takes_the_tint_wash_and_idle_stays_transparent() {
        let theme = gpui_component::Theme::default();
        let tint = ChromeColors::tint(&theme);

        let active = rail_button_colors(&theme, true);
        assert_eq!(active.rest, tint.opacity(ShellMetrics::RAIL_ACTIVE_ALPHA));
        assert_eq!(active.icon, tint);

        let idle = rail_button_colors(&theme, false);
        assert_eq!(idle.rest, gpui::transparent_black());
        assert_eq!(idle.hover, theme.secondary);
        assert_eq!(idle.icon, theme.muted_foreground);
    }
}
