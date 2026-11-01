//! `ActivityRail` — the column of view buttons at the left edge of the main
//! window (AppByzTable, DSAppPlan "ActivityRail").
//!
//! The rendering is Bolt's `shell::Rail`, the single implementation of this
//! surface. This adapter keeps the product-facing entry type (its `AppIcon`
//! icons) and publishes the `ActivityRail` key context, so the workspace
//! call sites are untouched while the fills, metrics and layout come from
//! the one rail implementation.

use std::rc::Rc;

use gpui::prelude::*;
use gpui::{App, ElementId, SharedString, Window};

use crate::icons::AppIcon;
use crate::key_contexts::ACTIVITY_RAIL;

pub use bolt::shell::{RailButtonColors, RailPlacement, rail_button_colors};

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
    pub placement: RailPlacement,
}

impl RailEntry {
    pub fn new(id: impl Into<SharedString>, icon: AppIcon, label: impl Into<SharedString>) -> Self {
        Self {
            id: id.into(),
            icon,
            label: label.into(),
            active: false,
            placement: RailPlacement::Top,
        }
    }

    pub fn active(mut self, active: bool) -> Self {
        self.active = active;
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

type RailSelectHandler = Rc<dyn Fn(&SharedString, &mut Window, &mut App)>;

/// The activity rail, rendered by Bolt's `shell::Rail`.
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
}

impl RenderOnce for ActivityRail {
    fn render(self, _window: &mut Window, _cx: &mut App) -> impl IntoElement {
        let entries = self
            .entries
            .into_iter()
            .map(|entry| {
                let mut mapped = bolt::shell::RailEntry::new(entry.id, entry.icon, entry.label)
                    .active(entry.active);
                if entry.placement == RailPlacement::Bottom {
                    mapped = mapped.bottom();
                }
                mapped
            })
            .collect();

        let mut rail = bolt::shell::Rail::new(self.id, entries);
        if let Some(handler) = self.on_select {
            rail = rail.on_select(move |id, window, cx| handler(id, window, cx));
        }

        rail.key_context(ACTIVITY_RAIL)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokens::{ChromeColors, ShellMetrics};
    use gpui::div;

    fn entries() -> Vec<RailEntry> {
        vec![
            RailEntry::new("connections", AppIcon::Database, "Connections").active(true),
            RailEntry::new("settings", AppIcon::Settings, "Settings").bottom(),
            RailEntry::new("scripts", AppIcon::SquareTerminal, "Scripts"),
            RailEntry::new("approvals", AppIcon::Bot, "Approvals"),
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
        assert_eq!(entry.placement, RailPlacement::Top);
        assert_eq!(entry.element_id().as_ref(), "rail-audit");

        let entry = entry.active(true).bottom();
        assert!(entry.active);
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

    /// Renders the rail as the workspace does and pins the token-derived
    /// layout: `ShellMetrics::RAIL_BUTTON` square buttons inside a
    /// `ShellMetrics::RAIL_WIDTH` rail, the bottom entry pinned to the
    /// bottom padding edge, `rail-<id>` element ids, and the click handler
    /// receiving the entry id. This is the contract the rendering must
    /// reproduce whatever implementation sits behind `ActivityRail`.
    #[gpui::test]
    fn rail_lays_out_at_the_shell_tokens_and_selects(cx: &mut gpui::TestAppContext) {
        use crate::tokens::BASE_REM;
        use gpui::prelude::*;
        use gpui::{Render, px};

        struct RailHost {
            selected: std::rc::Rc<std::cell::RefCell<Option<String>>>,
        }

        impl Render for RailHost {
            fn render(
                &mut self,
                _window: &mut gpui::Window,
                _cx: &mut gpui::Context<Self>,
            ) -> impl gpui::IntoElement {
                let selected = self.selected.clone();
                div()
                    .size_full()
                    .flex()
                    .flex_col()
                    .child(
                        ActivityRail::new(
                            "activity-rail",
                            vec![
                                RailEntry::new("first", AppIcon::Database, "First").active(true),
                                RailEntry::new("second", AppIcon::Settings, "Second"),
                                RailEntry::new("settings", AppIcon::Search, "Settings").bottom(),
                            ],
                        )
                        .on_select(move |id, _, _| {
                            selected.borrow_mut().replace(id.to_string());
                        }),
                    )
                    .child(div().flex_1())
            }
        }

        cx.update(crate::theme::init);

        let selected = std::rc::Rc::new(std::cell::RefCell::new(None::<String>));
        let selected_for_host = selected.clone();
        let (_, window) = cx.add_window_view(move |window, cx| {
            let host = cx.new(|_| RailHost {
                selected: selected_for_host,
            });
            gpui_component::Root::new(host, window, cx)
        });
        window.run_until_parked();

        let button_size = ShellMetrics::RAIL_BUTTON.to_pixels(px(BASE_REM));
        let rail_width = ShellMetrics::RAIL_WIDTH.to_pixels(px(BASE_REM));

        let rail = window
            .debug_bounds("activity-rail")
            .expect("the rail is laid out");
        assert_eq!(rail.size.width, rail_width);

        for id in ["first", "second", "settings"] {
            let button = window
                .debug_bounds(format!("rail-{id}").leak())
                .unwrap_or_else(|| panic!("the {id} entry is laid out"));
            assert_eq!(button.size.width, button_size);
            assert_eq!(button.size.height, button_size);
        }

        let first = window.debug_bounds("rail-first").unwrap();
        let settings = window.debug_bounds("rail-settings").unwrap();
        assert!(settings.top() > first.bottom());
        assert_eq!(
            settings.bottom(),
            rail.bottom() - ShellMetrics::RAIL_PADDING_Y,
            "the bottom entry is pinned to the rail's bottom padding edge"
        );

        let second = window
            .debug_bounds("rail-second")
            .expect("the second entry is laid out")
            .center();
        window.simulate_click(second, gpui::Modifiers::default());
        window.run_until_parked();

        assert_eq!(
            selected.borrow().as_deref(),
            Some("second"),
            "the click reaches the handler with the entry id"
        );
    }
}
