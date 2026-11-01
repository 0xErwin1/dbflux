//! The activity rail: the column of view buttons at the left edge of the
//! main window, directly on the desk.
//!
//! The host builds the entry list from its own state; the rail never knows
//! which view an entry opens. Exactly one entry is the active one — the view
//! on screen — and it takes the tint wash; the others are transparent with
//! the ghost hover and press fills. Entries sit stacked from the top, with
//! any bottom-placed entries pinned to the bottom edge of the rail.

use std::rc::Rc;

use gpui::prelude::*;
use gpui::{App, ElementId, Hsla, SharedString, Window, div};
use gpui_component::tooltip::Tooltip;
use gpui_component::{ActiveTheme, theme::Theme};

use crate::icon::IconSource;
use crate::primitives::{Chamfer, Icon};
use crate::tokens::{ButtonMetrics, ChamferCut, ChromeColors, ShellMetrics};

/// Where an entry sits in the rail: stacked from the top, or pinned to the
/// bottom edge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RailPlacement {
    Top,
    Bottom,
}

/// One button of the rail.
#[derive(Clone, Debug)]
pub struct RailEntry {
    /// Stable identifier, passed back to the selection handler and used as
    /// the element id (`rail-<id>`).
    pub id: SharedString,
    pub icon: IconSource,
    /// Tooltip and accessible name.
    pub label: SharedString,
    /// The view the entry opens is the one on screen.
    pub active: bool,
    pub placement: RailPlacement,
}

impl RailEntry {
    pub fn new(
        id: impl Into<SharedString>,
        icon: impl Into<IconSource>,
        label: impl Into<SharedString>,
    ) -> Self {
        Self {
            id: id.into(),
            icon: icon.into(),
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

/// Fills and icon color of a rail button: the active entry takes a tint wash
/// and a tint icon, the others are transparent with a muted icon and the
/// ghost hover and press fills.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RailButtonColors {
    pub rest: Hsla,
    pub hover: Hsla,
    pub pressed: Hsla,
    pub icon: Hsla,
}

pub fn rail_button_colors(theme: &Theme, active: bool) -> RailButtonColors {
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

/// The activity rail: `ShellMetrics::RAIL_WIDTH` wide directly on the desk,
/// with no fill or line of its own, `ShellMetrics::RAIL_BUTTON` buttons
/// stacked from the top and the bottom entries pinned to the bottom edge.
#[derive(IntoElement)]
pub struct Rail {
    id: ElementId,
    entries: Vec<RailEntry>,
    on_select: Option<RailSelectHandler>,
}

impl Rail {
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
            .id(ElementId::Name(element_id.clone()))
            .debug_selector(move || element_id.to_string())
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
                Icon::new(entry.icon.clone())
                    .size(ShellMetrics::RAIL_ICON)
                    .color(colors.icon),
            )
            .tooltip(move |window, cx| Tooltip::new(label.clone()).build(window, cx))
            .when_some(on_select, |button, handler| {
                button.on_click(move |_, window, cx| handler(&entry_id, window, cx))
            })
    }
}

impl RenderOnce for Rail {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let (top, bottom) = Self::partition(&self.entries);

        let top_buttons: Vec<_> = top
            .into_iter()
            .map(|entry| Self::render_button(entry, self.on_select.clone(), cx))
            .collect();
        let bottom_buttons: Vec<_> = bottom
            .into_iter()
            .map(|entry| Self::render_button(entry, self.on_select.clone(), cx))
            .collect();

        let mut rail = div().id(self.id.clone());
        if let ElementId::Name(name) = &self.id {
            let selector = name.to_string();
            rail = rail.debug_selector(move || selector);
        }

        rail.flex()
            .flex_col()
            .flex_shrink_0()
            .items_center()
            .gap(ShellMetrics::RAIL_GAP)
            .w(ShellMetrics::RAIL_WIDTH)
            .h_full()
            .pt(ShellMetrics::RAIL_PADDING_Y)
            .pb(ShellMetrics::RAIL_PADDING_Y)
            .children(top_buttons)
            .child(div().flex_1())
            .children(bottom_buttons)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokens::BASE_REM;
    use gpui::px;

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

    fn entries() -> Vec<RailEntry> {
        vec![
            RailEntry::new("first", gpui_component::IconName::Folder, "First").active(true),
            RailEntry::new("second", gpui_component::IconName::Search, "Second"),
            RailEntry::new("settings", gpui_component::IconName::Settings, "Settings").bottom(),
        ]
    }

    /// A host view that lists the same rail every frame, the way a product
    /// rebuilds its entries from its own state.
    struct RailHost {
        entries: Vec<RailEntry>,
        selected: std::rc::Rc<std::cell::RefCell<Option<String>>>,
    }

    impl Render for RailHost {
        fn render(
            &mut self,
            _window: &mut Window,
            _cx: &mut gpui::Context<Self>,
        ) -> impl IntoElement {
            let selected = self.selected.clone();
            Rail::new("rail", self.entries.clone())
                .on_select(move |id, _, _| {
                    *selected.borrow_mut() = Some(id.to_string());
                })
                .into_element()
        }
    }

    #[test]
    fn partition_keeps_order_within_each_edge() {
        let entries = entries();
        let (top, bottom) = Rail::partition(&entries);

        let top_ids: Vec<&str> = top.iter().map(|entry| entry.id.as_ref()).collect();
        let bottom_ids: Vec<&str> = bottom.iter().map(|entry| entry.id.as_ref()).collect();

        assert_eq!(top_ids, ["first", "second"]);
        assert_eq!(bottom_ids, ["settings"]);
    }

    #[test]
    fn builders_set_state_and_placement() {
        let entry = RailEntry::new("audit", gpui_component::IconName::Bell, "Audit");
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
        assert_eq!(active.hover, tint.opacity(ButtonMetrics::SOFT_FILL_HOVER));
        assert_eq!(
            active.pressed,
            tint.opacity(ButtonMetrics::SOFT_FILL_PRESSED)
        );
        assert_eq!(active.icon, tint);

        let idle = rail_button_colors(&theme, false);
        assert_eq!(idle.rest, gpui::transparent_black());
        assert_eq!(idle.hover, theme.secondary);
        assert_eq!(idle.pressed, theme.secondary_hover);
        assert_eq!(idle.icon, theme.muted_foreground);
    }

    #[gpui::test]
    fn rail_lays_out_square_buttons_at_the_token_metrics(cx: &mut gpui::TestAppContext) {
        install_test_theme(cx);

        let (_, window) = cx.add_window_view(|window, cx| {
            let host = cx.new(|_| RailHost {
                entries: entries(),
                selected: std::rc::Rc::default(),
            });
            gpui_component::Root::new(host, window, cx)
        });
        window.run_until_parked();

        let button_size = ShellMetrics::RAIL_BUTTON.to_pixels(px(BASE_REM));
        let rail_width = ShellMetrics::RAIL_WIDTH.to_pixels(px(BASE_REM));

        let first = window
            .debug_bounds("rail-first")
            .expect("the first entry is laid out");
        assert_eq!(first.size.width, button_size);
        assert_eq!(first.size.height, button_size);

        let rail = window.debug_bounds("rail").expect("the rail is laid out");
        assert_eq!(rail.size.width, rail_width);

        // The rail hugs its buttons: a button centers inside the rail width.
        let centered = (rail.size.width - button_size) / 2.0;
        assert_eq!(first.left() - rail.left(), centered);
    }

    #[gpui::test]
    fn bottom_entries_pinned_to_the_bottom_edge(cx: &mut gpui::TestAppContext) {
        install_test_theme(cx);

        let (_, window) = cx.add_window_view(|window, cx| {
            let host = cx.new(|_| RailHost {
                entries: entries(),
                selected: std::rc::Rc::default(),
            });
            gpui_component::Root::new(host, window, cx)
        });
        window.run_until_parked();

        let first = window
            .debug_bounds("rail-first")
            .expect("the first entry is laid out");
        let settings = window
            .debug_bounds("rail-settings")
            .expect("the bottom entry is laid out");
        let rail = window.debug_bounds("rail").expect("the rail is laid out");

        assert!(
            settings.top() > first.bottom(),
            "the bottom entry renders below the top entries"
        );
        assert_eq!(
            settings.bottom(),
            rail.bottom() - ShellMetrics::RAIL_PADDING_Y,
            "the bottom entry is pinned to the rail's bottom padding edge"
        );
    }

    #[gpui::test]
    fn clicking_an_entry_selects_it(cx: &mut gpui::TestAppContext) {
        install_test_theme(cx);

        let selected = std::rc::Rc::new(std::cell::RefCell::new(None::<String>));
        let selected_for_window = selected.clone();
        let (_, window) = cx.add_window_view(move |window, cx| {
            let host = cx.new(|_| RailHost {
                entries: entries(),
                selected: selected_for_window,
            });
            gpui_component::Root::new(host, window, cx)
        });
        window.run_until_parked();

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
