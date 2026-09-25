use gpui::prelude::*;
use gpui::{AnyElement, App, ElementId, Window, div};
use gpui_component::ActiveTheme;

use crate::controls::{Button, button_colors};
use crate::primitives::{Chamfer, ChamferCorners, ChamferRing};
use crate::tokens::ButtonMetrics;

/// A main action and its menu, joined by a 1 px seam (DSApp "Split").
///
/// The main action is a regular [`Button`] with only its top-left corner cut;
/// the menu segment takes the same variant fill with only its bottom-right
/// corner cut, and hosts the element that opens the menu (for example a
/// compact `Dropdown` trigger). Used for Refresh, Export and Run.
#[derive(IntoElement)]
pub struct SplitButton {
    id: ElementId,
    main: Button,
    menu: AnyElement,
    menu_focused: bool,
}

impl SplitButton {
    /// `main` keeps its own id, variant, size, and click handler; `menu` is
    /// laid out inside the menu segment at full height.
    pub fn new(id: impl Into<ElementId>, main: Button, menu: impl IntoElement) -> Self {
        Self {
            id: id.into(),
            main,
            menu: menu.into_any_element(),
            menu_focused: false,
        }
    }

    /// Shows the focus ring around the menu segment, for a caller that tracks
    /// keyboard focus itself.
    pub fn menu_focused(mut self, focused: bool) -> Self {
        self.menu_focused = focused;
        self
    }
}

impl RenderOnce for SplitButton {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let variant = self.main.current_variant();
        let size = self.main.current_size();
        let disabled = self.main.is_disabled();
        let (fills, content) = button_colors(theme, variant, false);

        let mut menu_shape = Chamfer::new(size.cut())
            .corners(ChamferCorners::BottomRight)
            .fill(fills.rest);

        if self.menu_focused && !disabled {
            menu_shape = menu_shape.ring(ChamferRing::focus_for(theme.ring, variant.fill_kind()));
        }

        if !disabled {
            menu_shape = menu_shape
                .fill_hover(fills.hover)
                .fill_active(fills.pressed)
                .interactive("split-menu-chamfer");
        }

        let main = self
            .main
            .corners(ChamferCorners::TopLeft)
            .padding_left(ButtonMetrics::SPLIT_MAIN_PADDING_LEFT);

        let menu = div()
            .id("split-menu")
            .relative()
            .flex()
            .flex_shrink_0()
            .items_center()
            .justify_center()
            .w(ButtonMetrics::SPLIT_MENU_WIDTH)
            .h(size.height())
            .text_color(content)
            .child(menu_shape)
            .child(self.menu)
            .when(disabled, |menu| {
                menu.opacity(ButtonMetrics::DISABLED_OPACITY)
            });

        div()
            .id(self.id)
            .flex()
            .flex_shrink_0()
            .items_center()
            .gap(ButtonMetrics::SPLIT_SEAM)
            .child(main)
            .child(menu)
    }
}
