use crate::ui::icons::AppIcon;
use crate::ui::tokens::FontSizes;
use dbflux_components::primitives::focus_frame;
use dbflux_components::primitives::{Icon, Text};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::list::ListItem;

use super::ConnectionManagerWindow;

impl ConnectionManagerWindow {
    pub(super) fn render_driver_select(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let drivers = self.available_drivers.clone();
        let focused_idx = self.driver_focus.index();
        let ring_color = theme.ring;

        div()
            .flex()
            .flex_col()
            .size_full()
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .p_3()
                    .border_b_1()
                    .border_color(theme.border)
                    .child(Text::heading("New Connection").font_size(FontSizes::LG)),
            )
            .child(
                div().flex_1().p_3().child(
                    div()
                        .flex()
                        .flex_col()
                        .gap_1()
                        .child(div().mb_2().child(Text::muted(
                            "Select database type (j/k to navigate, Enter to select)",
                        )))
                        .children(drivers.into_iter().enumerate().map(|(idx, driver_info)| {
                            let driver_id = driver_info.id.clone();
                            let icon = driver_info.icon;
                            let is_focused = idx == focused_idx;

                            focus_frame(
                                is_focused,
                                Some(ring_color),
                                ListItem::new(("driver", idx))
                                    .py(px(8.0))
                                    .on_click(cx.listener(move |this, _, window, cx| {
                                        this.select_driver(&driver_id, window, cx);
                                    }))
                                    .child(
                                        div()
                                            .flex()
                                            .flex_row()
                                            .items_center()
                                            .gap_3()
                                            .child(
                                                Icon::new(AppIcon::from_icon(icon))
                                                    .size(px(32.0))
                                                    .color(theme.foreground),
                                            )
                                            .child(
                                                div()
                                                    .flex()
                                                    .flex_col()
                                                    .gap_1()
                                                    .child(
                                                        Text::heading(driver_info.name)
                                                            .font_size(FontSizes::SM),
                                                    )
                                                    .child(
                                                        Text::muted(driver_info.description)
                                                            .font_size(FontSizes::XS),
                                                    ),
                                            ),
                                    ),
                                cx,
                            )
                        })),
                ),
            )
            .child(
                div().p_3().border_t_1().border_color(theme.border).child(
                    Text::muted("j/k Navigate  h/l Horizontal  Enter Select  Esc Close")
                        .font_size(FontSizes::XS),
                ),
            )
    }
}
