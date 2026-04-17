use crate::ui::icons::AppIcon;
use crate::ui::tokens::FontSizes;
use dbflux_components::primitives::Text;
use gpui::prelude::*;
use gpui::*;
use gpui_component::list::ListItem;
use gpui_component::ActiveTheme;

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
                    .child(
                        div()
                            .text_lg()
                            .font_weight(FontWeight::SEMIBOLD)
                            .child("New Connection"),
                    ),
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

                            div()
                                .rounded(px(6.0))
                                .border_2()
                                .when(is_focused, |d| d.border_color(ring_color))
                                .when(!is_focused, |d| d.border_color(gpui::transparent_black()))
                                .child(
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
                                                    svg()
                                                        .path(AppIcon::from_icon(icon).path())
                                                        .size_8()
                                                        .text_color(theme.foreground),
                                                )
                                                .child(
                                                    div()
                                                        .flex()
                                                        .flex_col()
                                                        .gap_1()
                                                        .child(
                                                            div()
                                                                .text_sm()
                                                                .font_weight(FontWeight::SEMIBOLD)
                                                                .child(driver_info.name),
                                                        )
                                                        .child(
                                                            Text::muted(driver_info.description)
                                                                .font_size(FontSizes::XS),
                                                        ),
                                                ),
                                        ),
                                )
                        })),
                ),
            )
            .child(
                div()
                    .p_3()
                    .border_t_1()
                    .border_color(theme.border)
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child("j/k Navigate  h/l Horizontal  Enter Select  Esc Close"),
            )
    }
}
