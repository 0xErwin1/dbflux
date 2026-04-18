use super::{SettingsSection, SettingsSectionId, layout};
use crate::ui::icons::AppIcon;
use crate::ui::tokens::FontSizes;
use dbflux_components::primitives::{Icon, Text};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::scroll::ScrollableElement;

pub(super) struct AboutSection;

impl AboutSection {
    pub(super) fn new(_cx: &mut Context<Self>) -> Self {
        Self
    }
}

impl SettingsSection for AboutSection {
    fn section_id(&self) -> SettingsSectionId {
        SettingsSectionId::About
    }
}

impl Render for AboutSection {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        const VERSION: &str = env!("CARGO_PKG_VERSION");
        const REPOSITORY: &str = env!("CARGO_PKG_REPOSITORY");
        const AUTHORS: &str = env!("CARGO_PKG_AUTHORS");
        const LICENSE: &str = env!("CARGO_PKG_LICENSE");

        #[cfg(debug_assertions)]
        const PROFILE: &str = "debug";
        #[cfg(not(debug_assertions))]
        const PROFILE: &str = "release";

        let issues_url = format!("{}/issues", REPOSITORY);
        let author_name = AUTHORS.split('<').next().unwrap_or(AUTHORS).trim();
        let license_display = LICENSE.replace(" OR ", " and ");

        div()
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(layout::section_header(
                "About",
                "Project information",
                theme,
            ))
            .child(
                div().flex_1().min_h_0().overflow_y_scrollbar().p_6().child(
                    div()
                        .flex()
                        .flex_col()
                        .gap_3()
                        .child(
                            div()
                                .flex()
                                .items_center()
                                .gap_3()
                                .child(Icon::new(AppIcon::DbFlux).size(px(65.0)).primary())
                                .child(
                                    div()
                                        .flex()
                                        .flex_col()
                                        .gap_1()
                                        .child(Text::title("DBFlux").font_size(FontSizes::XL))
                                        .child(Text::caption(format!("{} ({})", VERSION, PROFILE))),
                                ),
                        )
                        .child(
                            div().text_sm().child(
                                div()
                                    .flex()
                                    .gap_1()
                                    .child(
                                        div()
                                            .id("about-link-issues")
                                            .cursor_pointer()
                                            .hover(|d| d.underline())
                                            .on_mouse_down(MouseButton::Left, move |_, _, cx| {
                                                cx.open_url(&issues_url);
                                            })
                                            .child(Text::body("Report a bug").link()),
                                    )
                                    .child("or")
                                    .child(
                                        div()
                                            .id("about-link-repo")
                                            .cursor_pointer()
                                            .hover(|d| d.underline())
                                            .on_mouse_down(MouseButton::Left, |_, _, cx| {
                                                cx.open_url(REPOSITORY);
                                            })
                                            .child(Text::body("view the source code").link()),
                                    )
                                    .child("on GitHub."),
                            ),
                        )
                        .child(Text::caption(format!(
                            "Copyright © 2026 {} and contributors.",
                            author_name
                        )))
                        .child(Text::caption(format!(
                            "Licensed under the {} licenses.",
                            license_display
                        )))
                        .child(
                            div()
                                .mt_4()
                                .pt_4()
                                .border_t_1()
                                .border_color(theme.border)
                                .flex()
                                .flex_col()
                                .gap_2()
                                .child(
                                    Text::heading("Third-Party Licenses")
                                        .font_size(FontSizes::BASE),
                                )
                                .child(Text::caption("UI icons from Lucide (ISC License)"))
                                .child(Text::caption("Brand icons from Simple Icons (CC0 1.0)")),
                        ),
                ),
            )
    }
}
