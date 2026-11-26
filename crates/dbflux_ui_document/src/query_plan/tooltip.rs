//! The explanatory tooltip of the plan views: a bold title over a muted
//! sentence, built on the app's tooltip component so it follows the same
//! hover delay and placement as every other tooltip. It opens on hover only;
//! nothing here opens a tooltip because a view appeared or a node took focus.

use dbflux_components::tokens::{Spacing, ui};
use gpui::prelude::*;
use gpui::{AnyView, App, FontWeight, SharedString, Window, div};
use gpui_component::ActiveTheme;
use gpui_component::tooltip::Tooltip;

/// Widest a tooltip grows before its sentence wraps. (300 px)
const MAX_WIDTH: gpui::Rems = ui(300.0);

/// A tooltip with a title and, when there is one, a sentence under it.
pub(super) fn explain(
    title: impl Into<SharedString>,
    body: Option<SharedString>,
    window: &mut Window,
    cx: &mut App,
) -> AnyView {
    let title: SharedString = title.into();

    Tooltip::element(move |_window, cx| {
        let muted = cx.theme().muted_foreground;
        div()
            .flex()
            .flex_col()
            .gap(Spacing::XS)
            .max_w(MAX_WIDTH)
            .py(Spacing::XS)
            .child(div().font_weight(FontWeight::BOLD).child(title.clone()))
            .when_some(body.clone(), |column, body| {
                column.child(div().text_color(muted).child(body))
            })
    })
    .build(window, cx)
}

/// The builder `.tooltip(..)` takes, for a title and an optional sentence.
pub(super) fn tip(
    title: impl Into<SharedString>,
    body: Option<SharedString>,
) -> impl Fn(&mut Window, &mut App) -> AnyView + 'static {
    let title: SharedString = title.into();
    move |window, cx| explain(title.clone(), body.clone(), window, cx)
}
