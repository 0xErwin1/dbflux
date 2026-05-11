use crate::ui::icons::AppIcon;
use crate::ui::tokens::{BannerColors, FontSizes, Heights, Spacing};
use dbflux_components::icon::IconSource;
use dbflux_components::primitives::{IconButton, overlay_bg, surface_modal_container};
use dbflux_components::tokens::ChromeEdgeRole;
use gpui::prelude::*;
use gpui::{AnyElement, App, MouseButton, Pixels, SharedString, Window, div, px};
use gpui_component::ActiveTheme;
use gpui_component::scroll::ScrollableElement;

type CloseHandler = Box<dyn Fn(&mut Window, &mut App) + Send + Sync + 'static>;

/// Tone variant for `ModalShell`.
///
/// - `Default`: standard chrome with no accent border.
/// - `Danger`: red 2 px top-border signalling a destructive or critical action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModalVariant {
    Default,
    Danger,
}

/// Reusable modal shell providing:
/// - Scrim overlay (dimmed backdrop)
/// - Title bar with optional close button
/// - Scrollable body area (min-height 96 px, 16 px padding)
/// - Footer area (right-aligned, 12 px gap between items)
/// - Danger variant: 2 px red top-border accent
///
/// Use this as the chrome for any new modal. Pass pre-built `AnyElement`
/// values for `body` and `footer` to keep the component stateless.
///
/// S8 modals (e.g. drop-confirm, unsaved-changes) should use `ModalShell`
/// rather than implementing their own scrim/header/footer layout.
#[derive(IntoElement)]
pub struct ModalShell {
    title: SharedString,
    variant: ModalVariant,
    width: Pixels,
    body: AnyElement,
    footer: AnyElement,
    on_close: Option<CloseHandler>,
}

impl ModalShell {
    pub fn new(title: impl Into<SharedString>, body: AnyElement, footer: AnyElement) -> Self {
        Self {
            title: title.into(),
            variant: ModalVariant::Default,
            width: px(460.0),
            body,
            footer,
            on_close: None,
        }
    }

    /// Set the danger variant (red accent top-border).
    pub fn variant(mut self, v: ModalVariant) -> Self {
        self.variant = v;
        self
    }

    /// Override the modal width (default: 460 px).
    pub fn width(mut self, w: Pixels) -> Self {
        self.width = w;
        self
    }

    /// Attach an on-close handler for the X button.
    pub fn on_close(mut self, f: impl Fn(&mut Window, &mut App) + Send + Sync + 'static) -> Self {
        self.on_close = Some(Box::new(f));
        self
    }
}

impl RenderOnce for ModalShell {
    fn render(self, _window: &mut Window, cx: &mut App) -> impl IntoElement {
        let theme = cx.theme();
        let border_color = ChromeEdgeRole::ModalSeparator.resolve(theme);

        // Danger accent: 2 px red top-border.
        let danger_accent = if self.variant == ModalVariant::Danger {
            Some(BannerColors::danger_fg(theme))
        } else {
            None
        };

        let close_handler = self.on_close.map(std::sync::Arc::new);

        let close_btn = close_handler.as_ref().map(|handler| {
            let h = handler.clone();
            IconButton::new(
                "modal-shell-close",
                IconSource::Svg(AppIcon::X.path().into()),
            )
            .icon_size(Heights::ICON_SM)
            .on_click(move |_, window, cx| (h)(window, cx))
            .into_any_element()
        });

        // Header bar (32 px toolbar height).
        let header = div()
            .flex()
            .items_center()
            .justify_between()
            .px(Spacing::MD)
            .h(Heights::TOOLBAR)
            .border_b_1()
            .border_color(border_color)
            .child(
                div().flex().items_center().gap(Spacing::SM).child(
                    div()
                        .text_size(FontSizes::SM)
                        .font_weight(gpui::FontWeight::SEMIBOLD)
                        .text_color(theme.foreground)
                        .child(self.title),
                ),
            )
            .when_some(close_btn, |h, btn| h.child(btn));

        // Body area.
        let body = div()
            .p(Spacing::LG)
            .min_h(px(96.0))
            .overflow_y_scrollbar()
            .child(self.body);

        // Footer (right-aligned, 12 px gap).
        let footer = div()
            .flex()
            .items_center()
            .justify_end()
            .gap(Spacing::MD)
            .px(Spacing::MD)
            .py(Spacing::SM)
            .border_t_1()
            .border_color(border_color)
            .child(self.footer);

        // Card container.
        let mut card = surface_modal_container(cx)
            .w(self.width)
            .shadow_lg()
            .overflow_hidden()
            .flex()
            .flex_col()
            .child(header)
            .child(body)
            .child(footer);

        if let Some(accent) = danger_accent {
            card = card.border_t_2().border_color(accent);
        }

        let close_for_overlay = close_handler.clone();

        // Scrim / overlay backdrop.
        div()
            .absolute()
            .inset_0()
            .bg(overlay_bg(theme))
            .flex()
            .justify_center()
            .items_start()
            .pt(px(80.0))
            .on_mouse_down(MouseButton::Left, move |_, window, cx| {
                if let Some(ref handler) = close_for_overlay {
                    (handler)(window, cx);
                }
            })
            .child(
                div()
                    .on_mouse_down(MouseButton::Left, |_, _, cx| cx.stop_propagation())
                    .child(card),
            )
    }
}
