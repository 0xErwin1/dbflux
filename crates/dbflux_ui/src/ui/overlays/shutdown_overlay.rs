use crate::app::AppStateEntity;
use crate::ui::icons::AppIcon;
use crate::ui::tokens::Spacing;
use dbflux_components::primitives::{Icon, Text, surface_panel};
use gpui::*;
use gpui_component::ActiveTheme;

/// Overlay shown during graceful shutdown.
///
/// Displays a semi-transparent background with a spinner and status message
/// indicating the current shutdown phase.
pub struct ShutdownOverlay {
    app_state: Entity<AppStateEntity>,
    spin_angle: f32,
}

impl ShutdownOverlay {
    pub fn new(
        app_state: Entity<AppStateEntity>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        // Start animation timer for spinner
        cx.spawn(async move |this, cx| {
            Self::animate_spinner(this, cx).await;
        })
        .detach();

        Self {
            app_state,
            spin_angle: 0.0,
        }
    }

    async fn animate_spinner(this: WeakEntity<Self>, cx: &mut AsyncApp) {
        loop {
            cx.background_executor()
                .timer(std::time::Duration::from_millis(50))
                .await;

            let should_continue = match cx.update(|cx| {
                let Some(entity) = this.upgrade() else {
                    return false;
                };

                entity.update(cx, |overlay, cx| {
                    let phase = overlay.app_state.read(cx).shutdown_phase();
                    if phase.is_active() {
                        overlay.spin_angle = (overlay.spin_angle + 15.0) % 360.0;
                        cx.notify();
                        true
                    } else {
                        false
                    }
                })
            }) {
                Ok(continue_flag) => continue_flag,
                Err(_) => {
                    log::debug!("Shutdown animation stopped: context unavailable");
                    false
                }
            };

            if !should_continue {
                break;
            }
        }
    }

    #[allow(dead_code)]
    pub fn is_visible(&self, cx: &Context<Self>) -> bool {
        self.app_state.read(cx).shutdown_phase().is_active()
    }
}

impl Render for ShutdownOverlay {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let phase = self.app_state.read(cx).shutdown_phase();

        if !phase.is_active() {
            return div().into_any_element();
        }

        let theme = cx.theme();
        let message = phase.message();

        div()
            .id("shutdown-overlay")
            .absolute()
            .inset_0()
            .bg(theme.overlay.opacity(0.7))
            .flex()
            .items_center()
            .justify_center()
            .child(
                surface_panel(cx)
                    .p(Spacing::LG)
                    .min_w(px(250.0))
                    .flex()
                    .flex_col()
                    .items_center()
                    .gap(Spacing::MD)
                    .child(
                        Icon::new(AppIcon::Loader)
                            .size(px(32.0))
                            .color(theme.primary)
                            .with_transformation(Transformation::rotate(gpui::radians(
                                self.spin_angle.to_radians(),
                            ))),
                    )
                    .child(Text::label(message)),
            )
            .into_any_element()
    }
}
