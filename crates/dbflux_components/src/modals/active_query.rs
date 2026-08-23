use crate::modals::shell::{ModalShell, ModalVariant};
use crate::primitives::{Text, surface_raised};
use crate::tokens::{FontSizes, Spacing};
use crate::typography::AppFonts;
use gpui::prelude::*;
use gpui::{Context, EventEmitter, Task, Window, div, px};
use gpui_component::ActiveTheme;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::scroll::ScrollableElement;
use std::time::Duration;

/// The flow that triggered this modal — determines which footer buttons are shown.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ActiveQueryTrigger {
    /// The user is disconnecting while a query is still running.
    Disconnect,
    /// The app is shutting down while a query is still running.
    Shutdown,
}

/// Outcome emitted when the user resolves the modal.
#[derive(Clone, Debug)]
pub enum ActiveQueryOutcome {
    /// Cancel the running query and close the modal.
    CancelQuery,
    /// Keep waiting — dismiss the modal, let the query continue.
    KeepWaiting,
    /// Force disconnect/shutdown, abandon the query.
    ForceDisconnect,
}

/// Request payload for `pending_modal_open`.
#[derive(Clone)]
pub struct ActiveQueryRequest {
    /// The SQL text currently running.
    pub sql: String,
    pub trigger: ActiveQueryTrigger,
}

/// Modal entity for "active query running" confirmation.
///
/// Uses `ModalShell::Default` (520 px). Displays an elapsed timer that ticks
/// every second via a background task.
pub struct ModalActiveQuery {
    request: Option<ActiveQueryRequest>,
    visible: bool,
    elapsed_secs: u64,
    _elapsed_task: Option<Task<()>>,
}

impl ModalActiveQuery {
    pub fn new(_cx: &mut Context<Self>) -> Self {
        Self {
            request: None,
            visible: false,
            elapsed_secs: 0,
            _elapsed_task: None,
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(&mut self, request: ActiveQueryRequest, cx: &mut Context<Self>) {
        self.request = Some(request);
        self.visible = true;
        self.elapsed_secs = 0;
        self.start_timer(cx);
        cx.notify();
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.request = None;
        self._elapsed_task = None;
        cx.notify();
    }

    fn start_timer(&mut self, cx: &mut Context<Self>) {
        let entity = cx.entity().clone();
        let task = cx.spawn(async move |_, cx| {
            loop {
                cx.background_executor().timer(Duration::from_secs(1)).await;
                let should_continue = cx
                    .update(|cx| {
                        entity.update(cx, |this, cx| {
                            if !this.visible {
                                return false;
                            }
                            this.elapsed_secs += 1;
                            cx.notify();
                            true
                        })
                    })
                    .unwrap_or(false);
                if !should_continue {
                    break;
                }
            }
        });
        self._elapsed_task = Some(task);
    }
}

/// Label for the elapsed-time hint, with the elapsed seconds interpolated.
fn elapsed_label(seconds: u64) -> String {
    dbflux_i18n::t!("modals.active_query.elapsed", seconds = seconds)
}

impl EventEmitter<ActiveQueryOutcome> for ModalActiveQuery {}

impl Render for ModalActiveQuery {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        let Some(ref request) = self.request else {
            return div().into_any_element();
        };

        let theme = cx.theme();
        let sql = request.sql.clone();
        let trigger = request.trigger;
        let elapsed = self.elapsed_secs;
        let elapsed_label = elapsed_label(elapsed);

        let body = div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .child(Text::body(dbflux_i18n::t!("modals.active_query.prompt")).into_any_element())
            .child(
                surface_raised(cx)
                    .w_full()
                    .max_h(px(120.0))
                    .overflow_y_scrollbar()
                    .px(Spacing::SM)
                    .py(Spacing::XS)
                    .child(
                        div()
                            .text_size(FontSizes::XS)
                            .font_family(AppFonts::MONO)
                            .text_color(theme.foreground)
                            .child(sql),
                    ),
            )
            .child(
                div()
                    .text_size(FontSizes::SM)
                    .text_color(theme.muted_foreground)
                    .child(elapsed_label),
            );

        let on_cancel_query = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            cx.emit(ActiveQueryOutcome::CancelQuery);
            this.close(cx);
        });

        let on_keep_waiting = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            cx.emit(ActiveQueryOutcome::KeepWaiting);
            this.close(cx);
        });

        let on_force_disconnect = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            cx.emit(ActiveQueryOutcome::ForceDisconnect);
            this.close(cx);
        });

        let force_btn_label = match trigger {
            ActiveQueryTrigger::Disconnect => {
                dbflux_i18n::t!("modals.active_query.disconnect_anyway")
            }
            ActiveQueryTrigger::Shutdown => dbflux_i18n::t!("modals.active_query.quit_anyway"),
        };

        let footer = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(
                Button::new("active-force-action")
                    .label(force_btn_label)
                    .ghost()
                    .on_click(on_force_disconnect),
            )
            .child(div().flex_1())
            .child(
                Button::new("active-keep-waiting")
                    .label(dbflux_i18n::t!("modals.active_query.keep_waiting"))
                    .on_click(on_keep_waiting),
            )
            .child(
                Button::new("active-cancel-query")
                    .label(dbflux_i18n::t!("modals.active_query.cancel_query"))
                    .danger()
                    .on_click(on_cancel_query),
            );

        ModalShell::new(
            dbflux_i18n::t!("modals.active_query.title"),
            body.into_any_element(),
            footer.into_any_element(),
        )
        .variant(ModalVariant::Default)
        .width(px(520.0))
        .into_any_element()
    }
}

// ---------------------------------------------------------------------------
// Tests — translation key resolution and elapsed label formatting
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_query_keys_resolve_in_both_locales() {
        let keys = [
            "modals.active_query.title",
            "modals.active_query.prompt",
            "modals.active_query.elapsed",
            "modals.active_query.disconnect_anyway",
            "modals.active_query.quit_anyway",
            "modals.active_query.keep_waiting",
            "modals.active_query.cancel_query",
        ];

        for key in keys {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");
            assert!(!en.is_empty() && en != key, "en missing for {key}");
            assert!(!es.is_empty() && es != key, "es missing for {key}");
        }
    }

    #[test]
    fn active_query_keep_waiting_diverges_between_locales() {
        let en = dbflux_i18n::t!("modals.active_query.keep_waiting", locale = "en");
        let es = dbflux_i18n::t!("modals.active_query.keep_waiting", locale = "es");
        assert_ne!(en, es);
    }

    #[test]
    fn elapsed_label_contains_seconds_for_zero_and_one() {
        let zero = elapsed_label(0);
        assert!(zero.contains('0'));
        assert_eq!(
            zero,
            dbflux_i18n::t!("modals.active_query.elapsed", seconds = 0)
        );

        let one = elapsed_label(1);
        assert!(one.contains('1'));
        assert_eq!(
            one,
            dbflux_i18n::t!("modals.active_query.elapsed", seconds = 1)
        );
    }

    #[test]
    fn elapsed_label_contains_seconds_for_larger_value() {
        let label = elapsed_label(42);
        assert!(label.contains("42"));
        assert_eq!(
            label,
            dbflux_i18n::t!("modals.active_query.elapsed", seconds = 42)
        );
    }
}
