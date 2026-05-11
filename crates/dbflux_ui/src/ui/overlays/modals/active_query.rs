use crate::ui::tokens::{FontSizes, Spacing};
use dbflux_components::primitives::{Text, surface_raised};
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
        let elapsed_label = format!("Running for {}s", elapsed);

        let body = div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .child(
                Text::body(
                    "A query is still running on this connection. What would you like to do?",
                )
                .into_any_element(),
            )
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
                            .font_family(dbflux_components::typography::AppFonts::MONO)
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
            ActiveQueryTrigger::Disconnect => "Disconnect anyway",
            ActiveQueryTrigger::Shutdown => "Quit anyway",
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
                    .label("Keep waiting")
                    .on_click(on_keep_waiting),
            )
            .child(
                Button::new("active-cancel-query")
                    .label("Cancel query")
                    .danger()
                    .on_click(on_cancel_query),
            );

        use super::shell::{ModalShell, ModalVariant};

        ModalShell::new(
            "Active query running",
            body.into_any_element(),
            footer.into_any_element(),
        )
        .variant(ModalVariant::Default)
        .width(px(520.0))
        .into_any_element()
    }
}
