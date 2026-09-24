use crate::modals::shell::{ModalFocus, ModalShell, ModalVariant};
use crate::primitives::{Text, surface_raised};
use crate::tokens::{FontSizes, Spacing};
use crate::typography::AppFonts;
use dbflux_core::LogErr;
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
    /// How long the query has already been running when the modal opens, so
    /// the elapsed hint continues from the real start instead of from zero.
    pub elapsed_secs: u64,
    /// Names of the connections the running queries belong to. The shutdown
    /// prompt lists them, because quitting affects every connection.
    pub connection_names: Vec<String>,
}

/// Modal entity for "active query running" confirmation.
///
/// Uses `ModalShell::Default` (520 px). Displays an elapsed timer that ticks
/// every second via a background task.
pub struct ModalActiveQuery {
    request: Option<ActiveQueryRequest>,
    visible: bool,
    elapsed_secs: u64,
    focus: ModalFocus,
    _elapsed_task: Option<Task<()>>,
}

impl ModalActiveQuery {
    pub fn new(cx: &mut Context<Self>) -> Self {
        Self {
            request: None,
            visible: false,
            elapsed_secs: 0,
            focus: ModalFocus::new(cx),
            _elapsed_task: None,
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visible
    }

    pub fn open(&mut self, request: ActiveQueryRequest, cx: &mut Context<Self>) {
        self.elapsed_secs = request.elapsed_secs;
        self.request = Some(request);
        self.visible = true;
        self.start_timer(cx);
        self.focus.focus_on_next_render();
        cx.notify();
    }

    /// Resolve the modal as if "Cancel query" was clicked. The keyboard path
    /// (ConfirmModal keymap, Enter) uses this so it goes through the same
    /// outcome handler as a mouse click.
    pub fn confirm(&mut self, cx: &mut Context<Self>) {
        self.resolve(ActiveQueryOutcome::CancelQuery, cx);
    }

    /// Resolve the modal as if "Disconnect anyway" / "Quit anyway" was clicked.
    pub fn force(&mut self, cx: &mut Context<Self>) {
        self.resolve(ActiveQueryOutcome::ForceDisconnect, cx);
    }

    fn resolve(&mut self, outcome: ActiveQueryOutcome, cx: &mut Context<Self>) {
        if !self.visible {
            return;
        }

        cx.emit(outcome);
        self.close(cx);
    }

    pub fn close(&mut self, cx: &mut Context<Self>) {
        self.visible = false;
        self.request = None;
        self._elapsed_task = None;
        self.focus.restore(cx);
        cx.notify();
    }

    /// Dismiss the modal and let the query keep running. This is what the
    /// close button, a backdrop click and Escape do: none of them may cancel
    /// the query or force the disconnect.
    pub fn keep_waiting(&mut self, cx: &mut Context<Self>) {
        self.resolve(ActiveQueryOutcome::KeepWaiting, cx);
    }

    fn start_timer(&mut self, cx: &mut Context<Self>) {
        let entity = cx.entity().clone();
        let task = cx.spawn(async move |_, cx| {
            loop {
                cx.background_executor().timer(Duration::from_secs(1)).await;
                let should_continue = cx.update(|cx| {
                    entity.update(cx, |this, cx| {
                        if !this.visible {
                            return false;
                        }
                        this.elapsed_secs += 1;
                        cx.notify();
                        true
                    })
                });
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

/// Body prompt for the flow that opened the modal.
fn prompt_label(trigger: ActiveQueryTrigger, connection_names: &[String]) -> String {
    match trigger {
        ActiveQueryTrigger::Disconnect => dbflux_i18n::t!("modals.active_query.prompt"),
        ActiveQueryTrigger::Shutdown => dbflux_i18n::t!(
            "modals.active_query.prompt_shutdown",
            connections = connection_names.join(", ")
        ),
    }
}

impl EventEmitter<ActiveQueryOutcome> for ModalActiveQuery {}

impl Render for ModalActiveQuery {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        if !self.visible {
            return div().into_any_element();
        }

        self.focus.apply_pending(window, cx);

        let Some(ref request) = self.request else {
            return div().into_any_element();
        };

        let theme = cx.theme();
        let sql = request.sql.clone();
        let trigger = request.trigger;
        let elapsed = self.elapsed_secs;
        let elapsed_label = elapsed_label(elapsed);
        let prompt = prompt_label(trigger, &request.connection_names);

        let body = div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .child(Text::body(prompt).into_any_element())
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
            this.confirm(cx);
        });

        let on_keep_waiting = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            this.keep_waiting(cx);
        });

        let on_force_disconnect = cx.listener(|this, _: &gpui::ClickEvent, _, cx| {
            this.force(cx);
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
        .focus_handle(self.focus.handle())
        .on_close({
            let entity = cx.entity().downgrade();
            move |_, cx| {
                entity
                    .update(cx, |this, cx| this.keep_waiting(cx))
                    .log_err();
            }
        })
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
            "modals.active_query.prompt_shutdown",
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

#[cfg(test)]
mod outcome_tests {
    // Explicit imports rather than the parent glob: combining `use super::*`
    // with `#[gpui::test]` sends the gpui_macros expansion into unbounded
    // recursion.
    use super::{
        ActiveQueryOutcome, ActiveQueryRequest, ActiveQueryTrigger, ModalActiveQuery, prompt_label,
    };
    use gpui::{AppContext, Entity, TestAppContext};
    use std::cell::RefCell;
    use std::rc::Rc;

    fn request(trigger: ActiveQueryTrigger) -> ActiveQueryRequest {
        ActiveQueryRequest {
            sql: "SELECT pg_sleep(60)".to_string(),
            trigger,
            elapsed_secs: 12,
            connection_names: vec!["prod".to_string()],
        }
    }

    fn open_modal(
        cx: &mut TestAppContext,
        trigger: ActiveQueryTrigger,
    ) -> (
        Entity<ModalActiveQuery>,
        Rc<RefCell<Vec<ActiveQueryOutcome>>>,
    ) {
        let modal = cx.new(ModalActiveQuery::new);
        let outcomes: Rc<RefCell<Vec<ActiveQueryOutcome>>> = Rc::new(RefCell::new(Vec::new()));
        let sink = outcomes.clone();

        cx.update(|cx| {
            cx.subscribe(&modal, move |_, event: &ActiveQueryOutcome, _| {
                sink.borrow_mut().push(event.clone());
            })
            .detach();

            modal.update(cx, |modal, cx| modal.open(request(trigger), cx));
        });

        (modal, outcomes)
    }

    #[gpui::test]
    fn open_continues_the_elapsed_time_of_the_running_query(cx: &mut TestAppContext) {
        let (modal, _outcomes) = open_modal(cx, ActiveQueryTrigger::Disconnect);

        let (visible, elapsed) = cx.update(|cx| {
            let modal = modal.read(cx);
            (modal.is_visible(), modal.elapsed_secs)
        });

        assert!(visible);
        assert_eq!(elapsed, 12);
    }

    #[gpui::test]
    fn each_choice_emits_its_outcome_once_and_closes(cx: &mut TestAppContext) {
        type Choice = fn(&mut ModalActiveQuery, &mut gpui::Context<ModalActiveQuery>);
        let choices: [(Choice, &str); 3] = [
            (ModalActiveQuery::confirm, "CancelQuery"),
            (ModalActiveQuery::keep_waiting, "KeepWaiting"),
            (ModalActiveQuery::force, "ForceDisconnect"),
        ];

        for (choice, expected) in choices {
            let (modal, outcomes) = open_modal(cx, ActiveQueryTrigger::Disconnect);

            cx.update(|cx| {
                modal.update(cx, |modal, cx| choice(modal, cx));
                // A second resolution on a closed modal must not emit again.
                modal.update(cx, |modal, cx| choice(modal, cx));
            });

            let emitted: Vec<String> = outcomes
                .borrow()
                .iter()
                .map(|outcome| format!("{outcome:?}"))
                .collect();
            assert_eq!(emitted, vec![expected.to_string()]);

            let visible = cx.update(|cx| modal.read(cx).is_visible());
            assert!(!visible, "{expected} must close the modal");
        }
    }

    #[test]
    fn shutdown_prompt_names_the_connections_and_disconnect_prompt_does_not() {
        let names = vec!["prod".to_string(), "analytics".to_string()];

        let shutdown = prompt_label(ActiveQueryTrigger::Shutdown, &names);
        assert!(shutdown.contains("prod, analytics"), "{shutdown}");

        let disconnect = prompt_label(ActiveQueryTrigger::Disconnect, &names);
        assert_eq!(disconnect, dbflux_i18n::t!("modals.active_query.prompt"));
    }
}
