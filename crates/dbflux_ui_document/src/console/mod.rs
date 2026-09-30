//! Native command console, docked under a document (`Ctrl+\``).
//!
//! Hosts embed it for connections whose driver advertises
//! `DriverCapabilities::NATIVE_CONSOLE`; nothing here names a driver.
//! Commands run one at a time through `Connection::execute`, after the same
//! checks as the code editor: the driver's language service validates them
//! and flags dangerous commands, `classify_query_for_language` rates their
//! impact, and the shared dangerous-query rules decide whether they run, ask
//! first, or are refused. A confirmed command carries the governance ceiling
//! the confirmation authorised (`QueryRequest::confirmed_ceiling`), every
//! execution is audited like an editor query, and successful commands land
//! in the shared query history that Up/Down recalls from.

mod format;

use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::controls::{
    Button, Input, InputEvent, InputMoveDown, InputMoveUp, InputState,
};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Kbd, Text};
use dbflux_components::tokens::{ChromeColors, ConsoleMetrics, FontSizes, Spacing};
use dbflux_components::typography::AppFonts;
use dbflux_core::observability::actions as audit_actions;
use dbflux_core::observability::{
    EventCategory, EventOrigin, EventOutcome, EventRecord, EventSeverity,
};
use dbflux_core::{
    Connection, DangerousQueryKind, DbError, ExecutionClassification, HistoryEntry,
    NativeConsoleProfile, QueryRequest, QueryResult, ValidationResult, classify_query_for_language,
};
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use format::{
    ConsoleGate, ConsoleLine, ConsoleTone, console_gate, format_console_result, recall_list,
    step_history,
};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

/// Command/result pairs kept on screen.
const TRANSCRIPT_LIMIT: usize = 200;

/// Commands kept for recall that never reached the shared history.
const UNRECORDED_LIMIT: usize = format::HISTORY_LIMIT;

/// Where the console's commands run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeConsoleTarget {
    pub profile_id: Uuid,
    /// Passed as `QueryRequest::database`, and named by the prompt.
    pub database: Option<String>,
    /// How the header names the target (`db0`, `shop`).
    pub label: String,
}

/// What the console tells its host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NativeConsoleEvent {
    /// The input was clicked; the host should take document focus and treat
    /// the keyboard as being in a text field.
    InputFocused,
    /// A command finished. Hosts refresh what it may have changed.
    Executed {
        succeeded: bool,
        classification: ExecutionClassification,
    },
}

/// One command and what it printed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ConsoleEntry {
    pub prompt: String,
    pub command: String,
    pub output: Vec<ConsoleLine>,
}

/// A command waiting for the user to confirm it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PendingConsoleCommand {
    pub command: String,
    pub title: String,
    pub body: String,
    pub kind: Option<DangerousQueryKind>,
    pub ceiling: ExecutionClassification,
}

pub struct NativeConsole {
    app_state: Entity<AppStateEntity>,
    target: NativeConsoleTarget,
    profile: NativeConsoleProfile,
    /// The key context whose binding of `ToggleConsole` the header shows.
    shortcut_context: ContextId,
    open: bool,
    pub(crate) input: Entity<InputState>,
    input_focused: bool,
    pub(crate) transcript: Vec<ConsoleEntry>,
    unrecorded: Vec<(i64, String)>,
    recall: Vec<String>,
    history_cursor: Option<usize>,
    pub(crate) pending: Option<PendingConsoleCommand>,
    /// Whether the pending confirmation has been drawn. The single-line
    /// input's Enter action propagates, so one keystroke can emit
    /// `PressEnter` twice; only a confirmation the user has seen may be
    /// answered by Enter in the empty field.
    pending_shown: bool,
    running: bool,
    scroll: ScrollHandle,
    _subscription: Subscription,
}

impl EventEmitter<NativeConsoleEvent> for NativeConsole {}

impl NativeConsole {
    pub fn new(
        target: NativeConsoleTarget,
        profile: NativeConsoleProfile,
        shortcut_context: ContextId,
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let placeholder = if profile.example_command.is_empty() {
            dbflux_i18n::t!("document.console.placeholder")
        } else {
            dbflux_i18n::t!(
                "document.console.placeholder_example",
                example = profile.example_command
            )
        };

        let input = cx.new(|cx| InputState::new(window, cx).placeholder(placeholder));

        let subscription = cx.subscribe_in(
            &input,
            window,
            |this, _, event: &InputEvent, window, cx| match event {
                InputEvent::PressEnter { .. } => this.submit(window, cx),
                InputEvent::Focus => this.input_focused = true,
                InputEvent::Blur => this.input_focused = false,
                _ => {}
            },
        );

        Self {
            app_state,
            target,
            profile,
            shortcut_context,
            open: false,
            input,
            input_focused: false,
            transcript: Vec::new(),
            unrecorded: Vec::new(),
            recall: Vec::new(),
            history_cursor: None,
            pending: None,
            pending_shown: false,
            running: false,
            scroll: ScrollHandle::new(),
            _subscription: subscription,
        }
    }

    #[cfg(test)]
    pub(crate) fn is_open(&self) -> bool {
        self.open
    }

    /// Whether the keyboard is in the console's input, so the host reports a
    /// text-entry context and bare letters are typed rather than run.
    pub fn input_has_focus(&self) -> bool {
        self.open && self.input_focused
    }

    pub fn has_pending(&self) -> bool {
        self.pending.is_some()
    }

    /// Shows or hides the console, moving focus into its input when it
    /// opens. Returns whether it is now open; on close the host takes focus
    /// back.
    pub fn toggle(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        self.open = !self.open;

        if self.open {
            self.input.update(cx, |state, cx| state.focus(window, cx));
            // The Focus event arrives with the next frame; until then the
            // host would keep reporting its own context and claim the
            // letters typed.
            self.input_focused = true;
        } else {
            self.input_focused = false;
        }

        cx.notify();
        self.open
    }

    fn prompt(&self) -> String {
        let context = self
            .target
            .database
            .as_deref()
            .unwrap_or(&self.target.label);

        self.profile.prompt(context)
    }

    fn push_entry(&mut self, command: String, output: Vec<ConsoleLine>) {
        self.transcript.push(ConsoleEntry {
            prompt: self.prompt(),
            command,
            output,
        });

        if self.transcript.len() > TRANSCRIPT_LIMIT {
            self.transcript.remove(0);
        }

        self.scroll.scroll_to_bottom();
    }

    /// Keeps `command` recallable although it never reached the shared
    /// history.
    fn remember_unrecorded(&mut self, command: &str) {
        self.unrecorded.push((
            dbflux_core::chrono::Utc::now().timestamp(),
            command.to_string(),
        ));

        if self.unrecorded.len() > UNRECORDED_LIMIT {
            self.unrecorded.remove(0);
        }
    }

    fn connection_name(&self, cx: &App) -> Option<String> {
        self.app_state
            .read(cx)
            .connections()
            .get(&self.target.profile_id)
            .map(|connected| connected.profile.name.clone())
    }

    fn recall_history(&mut self, older: bool, window: &mut Window, cx: &mut Context<Self>) {
        if self.history_cursor.is_none() {
            let connection_name = self.connection_name(cx);
            let state = self.app_state.read(cx);
            let shared = state
                .history_entries()
                .iter()
                .filter(|entry| {
                    connection_name.is_some() && entry.connection_name == connection_name
                })
                .map(|entry| (entry.timestamp, entry.sql.as_str()));

            self.recall = recall_list(shared, &self.unrecorded);
        }

        let cursor = step_history(self.history_cursor, self.recall.len(), older);
        self.history_cursor = cursor;

        let text = cursor
            .and_then(|position| self.recall.get(position))
            .cloned()
            .unwrap_or_default();

        self.input
            .update(cx, |state, cx| state.set_value(text, window, cx));
        cx.notify();
    }

    fn resolve_connection(&self, cx: &App) -> Result<Arc<dyn Connection>, String> {
        let state = self.app_state.read(cx);
        let Some(connected) = state.connections().get(&self.target.profile_id) else {
            return Err(dbflux_i18n::t!("document.console.connection_inactive"));
        };

        connected
            .resolve_connection_for_execution(self.target.database.as_deref())
            .map_err(
                |dbflux_core::ConnectionResolutionError::PendingDatabaseConnection { database }| {
                    dbflux_i18n::t!("document.console.connecting", database = database)
                },
            )
    }

    /// Runs the typed command after the validation and dangerous-query
    /// checks, or holds it for confirmation.
    pub(crate) fn submit(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.running {
            return;
        }

        let command = self.input.read(cx).value().trim().to_string();
        if command.is_empty() {
            // Enter in the empty field answers a pending confirmation, as
            // Run anyway does.
            if self.pending.is_some() && self.pending_shown {
                self.confirm_pending(cx);
            }
            return;
        }

        self.history_cursor = None;
        self.input
            .update(cx, |state, cx| state.set_value("", window, cx));

        let connection = match self.resolve_connection(cx) {
            Ok(connection) => connection,
            Err(message) => {
                self.remember_unrecorded(&command);
                report_error(UserFacingError::new(ErrorKind::Network, message), cx);
                return;
            }
        };

        let language_service = connection.language_service();

        let validation_error = match language_service.validate(&command) {
            ValidationResult::Valid => None,
            ValidationResult::SyntaxError(diagnostic) => Some(diagnostic.message),
            ValidationResult::WrongLanguage { message, .. } => Some(message),
        };

        if let Some(message) = validation_error {
            self.remember_unrecorded(&command);
            self.push_entry(command, vec![ConsoleLine::new(message, ConsoleTone::Error)]);
            cx.notify();
            return;
        }

        let dangerous = language_service.detect_dangerous(&command);
        let classification =
            classify_query_for_language(&connection.metadata().query_language, &command);

        let gate = console_gate(dangerous, classification, |kind| {
            let state = self.app_state.read(cx);
            let is_suppressed = state.dangerous_query_suppressions().is_suppressed(kind);
            let effective = state.effective_settings_for_connection(Some(self.target.profile_id));
            let allow_flush = effective
                .driver_values
                .get("allow_flush")
                .is_some_and(|value| value == "true");

            crate::code::evaluate_dangerous_with_effective_settings(
                kind,
                is_suppressed,
                &effective,
                allow_flush,
            )
        });

        match gate {
            ConsoleGate::Run { ceiling } => {
                self.run(command, classification, ceiling, cx);
            }
            ConsoleGate::Confirm {
                title,
                body,
                kind,
                ceiling,
            } => {
                self.pending = Some(PendingConsoleCommand {
                    command,
                    title,
                    body,
                    kind,
                    ceiling,
                });
                self.pending_shown = false;
                cx.notify();
            }
            ConsoleGate::Refuse(message) => {
                self.remember_unrecorded(&command);
                self.push_entry(command, vec![ConsoleLine::new(message, ConsoleTone::Error)]);
                cx.notify();
            }
        }
    }

    /// Runs the pending command with the ceiling its confirmation
    /// authorises (Run anyway, or Enter in the empty field).
    pub fn confirm_pending(&mut self, cx: &mut Context<Self>) {
        let Some(pending) = self.pending.take() else {
            return;
        };

        if let Some(kind) = pending.kind {
            self.emit_audit(
                cx,
                EventRecord::new(
                    now_ms(),
                    EventSeverity::Warn,
                    EventCategory::Query,
                    EventOutcome::Success,
                )
                .with_typed_action(audit_actions::DANGEROUS_QUERY_CONFIRMED)
                .with_summary(format!("Dangerous query confirmed: {}", kind.message())),
                serde_json::json!({ "dangerous_kind": kind.message(), "source": "console" }),
            );
        }

        let classification = self
            .resolve_connection(cx)
            .map(|connection| {
                classify_query_for_language(&connection.metadata().query_language, &pending.command)
            })
            .unwrap_or(pending.ceiling);

        self.run(pending.command, classification, Some(pending.ceiling), cx);
    }

    /// Drops the pending command (Cancel, or Escape in the host).
    pub fn cancel_pending(&mut self, cx: &mut Context<Self>) {
        if let Some(pending) = self.pending.take() {
            self.remember_unrecorded(&pending.command);
            self.push_entry(
                pending.command,
                vec![ConsoleLine::new(
                    dbflux_i18n::t!("document.console.cancelled"),
                    ConsoleTone::Muted,
                )],
            );
        }

        cx.notify();
    }

    /// Records `event` with this console's connection context and origin.
    fn emit_audit(&self, cx: &App, event: EventRecord, details: serde_json::Value) {
        let event = self.with_connection_context(event, cx);
        let mut event = event.with_origin(EventOrigin::local());
        event.details_json = Some(details.to_string());

        if let Err(error) = self.app_state.read(cx).audit_service().record(event) {
            log::warn!("Failed to emit console audit event: {error}");
        }
    }

    fn with_connection_context(&self, event: EventRecord, cx: &App) -> EventRecord {
        let driver_id = self
            .app_state
            .read(cx)
            .connections()
            .get(&self.target.profile_id)
            .map(|connected| connected.profile.driver_id())
            .unwrap_or_default();

        event.with_connection_context(
            self.target.profile_id.to_string(),
            self.target.database.clone().unwrap_or_default(),
            driver_id,
        )
    }

    fn run(
        &mut self,
        command: String,
        classification: ExecutionClassification,
        ceiling: Option<ExecutionClassification>,
        cx: &mut Context<Self>,
    ) {
        let connection = match self.resolve_connection(cx) {
            Ok(connection) => connection,
            Err(message) => {
                self.remember_unrecorded(&command);
                report_error(UserFacingError::new(ErrorKind::Network, message), cx);
                return;
            }
        };

        self.running = true;
        cx.notify();

        let mut request =
            QueryRequest::new(command.clone()).with_database(self.target.database.clone());
        if let Some(ceiling) = ceiling {
            request = request.with_confirmed_ceiling(ceiling);
        }

        // Captured before spawning so the execution is audited even if the
        // document closes while the command runs.
        let audit_service = self.app_state.read(cx).audit_service().clone();
        let audit_template = self.with_connection_context(
            EventRecord::new(
                now_ms(),
                EventSeverity::Info,
                EventCategory::Query,
                EventOutcome::Success,
            ),
            cx,
        );
        let started_at = Instant::now();

        cx.spawn(async move |this, cx| {
            let result: Result<QueryResult, DbError> = cx
                .background_executor()
                .spawn(async move { connection.execute(&request) })
                .await;

            let duration_ms = started_at.elapsed().as_millis() as i64;
            let event = execution_audit_event(audit_template, &command, &result, duration_ms);
            if let Err(error) = audit_service.record(event) {
                log::warn!("Failed to emit console audit event: {error}");
            }

            let update = this.update(cx, |console, cx| {
                console.finish(command, classification, result, cx);
            });
            if update.is_err() {
                log::debug!("Console closed before its command finished");
            }
        })
        .detach();
    }

    fn finish(
        &mut self,
        command: String,
        classification: ExecutionClassification,
        result: Result<QueryResult, DbError>,
        cx: &mut Context<Self>,
    ) {
        self.running = false;

        let succeeded = result.is_ok();

        let output = match &result {
            Ok(result) => {
                let entry = HistoryEntry::new(
                    command.clone(),
                    self.target.database.clone(),
                    self.connection_name(cx),
                    result.execution_time,
                    Some(result.affected_rows.unwrap_or(result.rows.len() as u64) as usize),
                );
                self.app_state
                    .update(cx, |state, _| state.add_history_entry(entry));

                format_console_result(result)
            }
            Err(error) => {
                self.remember_unrecorded(&command);
                vec![ConsoleLine::new(error.to_string(), ConsoleTone::Error)]
            }
        };

        self.push_entry(command, output);

        cx.emit(NativeConsoleEvent::Executed {
            succeeded,
            classification,
        });
        cx.notify();
    }

    fn render_console(&self, cx: &mut Context<Self>) -> Stateful<Div> {
        let theme = cx.theme();
        let tint = ChromeColors::tint(theme);
        let strong = ChromeColors::strong(theme);
        let muted = theme.muted_foreground;
        let open = self.open;

        let header = div()
            .id("native-console-header")
            .flex()
            .flex_none()
            .items_center()
            .gap(ConsoleMetrics::GAP)
            .h(ConsoleMetrics::HEADER_HEIGHT)
            .px(ConsoleMetrics::PADDING_X)
            .when(open, |header| {
                header.border_b_1().border_color(theme.border)
            })
            .text_size(FontSizes::XS)
            .cursor_pointer()
            .on_click(cx.listener(|this, _, window, cx| {
                this.toggle(window, cx);
            }))
            .child(
                Icon::new(if open {
                    AppIcon::ChevronDown
                } else {
                    AppIcon::ChevronRight
                })
                .size(ConsoleMetrics::CHEVRON)
                .color(muted),
            )
            .child(
                Icon::new(AppIcon::SquareTerminal)
                    .size(ConsoleMetrics::ICON)
                    .color(tint),
            )
            .child(
                Text::body(dbflux_i18n::t!("document.console.title"))
                    .font_size(FontSizes::XS)
                    .font_weight(FontWeight::SEMIBOLD)
                    .color(strong),
            )
            .child(
                Text::body(dbflux_i18n::t!(
                    "document.console.subtitle",
                    database = self.target.label.clone()
                ))
                .font_size(FontSizes::XS)
                .color(muted),
            )
            .child(div().flex_1())
            .when_some(
                dbflux_ui_base::keymap::shortcut_label(
                    self.shortcut_context,
                    Command::ToggleConsole,
                ),
                |header, label| header.child(Kbd::new(label)),
            );

        let container = div()
            .id("native-console")
            .key_context(dbflux_components::key_contexts::NATIVE_CONSOLE)
            .flex()
            .flex_col()
            .flex_none()
            .border_t_1()
            .border_color(theme.input)
            .bg(theme.background)
            .child(header);

        if !open {
            return container;
        }

        container.child(self.render_body(cx))
    }

    fn render_body(&self, cx: &mut Context<Self>) -> Stateful<Div> {
        let theme = cx.theme();
        let tint = ChromeColors::tint(theme);
        let strong = ChromeColors::strong(theme);
        let muted = theme.muted_foreground;
        let result_color = theme.success;
        let error_color = theme.danger;
        let warning = theme.warning;

        let transcript = self.transcript.iter().enumerate().map(|(index, entry)| {
            div()
                .id(("native-console-entry", index))
                .flex()
                .flex_col()
                .child(
                    div()
                        .flex()
                        .gap(Spacing::SM)
                        .child(div().text_color(tint).child(entry.prompt.clone()))
                        .child(div().text_color(strong).child(entry.command.clone())),
                )
                .children(entry.output.iter().map(|line| {
                    let color = match line.tone {
                        ConsoleTone::Result => result_color,
                        ConsoleTone::Error => error_color,
                        ConsoleTone::Muted => muted,
                    };

                    div()
                        .whitespace_nowrap()
                        .text_color(color)
                        .child(line.text.clone())
                }))
        });

        let pending = self.pending.clone();

        div()
            .id("native-console-body")
            .flex()
            .flex_col()
            .pt(Spacing::SM)
            .pb(ConsoleMetrics::PADDING_BOTTOM)
            .px(ConsoleMetrics::PADDING_X)
            .font_family(AppFonts::MONO)
            .text_size(ConsoleMetrics::FONT)
            .line_height(ConsoleMetrics::LINE_HEIGHT)
            .capture_action(cx.listener(|this, _: &InputMoveUp, window, cx| {
                this.recall_history(true, window, cx);
                cx.stop_propagation();
            }))
            .capture_action(cx.listener(|this, _: &InputMoveDown, window, cx| {
                this.recall_history(false, window, cx);
                cx.stop_propagation();
            }))
            .child(
                div()
                    .id("native-console-transcript")
                    .flex()
                    .flex_col()
                    .max_h(ConsoleMetrics::TRANSCRIPT_HEIGHT)
                    .overflow_y_scroll()
                    .track_scroll(&self.scroll)
                    .children(transcript),
            )
            .when_some(pending, |body, pending| {
                body.child(
                    div()
                        .flex()
                        .items_center()
                        .gap(Spacing::SM)
                        .py(Spacing::XS)
                        .font_family(AppFonts::INTERFACE)
                        .child(
                            Icon::new(AppIcon::TriangleAlert)
                                .size(ConsoleMetrics::ICON)
                                .color(warning),
                        )
                        .child(
                            div()
                                .flex()
                                .flex_col()
                                .flex_1()
                                .min_w_0()
                                .child(Text::body(pending.title).color(strong))
                                .child(Text::caption(pending.body).color(muted)),
                        )
                        .child(
                            Button::new(
                                "native-console-run-anyway",
                                dbflux_i18n::t!("document.console.run_anyway"),
                            )
                            .danger()
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.confirm_pending(cx);
                            })),
                        )
                        .child(
                            Button::new(
                                "native-console-cancel",
                                dbflux_i18n::t!("document.console.cancel"),
                            )
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.cancel_pending(cx);
                            })),
                        ),
                )
            })
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(div().text_color(tint).child(self.prompt()))
                    .child(
                        div()
                            .flex_1()
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(|this, _, _, cx| {
                                    this.input_focused = true;
                                    cx.emit(NativeConsoleEvent::InputFocused);
                                    cx.stop_propagation();
                                }),
                            )
                            .child(
                                Input::new(&self.input)
                                    .id("native-console-input")
                                    .appearance(false)
                                    .w_full(),
                            ),
                    )
                    .when(self.running, |row| {
                        row.child(
                            Icon::new(AppIcon::Loader)
                                .size(ConsoleMetrics::ICON)
                                .color(muted),
                        )
                    }),
            )
    }
}

impl Render for NativeConsole {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.pending_shown = self.open && self.pending.is_some();
        self.render_console(cx)
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

/// The audit row of one console execution, shaped like the code editor's
/// (`query_execute` / `query_execute_failed`, the query in `details_json`,
/// driver-provided `metadata_extra` merged in).
fn execution_audit_event(
    template: EventRecord,
    command: &str,
    result: &Result<QueryResult, DbError>,
    duration_ms: i64,
) -> EventRecord {
    let mut details = serde_json::Map::new();
    details.insert(
        "query".to_string(),
        serde_json::Value::String(command.to_string()),
    );
    details.insert(
        "source".to_string(),
        serde_json::Value::String("console".to_string()),
    );

    let mut event = template.with_origin(EventOrigin::local());
    event.ts_ms = now_ms();
    event.duration_ms = Some(duration_ms);

    match result {
        Ok(result) => {
            let (count, label) = match result.affected_rows {
                Some(count) => (count, "affected"),
                None => (result.rows.len() as u64, "returned"),
            };

            if let Some(extra) = &result.metadata_extra {
                for (key, value) in extra {
                    details.insert(key.clone(), value.clone());
                }
            }

            event = event
                .with_typed_action(audit_actions::QUERY_EXECUTE)
                .with_summary(format!("Query executed successfully: {count} rows {label}"));
        }
        Err(error) => {
            event.level = EventSeverity::Error;
            event.outcome = EventOutcome::Failure;
            event.error_message = Some(error.to_string());
            event = event
                .with_typed_action(audit_actions::QUERY_EXECUTE_FAILED)
                .with_summary(format!("Query failed: {error}"));
        }
    }

    event.details_json = Some(serde_json::Value::Object(details).to_string());
    event
}

#[cfg(test)]
mod tests;
