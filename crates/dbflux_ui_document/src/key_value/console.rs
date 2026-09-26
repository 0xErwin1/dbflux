//! Command console under the key list and value pane (`Ctrl+\``).
//!
//! Commands run against the document's database through the connection's
//! query path. Before anything runs, the command goes through the same
//! checks as the code editor: the driver's language service validates it
//! and flags dangerous commands, `classify_query_for_language` rates its
//! impact, and the shared dangerous-query rules decide whether it runs,
//! asks first, or is refused.

use crate::handle::DocumentEvent;
use dbflux_components::controls::Button;
use dbflux_components::controls::{Input, InputEvent, InputMoveDown, InputMoveUp, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Kbd, Text};
use dbflux_components::tokens::{ChromeColors, FontSizes, KeyValueMetrics, Spacing};
use dbflux_components::typography::AppFonts;
use dbflux_core::{
    DangerousAction, DangerousQueryKind, DbError, ExecutionClassification, QueryRequest,
    QueryResult, ValidationResult, classify_query_for_language,
};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

/// Commands remembered for Up/Down recall.
const HISTORY_LIMIT: usize = 100;

/// Command/result pairs kept on screen.
const TRANSCRIPT_LIMIT: usize = 200;

/// Lines of one result shown before the rest is summarized.
const RESULT_LINE_LIMIT: usize = 200;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ConsoleTone {
    Result,
    Error,
    Muted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ConsoleLine {
    pub text: String,
    pub tone: ConsoleTone,
}

impl ConsoleLine {
    fn new(text: impl Into<String>, tone: ConsoleTone) -> Self {
        Self {
            text: text.into(),
            tone,
        }
    }
}

/// One command and what it printed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ConsoleEntry {
    pub prompt: String,
    pub command: String,
    pub output: Vec<ConsoleLine>,
}

/// A command waiting for the user to confirm it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PendingConsoleCommand {
    pub command: String,
    pub title: String,
    pub body: String,
}

/// What happens to a command before it runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ConsoleGate {
    Run,
    Confirm { title: String, body: String },
    Refuse(String),
}

/// Applies the dangerous-query rules to a console command.
///
/// `dangerous` is the driver's verdict (`LanguageService::detect_dangerous`)
/// and `decide` the shared settings-based decision for it. A command the
/// driver does not flag still asks first when `classify_query_for_language`
/// rates it as destructive.
pub(super) fn console_gate(
    dangerous: Option<DangerousQueryKind>,
    classification: ExecutionClassification,
    decide: impl FnOnce(DangerousQueryKind) -> DangerousAction,
) -> ConsoleGate {
    if let Some(kind) = dangerous {
        return match decide(kind) {
            DangerousAction::Allow => ConsoleGate::Run,
            DangerousAction::Confirm(kind) => ConsoleGate::Confirm {
                title: crate::labels::dangerous_query_title(kind),
                body: crate::labels::dangerous_query_body(kind),
            },
            DangerousAction::Block(message) => ConsoleGate::Refuse(message),
        };
    }

    if matches!(
        classification,
        ExecutionClassification::Destructive | ExecutionClassification::AdminDestructive
    ) {
        return ConsoleGate::Confirm {
            title: dbflux_i18n::t!("document.key_value.console.destructive_title"),
            body: dbflux_i18n::t!("document.key_value.console.destructive_body"),
        };
    }

    ConsoleGate::Run
}

/// Result of a command as console lines, in the style of a command-line
/// client: text as printed, arrays numbered, binary replies summarized.
pub(super) fn format_console_result(result: &QueryResult) -> Vec<ConsoleLine> {
    let mut lines: Vec<ConsoleLine> = Vec::new();

    if let Some(text) = &result.text_body {
        lines.extend(
            text.lines()
                .map(|line| ConsoleLine::new(line, ConsoleTone::Result)),
        );
    } else if let Some(bytes) = &result.raw_bytes {
        lines.push(ConsoleLine::new(
            dbflux_i18n::t!("document.key_value.console.binary", count = bytes.len()),
            ConsoleTone::Muted,
        ));
    } else {
        let skip_index_column = result
            .columns
            .first()
            .is_some_and(|column| column.name == "#");

        for (index, row) in result.rows.iter().enumerate() {
            let cells: Vec<String> = row
                .iter()
                .skip(usize::from(skip_index_column))
                .map(|value| value.as_display_string())
                .collect();

            lines.push(ConsoleLine::new(
                format!("{}) {}", index + 1, cells.join(" ")),
                ConsoleTone::Result,
            ));
        }
    }

    if lines.is_empty() {
        lines.push(ConsoleLine::new(
            dbflux_i18n::t!("document.key_value.console.empty"),
            ConsoleTone::Muted,
        ));
    }

    if lines.len() > RESULT_LINE_LIMIT {
        let hidden = lines.len() - RESULT_LINE_LIMIT;
        lines.truncate(RESULT_LINE_LIMIT);
        lines.push(ConsoleLine::new(
            dbflux_i18n::t!("document.key_value.console.more_lines", count = hidden),
            ConsoleTone::Muted,
        ));
    }

    lines
}

/// Next history position for Up (`older = true`) or Down.
///
/// `None` means the input is past the newest entry (a fresh line).
pub(super) fn step_history(cursor: Option<usize>, len: usize, older: bool) -> Option<usize> {
    if len == 0 {
        return None;
    }

    match (cursor, older) {
        (None, true) => Some(len - 1),
        (None, false) => None,
        (Some(0), true) => Some(0),
        (Some(position), true) => Some(position - 1),
        (Some(position), false) if position + 1 < len => Some(position + 1),
        (Some(_), false) => None,
    }
}

/// Appends `command` to the history unless it repeats the newest entry.
pub(super) fn remember_command(history: &mut Vec<String>, command: &str) {
    if history.last().map(String::as_str) == Some(command) {
        return;
    }

    history.push(command.to_string());

    if history.len() > HISTORY_LIMIT {
        history.remove(0);
    }
}

/// Console state owned by the key-value document.
pub(super) struct ConsoleState {
    pub open: bool,
    pub input: Entity<InputState>,
    pub transcript: Vec<ConsoleEntry>,
    pub history: Vec<String>,
    pub history_cursor: Option<usize>,
    pub pending: Option<PendingConsoleCommand>,
    pub running: bool,
    pub scroll: ScrollHandle,
    _subscription: Subscription,
}

impl ConsoleState {
    pub(super) fn new(window: &mut Window, cx: &mut Context<super::KeyValueDocument>) -> Self {
        let input = cx.new(|cx| {
            InputState::new(window, cx)
                .placeholder(dbflux_i18n::t!("document.key_value.console.placeholder"))
        });

        let subscription =
            cx.subscribe_in(&input, window, |this, _, event: &InputEvent, window, cx| {
                if let InputEvent::PressEnter { .. } = event {
                    this.submit_console_command(window, cx);
                }
            });

        Self {
            open: false,
            input,
            transcript: Vec::new(),
            history: Vec::new(),
            history_cursor: None,
            pending: None,
            running: false,
            scroll: ScrollHandle::new(),
            _subscription: subscription,
        }
    }

    fn push_entry(&mut self, entry: ConsoleEntry) {
        self.transcript.push(entry);

        if self.transcript.len() > TRANSCRIPT_LIMIT {
            self.transcript.remove(0);
        }

        self.scroll.scroll_to_bottom();
    }
}

impl super::KeyValueDocument {
    fn console_prompt(&self) -> String {
        format!("{}>", self.database)
    }

    /// Shows or hides the console (`Ctrl+\``), moving focus into it when it
    /// opens.
    pub(super) fn toggle_console(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.console.open = !self.console.open;

        if self.console.open {
            self.console
                .input
                .update(cx, |state, cx| state.focus(window, cx));
            self.focus_mode = super::KeyValueFocusMode::TextInput;
        } else {
            self.focus_mode = super::KeyValueFocusMode::List;
            self.focus_handle.focus(window, cx);
        }

        cx.notify();
    }

    fn recall_history(&mut self, older: bool, window: &mut Window, cx: &mut Context<Self>) {
        let cursor = step_history(
            self.console.history_cursor,
            self.console.history.len(),
            older,
        );
        self.console.history_cursor = cursor;

        let text = cursor
            .and_then(|position| self.console.history.get(position))
            .cloned()
            .unwrap_or_default();

        self.console
            .input
            .update(cx, |state, cx| state.set_value(text, window, cx));
        cx.notify();
    }

    /// Runs the typed command after the validation and dangerous-query
    /// checks, or holds it for confirmation.
    pub(super) fn submit_console_command(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.console.running {
            return;
        }

        let command = self.console.input.read(cx).value().trim().to_string();
        if command.is_empty() {
            return;
        }

        remember_command(&mut self.console.history, &command);
        self.console.history_cursor = None;
        self.console
            .input
            .update(cx, |state, cx| state.set_value("", window, cx));

        let Some(connection) = self.get_connection(cx) else {
            self.report_connection_inactive(cx);
            return;
        };

        let language_service = connection.language_service();

        let validation_error = match language_service.validate(&command) {
            ValidationResult::Valid => None,
            ValidationResult::SyntaxError(diagnostic) => Some(diagnostic.message),
            ValidationResult::WrongLanguage { message, .. } => Some(message),
        };

        if let Some(message) = validation_error {
            let prompt = self.console_prompt();
            self.console.push_entry(ConsoleEntry {
                prompt,
                command,
                output: vec![ConsoleLine::new(message, ConsoleTone::Error)],
            });
            cx.notify();
            return;
        }

        let dangerous = language_service.detect_dangerous(&command);
        let classification =
            classify_query_for_language(&connection.metadata().query_language, &command);

        let gate = console_gate(dangerous, classification, |kind| {
            let state = self.app_state.read(cx);
            let is_suppressed = state.dangerous_query_suppressions().is_suppressed(kind);
            let effective = state.effective_settings_for_connection(Some(self.profile_id));
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
            ConsoleGate::Run => self.run_console_command(command, cx),
            ConsoleGate::Confirm { title, body } => {
                self.console.pending = Some(PendingConsoleCommand {
                    command,
                    title,
                    body,
                });
                cx.notify();
            }
            ConsoleGate::Refuse(message) => {
                let prompt = self.console_prompt();
                self.console.push_entry(ConsoleEntry {
                    prompt,
                    command,
                    output: vec![ConsoleLine::new(message, ConsoleTone::Error)],
                });
                cx.notify();
            }
        }
    }

    pub(super) fn confirm_console_command(&mut self, cx: &mut Context<Self>) {
        if let Some(pending) = self.console.pending.take() {
            self.run_console_command(pending.command, cx);
        }
    }

    pub(super) fn cancel_console_command(&mut self, cx: &mut Context<Self>) {
        if let Some(pending) = self.console.pending.take() {
            let prompt = self.console_prompt();
            self.console.push_entry(ConsoleEntry {
                prompt,
                command: pending.command,
                output: vec![ConsoleLine::new(
                    dbflux_i18n::t!("document.key_value.console.cancelled"),
                    ConsoleTone::Muted,
                )],
            });
        }
        cx.notify();
    }

    fn run_console_command(&mut self, command: String, cx: &mut Context<Self>) {
        let Some(connection) = self.get_connection(cx) else {
            self.report_connection_inactive(cx);
            return;
        };

        self.console.running = true;
        cx.notify();

        let prompt = self.console_prompt();
        let request = QueryRequest::new(command.clone()).with_database(Some(self.database.clone()));
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let result: Result<QueryResult, DbError> = cx
                .background_executor()
                .spawn(async move { connection.execute(&request) })
                .await;

            let output = match &result {
                Ok(result) => format_console_result(result),
                Err(error) => vec![ConsoleLine::new(error.to_string(), ConsoleTone::Error)],
            };

            cx.update(|cx| {
                entity.update(cx, |this, cx| {
                    this.console.running = false;
                    this.console.push_entry(ConsoleEntry {
                        prompt,
                        command,
                        output,
                    });

                    // A command may have changed the open key or the key
                    // list; the value is cheap to re-read.
                    if result.is_ok() {
                        this.reload_selected_value(cx);
                    }

                    cx.notify();
                });
            });
        })
        .detach();
    }

    pub(super) fn render_console(&self, cx: &mut Context<Self>) -> impl IntoElement + use<> {
        let theme = cx.theme();
        let tint = ChromeColors::tint(theme);
        let strong = ChromeColors::strong(theme);
        let muted = theme.muted_foreground;
        let open = self.console.open;

        let header = div()
            .id("kv-console-header")
            .flex()
            .flex_none()
            .items_center()
            .gap(KeyValueMetrics::CONSOLE_GAP)
            .h(KeyValueMetrics::CONSOLE_HEADER_HEIGHT)
            .px(KeyValueMetrics::CONSOLE_PADDING_X)
            .when(open, |header| {
                header.border_b_1().border_color(theme.border)
            })
            .text_size(FontSizes::XS)
            .cursor_pointer()
            .on_click(cx.listener(|this, _, window, cx| {
                this.toggle_console(window, cx);
            }))
            .child(
                Icon::new(if open {
                    AppIcon::ChevronDown
                } else {
                    AppIcon::ChevronRight
                })
                .size(KeyValueMetrics::CONSOLE_CHEVRON)
                .color(muted),
            )
            .child(
                Icon::new(AppIcon::SquareTerminal)
                    .size(KeyValueMetrics::CONSOLE_ICON)
                    .color(tint),
            )
            .child(
                Text::body(dbflux_i18n::t!("document.key_value.console.title"))
                    .font_size(FontSizes::XS)
                    .font_weight(FontWeight::SEMIBOLD)
                    .color(strong),
            )
            .child(
                Text::body(dbflux_i18n::t!(
                    "document.key_value.console.subtitle",
                    database = crate::key_value::parsing::database_label(&self.database)
                ))
                .font_size(FontSizes::XS)
                .color(muted),
            )
            .child(div().flex_1())
            .when_some(
                dbflux_ui_base::keymap::shortcut_label(
                    dbflux_app::keymap::ContextId::KeyValue,
                    dbflux_app::keymap::Command::ToggleConsole,
                ),
                |header, label| header.child(Kbd::new(label)),
            );

        let mut container = div()
            .id("kv-console")
            .key_context(dbflux_components::key_contexts::KEY_VALUE_CONSOLE)
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

        let result_color = theme.success;
        let error_color = theme.danger;

        let transcript = self
            .console
            .transcript
            .iter()
            .enumerate()
            .map(|(index, entry)| {
                div()
                    .id(("kv-console-entry", index))
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

        let pending = self.console.pending.clone();

        let body = div()
            .id("kv-console-body")
            .flex()
            .flex_col()
            .pt(Spacing::SM)
            .pb(KeyValueMetrics::CONSOLE_PADDING_BOTTOM)
            .px(KeyValueMetrics::CONSOLE_PADDING_X)
            .font_family(AppFonts::MONO)
            .text_size(KeyValueMetrics::CONSOLE_FONT)
            .line_height(KeyValueMetrics::CONSOLE_LINE_HEIGHT)
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
                    .id("kv-console-transcript")
                    .flex()
                    .flex_col()
                    .max_h(KeyValueMetrics::CONSOLE_TRANSCRIPT_HEIGHT)
                    .overflow_y_scroll()
                    .track_scroll(&self.console.scroll)
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
                                .size(KeyValueMetrics::CONSOLE_ICON)
                                .color(theme.warning),
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
                                "kv-console-run-anyway",
                                dbflux_i18n::t!("document.key_value.console.run_anyway"),
                            )
                            .danger()
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.confirm_console_command(cx);
                            })),
                        )
                        .child(
                            Button::new(
                                "kv-console-cancel",
                                dbflux_i18n::t!("document.key_value.console.cancel"),
                            )
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.cancel_console_command(cx);
                            })),
                        ),
                )
            })
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(div().text_color(tint).child(self.console_prompt()))
                    .child(
                        div()
                            .flex_1()
                            .on_mouse_down(
                                MouseButton::Left,
                                cx.listener(|this, _, _, cx| {
                                    this.focus_mode = super::KeyValueFocusMode::TextInput;
                                    cx.emit(DocumentEvent::RequestFocus);
                                    cx.stop_propagation();
                                }),
                            )
                            .child(
                                Input::new(&self.console.input)
                                    .id("kv-console-input")
                                    .appearance(false)
                                    .w_full(),
                            ),
                    )
                    .when(self.console.running, |row| {
                        row.child(
                            Icon::new(AppIcon::Loader)
                                .size(KeyValueMetrics::CONSOLE_ICON)
                                .color(muted),
                        )
                    }),
            );

        container = container.child(body);
        container
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ConsoleGate, ConsoleTone, HISTORY_LIMIT, RESULT_LINE_LIMIT, console_gate,
        format_console_result, remember_command, step_history,
    };
    use dbflux_core::{
        ColumnKind, ColumnMeta, DangerousAction, DangerousQueryKind, ExecutionClassification,
        QueryResult, Value, classify_query_for_language,
    };
    use std::time::Duration;

    #[test]
    fn flagged_commands_follow_the_shared_decision() {
        let confirm = console_gate(
            Some(DangerousQueryKind::RedisFlushDb),
            ExecutionClassification::Destructive,
            DangerousAction::Confirm,
        );
        assert!(matches!(confirm, ConsoleGate::Confirm { .. }));

        let refused = console_gate(
            Some(DangerousQueryKind::RedisFlushAll),
            ExecutionClassification::Destructive,
            |_| DangerousAction::Block("flush disabled".to_string()),
        );
        assert_eq!(refused, ConsoleGate::Refuse("flush disabled".to_string()));

        let allowed = console_gate(
            Some(DangerousQueryKind::RedisKeysPattern),
            ExecutionClassification::Read,
            |_| DangerousAction::Allow,
        );
        assert_eq!(allowed, ConsoleGate::Run);
    }

    #[test]
    fn destructive_commands_ask_even_when_the_driver_does_not_flag_them() {
        let gate = console_gate(None, ExecutionClassification::Destructive, |_| {
            DangerousAction::Allow
        });

        assert!(matches!(gate, ConsoleGate::Confirm { .. }));
    }

    #[test]
    fn ordinary_commands_run_without_asking() {
        let mut decided = false;
        let gate = console_gate(None, ExecutionClassification::Read, |_| {
            decided = true;
            DangerousAction::Allow
        });

        assert_eq!(gate, ConsoleGate::Run);
        assert!(!decided, "no decision is needed for an unflagged command");
    }

    #[test]
    fn redis_dangerous_commands_are_classified_by_the_core_dispatcher() {
        let language = dbflux_core::QueryLanguage::RedisCommands;

        assert_eq!(
            classify_query_for_language(&language, "FLUSHDB"),
            ExecutionClassification::Destructive
        );
        assert_eq!(
            classify_query_for_language(&language, "HGET user:1042 plan"),
            ExecutionClassification::Read
        );
    }

    #[test]
    fn text_results_print_line_by_line() {
        let result = QueryResult::text("line one\nline two".to_string(), Duration::ZERO);

        let lines = format_console_result(&result);

        assert_eq!(lines.len(), 2);
        assert_eq!(lines[1].text, "line two");
        assert_eq!(lines[1].tone, ConsoleTone::Result);
    }

    #[test]
    fn array_results_are_numbered_without_the_index_column() {
        let columns = vec![
            ColumnMeta {
                name: "#".to_string(),
                type_name: "int".to_string(),
                kind: ColumnKind::Integer,
                nullable: false,
                is_primary_key: false,
            },
            ColumnMeta {
                name: "value".to_string(),
                type_name: "redis".to_string(),
                kind: ColumnKind::Text,
                nullable: true,
                is_primary_key: false,
            },
        ];
        let rows = vec![
            vec![Value::Int(0), Value::Text("alpha".to_string())],
            vec![Value::Int(1), Value::Text("beta".to_string())],
        ];
        let result = QueryResult::table(columns, rows, None, Duration::ZERO);

        let lines: Vec<String> = format_console_result(&result)
            .into_iter()
            .map(|line| line.text)
            .collect();

        assert_eq!(lines, vec!["1) alpha", "2) beta"]);
    }

    #[test]
    fn binary_and_empty_results_are_summarized() {
        let binary = QueryResult::binary(vec![0, 1, 2], Duration::ZERO);
        let empty = QueryResult::empty();

        assert_eq!(format_console_result(&binary)[0].tone, ConsoleTone::Muted);
        assert_eq!(format_console_result(&empty)[0].tone, ConsoleTone::Muted);
    }

    #[test]
    fn long_results_are_cut_with_a_count_of_the_rest() {
        let body = (0..250)
            .map(|index| index.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        let result = QueryResult::text(body, Duration::ZERO);

        let lines = format_console_result(&result);

        assert_eq!(lines.len(), RESULT_LINE_LIMIT + 1);
        assert!(lines[RESULT_LINE_LIMIT].text.contains("50"));
    }

    #[test]
    fn history_steps_back_and_forward() {
        assert_eq!(step_history(None, 0, true), None);
        assert_eq!(step_history(None, 3, true), Some(2));
        assert_eq!(step_history(Some(2), 3, true), Some(1));
        assert_eq!(step_history(Some(0), 3, true), Some(0));
        assert_eq!(step_history(Some(1), 3, false), Some(2));
        assert_eq!(step_history(Some(2), 3, false), None);
        assert_eq!(step_history(None, 3, false), None);
    }

    #[test]
    fn history_skips_repeats_and_keeps_a_bounded_length() {
        let mut history = Vec::new();

        remember_command(&mut history, "PING");
        remember_command(&mut history, "PING");
        remember_command(&mut history, "DBSIZE");
        assert_eq!(history, vec!["PING", "DBSIZE"]);

        for index in 0..HISTORY_LIMIT {
            remember_command(&mut history, &format!("GET {index}"));
        }
        assert_eq!(history.len(), HISTORY_LIMIT);
        assert_eq!(history.last().map(String::as_str), Some("GET 99"));
    }

    /// The key-value keys come from the keymap: the console toggle works
    /// from the document and from the console input, and a letter the
    /// document binds is typed into a text field instead of running.
    #[gpui::test]
    fn key_value_keys_run_from_the_keymap(cx: &mut gpui::TestAppContext) {
        use crate::key_value::KeyValueDocument;
        use crate::keyboard_test_support::{host_document, init_keyboard_runtime};
        use gpui::{AppContext as _, Focusable as _};

        init_keyboard_runtime(cx);
        let app_state: gpui::Entity<dbflux_ui_base::AppStateEntity> = cx.update(|cx| {
            cx.new(|_| {
                let runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                    .expect("in-memory storage");
                dbflux_ui_base::AppStateEntity::new_with_storage_runtime(runtime)
                    .expect("test storage setup")
            })
        });

        let (host, window) = host_document(
            cx,
            move |window, cx| {
                cx.new(|cx| {
                    KeyValueDocument::new(uuid::Uuid::nil(), "0".to_string(), app_state, window, cx)
                })
            },
            |document, cx| document.active_context(cx),
            KeyValueDocument::dispatch_command,
        );
        let document = window.update(|_, cx| host.read(cx).document.clone());

        window.update(|window, cx| document.update(cx, |doc, cx| doc.focus(window, cx)));
        window.run_until_parked();

        window.simulate_keystrokes("ctrl-`");
        assert!(
            window.update(|_, cx| document.read(cx).console.open),
            "Ctrl+` opens the console"
        );
        let console_focused = window.update(|window, cx| {
            document
                .read(cx)
                .console
                .input
                .read(cx)
                .focus_handle(cx)
                .is_focused(window)
        });
        assert!(console_focused, "the console input takes focus");

        window.simulate_keystrokes("ctrl-`");
        assert!(
            !window.update(|_, cx| document.read(cx).console.open),
            "Ctrl+` closes it again from its input"
        );

        window.update(|window, cx| {
            let filter = document.read(cx).filter_input.clone();
            filter.update(cx, |state, cx| state.focus(window, cx));
        });
        window.run_until_parked();
        window.simulate_keystrokes("t");

        assert_eq!(
            window.update(|_, cx| document.read(cx).filter_input.read(cx).value().to_string()),
            "t",
            "`t` in the filter is text, not the expiry editor"
        );
        assert!(
            window.update(|_, cx| document.read(cx).expiry_editor.is_none()),
            "the expiry editor stays closed"
        );
    }
}
