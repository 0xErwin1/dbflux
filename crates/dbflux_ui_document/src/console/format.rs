//! Driver-neutral rules of the native console: what happens to a command
//! before it runs, how a result prints, and how Up/Down recall walks the
//! history. Kept free of GPUI so it can be tested directly.

use dbflux_core::{
    DangerousAction, DangerousQueryKind, ExecutionClassification, QueryResult, QueryResultShape,
    Value,
};

/// Commands offered by Up/Down recall.
pub(crate) const HISTORY_LIMIT: usize = 100;

/// Lines of one result shown before the rest is summarized.
pub(crate) const RESULT_LINE_LIMIT: usize = 200;

/// Characters of one table cell before it is cut.
const CELL_WIDTH_LIMIT: usize = 40;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConsoleTone {
    Result,
    Error,
    Muted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ConsoleLine {
    pub text: String,
    pub tone: ConsoleTone,
}

impl ConsoleLine {
    pub(crate) fn new(text: impl Into<String>, tone: ConsoleTone) -> Self {
        Self {
            text: text.into(),
            tone,
        }
    }
}

/// What happens to a command before it runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ConsoleGate {
    /// Runs now. `ceiling` is the governance ceiling the settings already
    /// authorised (a flagged command that the settings allow).
    Run {
        ceiling: Option<ExecutionClassification>,
    },
    /// Asks first; `ceiling` is what confirming authorises.
    Confirm {
        title: String,
        body: String,
        kind: Option<DangerousQueryKind>,
        ceiling: ExecutionClassification,
    },
    Refuse(String),
}

/// Applies the dangerous-query rules to a console command.
///
/// `dangerous` is the driver's verdict (`LanguageService::detect_dangerous`)
/// and `decide` the shared settings-based decision for it. A command the
/// driver does not flag still asks first when `classify_query_for_language`
/// rates it as destructive.
///
/// The ceilings mirror the code editor: a flagged command that runs, after
/// confirmation or because the settings allow it, carries `Destructive`
/// (see `QueryRequest::confirmed_ceiling`); an unflagged destructive command
/// carries its own classification once confirmed.
pub(crate) fn console_gate(
    dangerous: Option<DangerousQueryKind>,
    classification: ExecutionClassification,
    decide: impl FnOnce(DangerousQueryKind) -> DangerousAction,
) -> ConsoleGate {
    if let Some(kind) = dangerous {
        return match decide(kind) {
            DangerousAction::Allow => ConsoleGate::Run {
                ceiling: Some(ExecutionClassification::Destructive),
            },
            DangerousAction::Confirm(kind) => ConsoleGate::Confirm {
                title: crate::labels::dangerous_query_title(kind),
                body: crate::labels::dangerous_query_body(kind),
                kind: Some(kind),
                ceiling: ExecutionClassification::Destructive,
            },
            DangerousAction::Block(message) => ConsoleGate::Refuse(message),
        };
    }

    if matches!(
        classification,
        ExecutionClassification::Destructive | ExecutionClassification::AdminDestructive
    ) {
        return ConsoleGate::Confirm {
            title: dbflux_i18n::t!("document.console.destructive_title"),
            body: dbflux_i18n::t!("document.console.destructive_body"),
            kind: None,
            ceiling: classification,
        };
    }

    ConsoleGate::Run { ceiling: None }
}

/// A result as console lines, in the style of a command-line client: text
/// as printed, documents as one JSON object per line, a single column as a
/// numbered list, several columns as an aligned table, binary summarized.
/// Every result set of the batch is printed, each after a separator.
pub(crate) fn format_console_result(result: &QueryResult) -> Vec<ConsoleLine> {
    let sets: Vec<&QueryResult> = result.iter_result_sets().collect();
    let numbered = sets.len() > 1;
    let mut lines: Vec<ConsoleLine> = Vec::new();

    for (index, set) in sets.into_iter().enumerate() {
        if numbered {
            lines.push(ConsoleLine::new(
                dbflux_i18n::t!("document.console.result_set", index = index + 1),
                ConsoleTone::Muted,
            ));
        }

        lines.extend(format_result_set(set));
    }

    if lines.len() > RESULT_LINE_LIMIT {
        let hidden = lines.len() - RESULT_LINE_LIMIT;
        lines.truncate(RESULT_LINE_LIMIT);
        lines.push(ConsoleLine::new(
            dbflux_i18n::t!("document.console.more_lines", count = hidden),
            ConsoleTone::Muted,
        ));
    }

    lines
}

fn format_result_set(result: &QueryResult) -> Vec<ConsoleLine> {
    if let Some(text) = &result.text_body {
        let lines: Vec<ConsoleLine> = text
            .lines()
            .map(|line| ConsoleLine::new(line, ConsoleTone::Result))
            .collect();

        return non_empty_or_placeholder(lines);
    }

    if let Some(bytes) = &result.raw_bytes {
        return vec![ConsoleLine::new(
            dbflux_i18n::t!("document.console.binary", count = bytes.len()),
            ConsoleTone::Muted,
        )];
    }

    if result.rows.is_empty() {
        let summary = match result.affected_rows {
            Some(count) => dbflux_i18n::t!("document.console.affected", count = count),
            None => dbflux_i18n::t!("document.console.empty"),
        };

        return vec![ConsoleLine::new(summary, ConsoleTone::Muted)];
    }

    if result.shape == QueryResultShape::Json {
        return format_documents(result);
    }

    format_table(result)
}

fn non_empty_or_placeholder(lines: Vec<ConsoleLine>) -> Vec<ConsoleLine> {
    if lines.is_empty() {
        return vec![ConsoleLine::new(
            dbflux_i18n::t!("document.console.empty"),
            ConsoleTone::Muted,
        )];
    }

    lines
}

/// One compact JSON object per row, keyed by column name.
fn format_documents(result: &QueryResult) -> Vec<ConsoleLine> {
    result
        .rows
        .iter()
        .map(|row| {
            let object: serde_json::Map<String, serde_json::Value> = result
                .columns
                .iter()
                .zip(row)
                .map(|(column, value)| (column.name.clone(), Value::to_serde_json(value)))
                .collect();

            ConsoleLine::new(
                serde_json::Value::Object(object).to_string(),
                ConsoleTone::Result,
            )
        })
        .collect()
}

fn format_table(result: &QueryResult) -> Vec<ConsoleLine> {
    let skipped = ordinal_column(result);

    let visible: Vec<usize> = (0..result.columns.len())
        .filter(|index| Some(*index) != skipped)
        .collect();

    let rows: Vec<Vec<String>> = result
        .rows
        .iter()
        .map(|row| {
            visible
                .iter()
                .map(|index| row.get(*index).map(cell_text).unwrap_or_default())
                .collect()
        })
        .collect();

    if visible.len() == 1 {
        return rows
            .into_iter()
            .enumerate()
            .map(|(index, cells)| {
                ConsoleLine::new(
                    format!("{}) {}", index + 1, cells.concat()),
                    ConsoleTone::Result,
                )
            })
            .collect();
    }

    let header: Vec<String> = visible
        .iter()
        .map(|index| clip(&result.columns[*index].name))
        .collect();

    let mut widths: Vec<usize> = header.iter().map(|name| name.chars().count()).collect();
    for cells in &rows {
        for (width, cell) in widths.iter_mut().zip(cells) {
            *width = (*width).max(cell.chars().count());
        }
    }

    let mut lines = vec![ConsoleLine::new(
        aligned_row(&header, &widths),
        ConsoleTone::Muted,
    )];

    lines.extend(
        rows.iter()
            .map(|cells| ConsoleLine::new(aligned_row(cells, &widths), ConsoleTone::Result)),
    );

    lines
}

/// The index of a leading column that only numbers the rows (0-based or
/// 1-based), which the numbered list already shows. Decided from the values,
/// not the column name, so it holds for any driver.
fn ordinal_column(result: &QueryResult) -> Option<usize> {
    if result.columns.len() < 2 {
        return None;
    }

    let firsts: Vec<Option<&Value>> = result.rows.iter().map(|row| row.first()).collect();

    let matches_offset = |offset: i64| {
        firsts.iter().enumerate().all(|(index, value)| {
            matches!(value, Some(Value::Int(number)) if *number == index as i64 + offset)
        })
    };

    (matches_offset(0) || matches_offset(1)).then_some(0)
}

fn cell_text(value: &Value) -> String {
    let text = if value.is_complex() {
        value.to_json_string()
    } else {
        value.as_display_string()
    };

    clip(&text.replace(['\r', '\n'], " "))
}

fn clip(text: &str) -> String {
    if text.chars().count() <= CELL_WIDTH_LIMIT {
        return text.to_string();
    }

    let mut clipped: String = text.chars().take(CELL_WIDTH_LIMIT - 1).collect();
    clipped.push('…');
    clipped
}

fn aligned_row(cells: &[String], widths: &[usize]) -> String {
    let padded: Vec<String> = cells
        .iter()
        .zip(widths)
        .map(|(cell, width)| format!("{cell:<width$}"))
        .collect();

    padded.join("  ").trim_end().to_string()
}

/// Next history position for Up (`older = true`) or Down.
///
/// `None` means the input is past the newest entry (a fresh line).
pub(crate) fn step_history(cursor: Option<usize>, len: usize, older: bool) -> Option<usize> {
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

/// The commands Up/Down walks through, oldest first.
///
/// `shared` is the connection's entries in the shared query history
/// (newest first, as the history manager keeps them) and `unrecorded` the
/// commands this console ran that never reached it (refused, cancelled or
/// failed), oldest first. Both carry their Unix timestamp. Multi-line
/// entries (editor scripts) are left out because the console takes one
/// line; consecutive repeats collapse.
pub(crate) fn recall_list<'a>(
    shared: impl IntoIterator<Item = (i64, &'a str)>,
    unrecorded: &[(i64, String)],
) -> Vec<String> {
    let mut entries: Vec<(i64, &str)> = shared.into_iter().collect();
    entries.reverse();
    entries.extend(
        unrecorded
            .iter()
            .map(|(timestamp, command)| (*timestamp, command.as_str())),
    );
    entries.sort_by_key(|(timestamp, _)| *timestamp);

    let mut commands: Vec<String> = Vec::new();
    for (_, command) in entries {
        let command = command.trim();

        if command.is_empty() || command.contains('\n') {
            continue;
        }

        if commands.last().map(String::as_str) == Some(command) {
            continue;
        }

        commands.push(command.to_string());
    }

    let excess = commands.len().saturating_sub(HISTORY_LIMIT);
    commands.drain(..excess);
    commands
}

#[cfg(test)]
mod tests {
    use super::{
        ConsoleGate, ConsoleTone, HISTORY_LIMIT, RESULT_LINE_LIMIT, console_gate,
        format_console_result, recall_list, step_history,
    };
    use dbflux_core::{
        ColumnKind, ColumnMeta, DangerousAction, DangerousQueryKind, ExecutionClassification,
        QueryResult, Value, classify_query_for_language,
    };
    use std::collections::BTreeMap;
    use std::time::Duration;

    fn column(name: &str, kind: ColumnKind) -> ColumnMeta {
        ColumnMeta {
            name: name.to_string(),
            type_name: "test".to_string(),
            kind,
            nullable: true,
            is_primary_key: false,
        }
    }

    fn texts(result: &QueryResult) -> Vec<String> {
        format_console_result(result)
            .into_iter()
            .map(|line| line.text)
            .collect()
    }

    #[test]
    fn flagged_commands_follow_the_shared_decision() {
        let confirm = console_gate(
            Some(DangerousQueryKind::RedisFlushDb),
            ExecutionClassification::Destructive,
            DangerousAction::Confirm,
        );
        assert!(matches!(
            confirm,
            ConsoleGate::Confirm {
                kind: Some(DangerousQueryKind::RedisFlushDb),
                ceiling: ExecutionClassification::Destructive,
                ..
            }
        ));

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
        assert_eq!(
            allowed,
            ConsoleGate::Run {
                ceiling: Some(ExecutionClassification::Destructive)
            },
            "a flagged command the settings allow carries the editor's ceiling"
        );
    }

    #[test]
    fn destructive_commands_ask_even_when_the_driver_does_not_flag_them() {
        let gate = console_gate(None, ExecutionClassification::AdminDestructive, |_| {
            DangerousAction::Allow
        });

        assert!(matches!(
            gate,
            ConsoleGate::Confirm {
                kind: None,
                ceiling: ExecutionClassification::AdminDestructive,
                ..
            }
        ));
    }

    #[test]
    fn ordinary_commands_run_without_asking_or_a_ceiling() {
        let mut decided = false;
        let gate = console_gate(None, ExecutionClassification::Read, |_| {
            decided = true;
            DangerousAction::Allow
        });

        assert_eq!(gate, ConsoleGate::Run { ceiling: None });
        assert!(!decided, "no decision is needed for an unflagged command");
    }

    #[test]
    fn dangerous_commands_are_classified_by_the_core_dispatcher() {
        let redis = dbflux_core::QueryLanguage::RedisCommands;
        assert_eq!(
            classify_query_for_language(&redis, "FLUSHDB"),
            ExecutionClassification::Destructive
        );
        assert_eq!(
            classify_query_for_language(&redis, "HGET user:1042 plan"),
            ExecutionClassification::Read
        );

        let mongo = dbflux_core::QueryLanguage::MongoQuery;
        assert_eq!(
            classify_query_for_language(&mongo, "db.users.find({})"),
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
    fn an_ordinal_column_is_dropped_for_a_numbered_list() {
        let columns = vec![
            column("#", ColumnKind::Integer),
            column("value", ColumnKind::Text),
        ];
        let rows = vec![
            vec![Value::Int(0), Value::Text("alpha".to_string())],
            vec![Value::Int(1), Value::Text("beta".to_string())],
        ];
        let result = QueryResult::table(columns, rows, None, Duration::ZERO);

        assert_eq!(texts(&result), vec!["1) alpha", "2) beta"]);
    }

    #[test]
    fn an_integer_column_that_is_not_an_ordinal_is_kept() {
        let columns = vec![
            column("id", ColumnKind::Integer),
            column("name", ColumnKind::Text),
        ];
        let rows = vec![
            vec![Value::Int(7), Value::Text("alpha".to_string())],
            vec![Value::Int(42), Value::Text("beta".to_string())],
        ];
        let result = QueryResult::table(columns, rows, None, Duration::ZERO);

        assert_eq!(texts(&result), vec!["id  name", "7   alpha", "42  beta"]);
        assert_eq!(format_console_result(&result)[0].tone, ConsoleTone::Muted);
    }

    #[test]
    fn documents_print_as_one_json_object_per_line() {
        let mut address = BTreeMap::new();
        address.insert("city".to_string(), Value::Text("Lima".to_string()));
        let columns = vec![
            column("_id", ColumnKind::Unknown),
            column("address", ColumnKind::Unknown),
        ];
        let rows = vec![vec![
            Value::ObjectId("64b0".to_string()),
            Value::Document(address),
        ]];
        let result = QueryResult::json(columns, rows, Duration::ZERO);

        assert_eq!(
            texts(&result),
            vec![r#"{"_id":{"$oid":"64b0"},"address":{"city":"Lima"}}"#]
        );
    }

    #[test]
    fn every_result_set_is_printed_after_a_separator() {
        let mut result = QueryResult::text("first".to_string(), Duration::ZERO);
        result.push_additional_result(QueryResult::text("second".to_string(), Duration::ZERO));

        let lines = format_console_result(&result);

        assert_eq!(lines.len(), 4);
        assert_eq!(lines[0].tone, ConsoleTone::Muted);
        assert_eq!(lines[1].text, "first");
        assert_eq!(lines[3].text, "second");
    }

    #[test]
    fn binary_empty_and_write_results_are_summarized() {
        let binary = QueryResult::binary(vec![0, 1, 2], Duration::ZERO);
        let empty = QueryResult::empty();
        let mut write = QueryResult::json(Vec::new(), Vec::new(), Duration::ZERO);
        write.affected_rows = Some(3);

        assert_eq!(format_console_result(&binary)[0].tone, ConsoleTone::Muted);
        assert_eq!(format_console_result(&empty)[0].tone, ConsoleTone::Muted);
        assert!(texts(&write)[0].contains('3'));
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
    fn recall_merges_shared_and_unrecorded_commands_in_time_order() {
        let shared = vec![(30, "DBSIZE"), (10, "PING")];
        let unrecorded = vec![(20, "BADCMD".to_string()), (40, "DBSIZE".to_string())];

        let commands = recall_list(shared, &unrecorded);

        assert_eq!(commands, vec!["PING", "BADCMD", "DBSIZE"]);
    }

    #[test]
    fn recall_skips_multi_line_entries_and_keeps_a_bounded_length() {
        let shared: Vec<(i64, String)> = (0..HISTORY_LIMIT as i64 + 5)
            .rev()
            .map(|index| (index, format!("GET {index}")))
            .chain(std::iter::once((
                -1,
                "db.a.find()\ndb.b.find()".to_string(),
            )))
            .collect();

        let commands = recall_list(
            shared
                .iter()
                .map(|(timestamp, command)| (*timestamp, command.as_str())),
            &[],
        );

        assert_eq!(commands.len(), HISTORY_LIMIT);
        assert_eq!(commands.last().map(String::as_str), Some("GET 104"));
        assert!(commands.iter().all(|command| !command.contains('\n')));
    }
}
