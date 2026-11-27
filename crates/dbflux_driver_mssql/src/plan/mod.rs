//! Structured query plan for SQL Server, built from `SET SHOWPLAN_XML`.
//!
//! The estimated plan is produced by compiling the statement with
//! `SHOWPLAN_XML` on, so the statement never runs. That setting is session
//! state: [`capture_showplan`] is the only place that turns it on, and it
//! turns it off again on every path.

use dbflux_core::{DbError, PlanFormat, PlanNode, QueryPlan};
use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};

mod condition;
mod glossary;

use condition::{ConditionCapture, SEEK_LABEL};

const SHOWPLAN_ON: &str = "SET SHOWPLAN_XML ON";
const SHOWPLAN_OFF: &str = "SET SHOWPLAN_XML OFF";

/// One session able to run T-SQL batches, returning the first cell of the
/// first row of every result set the batch produced, in order.
pub(crate) trait ShowplanSession {
    async fn batch(&mut self, sql: &str) -> Result<Vec<String>, DbError>;
}

/// Outcome of [`capture_showplan`]. `restore` is the result of turning
/// `SHOWPLAN_XML` off; when it is an error the session may still be in plan
/// mode and the caller must not reuse it.
pub(crate) struct ShowplanRun {
    pub plan: Result<Vec<String>, DbError>,
    pub restore: Result<(), DbError>,
}

/// Runs `SET SHOWPLAN_XML ON`, the statement and `SET SHOWPLAN_XML OFF` as one
/// unit. OFF is sent even when ON or the statement failed, because a failed ON
/// may still have taken effect.
pub(crate) async fn capture_showplan(
    session: &mut impl ShowplanSession,
    statement: &str,
) -> ShowplanRun {
    let plan = match session.batch(SHOWPLAN_ON).await {
        Ok(_) => session.batch(statement).await.and_then(|plans| {
            if plans.is_empty() {
                Err(DbError::query_failed("SHOWPLAN_XML returned no plan"))
            } else {
                Ok(plans)
            }
        }),
        Err(error) => Err(error),
    };

    let restore = session.batch(SHOWPLAN_OFF).await.map(|_| ());

    ShowplanRun { plan, restore }
}

/// Runs `on_drop` when dropped unless disarmed. Held across the ON..OFF window
/// so an unwind cannot leave a session in plan mode.
pub(crate) struct RestoreGuard<F: FnMut()> {
    on_drop: F,
    armed: bool,
}

impl<F: FnMut()> RestoreGuard<F> {
    pub(crate) fn new(on_drop: F) -> Self {
        Self {
            on_drop,
            armed: true,
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl<F: FnMut()> Drop for RestoreGuard<F> {
    fn drop(&mut self) {
        if self.armed {
            (self.on_drop)();
        }
    }
}

/// Returns the single statement of `query`.
///
/// Statements are separated by `;` and by `GO` lines. This mirrors the
/// single-statement guard of the other drivers and keeps their messages
/// verbatim; it exists locally because the shared SQL splitter does not know
/// `GO` or bracketed identifiers.
pub(crate) fn plan_statement(query: &str) -> Result<String, DbError> {
    let mut statements = split_batch_statements(query);

    match statements.len() {
        0 => Err(DbError::query_failed(
            "A query plan needs a statement; the query is empty.",
        )),
        1 => Ok(statements.remove(0)),
        count => Err(DbError::query_failed(format!(
            "A query plan covers exactly one statement; the query has {count}. Select a single statement."
        ))),
    }
}

#[derive(Default)]
struct StatementCollector {
    statements: Vec<String>,
    segment_start: usize,
    has_content: bool,
}

impl StatementCollector {
    /// Ends the current segment at `end`; the next one starts at `next_start`.
    fn finish(&mut self, sql: &str, end: usize, next_start: usize) {
        if self.has_content
            && let Some(text) = sql.get(self.segment_start..end)
        {
            self.statements.push(text.trim().to_string());
        }
        self.has_content = false;
        self.segment_start = next_start;
    }
}

fn split_batch_statements(sql: &str) -> Vec<String> {
    let bytes = sql.as_bytes();
    let mut collector = StatementCollector::default();
    let mut index = 0;
    let mut line_start = true;

    while let Some(&byte) = bytes.get(index) {
        if line_start {
            let line_end = line_end(sql, index);

            if sql.get(index..line_end).is_some_and(is_go_line) {
                collector.finish(sql, index, line_end);
                index = line_end;
                continue;
            }
            line_start = false;
        }

        match byte {
            b'\n' => {
                line_start = true;
                index += 1;
            }
            b'-' if bytes.get(index + 1) == Some(&b'-') => index = line_end(sql, index),
            b'/' if bytes.get(index + 1) == Some(&b'*') => {
                index = skip_block_comment(bytes, index);
            }
            b'\'' | b'"' => {
                collector.has_content = true;
                index = skip_delimited(bytes, index, byte);
            }
            b'[' => {
                collector.has_content = true;
                index = skip_delimited(bytes, index, b']');
            }
            b';' => {
                // The terminator stays with its statement: T-SQL requires it
                // after MERGE.
                collector.finish(sql, index + 1, index + 1);
                index += 1;
            }
            other => {
                collector.has_content |= !other.is_ascii_whitespace();
                index += 1;
            }
        }
    }

    collector.finish(sql, sql.len(), sql.len());
    collector.statements
}

/// Index of the newline ending the line that contains `from`, or the end of `sql`.
fn line_end(sql: &str, from: usize) -> usize {
    sql.get(from..)
        .and_then(|rest| rest.find('\n'))
        .map_or(sql.len(), |offset| from + offset)
}

/// A `GO` batch separator: the word alone on its line, optionally with a count.
fn is_go_line(line: &str) -> bool {
    let line = line.trim();
    let Some((word, rest)) = line.split_at_checked(2) else {
        return false;
    };

    word.eq_ignore_ascii_case("GO")
        && (rest.is_empty()
            || (rest.starts_with(char::is_whitespace)
                && rest.trim().chars().all(|c| c.is_ascii_digit())))
}

/// Index just past the block comment at `start`; comments nest in T-SQL.
fn skip_block_comment(bytes: &[u8], start: usize) -> usize {
    let mut depth = 0usize;
    let mut index = start;

    while let Some(&byte) = bytes.get(index) {
        match (byte, bytes.get(index + 1)) {
            (b'/', Some(b'*')) => {
                depth += 1;
                index += 2;
            }
            (b'*', Some(b'/')) => {
                depth = depth.saturating_sub(1);
                index += 2;
                if depth == 0 {
                    return index;
                }
            }
            _ => index += 1,
        }
    }

    bytes.len()
}

/// Index just past the delimited token at `start`, where a doubled closing
/// delimiter is an escape.
fn skip_delimited(bytes: &[u8], start: usize, closing: u8) -> usize {
    let mut index = start + 1;

    while let Some(&byte) = bytes.get(index) {
        if byte == closing {
            if bytes.get(index + 1) == Some(&closing) {
                index += 2;
                continue;
            }
            return index + 1;
        }
        index += 1;
    }

    bytes.len()
}

type Attributes = Vec<(String, String)>;

fn attribute<'a>(attributes: &'a Attributes, name: &str) -> Option<&'a str> {
    attributes
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
}

fn invalid(detail: impl std::fmt::Display) -> DbError {
    DbError::query_failed(format!("SHOWPLAN_XML returned an invalid plan: {detail}"))
}

fn element_attributes(element: &BytesStart<'_>) -> Result<Attributes, DbError> {
    element
        .attributes()
        .map(|attribute| {
            let attribute = attribute.map_err(invalid)?;
            let key = String::from_utf8_lossy(attribute.key.local_name().as_ref()).into_owned();
            let value = attribute.unescape_value().map_err(invalid)?.into_owned();
            Ok((key, value))
        })
        .collect()
}

/// `[dbo]` -> `dbo`; anything not wrapped in brackets is kept as is.
fn unbracket(name: &str) -> &str {
    name.strip_prefix('[')
        .and_then(|inner| inner.strip_suffix(']'))
        .unwrap_or(name)
}

/// Operator attributes surfaced as node details, in display order.
const OPERATOR_DETAILS: [(&str, &str); 6] = [
    ("LogicalOp", "Logical Operation"),
    ("EstimateIO", "Estimate IO"),
    ("EstimateCPU", "Estimate CPU"),
    ("AvgRowSize", "Average Row Size"),
    ("EstimatedExecutionMode", "Execution Mode"),
    ("Parallel", "Parallel"),
];

fn relop_node(attributes: &Attributes) -> Result<PlanNode, DbError> {
    let operation = attribute(attributes, "PhysicalOp")
        .or_else(|| attribute(attributes, "LogicalOp"))
        .filter(|name| !name.is_empty())
        .ok_or_else(|| invalid("a RelOp has neither PhysicalOp nor LogicalOp"))?;

    let number = |name: &str| attribute(attributes, name).and_then(|v| v.parse::<f64>().ok());

    let mut node = PlanNode::new(operation);
    node.description = glossary::describe(operation).map(str::to_string);
    node.estimated_rows = number("EstimateRows");
    node.estimated_total_cost = number("EstimatedTotalSubtreeCost");
    node.details = OPERATOR_DETAILS
        .iter()
        .filter_map(|(key, label)| {
            attribute(attributes, key).map(|value| (label.to_string(), value.to_string()))
        })
        .collect();

    Ok(node)
}

fn apply_object(node: &mut PlanNode, attributes: &Attributes) {
    if node.relation.is_some() || node.index.is_some() {
        return;
    }

    if let Some(table) = attribute(attributes, "Table") {
        node.relation = Some(match attribute(attributes, "Schema") {
            Some(schema) => format!("{}.{}", unbracket(schema), unbracket(table)),
            None => unbracket(table).to_string(),
        });
    }
    node.index = attribute(attributes, "Index").map(|index| unbracket(index).to_string());

    for (key, label) in [("Alias", "Alias"), ("IndexKind", "Index Kind")] {
        if let Some(value) = attribute(attributes, key) {
            node.details
                .push((label.to_string(), unbracket(value).to_string()));
        }
    }
}

struct WarningsContext {
    depth: usize,
    entry: Option<(String, Vec<String>)>,
}

#[derive(Default)]
struct MissingIndex {
    target: String,
    impact: Option<String>,
    usage: Option<String>,
    groups: Vec<(String, Vec<String>)>,
}

impl MissingIndex {
    fn describe(&self) -> String {
        let groups = self
            .groups
            .iter()
            .map(|(usage, columns)| format!("{} ({})", usage.to_lowercase(), columns.join(", ")))
            .collect::<Vec<_>>()
            .join("; ");
        let impact = self
            .impact
            .as_ref()
            .map(|impact| format!(" (impact {impact}%)"))
            .unwrap_or_default();

        format!("Missing index on {}{impact}: {groups}", self.target)
    }
}

#[derive(Default)]
struct ShowplanParser {
    path: Vec<String>,
    open_nodes: Vec<PlanNode>,
    roots: Vec<PlanNode>,
    plan_warnings: Vec<String>,
    warnings: Option<WarningsContext>,
    missing_group_impact: Option<String>,
    missing_index: Option<MissingIndex>,
    top_level_statements: usize,
    conditions: ConditionCapture,
}

fn attribute_texts(attributes: &Attributes) -> Vec<String> {
    attributes
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect()
}

impl ShowplanParser {
    fn open(&mut self, name: &str, attributes: &Attributes) -> Result<(), DbError> {
        self.path.push(name.to_string());

        if self.path.len() == 1 && name != "ShowPlanXML" {
            return Err(invalid(format!(
                "the root element is `{name}`, not `ShowPlanXML`"
            )));
        }

        if let Some(context) = self.warnings.as_mut()
            && self.path.len() > context.depth
        {
            if self.path.len() == context.depth + 1 {
                context.entry = Some((name.to_string(), attribute_texts(attributes)));
            } else if let Some((_, details)) = context.entry.as_mut() {
                details.extend(attribute_texts(attributes));
            }
        }

        if name.starts_with("Stmt") && self.is_batch_statement() {
            self.top_level_statements += 1;
        }

        self.conditions
            .open(name, attributes, &self.path, self.open_nodes.len());

        match name {
            "RelOp" => self.open_nodes.push(relop_node(attributes)?),
            "Object" => {
                if let Some(node) = self.open_nodes.last_mut() {
                    apply_object(node, attributes);
                }
            }
            "Warnings" => {
                self.warnings = Some(WarningsContext {
                    depth: self.path.len(),
                    entry: None,
                });
                for (key, value) in attributes {
                    if value == "true" || value == "1" {
                        self.push_warning(key.clone());
                    }
                }
            }
            "MissingIndexGroup" => {
                self.missing_group_impact = attribute(attributes, "Impact").map(str::to_string);
            }
            "MissingIndex" => {
                let parts: Vec<&str> = ["Database", "Schema", "Table"]
                    .iter()
                    .filter_map(|key| attribute(attributes, key))
                    .collect();
                self.missing_index = Some(MissingIndex {
                    target: parts.join("."),
                    impact: self.missing_group_impact.clone(),
                    ..MissingIndex::default()
                });
            }
            "ColumnGroup" => {
                if let Some(missing) = self.missing_index.as_mut() {
                    let usage = attribute(attributes, "Usage")
                        .unwrap_or("UNKNOWN")
                        .to_string();
                    missing.usage = Some(usage.clone());
                    missing.groups.push((usage, Vec::new()));
                }
            }
            "Column" => {
                if let Some(missing) = self.missing_index.as_mut()
                    && missing.usage.is_some()
                    && let Some(column) = attribute(attributes, "Name")
                    && let Some((_, columns)) = missing.groups.last_mut()
                {
                    columns.push(column.to_string());
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// Whether the element just opened is a direct child of a batch's
    /// `Statements`; statements nested in an `IF` or loop body are not.
    fn is_batch_statement(&self) -> bool {
        let depth = self.path.len();
        depth >= 3
            && self.path.get(depth - 2).map(String::as_str) == Some("Statements")
            && self.path.get(depth - 3).map(String::as_str) == Some("Batch")
    }

    fn close(&mut self, name: &str) -> Result<(), DbError> {
        let finished_entry = match self.warnings.as_mut() {
            Some(context) if self.path.len() == context.depth + 1 => context.entry.take(),
            _ => None,
        };

        if let Some((entry_name, details)) = finished_entry {
            self.push_warning(if details.is_empty() {
                entry_name
            } else {
                format!("{entry_name} ({})", details.join(", "))
            });
        }

        if let Some(condition) = self.conditions.close(name, &self.path)
            && let Some(node) = self.open_nodes.last_mut()
            && (condition.label == SEEK_LABEL || node.key_condition.is_none())
        {
            node.key_condition = Some(condition);
        }

        match name {
            "RelOp" => {
                let node = self
                    .open_nodes
                    .pop()
                    .ok_or_else(|| invalid("unbalanced RelOp"))?;
                match self.open_nodes.last_mut() {
                    Some(parent) => parent.children.push(node),
                    None => self.roots.push(node),
                }
            }
            "Warnings" => self.warnings = None,
            "MissingIndexGroup" => self.missing_group_impact = None,
            "MissingIndex" => {
                if let Some(missing) = self.missing_index.take() {
                    self.push_warning(missing.describe());
                }
            }
            _ => {}
        }

        self.path.pop();
        Ok(())
    }

    fn push_warning(&mut self, text: String) {
        match self.open_nodes.last_mut() {
            Some(node) => node.warnings.push(text),
            None => self.plan_warnings.push(text),
        }
    }
}

/// Maps a `ShowPlanXML` document into the driver-agnostic plan model.
///
/// Child operators sit inside operator-specific elements (`NestedLoops`,
/// `Sort`, ...), so the tree is rebuilt from `RelOp` nesting, whatever the
/// wrapper. Missing indexes are reported as warning text only.
pub(crate) fn parse_showplan_xml(raw: &str) -> Result<QueryPlan, DbError> {
    let mut reader = Reader::from_str(raw);
    let mut parser = ShowplanParser::default();
    let mut saw_root = false;

    loop {
        match reader.read_event().map_err(invalid)? {
            Event::Start(element) => {
                saw_root = true;
                let name = String::from_utf8_lossy(element.local_name().as_ref()).into_owned();
                parser.open(&name, &element_attributes(&element)?)?;
            }
            Event::Empty(element) => {
                saw_root = true;
                let name = String::from_utf8_lossy(element.local_name().as_ref()).into_owned();
                parser.open(&name, &element_attributes(&element)?)?;
                parser.close(&name)?;
            }
            Event::End(element) => {
                let name = String::from_utf8_lossy(element.local_name().as_ref()).into_owned();
                parser.close(&name)?;
            }
            Event::Eof => break,
            _ => {}
        }
    }

    if !saw_root {
        return Err(invalid("the document is empty"));
    }
    if !parser.path.is_empty() {
        return Err(invalid("the document ends inside an open element"));
    }

    if parser.top_level_statements > 1 {
        return Err(DbError::query_failed(format!(
            "A query plan covers exactly one statement; the query has {}. Select a single statement.",
            parser.top_level_statements
        )));
    }

    let mut plan = QueryPlan::new(parser.roots, raw, PlanFormat::Xml);
    plan.warnings = parser.plan_warnings;
    Ok(plan)
}

#[cfg(test)]
mod tests;
