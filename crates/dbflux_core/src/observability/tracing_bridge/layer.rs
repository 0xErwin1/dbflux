use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use tracing::field::{Field, Visit};
use tracing_subscriber::Layer;

use crate::observability::types::{EventActorType, EventOutcome, EventRecord, EventSeverity};

use super::category::{BRIDGE_INTERNAL_TARGET, resolve_category};

const SUMMARY_MAX_CHARS: usize = 512;

/// Target prefix that gates audit capture.
///
/// Only events emitted from dbflux crates (or `log::*!` calls bridged through
/// `tracing-log` with a `dbflux*` target) are mirrored to the audit log. Events
/// from upstream deps such as `gpui`, `blade_graphics`, `naga`, `wgpu`, `hyper`,
/// etc. would otherwise drown the audit table in render-loop and HTTP noise
/// (texture/buffer create+destroy, surface present mode, request lifecycle).
/// They still flow through the fmt layer and stay visible via `RUST_LOG`.
const BRIDGE_TARGET_PREFIX: &str = "dbflux";

/// Tracing layer that routes events to the audit store via a bounded channel.
pub(crate) struct AuditLayer {
    pub(crate) min_level: Arc<AtomicU8>,
    pub(crate) drop_counter: Arc<AtomicU64>,
    pub(crate) queue_tx: Arc<std::sync::mpsc::SyncSender<EventRecord>>,
    pub(crate) in_flight: Arc<std::sync::atomic::AtomicUsize>,
}

impl<S> Layer<S> for AuditLayer
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if !passes_level_gate(
            event.metadata().level(),
            self.min_level.load(Ordering::Relaxed),
        ) {
            return;
        }

        let target = event.metadata().target();

        if target.starts_with(BRIDGE_INTERNAL_TARGET) {
            return;
        }

        if !passes_target_gate(target) {
            return;
        }

        let record = match build_record(event) {
            Some(r) => r,
            None => return,
        };

        match self.queue_tx.try_send(record) {
            Ok(()) => {
                self.in_flight.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.drop_counter.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Returns true if the event target is one we want to audit.
///
/// Upstream crates (GPUI rendering, networking stacks, etc.) emit verbose
/// INFO-level traces that have no operational value in the audit log; this
/// gate keeps the audit table focused on dbflux-originated events.
pub(crate) fn passes_target_gate(target: &str) -> bool {
    target.starts_with(BRIDGE_TARGET_PREFIX)
}

/// Returns true if the event level meets or exceeds the minimum threshold.
///
/// `TRACE` and `DEBUG` always return false — they are never written to audit
/// regardless of the configured threshold.
pub(crate) fn passes_level_gate(level: &tracing::Level, min_code: u8) -> bool {
    let event_code = level_to_code(level);
    if event_code <= 1 {
        return false;
    }
    event_code >= min_code
}

fn level_to_code(level: &tracing::Level) -> u8 {
    match *level {
        tracing::Level::TRACE => 0,
        tracing::Level::DEBUG => 1,
        tracing::Level::INFO => 2,
        tracing::Level::WARN => 3,
        tracing::Level::ERROR => 4,
    }
}

pub(crate) fn level_to_severity(level: &tracing::Level) -> EventSeverity {
    match *level {
        tracing::Level::TRACE => EventSeverity::Trace,
        tracing::Level::DEBUG => EventSeverity::Debug,
        tracing::Level::INFO => EventSeverity::Info,
        tracing::Level::WARN => EventSeverity::Warn,
        tracing::Level::ERROR => EventSeverity::Error,
    }
}

/// Builds an `EventRecord` from a tracing event.
///
/// Returns `None` only if the resulting record would be trivially invalid
/// (empty summary after truncation — practically impossible).
fn build_record(event: &tracing::Event<'_>) -> Option<EventRecord> {
    let meta = event.metadata();
    let target = meta.target();
    let severity = level_to_severity(meta.level());

    let mut visitor = AuditFieldVisitor::default();
    event.record(&mut visitor);

    let category = resolve_category(target, visitor.category.as_deref());

    let action = visitor
        .action
        .or_else(|| {
            target
                .rsplit("::")
                .next()
                .filter(|s| !s.is_empty())
                .map(|s| s.to_owned())
        })
        .unwrap_or_else(|| "log_event".to_owned());

    let (summary, overflow_message) = truncate_summary(visitor.message.unwrap_or_default());

    if summary.is_empty() {
        return None;
    }

    let outcome = visitor
        .outcome
        .and_then(|s| parse_outcome(&s))
        .unwrap_or(EventOutcome::Success);

    let actor_type = visitor
        .actor_type
        .and_then(|s| parse_actor_type(&s))
        .unwrap_or(EventActorType::System);

    let now_ms = chrono::Utc::now().timestamp_millis();

    let mut record = EventRecord::new(now_ms, severity, category, outcome)
        .with_action(action)
        .with_summary(summary);

    record.actor_type = actor_type;

    if let Some(actor_id) = visitor.actor_id {
        record = record.with_actor_id(actor_id);
    }

    if let Some(conn_id) = visitor.connection_id {
        if let Some(db_name) = visitor.database_name {
            if let Some(driver_id) = visitor.driver_id {
                record = record.with_connection_context(conn_id, db_name, driver_id);
            } else {
                record.connection_id = Some(conn_id);
                record.database_name = Some(db_name);
            }
        } else {
            record.connection_id = Some(conn_id);
        }
    }

    if let Some(cid) = visitor.correlation_id {
        record.correlation_id = Some(cid);
    }

    if let Some(details_json_raw) = visitor.details_json {
        record = record.with_details_json(details_json_raw);
    } else if !visitor.extra_fields.is_empty() || overflow_message.is_some() {
        let mut map = visitor.extra_fields;
        if let Some(full_msg) = overflow_message {
            map.insert("message".to_owned(), serde_json::Value::String(full_msg));
        }
        if !map.is_empty()
            && let Ok(json) = serde_json::to_string(&map)
        {
            record = record.with_details_json(json);
        }
    }

    Some(record)
}

/// Truncates a message to `SUMMARY_MAX_CHARS` chars.
///
/// Returns the truncated summary and, if truncation occurred, the original
/// full message so the caller can store it in `details_json["message"]`.
fn truncate_summary(message: String) -> (String, Option<String>) {
    if message.chars().count() <= SUMMARY_MAX_CHARS {
        return (message, None);
    }

    let truncated: String = message.chars().take(SUMMARY_MAX_CHARS).collect();
    let summary = format!("{truncated}…");
    (summary, Some(message))
}

fn parse_outcome(s: &str) -> Option<EventOutcome> {
    match s {
        "success" => Some(EventOutcome::Success),
        "failure" => Some(EventOutcome::Failure),
        "cancelled" => Some(EventOutcome::Cancelled),
        "pending" => Some(EventOutcome::Pending),
        _ => None,
    }
}

fn parse_actor_type(s: &str) -> Option<EventActorType> {
    match s {
        "system" => Some(EventActorType::System),
        "user" => Some(EventActorType::User),
        "app" => Some(EventActorType::App),
        "mcp_client" => Some(EventActorType::McpClient),
        "hook" => Some(EventActorType::Hook),
        "script" => Some(EventActorType::Script),
        _ => None,
    }
}

/// Visits tracing event fields and extracts known named fields into typed slots.
///
/// Unknown fields accumulate in `extra_fields` for inclusion in `details_json`.
/// The `correlation_id` slot is populated explicitly so the value lands in
/// `EventRecord.correlation_id` rather than in `details_json`/`extra_fields`.
#[derive(Default)]
struct AuditFieldVisitor {
    pub category: Option<String>,
    pub actor_type: Option<String>,
    pub actor_id: Option<String>,
    pub connection_id: Option<String>,
    pub database_name: Option<String>,
    pub driver_id: Option<String>,
    pub action: Option<String>,
    pub outcome: Option<String>,
    pub details_json: Option<String>,
    pub message: Option<String>,
    pub correlation_id: Option<String>,
    pub extra_fields: serde_json::Map<String, serde_json::Value>,
}

impl Visit for AuditFieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_string(field, value.to_owned());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let name = field.name();
        if name == "message" {
            self.message = Some(format!("{value:?}").trim_matches('"').to_owned());
            return;
        }

        // Strip the surrounding `"..."` that Debug adds for string-like values
        // so the % / Display sigil round-trips into the typed slots cleanly.
        let raw = format!("{value:?}");
        let string_value = raw.trim_matches('"').to_owned();
        self.record_string_by_name(name, string_value);
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.extra_fields.insert(
            field.name().to_owned(),
            serde_json::Value::Number(
                serde_json::Number::from_f64(value).unwrap_or(serde_json::Number::from(0u64)),
            ),
        );
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.extra_fields.insert(
            field.name().to_owned(),
            serde_json::Value::Number(serde_json::Number::from(value)),
        );
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.extra_fields.insert(
            field.name().to_owned(),
            serde_json::Value::Number(serde_json::Number::from(value)),
        );
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.extra_fields
            .insert(field.name().to_owned(), serde_json::Value::Bool(value));
    }
}

impl AuditFieldVisitor {
    fn record_string(&mut self, field: &Field, value: String) {
        self.record_string_by_name(field.name(), value);
    }

    fn record_string_by_name(&mut self, name: &str, value: String) {
        match name {
            "message" => self.message = Some(value),
            "category" => self.category = Some(value),
            "actor_type" => self.actor_type = Some(value),
            "actor_id" => self.actor_id = Some(value),
            "connection_id" => self.connection_id = Some(value),
            "database_name" => self.database_name = Some(value),
            "driver_id" => self.driver_id = Some(value),
            "action" => self.action = Some(value),
            "outcome" => self.outcome = Some(value),
            "details_json" => self.details_json = Some(value),
            "correlation_id" => self.correlation_id = Some(value),
            other => {
                self.extra_fields
                    .insert(other.to_owned(), serde_json::Value::String(value));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- M1-T1: AuditFieldVisitor correlation_id extraction tests --

    #[test]
    fn visitor_captures_correlation_id_into_slot_not_extra_fields() {
        let mut visitor = AuditFieldVisitor::default();

        visitor.record_string_by_name(
            "correlation_id",
            "0192cf4a-dead-7000-beef-000000000001".to_owned(),
        );

        assert_eq!(
            visitor.correlation_id.as_deref(),
            Some("0192cf4a-dead-7000-beef-000000000001"),
            "correlation_id must land in the dedicated slot"
        );
        assert!(
            !visitor.extra_fields.contains_key("correlation_id"),
            "correlation_id must NOT appear in extra_fields"
        );
    }

    #[test]
    fn visitor_other_unknown_fields_go_to_extra_fields() {
        let mut visitor = AuditFieldVisitor::default();
        visitor.record_string_by_name("some_custom_field", "some_value".to_owned());

        assert!(
            visitor.extra_fields.contains_key("some_custom_field"),
            "unknown fields still go to extra_fields"
        );
        assert!(visitor.correlation_id.is_none());
    }

    #[test]
    fn passes_level_gate_filters_debug_and_trace() {
        // TRACE (code 0) is always filtered
        assert!(!passes_level_gate(&tracing::Level::TRACE, 0));
        // DEBUG (code 1) is always filtered
        assert!(!passes_level_gate(&tracing::Level::DEBUG, 0));
    }

    #[test]
    fn passes_level_gate_info_above_warn_threshold() {
        // INFO (2) < WARN threshold (3) — filtered
        assert!(!passes_level_gate(&tracing::Level::INFO, 3));
        // WARN (3) >= WARN threshold (3) — passes
        assert!(passes_level_gate(&tracing::Level::WARN, 3));
        // ERROR (4) >= WARN threshold (3) — passes
        assert!(passes_level_gate(&tracing::Level::ERROR, 3));
    }

    #[test]
    fn passes_level_gate_info_with_info_threshold() {
        // INFO (2) >= INFO threshold (2) — passes
        assert!(passes_level_gate(&tracing::Level::INFO, 2));
    }

    #[test]
    fn target_gate_passes_dbflux_targets() {
        assert!(passes_target_gate("dbflux"));
        assert!(passes_target_gate("dbflux_core::connection::manager"));
        assert!(passes_target_gate("dbflux_app::access_manager"));
        assert!(passes_target_gate("dbflux_driver_postgres"));
    }

    #[test]
    fn target_gate_rejects_upstream_dep_targets() {
        // These are the actual noise sources observed in production audit
        // dumps: GPUI render-loop, graphics backend, naga shader compiler.
        assert!(!passes_target_gate("gpui"));
        assert!(!passes_target_gate("gpui::renderer"));
        assert!(!passes_target_gate("blade_graphics"));
        assert!(!passes_target_gate("blade_graphics::vulkan::device"));
        assert!(!passes_target_gate("naga::back"));
        assert!(!passes_target_gate("wgpu"));
        assert!(!passes_target_gate("hyper::client"));
        assert!(!passes_target_gate("tokio::runtime"));
        // Empty target or unrelated module
        assert!(!passes_target_gate(""));
        assert!(!passes_target_gate("some_random_crate"));
    }

    #[test]
    fn truncate_summary_short_message_unchanged() {
        let msg = "short message".to_owned();
        let (summary, overflow) = truncate_summary(msg.clone());
        assert_eq!(summary, msg);
        assert!(overflow.is_none());
    }

    #[test]
    fn truncate_summary_long_message_gets_ellipsis() {
        let msg = "a".repeat(600);
        let (summary, overflow) = truncate_summary(msg.clone());
        let char_count = summary.chars().count();
        // 512 chars + 1 for '…'
        assert_eq!(char_count, 513, "expected 512 + ellipsis");
        assert!(summary.ends_with('…'));
        assert_eq!(overflow, Some(msg));
    }
}
