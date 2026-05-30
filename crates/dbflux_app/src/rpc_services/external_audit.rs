use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use dbflux_core::observability::{
    EventActorType, EventCategory, EventOutcome, EventRecord, EventSeverity, EventSink,
    EventSourceId,
};
use dbflux_ipc::audit::{
    AuditEventEmitDto, EventCategoryDto, EventOutcomeDto, EventSeverityDto, ExternalAuditEmitter,
    ExternalAuditSource,
};
use uuid::Uuid;

// ============================================================================
// Rate limiter
// ============================================================================

/// Token bucket for per-socket-id rate limiting.
///
/// Each external RPC socket gets one bucket. Buckets refill at a fixed rate up
/// to `capacity`. Allocated lazily on first emit; never evicted (bounded by the
/// number of active RPC services, which is small in practice).
pub(crate) struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(initial_tokens: f64) -> Self {
        Self {
            tokens: initial_tokens,
            last_refill: Instant::now(),
        }
    }

    /// Attempt to consume one token, refilling from elapsed time first.
    ///
    /// Returns `true` if a token was available and consumed.
    fn consume(&mut self, now: Instant, config: &ExternalAuditConfig) -> bool {
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * config.refill_rate).min(config.capacity);
        self.last_refill = now;

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Configuration for the external-audit sanitizer.
pub(crate) struct ExternalAuditConfig {
    /// Maximum tokens per bucket.
    pub capacity: f64,
    /// Tokens added per second.
    pub refill_rate: f64,
    /// Maximum tolerated drift between DTO timestamp and host wall clock
    /// before the host clamps the value to now (in milliseconds).
    pub max_ts_drift_ms: i64,
}

impl Default for ExternalAuditConfig {
    fn default() -> Self {
        Self {
            capacity: 100.0,
            refill_rate: 100.0 / 60.0,
            max_ts_drift_ms: 5 * 60 * 1_000,
        }
    }
}

// ============================================================================
// Context provider
// ============================================================================

/// Resolved context for a driver emit event.
pub(crate) struct DriverEmitContext {
    pub connection_id: Option<Uuid>,
    pub database_name: Option<String>,
    pub driver_id: String,
}

/// Provides per-connection context for emitted audit events without coupling
/// the transport layer to `AppState`.
///
/// The implementation resolves `(socket_id, session_id)` to connection metadata
/// at emit time. If the lookup fails (e.g. the connection is not active, or the
/// context provider holds a `Weak` reference that has expired), all three fields
/// return as `None` / `"rpc:<socket_id>"`.
pub(crate) trait ExternalAuditContextProvider: Send + Sync {
    fn driver_context(
        &self,
        socket_id: &str,
        session_id: Option<Uuid>,
    ) -> Option<DriverEmitContext>;
}

/// No-op context provider used when `AppState` wiring is not yet in place or
/// in unit tests. All lookups return `None`, so `connection_id` and
/// `database_name` are blank but events still record with the correct actor
/// and driver_id.
pub(crate) struct NoOpContextProvider;

impl ExternalAuditContextProvider for NoOpContextProvider {
    fn driver_context(&self, socket_id: &str, _session_id: Option<Uuid>) -> Option<DriverEmitContext> {
        Some(DriverEmitContext {
            connection_id: None,
            database_name: None,
            driver_id: format!("rpc:{}", socket_id),
        })
    }
}

// ============================================================================
// DTO conversions
// ============================================================================

fn severity_from_dto(dto: EventSeverityDto) -> EventSeverity {
    match dto {
        EventSeverityDto::Trace => EventSeverity::Trace,
        EventSeverityDto::Debug => EventSeverity::Debug,
        EventSeverityDto::Info => EventSeverity::Info,
        EventSeverityDto::Warn => EventSeverity::Warn,
        EventSeverityDto::Error => EventSeverity::Error,
        EventSeverityDto::Fatal => EventSeverity::Fatal,
    }
}

fn category_from_dto(dto: EventCategoryDto) -> EventCategory {
    match dto {
        EventCategoryDto::Config => EventCategory::Config,
        EventCategoryDto::Connection => EventCategory::Connection,
        EventCategoryDto::Query => EventCategory::Query,
        EventCategoryDto::Hook => EventCategory::Hook,
        EventCategoryDto::Script => EventCategory::Script,
        EventCategoryDto::System => EventCategory::System,
        EventCategoryDto::Mcp => EventCategory::Mcp,
        EventCategoryDto::Governance => EventCategory::Governance,
    }
}

fn outcome_from_dto(dto: EventOutcomeDto) -> EventOutcome {
    match dto {
        EventOutcomeDto::Success => EventOutcome::Success,
        EventOutcomeDto::Failure => EventOutcome::Failure,
        EventOutcomeDto::Cancelled => EventOutcome::Cancelled,
        EventOutcomeDto::Pending => EventOutcome::Pending,
    }
}

/// Clamp a service-provided timestamp to the host wall clock if the drift
/// exceeds the configured maximum.
fn clamp_timestamp(ts_ms: i64, config: &ExternalAuditConfig) -> i64 {
    let now_ms = chrono::Utc::now().timestamp_millis();
    if (ts_ms - now_ms).abs() > config.max_ts_drift_ms {
        now_ms
    } else {
        ts_ms
    }
}

// ============================================================================
// Sanitizing sink
// ============================================================================

/// Sanitizing implementation of [`ExternalAuditEmitter`].
///
/// Receives raw frames from driver and auth-provider transport loops, applies:
/// 1. Rate limiting (per socket_id token bucket)
/// 2. Category whitelist (drivers: Connection/Query/System; providers: Connection)
/// 3. Required-field validation (action + summary must be non-empty)
/// 4. Identity/context override (host-authoritative; DTO cannot set identity)
/// 5. Timestamp clamping (prevents rewriting history)
///
/// Then forwards the sanitized record to `AuditService` via `EventSink::record`.
/// All drop paths increment `drop_counter` and log at `debug!`; IPC is never
/// blocked or errored due to audit failures.
pub(crate) struct ExternalAuditSink {
    audit: Arc<dyn EventSink>,
    drop_counter: Arc<AtomicU64>,
    rate_limiters: Mutex<HashMap<String, TokenBucket>>,
    context_provider: Arc<dyn ExternalAuditContextProvider>,
    config: ExternalAuditConfig,
}

impl ExternalAuditSink {
    pub(crate) fn new(
        audit: Arc<dyn EventSink>,
        drop_counter: Arc<AtomicU64>,
        context_provider: Arc<dyn ExternalAuditContextProvider>,
        config: ExternalAuditConfig,
    ) -> Self {
        Self {
            audit,
            drop_counter,
            rate_limiters: Mutex::new(HashMap::new()),
            context_provider,
            config,
        }
    }

    fn consume_token(&self, socket_id: &str) -> bool {
        let mut map = self.rate_limiters.lock().unwrap_or_else(|p| p.into_inner());
        let bucket = map
            .entry(socket_id.to_string())
            .or_insert_with(|| TokenBucket::new(self.config.capacity));
        bucket.consume(Instant::now(), &self.config)
    }

    fn drop(&self, socket_id: &str, reason: &str) {
        self.drop_counter.fetch_add(1, Ordering::Relaxed);
        log::debug!(
            target: "external_audit",
            "dropped emit from socket={}: {}",
            socket_id,
            reason
        );
    }
}

impl ExternalAuditEmitter for ExternalAuditSink {
    fn emit(&self, source: ExternalAuditSource, dto: AuditEventEmitDto) {
        let socket_id = source.socket_id().to_string();

        // Step 1: rate limit.
        if !self.consume_token(&socket_id) {
            self.drop(&socket_id, "rate-limited");
            return;
        }

        // Step 2: category whitelist.
        let category = category_from_dto(dto.category);
        let allowed = match &source {
            ExternalAuditSource::Driver { .. } => matches!(
                category,
                EventCategory::Connection | EventCategory::Query | EventCategory::System
            ),
            ExternalAuditSource::AuthProvider { .. } => {
                matches!(category, EventCategory::Connection)
            }
        };

        if !allowed {
            self.drop(
                &socket_id,
                &format!("category {:?} not allowed for {}", category, source.kind_label()),
            );
            return;
        }

        // Step 3: required-field validation.
        if dto.action.trim().is_empty() || dto.summary.trim().is_empty() {
            self.drop(&socket_id, "empty action or summary");
            return;
        }

        // Step 4: identity/context override.
        let (actor_type, source_id, actor_id, connection_id, database_name, driver_id, correlation_id) =
            match &source {
                ExternalAuditSource::Driver {
                    socket_id,
                    session_id,
                    correlation_id,
                } => {
                    let ctx = self.context_provider.driver_context(socket_id, *session_id);
                    (
                        EventActorType::ExternalDriver,
                        EventSourceId::ExternalDriver,
                        Some(format!("rpc:{}", socket_id)),
                        ctx.as_ref()
                            .and_then(|c| c.connection_id.map(|u| u.to_string())),
                        ctx.as_ref().and_then(|c| c.database_name.clone()),
                        Some(
                            ctx.map(|c| c.driver_id)
                                .unwrap_or_else(|| format!("rpc:{}", socket_id)),
                        ),
                        Some(correlation_id.clone()),
                    )
                }
                ExternalAuditSource::AuthProvider {
                    provider_id,
                    correlation_id,
                    ..
                } => (
                    EventActorType::ExternalAuthProvider,
                    EventSourceId::ExternalAuthProvider,
                    Some(provider_id.clone()),
                    None,
                    None,
                    None,
                    Some(correlation_id.clone()),
                ),
            };

        // Step 5: build and store the record.
        let record = EventRecord {
            id: None,
            ts_ms: clamp_timestamp(dto.ts_ms, &self.config),
            level: severity_from_dto(dto.level),
            category,
            action: dto.action,
            outcome: outcome_from_dto(dto.outcome),
            actor_type,
            actor_id,
            source_id,
            connection_id,
            database_name,
            driver_id,
            object_type: dto.object_type,
            object_id: dto.object_id,
            summary: dto.summary,
            details_json: dto.details_json,
            error_code: dto.error_code,
            error_message: dto.error_message,
            duration_ms: dto.duration_ms,
            session_id: None,
            correlation_id,
        };

        if let Err(err) = self.audit.record(record) {
            log::debug!(
                target: "external_audit",
                "audit record failed for socket={}: {}",
                socket_id,
                err
            );
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use dbflux_core::observability::{EventRecord, EventSink, EventSinkError};
    use dbflux_ipc::audit::{
        AuditEventEmitDto, EventCategoryDto, EventOutcomeDto, EventSeverityDto, ExternalAuditSource,
    };

    // --- Test doubles ---

    /// Records all events for assertion; never fails.
    #[derive(Default)]
    struct RecordingEventSink {
        records: Arc<Mutex<Vec<EventRecord>>>,
    }

    impl RecordingEventSink {
        fn count(&self) -> usize {
            self.records.lock().unwrap().len()
        }

        fn all(&self) -> Vec<EventRecord> {
            self.records.lock().unwrap().clone()
        }
    }

    impl EventSink for RecordingEventSink {
        fn record(&self, event: EventRecord) -> Result<EventRecord, EventSinkError> {
            self.records.lock().unwrap().push(event.clone());
            Ok(event)
        }
    }

    fn minimal_dto(category: EventCategoryDto) -> AuditEventEmitDto {
        AuditEventEmitDto {
            ts_ms: chrono::Utc::now().timestamp_millis(),
            level: EventSeverityDto::Info,
            category,
            action: "test.action".to_string(),
            outcome: EventOutcomeDto::Success,
            summary: "test summary".to_string(),
            object_type: None,
            object_id: None,
            duration_ms: None,
            error_code: None,
            error_message: None,
            details_json: None,
        }
    }

    fn driver_source(socket_id: &str) -> ExternalAuditSource {
        ExternalAuditSource::Driver {
            socket_id: socket_id.to_string(),
            session_id: None,
            correlation_id: Uuid::new_v4().to_string(),
        }
    }

    fn auth_provider_source(socket_id: &str) -> ExternalAuditSource {
        ExternalAuditSource::AuthProvider {
            socket_id: socket_id.to_string(),
            provider_id: "test-auth".to_string(),
            correlation_id: Uuid::new_v4().to_string(),
        }
    }

    fn make_sink(recording_sink: Arc<RecordingEventSink>) -> ExternalAuditSink {
        let drop_counter = Arc::new(AtomicU64::new(0));
        ExternalAuditSink::new(
            recording_sink as Arc<dyn EventSink>,
            drop_counter,
            Arc::new(NoOpContextProvider),
            ExternalAuditConfig::default(),
        )
    }

    fn make_sink_with_drop_counter(
        recording_sink: Arc<RecordingEventSink>,
    ) -> (ExternalAuditSink, Arc<AtomicU64>) {
        let drop_counter = Arc::new(AtomicU64::new(0));
        let sink = ExternalAuditSink::new(
            recording_sink as Arc<dyn EventSink>,
            drop_counter.clone(),
            Arc::new(NoOpContextProvider),
            ExternalAuditConfig::default(),
        );
        (sink, drop_counter)
    }

    // --- Layer A tests ---

    /// Scenario S-01-a: happy path driver emit produces correct actor/source metadata.
    #[test]
    fn test_happy_path_driver_emit() {
        let recording = Arc::new(RecordingEventSink::default());
        let sink = make_sink(recording.clone());

        sink.emit(
            driver_source("foo"),
            minimal_dto(EventCategoryDto::Connection),
        );

        let records = recording.all();
        assert_eq!(records.len(), 1);

        let record = &records[0];
        assert_eq!(record.actor_type, EventActorType::ExternalDriver);
        assert_eq!(record.source_id, EventSourceId::ExternalDriver);
        assert_eq!(record.actor_id.as_deref(), Some("rpc:foo"));
        assert_eq!(record.driver_id.as_deref(), Some("rpc:foo"));
        assert!(record.correlation_id.is_some());
    }

    /// Scenario S-03-b: driver emitting category=Mcp is dropped.
    #[test]
    fn test_driver_category_mcp_dropped() {
        let recording = Arc::new(RecordingEventSink::default());
        let (sink, drop_counter) = make_sink_with_drop_counter(recording.clone());

        sink.emit(driver_source("foo"), minimal_dto(EventCategoryDto::Mcp));

        assert_eq!(recording.count(), 0);
        assert_eq!(drop_counter.load(Ordering::Relaxed), 1);
    }

    /// Scenario S-03-c: driver emitting Connection, Query, System all succeed.
    #[test]
    fn test_driver_all_whitelisted_categories_accepted() {
        let recording = Arc::new(RecordingEventSink::default());
        let sink = make_sink(recording.clone());

        for category in [
            EventCategoryDto::Connection,
            EventCategoryDto::Query,
            EventCategoryDto::System,
        ] {
            sink.emit(driver_source("foo"), minimal_dto(category));
        }

        assert_eq!(recording.count(), 3);
    }

    /// Scenario S-04-b: auth provider emitting category=Query is dropped.
    #[test]
    fn test_auth_provider_category_query_dropped() {
        let recording = Arc::new(RecordingEventSink::default());
        let (sink, drop_counter) = make_sink_with_drop_counter(recording.clone());

        sink.emit(
            auth_provider_source("auth-sock"),
            minimal_dto(EventCategoryDto::Query),
        );

        assert_eq!(recording.count(), 0);
        assert_eq!(drop_counter.load(Ordering::Relaxed), 1);
    }

    /// Scenario S-04-a: auth provider emitting category=Connection succeeds.
    #[test]
    fn test_auth_provider_connection_accepted() {
        let recording = Arc::new(RecordingEventSink::default());
        let sink = make_sink(recording.clone());

        sink.emit(
            auth_provider_source("auth-sock"),
            minimal_dto(EventCategoryDto::Connection),
        );

        assert_eq!(recording.count(), 1);
        let record = &recording.all()[0];
        assert_eq!(record.actor_type, EventActorType::ExternalAuthProvider);
        assert_eq!(record.source_id, EventSourceId::ExternalAuthProvider);
    }

    /// Scenario R-01-a/R-01-b: 100 emits pass, 101st is rate-limited.
    #[test]
    fn test_rate_limit_100_pass_101_drops() {
        let recording = Arc::new(RecordingEventSink::default());
        let drop_counter = Arc::new(AtomicU64::new(0));

        let sink = ExternalAuditSink::new(
            recording.clone() as Arc<dyn EventSink>,
            drop_counter.clone(),
            Arc::new(NoOpContextProvider),
            ExternalAuditConfig::default(),
        );

        for _ in 0..200 {
            sink.emit(
                driver_source("rate-sock"),
                minimal_dto(EventCategoryDto::System),
            );
        }

        assert_eq!(recording.count(), 100, "exactly 100 events should pass");
        assert_eq!(
            drop_counter.load(Ordering::Relaxed),
            100,
            "exactly 100 drops due to rate limit"
        );
    }

    /// Scenario R-02-a: drop counter accumulates across multiple bursts.
    #[test]
    fn test_drop_counter_accumulates() {
        let recording = Arc::new(RecordingEventSink::default());
        let drop_counter = Arc::new(AtomicU64::new(0));

        let sink = ExternalAuditSink::new(
            recording.clone() as Arc<dyn EventSink>,
            drop_counter.clone(),
            Arc::new(NoOpContextProvider),
            ExternalAuditConfig::default(),
        );

        // Exhaust the bucket (100 events).
        for _ in 0..100 {
            sink.emit(
                driver_source("accum-sock"),
                minimal_dto(EventCategoryDto::System),
            );
        }

        // 5 more should all drop.
        for _ in 0..5 {
            sink.emit(
                driver_source("accum-sock"),
                minimal_dto(EventCategoryDto::System),
            );
        }

        assert_eq!(drop_counter.load(Ordering::Relaxed), 5);
    }

    /// Scenario S-06-a: correlation_id is host-generated (taken from ExternalAuditSource).
    /// DTO has no correlation_id field — compile-time guarantee.
    #[test]
    fn test_correlation_id_host_generated() {
        let recording = Arc::new(RecordingEventSink::default());
        let sink = make_sink(recording.clone());

        let source = ExternalAuditSource::Driver {
            socket_id: "corr-sock".to_string(),
            session_id: None,
            correlation_id: "host-corr-123".to_string(),
        };

        sink.emit(source, minimal_dto(EventCategoryDto::Connection));

        let records = recording.all();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].correlation_id.as_deref(),
            Some("host-corr-123"),
            "record must use the host-generated correlation_id from ExternalAuditSource"
        );
    }

    /// Timestamp drift: DTO ts_ms that is far in the past should be clamped to now.
    #[test]
    fn test_timestamp_clamped_on_drift() {
        let recording = Arc::new(RecordingEventSink::default());
        let sink = make_sink(recording.clone());

        let ten_hours_ago = chrono::Utc::now().timestamp_millis() - (10 * 60 * 60 * 1_000);

        let mut dto = minimal_dto(EventCategoryDto::System);
        dto.ts_ms = ten_hours_ago;

        sink.emit(driver_source("drift-sock"), dto);

        let records = recording.all();
        assert_eq!(records.len(), 1);

        let now_ms = chrono::Utc::now().timestamp_millis();
        let stored_ts = records[0].ts_ms;
        assert!(
            (stored_ts - now_ms).abs() < 2_000,
            "clamped timestamp must be within 2 seconds of now (was {}ms off)",
            (stored_ts - now_ms).abs()
        );
    }

    /// Scenario: context provider returns None — event records without connection_id.
    #[test]
    fn test_context_provider_returns_none_gracefully() {
        struct NullProvider;
        impl ExternalAuditContextProvider for NullProvider {
            fn driver_context(
                &self,
                _socket_id: &str,
                _session_id: Option<Uuid>,
            ) -> Option<DriverEmitContext> {
                None
            }
        }

        let recording = Arc::new(RecordingEventSink::default());
        let drop_counter = Arc::new(AtomicU64::new(0));

        let sink = ExternalAuditSink::new(
            recording.clone() as Arc<dyn EventSink>,
            drop_counter,
            Arc::new(NullProvider),
            ExternalAuditConfig::default(),
        );

        sink.emit(
            driver_source("null-ctx-sock"),
            minimal_dto(EventCategoryDto::Connection),
        );

        assert_eq!(recording.count(), 1, "event must still record without connection context");

        let record = &recording.all()[0];
        assert!(record.connection_id.is_none(), "connection_id must be None when context is unavailable");
        assert!(record.database_name.is_none(), "database_name must be None when context is unavailable");
        assert_eq!(
            record.driver_id.as_deref(),
            Some("rpc:null-ctx-sock"),
            "driver_id must default to rpc:<socket_id> when context is unavailable"
        );
    }
}
