//! Split-phase telemetry: persist on the storage owner, then move only an owned
//! job to a background executor. No SQLite connection crosses that boundary.

use chrono::{DateTime, Utc};
use dbflux_core::observability::usage::{UsageController, UsageSource};
use dbflux_core::observability::{
    EventCategory, EventOutcome, EventRecord, EventSeverity, EventSink,
};
use dbflux_core::release_channel::ReleaseChannel;
use dbflux_storage::repositories::telemetry::{TelemetryRepository, TelemetryRepositoryError};
/// The consent level, re-exported so UI crates need no protocol dependency.
pub use dbflux_telemetry::Level as TelemetryLevel;
use dbflux_telemetry::transport::{Endpoint, Transport, TransportError};
use dbflux_telemetry::{
    Acknowledgement, Architecture, Build, Channel, Decision, Feature, Level, Platform,
    TelemetryState,
};
use std::collections::HashSet;
use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelemetryError {
    Storage,
    InvalidConfiguration,
    UnsupportedBuild,
    Audit,
}
impl std::fmt::Display for TelemetryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "telemetry: {self:?}")
    }
}
impl std::error::Error for TelemetryError {}

/// The production collector, compiled in so a release build reports without
/// configuration. The transport still requires https and this exact path.
pub const DEFAULT_TELEMETRY_ENDPOINT: &str = "https://telemetry.dbflux.dev/v1/events";

#[derive(Clone, Default)]
pub struct TelemetryConfig {
    endpoint: Option<Endpoint>,
}
impl TelemetryConfig {
    /// HTTP requires all three: a debug build, the exact opt-in flag and an
    /// explicitly isolated configuration supplied by the caller.
    pub fn from_values(
        endpoint: Option<&str>,
        allow_loopback: Option<&str>,
        isolated: bool,
    ) -> Result<Self, TelemetryError> {
        Ok(Self {
            endpoint: endpoint
                .map(|endpoint| {
                    Endpoint::parse(Some(endpoint), isolated && allow_loopback == Some("1"))
                        .map_err(|_| TelemetryError::InvalidConfiguration)
                })
                .transpose()?,
        })
    }
    /// This reader exists only when the app's optional telemetry module exists.
    /// Construction/startup never invokes it implicitly.
    ///
    /// Without overrides the compiled collector is used. `DBFLUX_TELEMETRY=off`
    /// disables delivery regardless of any endpoint.
    pub fn from_environment(isolated: bool) -> Result<Self, TelemetryError> {
        Self::from_lookup(
            |name| match std::env::var(name) {
                Ok(value) => Ok(Some(value)),
                Err(std::env::VarError::NotPresent) => Ok(None),
                Err(_) => Err(TelemetryError::InvalidConfiguration),
            },
            isolated,
        )
    }
    /// The kill switch is resolved before the endpoint override is read, so a
    /// disabled install never fails on an override it would not use.
    fn from_lookup(
        read: impl Fn(&str) -> Result<Option<String>, TelemetryError>,
        isolated: bool,
    ) -> Result<Self, TelemetryError> {
        let switch = read("DBFLUX_TELEMETRY")?;
        if Self::switched_off(switch.as_deref()) {
            return Ok(Self::default());
        }
        let endpoint = read("DBFLUX_TELEMETRY_ENDPOINT")?;
        let allow = read("DBFLUX_TELEMETRY_ALLOW_LOOPBACK_HTTP").ok().flatten();
        Self::resolve(
            endpoint.as_deref(),
            allow.as_deref(),
            isolated,
            switch.as_deref(),
        )
    }
    fn switched_off(switch: Option<&str>) -> bool {
        switch.is_some_and(|value| value.trim().eq_ignore_ascii_case("off"))
    }
    /// Pure resolution of the environment inputs. An invalid override fails
    /// closed instead of falling back to the compiled endpoint.
    fn resolve(
        endpoint_override: Option<&str>,
        allow_loopback: Option<&str>,
        isolated: bool,
        switch: Option<&str>,
    ) -> Result<Self, TelemetryError> {
        if Self::switched_off(switch) {
            return Ok(Self::default());
        }
        Self::from_values(
            Some(endpoint_override.unwrap_or(DEFAULT_TELEMETRY_ENDPOINT)),
            allow_loopback,
            isolated,
        )
    }
}

#[derive(Clone)]
pub struct BuildIdentity {
    build: Build,
    platform: Platform,
    architecture: Architecture,
}
impl BuildIdentity {
    pub fn published(
        version: &str,
        channel: ReleaseChannel,
        platform: Platform,
        architecture: Architecture,
    ) -> Result<Self, TelemetryError> {
        let version_info =
            semver::Version::parse(version).map_err(|_| TelemetryError::UnsupportedBuild)?;
        let admitted = match channel {
            ReleaseChannel::Stable => version_info.pre.is_empty(),
            ReleaseChannel::Rc => version_info
                .pre
                .as_str()
                .strip_prefix("rc.")
                .is_some_and(|number| number.parse::<u64>().is_ok()),
            ReleaseChannel::Nightly => {
                version_info.pre.as_str() == "nightly"
                    || version_info.pre.as_str().starts_with("nightly.")
            }
        };
        if !admitted || version.len() > 256 || ReleaseChannel::from_version(version) != channel {
            return Err(TelemetryError::UnsupportedBuild);
        }
        Ok(Self {
            build: Build {
                version: version.into(),
                channel: match channel {
                    ReleaseChannel::Stable => Channel::Stable,
                    ReleaseChannel::Rc => Channel::Rc,
                    ReleaseChannel::Nightly => Channel::Nightly,
                },
            },
            platform,
            architecture,
        })
    }
    /// Use the exact compiled identity, never substitute a fixture release.
    pub fn compiled() -> Result<Self, TelemetryError> {
        let platform = match std::env::consts::OS {
            "linux" => Platform::Linux,
            "macos" => Platform::Darwin,
            "windows" => Platform::Windows,
            _ => return Err(TelemetryError::UnsupportedBuild),
        };
        let architecture = match std::env::consts::ARCH {
            "x86_64" => Architecture::Amd64,
            "aarch64" => Architecture::Arm64,
            _ => return Err(TelemetryError::UnsupportedBuild),
        };
        Self::published(
            env!("CARGO_PKG_VERSION"),
            ReleaseChannel::current(),
            platform,
            architecture,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    NoEndpoint,
    UnsupportedBuild,
    Busy,
    NotDue,
}
pub enum Preparation {
    Job(DeliveryJob),
    Skipped(SkipReason),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionDisposition {
    Applied,
    Stale,
}

const QUEUED: u8 = 0;
const DISPATCHING: u8 = 1;
const FINISHED: u8 = 2;
const CANCELLED: u8 = 3;
struct Lease {
    id: Uuid,
    basic: bool,
    active: bool,
    phase: Arc<AtomicU8>,
}
impl Lease {
    fn cancel_queued(&self) {
        self.phase
            .compare_exchange(QUEUED, CANCELLED, Ordering::SeqCst, Ordering::SeqCst)
            .ok();
    }
}

/// Move this value (not the scheduler) to the background executor. Dropping an
/// unstarted job releases its lease. A finished attempt transfers release
/// responsibility to its completion until the foreground owner consumes it.
pub struct DeliveryJob {
    id: Uuid,
    correlation: Uuid,
    payload: String,
    level: Level,
    endpoint: Endpoint,
    phase: Arc<AtomicU8>,
    audit: Arc<dyn EventSink>,
}
impl Drop for DeliveryJob {
    fn drop(&mut self) {
        self.phase
            .compare_exchange(QUEUED, CANCELLED, Ordering::SeqCst, Ordering::SeqCst)
            .ok();
        // Only this job can own DISPATCHING. Unwind ends its blocking call;
        // release that lease rather than leaving the owner permanently busy.
        self.phase
            .compare_exchange(DISPATCHING, CANCELLED, Ordering::SeqCst, Ordering::SeqCst)
            .ok();
    }
}
impl DeliveryJob {
    pub fn payload(&self) -> &str {
        &self.payload
    }
    pub fn delivery_id(&self) -> Uuid {
        self.id
    }
    pub fn correlation_id(&self) -> Uuid {
        self.correlation
    }
    /// Blocking, bounded attempt. Transport construction also stays off the
    /// foreground thread. No endpoint or underlying diagnostic is logged.
    pub fn attempt(self) -> DeliveryCompletion {
        self.attempt_with(Utc::now(), |dispatch| {
            let transport = Transport::new(self.endpoint.clone())?;
            // Construction/audit can race with Off/reset; check immediately
            // before I/O. Once this transition wins, the 10s request may finish.
            if !dispatch() {
                return Ok(None);
            }
            guarded_request(|| transport.attempt(&self.payload)).map(Some)
        })
    }
    fn attempt_with(
        &self,
        now: DateTime<Utc>,
        send: impl FnOnce(&dyn Fn() -> bool) -> Result<Option<Acknowledgement>, TransportError>,
    ) -> DeliveryCompletion {
        let started_attempt = self.phase.load(Ordering::SeqCst) != CANCELLED;
        let mut result = if !started_attempt {
            AttemptOutcome::Cancelled
        } else if self
            .record(now, "attempt", "attempt", EventOutcome::Pending)
            .is_err()
        {
            AttemptOutcome::AuditFailure
        } else {
            let dispatch = || {
                self.phase
                    .compare_exchange(QUEUED, DISPATCHING, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            };
            match send(&dispatch) {
                Ok(Some(acknowledgement)) => AttemptOutcome::Acknowledged(acknowledgement),
                Ok(None) => AttemptOutcome::Cancelled,
                Err(error) => AttemptOutcome::Transport(error),
            }
        };
        // No attempt event is emitted for an already cancelled, unstarted job.
        if started_attempt {
            let outcome = match result {
                AttemptOutcome::Acknowledged(_) => EventOutcome::Success,
                AttemptOutcome::Cancelled => EventOutcome::Cancelled,
                _ => EventOutcome::Failure,
            };
            if self
                .record(Utc::now(), "outcome", result.code(), outcome)
                .is_err()
            {
                result = AttemptOutcome::AuditFailure;
            }
            // Never resurrect a lease that Off/reset cancelled: finish only
            // from a phase this attempt owns, atomically.
            if self
                .phase
                .compare_exchange(DISPATCHING, FINISHED, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                self.phase
                    .compare_exchange(QUEUED, FINISHED, Ordering::SeqCst, Ordering::SeqCst)
                    .ok();
            }
        }
        DeliveryCompletion {
            id: self.id,
            correlation: self.correlation,
            phase: self.phase.clone(),
            result,
        }
    }
    fn record(
        &self,
        now: DateTime<Utc>,
        phase: &str,
        code: &str,
        outcome: EventOutcome,
    ) -> Result<(), TelemetryError> {
        let details = serde_json::json!({ "payload": self.payload, "level": level_marker(self.level), "phase": phase, "code": code, "attempt_id": self.correlation });
        let event = EventRecord::new(
            now.timestamp_millis(),
            EventSeverity::Info,
            EventCategory::System,
            outcome,
        )
        .with_action("telemetry.delivery")
        .with_summary("Telemetry delivery")
        .with_correlation_id(self.correlation.to_string())
        .with_details_json(details.to_string());
        let stored = self.audit.record(event).map_err(|_| {
            log::error!("telemetry.audit_failure");
            TelemetryError::Audit
        })?;
        // AuditService returns the input with no ID when disabled. A trait Ok
        // alone is not durability evidence, nor is a truncated/redacted body.
        if stored.id.is_none()
            || stored
                .details_json
                .as_deref()
                .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
                .as_ref()
                != Some(&details)
        {
            log::error!("telemetry.audit_failure");
            return Err(TelemetryError::Audit);
        }
        Ok(())
    }
}
/// Marker of the consent level stored with each delivery audit row.
/// A panic inside the HTTP stack is reported as a retryable network failure so
/// the delivery backs off instead of being retried on every tick.
fn guarded_request(
    request: impl FnOnce() -> Result<Acknowledgement, TransportError>,
) -> Result<Acknowledgement, TransportError> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(request))
        .unwrap_or(Err(TransportError::Network))
}

fn level_marker(level: Level) -> &'static str {
    match level {
        Level::Off => "off",
        Level::Minimal => "minimal",
        Level::Basic => "basic",
    }
}

enum AttemptOutcome {
    Acknowledged(Acknowledgement),
    Transport(TransportError),
    AuditFailure,
    Cancelled,
}
impl AttemptOutcome {
    fn code(&self) -> &'static str {
        match self {
            Self::Acknowledged(ack) => match ack.decision {
                Decision::Stored => "stored",
                Decision::Duplicate => "duplicate",
            },
            Self::Cancelled => "cancelled",
            Self::AuditFailure => "audit_failure",
            Self::Transport(error) => match error {
                TransportError::MissingEndpoint => "missing_endpoint",
                TransportError::InvalidEndpoint => "invalid_endpoint",
                TransportError::InvalidPayload => "invalid_payload",
                TransportError::ClientConfiguration => "client_configuration",
                TransportError::BadRequest => "bad_request",
                TransportError::Conflict => "conflict",
                TransportError::RateLimited => "rate_limited",
                TransportError::Unavailable => "unavailable",
                TransportError::UnexpectedStatus => "unexpected_status",
                TransportError::BadResponse => "bad_response",
                TransportError::Timeout => "timeout",
                TransportError::Network => "network",
            },
        }
    }
}
pub struct DeliveryCompletion {
    id: Uuid,
    correlation: Uuid,
    phase: Arc<AtomicU8>,
    result: AttemptOutcome,
}
impl Drop for DeliveryCompletion {
    fn drop(&mut self) {
        // A failed weak-owner update or cancelled foreground task can discard
        // a finished result. This Arc belongs to exactly this lease generation;
        // it cannot cancel a replacement even when the durable ID is unchanged.
        self.phase
            .compare_exchange(FINISHED, CANCELLED, Ordering::SeqCst, Ordering::SeqCst)
            .ok();
    }
}
impl DeliveryCompletion {
    pub fn delivery_id(&self) -> Uuid {
        self.id
    }
    pub fn correlation_id(&self) -> Uuid {
        self.correlation
    }
    pub fn code(&self) -> &'static str {
        self.result.code()
    }
}

/// Read-only view of the newest confirmed telemetry delivery in the durable
/// audit log, for UI report surfaces. `payload` keeps the stored bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryLastReport {
    pub payload: String,
    pub payload_len: usize,
    /// `"minimal"` or `"basic"`, as stored by the delivery bridge. Rows written
    /// before the Minimal level existed carry `"off"` for the same ping.
    pub level: Option<String>,
    /// Row creation time of the confirmed `outcome` event.
    pub ack_at_epoch_ms: i64,
    pub correlation: Option<Uuid>,
}

/// Bounds both the SQL `LIMIT` and how many rows are parsed.
const LAST_REPORT_QUERY_BOUND: usize = 8;

/// Newest confirmed (`stored` / `duplicate`) `telemetry.delivery` outcome in
/// the durable audit, or `None` when no delivery was ever confirmed. The
/// success filter is part of the SQL so the row bound applies to candidate
/// reports, not to the attempt/failure noise around them. Pending attempts,
/// failures and rows without a real closed-schema v1 payload are not reports.
/// Audit errors are returned, never swallowed; if the bound could hide a real
/// confirmed report behind malformed newer rows, that is a conservative
/// error rather than a pretend "nothing sent yet".
pub fn last_report(
    audit: &dbflux_audit::AuditService,
) -> Result<Option<TelemetryLastReport>, TelemetryError> {
    let filter = dbflux_audit::query::AuditQueryFilter {
        category: Some(EventCategory::System.as_str().to_owned()),
        action: Some("telemetry.delivery".to_owned()),
        outcome: Some(EventOutcome::Success.as_str().to_owned()),
        limit: Some(LAST_REPORT_QUERY_BOUND),
        ..Default::default()
    };
    let events = audit
        .query_extended(&filter)
        .map_err(|_| TelemetryError::Audit)?;

    // Confirmed candidates are compared by (created_at, id) so the result
    // never depends on the store's row ordering.
    let mut best: Option<(
        &dbflux_storage::repositories::audit::AuditEventDto,
        serde_json::Value,
        String,
    )> = None;
    for event in &events {
        let Some(details_json) = event.details_json.as_deref() else {
            continue;
        };
        let Ok(details) = serde_json::from_str::<serde_json::Value>(details_json) else {
            continue;
        };
        if details.get("phase").and_then(serde_json::Value::as_str) != Some("outcome") {
            continue;
        }
        if !matches!(
            details.get("code").and_then(serde_json::Value::as_str),
            Some("stored") | Some("duplicate")
        ) {
            continue;
        }
        let Some(payload) = details
            .get("payload")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
        else {
            continue;
        };
        if dbflux_telemetry::validate_payload(&payload).is_err() {
            continue;
        }
        let is_newer = match &best {
            Some((current, _, _)) => {
                (event.created_at_epoch_ms, event.id) > (current.created_at_epoch_ms, current.id)
            }
            None => true,
        };
        if is_newer {
            best = Some((event, details, payload));
        }
    }
    if let Some((event, details, payload)) = best {
        return Ok(Some(TelemetryLastReport {
            payload_len: payload.len(),
            level: details
                .get("level")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            payload,
            ack_at_epoch_ms: event.created_at_epoch_ms,
            correlation: event
                .correlation_id
                .as_deref()
                .and_then(|id| Uuid::parse_str(id).ok()),
        }));
    }

    if events.len() >= LAST_REPORT_QUERY_BOUND {
        // Every candidate inside the bound was malformed or unconfirmed. A
        // real report may exist just beyond it: fail closed instead of
        // showing "no report sent yet".
        return Err(TelemetryError::Audit);
    }
    Ok(None)
}

pub struct TelemetryScheduler {
    repository: TelemetryRepository,
    state: TelemetryState,
    config: TelemetryConfig,
    identity: Result<BuildIdentity, TelemetryError>,
    audit: Arc<dyn EventSink>,
    lease: Option<Lease>,
    // Nonretryable responses retain their frozen queue but are not dispatched
    // again during this session. A restart may make one new diagnostic attempt.
    suppressed: HashSet<Uuid>,
    usage: UsageController,
    audit_available: Arc<dyn Fn() -> bool + Send + Sync>,
    // A failed withdrawal/rotation cannot be repaired by an ordinary timer tick.
    runtime_failure: bool,
    reset_pending: bool,
    // Unlike withdrawal, a count-save failure retains the same consent epoch.
    count_save_failed: bool,
}
impl Drop for TelemetryScheduler {
    fn drop(&mut self) {
        // Already-dispatched requests keep their bounded transport deadline.
        if let Some(lease) = &self.lease {
            lease.cancel_queued();
        }
    }
}
impl TelemetryScheduler {
    pub fn new(
        repository: TelemetryRepository,
        config: TelemetryConfig,
        identity: Result<BuildIdentity, TelemetryError>,
        audit: Arc<dyn EventSink>,
        audit_available: Arc<dyn Fn() -> bool + Send + Sync>,
        now: DateTime<Utc>,
    ) -> Result<Self, TelemetryError> {
        let state = repository
            .update(now, |_| Ok(()))
            .map_err(|_| TelemetryError::Storage)?;
        let mut usage = UsageController::with_permission(audit_available.clone());
        usage
            .set_enabled(state.level() == Level::Basic && state.disclosure_dismissed())
            .map_err(|_| TelemetryError::Storage)?;
        Ok(Self {
            repository,
            state,
            config,
            identity,
            audit,
            lease: None,
            suppressed: HashSet::new(),
            usage,
            audit_available,
            runtime_failure: false,
            reset_pending: false,
            count_save_failed: false,
        })
    }
    /// Last successfully committed state on this owner; no mutable cache access.
    pub fn state(&self) -> &TelemetryState {
        &self.state
    }
    pub fn is_disclosure_dismissed(&self) -> bool {
        self.state.disclosure_dismissed()
    }

    /// Whether a delivery endpoint is configured. Without one, delivery is
    /// unavailable and nothing is ever queued; UI copy stays honest about it.
    pub fn endpoint_configured(&self) -> bool {
        self.config.endpoint.is_some()
    }
    fn update(
        &mut self,
        now: DateTime<Utc>,
        change: impl FnOnce(&mut TelemetryState) -> Result<(), TelemetryRepositoryError>,
    ) -> Result<(), TelemetryError> {
        let old_id = self.state.install_id();
        let usage = &mut self.usage;
        let lease = &self.lease;
        let mut identity_changed = false;
        let result = self.repository.update(now, |state| {
            if state.install_id() != old_id {
                identity_changed = true;
                usage
                    .invalidate()
                    .map_err(|_| TelemetryRepositoryError::Update)?;
                if let Some(lease) = lease {
                    lease.cancel_queued();
                }
            }
            change(state)?;
            if state.install_id() != old_id && !identity_changed {
                identity_changed = true;
                usage
                    .invalidate()
                    .map_err(|_| TelemetryRepositoryError::Update)?;
                if let Some(lease) = lease {
                    lease.cancel_queued();
                }
            }
            Ok(())
        });
        if identity_changed && result.is_err() {
            self.runtime_failure = true;
        }
        self.state = result.map_err(|_| TelemetryError::Storage)?;
        self.sync_usage()
    }
    fn sync_usage(&mut self) -> Result<(), TelemetryError> {
        self.usage
            .set_enabled(
                !self.runtime_failure
                    && !self.count_save_failed
                    && (self.audit_available)()
                    && self.state.level() == Level::Basic
                    && self.state.disclosure_dismissed(),
            )
            .map_err(|_| TelemetryError::Storage)
    }
    /// Capture before work starts. The source cannot control consent or storage.
    pub fn usage_source(
        &mut self,
        now: DateTime<Utc>,
    ) -> Result<Option<UsageSource>, TelemetryError> {
        self.ensure_current_epoch(now)?;
        self.sync_usage()?;
        let source = self.usage.source();
        Ok(source
            .ticket()
            .map_err(|_| TelemetryError::Storage)?
            .map(|_| source))
    }
    fn fence(&mut self, basic_only: bool) -> Result<(), TelemetryError> {
        self.runtime_failure = true;
        if let Some(lease) = &self.lease
            && (!basic_only || lease.basic)
        {
            lease.cancel_queued();
        }
        self.usage.invalidate().map_err(|_| TelemetryError::Storage)
    }
    /// Nothing can change the epoch on its own any more: the identity is
    /// stable and an explicit reset goes through `reset`. So this is a check,
    /// not a write — capturing a usage ticket runs on the foreground for every
    /// counted action, and a storage write there put a SQLite transaction on
    /// the UI thread for no reason.
    fn ensure_current_epoch(&mut self, _now: DateTime<Utc>) -> Result<(), TelemetryError> {
        if self.runtime_failure {
            return Err(TelemetryError::Storage);
        }
        Ok(())
    }
    /// Another process on the same database can commit Off or a reset while
    /// this owner holds a cached report in backoff. A read (no write
    /// transaction on an idle tick) before dispatch keeps a withdrawn
    /// consent from sending the old payload; an unchanged row costs nothing.
    fn adopt_durable_state(&mut self) -> Result<(), TelemetryError> {
        let durable = self
            .repository
            .load()
            .map_err(|_| TelemetryError::Storage)?;
        let Some(durable) = durable else {
            return Ok(());
        };
        if durable == self.state {
            return Ok(());
        }
        if durable.install_id() != self.state.install_id() {
            self.usage
                .invalidate()
                .map_err(|_| TelemetryError::Storage)?;
            if let Some(lease) = &self.lease {
                lease.cancel_queued();
            }
        }
        self.state = durable;
        self.sync_usage()
    }
    fn flush_usage(&mut self, now: DateTime<Utc>) -> Result<(), TelemetryError> {
        let expected_id = self.state.install_id();
        let repository = &self.repository;
        let cached = &mut self.state;
        let result = self
            .usage
            .commit(|counts| {
                if counts == [0; 6] {
                    return Ok(());
                }
                let state = repository
                    .update(now, |state| {
                        if state.install_id() != expected_id {
                            return Err(TelemetryRepositoryError::Update);
                        }
                        let [
                            query_run,
                            visual_query_builder,
                            dashboard_open,
                            script_run,
                            mcp_tool,
                            schema_inspect,
                        ] = counts;
                        state.increment_batch([
                            (Feature::QueryRun, query_run),
                            (Feature::VisualQueryBuilder, visual_query_builder),
                            (Feature::DashboardOpen, dashboard_open),
                            (Feature::ScriptRun, script_run),
                            (Feature::McpTool, mcp_tool),
                            (Feature::SchemaInspect, schema_inspect),
                        ]);
                        Ok(())
                    })
                    .map_err(|_| TelemetryError::Storage)?;
                *cached = state;
                Ok(())
            })
            .map_err(|_| TelemetryError::Storage)?;
        self.count_save_failed = result.is_err();
        result?;
        self.sync_usage()
    }
    pub fn disclosure_dismissed(&mut self, now: DateTime<Utc>) -> Result<(), TelemetryError> {
        self.update(now, |state| {
            state.dismiss_disclosure();
            Ok(())
        })
    }
    pub fn set_basic(&mut self, enabled: bool, now: DateTime<Utc>) -> Result<(), TelemetryError> {
        self.set_level(if enabled { Level::Basic } else { Level::Off }, now)
    }

    /// Persists the consent level. Leaving Basic cancels a queued Basic
    /// delivery; Off also cancels a queued ping; entering Basic cancels a queued
    /// Minimal `active` event. A delivery already handed to the transport may
    /// still complete.
    pub fn set_level(&mut self, level: Level, now: DateTime<Utc>) -> Result<(), TelemetryError> {
        let basic_only = level == Level::Minimal;

        if level != Level::Basic {
            self.fence(basic_only)?;
        }

        let recover = self.runtime_failure;
        let reset_pending = self.reset_pending;
        self.update(now, |state| {
            if recover {
                // Drops the usage of the failed epoch and keeps a queued ping.
                state.set_level(Level::Minimal);
            }
            if reset_pending {
                state.reset_identity(now);
            }
            state.set_level(level);
            Ok(())
        })?;

        self.runtime_failure = false;
        self.reset_pending = false;
        self.count_save_failed = false;
        self.sync_usage()?;

        if let Some(lease) = &self.lease {
            let superseded = if level == Level::Basic {
                // Basic replaces the Minimal weekly event.
                lease.active
            } else {
                !basic_only || lease.basic
            };
            if superseded {
                lease.cancel_queued();
            }
        }
        Ok(())
    }
    pub fn reset(&mut self, now: DateTime<Utc>) -> Result<(), TelemetryError> {
        self.reset_pending = true;
        self.fence(false)?;
        self.update(now, |state| {
            state.reset_identity(now);
            Ok(())
        })?;
        if let Some(lease) = &self.lease {
            lease.cancel_queued();
        }
        self.suppressed.clear();
        self.reset_pending = false;
        self.count_save_failed = false;
        self.runtime_failure = false;
        self.sync_usage()
    }
    pub fn increment(
        &mut self,
        feature: Feature,
        now: DateTime<Utc>,
    ) -> Result<(), TelemetryError> {
        self.ensure_current_epoch(now)?;
        if !(self.audit_available)() || !self.state.disclosure_dismissed() {
            return Ok(());
        }
        self.update(now, |state| {
            state.increment(feature);
            Ok(())
        })
    }
    pub fn prepare(
        &mut self,
        now: DateTime<Utc>,
        system_locale: &str,
    ) -> Result<Preparation, TelemetryError> {
        self.ensure_current_epoch(now)?;
        if !(self.audit_available)() {
            self.usage
                .set_enabled(false)
                .map_err(|_| TelemetryError::Storage)?;
            return Err(TelemetryError::Audit);
        }
        self.flush_usage(now)?;
        self.adopt_durable_state()?;
        let Some(endpoint) = self.config.endpoint.clone() else {
            return Ok(Preparation::Skipped(SkipReason::NoEndpoint));
        };
        let Ok(identity) = self.identity.clone() else {
            return Ok(Preparation::Skipped(SkipReason::UnsupportedBuild));
        };
        if self
            .lease
            .as_ref()
            .is_some_and(|lease| lease.phase.load(Ordering::SeqCst) != CANCELLED)
        {
            return Ok(Preparation::Skipped(SkipReason::Busy));
        }
        self.lease = None;
        let schedule = |state: &mut TelemetryState| {
            state
                .schedule(
                    now,
                    &identity.build,
                    identity.platform,
                    identity.architecture,
                    system_locale,
                    None,
                )
                .map_err(|_| TelemetryRepositoryError::Update)
        };
        // Decide on the committed cache first: an idle tick must not open a
        // write transaction on the UI thread. This owner is the only writer of
        // its cache, so an unchanged candidate means there is nothing to store.
        let mut candidate = self.state.clone();
        schedule(&mut candidate).map_err(|_| TelemetryError::Storage)?;
        if candidate != self.state {
            self.update(now, schedule)?;
        }
        self.suppressed.retain(|id| {
            self.state
                .pending()
                .iter()
                .any(|delivery| delivery.id() == *id)
        });
        let Some(delivery) = self
            .state
            .pending()
            .iter()
            .find(|delivery| delivery.ready(now) && !self.suppressed.contains(&delivery.id()))
        else {
            return Ok(Preparation::Skipped(SkipReason::NotDue));
        };
        let payload = delivery.json().to_owned();
        let event = serde_json::from_str::<serde_json::Value>(&payload)
            .map_err(|_| TelemetryError::Storage)?
            .get("event")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        let basic = event.as_deref() == Some("basic");
        let active = event.as_deref() == Some("active");
        let phase = Arc::new(AtomicU8::new(QUEUED));
        let id = delivery.id();
        self.lease = Some(Lease {
            id,
            basic,
            active,
            phase: phase.clone(),
        });
        Ok(Preparation::Job(DeliveryJob {
            id,
            correlation: Uuid::new_v4(),
            payload,
            level: self.state.level(),
            endpoint,
            phase,
            audit: self.audit.clone(),
        }))
    }
    /// Apply to the CURRENT transaction state, not the preparation snapshot.
    /// Off/reset can remove the ID while a bounded dispatched request finishes.
    pub fn complete(
        &mut self,
        completion: DeliveryCompletion,
        now: DateTime<Utc>,
    ) -> Result<CompletionDisposition, TelemetryError> {
        if self.runtime_failure {
            return Ok(CompletionDisposition::Stale);
        }
        if !self.lease.as_ref().is_some_and(|lease| {
            lease.id == completion.id && Arc::ptr_eq(&lease.phase, &completion.phase)
        }) {
            return Ok(CompletionDisposition::Stale);
        }
        let mut found = false;
        let committed = self.update(now, |state| {
            found = state
                .pending()
                .iter()
                .any(|delivery| delivery.id() == completion.id);
            if found {
                match &completion.result {
                    AttemptOutcome::Acknowledged(ack) => state
                        .acknowledge(completion.id, ack, now)
                        .map_err(|_| TelemetryRepositoryError::Update)?,
                    AttemptOutcome::Transport(error) if error.retryable() => {
                        state.retry_later(completion.id, now)
                    }
                    AttemptOutcome::AuditFailure => state.retry_later(completion.id, now),
                    _ => {}
                }
            }
            Ok(())
        });
        // A failed commit leaves the durable queue intact. Release the finished
        // lease so the next trigger can retry identical bytes (duplicate ACK is
        // safe), rather than losing the consumed completion and staying busy.
        self.lease = None;
        // A permanent rejection is suppressed for this session even when its
        // completion could not be stored, or the unchanged delivery would be
        // sent again on the next tick.
        if let AttemptOutcome::Transport(error) = &completion.result
            && !error.retryable()
        {
            self.suppressed.insert(completion.id);
        }
        committed?;
        if matches!(completion.result, AttemptOutcome::AuditFailure) {
            return Err(TelemetryError::Audit);
        }
        Ok(if found {
            CompletionDisposition::Applied
        } else {
            CompletionDisposition::Stale
        })
    }
}

#[cfg(test)]
mod tests;
