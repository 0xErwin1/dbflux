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
        let read = |name: &str| match std::env::var(name) {
            Ok(value) => Ok(Some(value)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(_) => Err(TelemetryError::InvalidConfiguration),
        };
        let switch = read("DBFLUX_TELEMETRY")?;
        let endpoint = read("DBFLUX_TELEMETRY_ENDPOINT")?;
        let allow = std::env::var("DBFLUX_TELEMETRY_ALLOW_LOOPBACK_HTTP").ok();
        Self::resolve(
            endpoint.as_deref(),
            allow.as_deref(),
            isolated,
            switch.as_deref(),
        )
    }
    /// Pure resolution of the environment inputs. An invalid override fails
    /// closed instead of falling back to the compiled endpoint.
    fn resolve(
        endpoint_override: Option<&str>,
        allow_loopback: Option<&str>,
        isolated: bool,
        switch: Option<&str>,
    ) -> Result<Self, TelemetryError> {
        if switch.is_some_and(|value| value.trim().eq_ignore_ascii_case("off")) {
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
            // Do not resurrect a cancelled queued lease after Off/reset.
            if self.phase.load(Ordering::SeqCst) != CANCELLED {
                self.phase.store(FINISHED, Ordering::SeqCst);
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
    /// delivery; Off also cancels a queued ping. A delivery already handed to
    /// the transport may still complete.
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

        if level != Level::Basic
            && let Some(lease) = &self.lease
            && (!basic_only || lease.basic)
        {
            lease.cancel_queued();
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
        let basic = serde_json::from_str::<serde_json::Value>(&payload)
            .map_err(|_| TelemetryError::Storage)?
            .get("event")
            .and_then(serde_json::Value::as_str)
            == Some("basic");
        let phase = Arc::new(AtomicU8::new(QUEUED));
        let id = delivery.id();
        self.lease = Some(Lease {
            id,
            basic,
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
        committed?;
        if found
            && let AttemptOutcome::Transport(error) = &completion.result
            && !error.retryable()
        {
            self.suppressed.insert(completion.id);
        }
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
mod tests {
    use super::*;
    use dbflux_core::observability::usage::UsageFeature;
    use dbflux_storage::bootstrap::StorageRuntime;
    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .expect("clock")
            .with_timezone(&Utc)
    }
    fn fixture(
        config: TelemetryConfig,
    ) -> (
        TelemetryScheduler,
        dbflux_audit::AuditService,
        StorageRuntime,
    ) {
        // Never use the default app database. Both repositories use this exact
        // UUID-named scratch directory, including the actual persisted audit.
        let directory =
            std::env::temp_dir().join(format!("dbflux-telemetry-test-{}", Uuid::new_v4()));
        std::fs::create_dir(&directory).expect("exclusive scratch directory");
        let storage =
            StorageRuntime::for_path(directory.join("fixture.sqlite")).expect("scratch storage");
        let audit = dbflux_audit::AuditService::new_sqlite(storage.dbflux_db_path())
            .expect("scratch audit");
        let scheduler = TelemetryScheduler::new(
            storage.telemetry(),
            config,
            BuildIdentity::published(
                "1.2.3",
                ReleaseChannel::Stable,
                Platform::Linux,
                Architecture::Amd64,
            ),
            Arc::new(audit.clone()),
            {
                let audit = audit.clone();
                Arc::new(move || audit.is_enabled())
            },
            now(),
        )
        .expect("scheduler");
        (scheduler, audit, storage)
    }
    fn configured() -> (
        TelemetryScheduler,
        dbflux_audit::AuditService,
        StorageRuntime,
    ) {
        fixture(
            TelemetryConfig::from_values(Some("https://example.invalid/v1/events"), None, false)
                .expect("config"),
        )
    }
    /// Build pings only: the default Basic queue also holds the weekly report
    /// and a Minimal queue the weekly `active`, so a per-build assertion must
    /// not count every delivery.
    fn pending_pings(scheduler: &TelemetryScheduler) -> usize {
        scheduler
            .state()
            .pending()
            .iter()
            .filter(|delivery| delivery.json().contains("\"event\":\"ping\""))
            .count()
    }
    fn job(scheduler: &mut TelemetryScheduler, time: DateTime<Utc>) -> DeliveryJob {
        let Preparation::Job(job) = scheduler.prepare(time, "es_AR").expect("prepare") else {
            panic!("expected job");
        };
        job
    }
    fn acknowledged(job: &DeliveryJob) -> DeliveryCompletion {
        job.attempt_with(now(), |dispatch| {
            assert!(dispatch());
            Ok(Some(
                Acknowledgement::parse(
                    200,
                    br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored"}"#,
                )
                .expect("ack"),
            ))
        })
    }
    fn usage_ticket(
        scheduler: &mut TelemetryScheduler,
    ) -> dbflux_core::observability::usage::UsageTicket {
        scheduler
            .usage_source(now())
            .expect("source")
            .expect("enabled")
            .ticket()
            .expect("capture")
            .expect("ticket")
    }
    fn fail_writes(storage: &StorageRuntime) {
        storage.dbflux_db().execute_batch("CREATE TRIGGER fail_telemetry BEFORE UPDATE ON cfg_telemetry BEGIN SELECT RAISE(FAIL, 'fixture'); END;").expect("failure injection");
    }
    fn restore_writes(storage: &StorageRuntime) {
        storage
            .dbflux_db()
            .execute_batch("DROP TRIGGER fail_telemetry;")
            .expect("restore fixture writes");
    }
    #[test]
    fn telemetry_bridge_requires_basic_disclosure_and_live_audit() {
        let (mut scheduler, audit, _) = configured();
        assert!(scheduler.usage_source(now()).expect("off").is_none());
        scheduler.set_basic(true, now()).expect("basic");
        assert!(
            scheduler
                .usage_source(now())
                .expect("undisclosed")
                .is_none()
        );
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        let ticket = usage_ticket(&mut scheduler);
        audit.set_enabled(false);
        assert!(
            !ticket
                .record(UsageFeature::QueryRun)
                .expect("live audit gate")
        );
        assert!(
            scheduler
                .usage_source(now())
                .expect("disabled audit")
                .is_none()
        );
        assert!(matches!(
            scheduler.prepare(now(), "en"),
            Err(TelemetryError::Audit)
        ));
        assert_eq!(pending_pings(&scheduler), 0);
    }
    #[test]
    fn telemetry_bridge_batches_six_once_and_ack_preserves_newer_credits() {
        let (mut scheduler, _, storage) = configured();
        scheduler.set_basic(true, now()).expect("basic");
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        let ticket = usage_ticket(&mut scheduler);
        for feature in UsageFeature::ALL {
            assert!(ticket.record(feature).expect("credit"));
            assert!(!ticket.clone().record(feature).expect("duplicate"));
            assert!(usage_ticket(&mut scheduler).record(feature).expect("fresh"));
        }
        let revision_before: i64 = storage
            .dbflux_db()
            .query_row("SELECT revision FROM cfg_telemetry WHERE id=1", [], |row| {
                row.get(0)
            })
            .expect("revision");
        scheduler.flush_usage(now()).expect("atomic batch");
        let revision_after: i64 = storage
            .dbflux_db()
            .query_row("SELECT revision FROM cfg_telemetry WHERE id=1", [], |row| {
                row.get(0)
            })
            .expect("revision");
        assert_eq!(revision_after, revision_before + 1);
        let ping = job(&mut scheduler, now());
        scheduler
            .complete(acknowledged(&ping), now())
            .expect("ping");
        let basic = job(&mut scheduler, now());
        let frozen: serde_json::Value = serde_json::from_str(basic.payload()).expect("json");
        assert_eq!(frozen["features"].as_object().expect("six").len(), 6);
        assert!(
            frozen["features"]
                .as_object()
                .expect("six")
                .values()
                .all(|value| *value == 2)
        );
        assert!(
            usage_ticket(&mut scheduler)
                .record(UsageFeature::QueryRun)
                .expect("newer")
        );
        scheduler.flush_usage(now()).expect("newer batch");
        scheduler
            .complete(acknowledged(&basic), now())
            .expect("ack snapshot");
        let next = job(&mut scheduler, now() + chrono::Duration::days(7));
        let value: serde_json::Value = serde_json::from_str(next.payload()).expect("json");
        assert_eq!(value["features"]["query_run"], 1);
    }
    #[test]
    fn telemetry_bridge_count_save_failure_retries_same_epoch_exactly_once() {
        let (mut scheduler, _, storage) = configured();
        scheduler.set_basic(true, now()).expect("basic");
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        let ticket = usage_ticket(&mut scheduler);
        assert!(ticket.record(UsageFeature::ScriptRun).expect("credit"));
        fail_writes(&storage);
        assert_eq!(scheduler.flush_usage(now()), Err(TelemetryError::Storage));
        assert!(
            !ticket
                .record(UsageFeature::QueryRun)
                .expect("disabled until retry")
        );
        restore_writes(&storage);
        assert!(scheduler.usage_source(now()).expect("retry gate").is_none());
        scheduler.flush_usage(now()).expect("retry");
        scheduler.flush_usage(now()).expect("no double credit");
        let ping = job(&mut scheduler, now());
        scheduler
            .complete(acknowledged(&ping), now())
            .expect("ping");
        let basic = job(&mut scheduler, now());
        let value: serde_json::Value = serde_json::from_str(basic.payload()).expect("json");
        assert_eq!(value["features"]["script_run"], 1);
        assert!(value["features"].get("query_run").is_none());
    }
    #[test]
    fn telemetry_bridge_epoch_fences_off_reset_rotation_failures_and_drop() {
        // Scenario 5 (a failed rotation) no longer exists: the identity is
        // stable, so there is no deadline-driven epoch write that can fail.
        for scenario in 0..5 {
            let (mut scheduler, _, storage) = configured();
            scheduler.set_basic(true, now()).expect("basic");
            scheduler.disclosure_dismissed(now()).expect("dismiss");
            let ticket = usage_ticket(&mut scheduler);
            assert!(ticket.record(UsageFeature::QueryRun).expect("old credit"));
            let old_id = scheduler.state().install_id();
            match scenario {
                0 => {
                    scheduler.set_basic(false, now()).expect("off");
                    scheduler.set_basic(true, now()).expect("on");
                }
                1 => {
                    scheduler.reset(now()).expect("reset");
                }
                2 => {
                    // The identity no longer rotates on a deadline, so time
                    // passing is not an epoch change. Preparation at any date
                    // leaves the tickets of the current identity valid; only an
                    // explicit reset fences them.
                    scheduler
                        .prepare(now() + chrono::Duration::days(400), "en")
                        .expect("prepare after more than a year");
                }
                3 | 4 => {
                    fail_writes(&storage);
                    let result = if scenario == 3 {
                        scheduler.set_basic(false, now())
                    } else {
                        scheduler.reset(now())
                    };
                    assert_eq!(result, Err(TelemetryError::Storage));
                    restore_writes(&storage);
                    assert!(
                        scheduler.prepare(now(), "en").is_err(),
                        "failure fence survives next tick"
                    );
                    scheduler.set_basic(true, now()).expect("durable recovery");
                }
                5 => {
                    fail_writes(&storage);
                    assert!(
                        scheduler
                            .prepare(now() + chrono::Duration::days(90), "en")
                            .is_err()
                    );
                    // Nothing rotated on a deadline any more, so a failed
                    // write fences the fence, not the epoch: the ticket of the
                    // current identity stays valid until an explicit reset.
                    assert!(
                        ticket
                            .record(UsageFeature::ScriptRun)
                            .expect("no rotation to fail")
                    );
                    restore_writes(&storage);
                    assert!(scheduler.prepare(now(), "en").is_err());
                    scheduler
                        .reset(now() + chrono::Duration::days(90))
                        .expect("durable rotation");
                }
                _ => {
                    let source = scheduler.usage.source();
                    drop(scheduler);
                    assert!(source.ticket().expect("dead owner").is_none());
                    assert!(!ticket.record(UsageFeature::QueryRun).expect("stale"));
                    continue;
                }
            }
            if scenario == 2 {
                // Nothing rotated: the ticket of this identity is still valid.
                assert_eq!(scheduler.state().install_id(), old_id);
                assert!(ticket.record(UsageFeature::ScriptRun).expect("same epoch"));
            } else {
                assert!(!ticket.record(UsageFeature::ScriptRun).expect("stale"));
            }
            if matches!(scenario, 1 | 4 | 5) {
                assert_ne!(scheduler.state().install_id(), old_id);
            }
            assert!(
                usage_ticket(&mut scheduler)
                    .record(UsageFeature::ScriptRun)
                    .expect("fresh epoch")
            );
            scheduler.flush_usage(now()).expect("flush");
            let value = serde_json::to_value(scheduler.state()).expect("state");
            assert!(value["counters"].get("query_run").is_none());
            // Scenario 2 kept its identity, so its earlier credit survived and
            // the fresh ticket added one more.
            let expected = if scenario == 2 { 2 } else { 1 };
            assert_eq!(value["counters"]["script_run"], expected);
        }
    }

    #[test]
    fn telemetry_failed_withdrawal_cancels_queued_basic_before_storage() {
        let (mut scheduler, _, storage) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        scheduler.set_basic(true, now()).expect("basic");
        let ping = job(&mut scheduler, now());
        scheduler
            .complete(acknowledged(&ping), now())
            .expect("ping");
        let basic = job(&mut scheduler, now());
        storage.dbflux_db().execute_batch("CREATE TRIGGER fail_telemetry BEFORE UPDATE ON cfg_telemetry BEGIN SELECT RAISE(FAIL, 'fixture'); END;").expect("failure injection");
        assert_eq!(
            scheduler.set_basic(false, now()),
            Err(TelemetryError::Storage)
        );
        assert_eq!(basic.phase.load(Ordering::SeqCst), CANCELLED);
        assert!(scheduler.prepare(now(), "en").is_err());
    }

    #[test]
    fn telemetry_last_report_reads_byte_exact_payload_from_durable_audit_after_ack() {
        let (mut scheduler, audit, storage) = configured();
        assert_eq!(last_report(&audit).expect("empty audit"), None);

        scheduler.disclosure_dismissed(now()).expect("dismiss");
        scheduler.set_basic(true, now()).expect("basic");
        let ping = job(&mut scheduler, now());
        scheduler
            .complete(acknowledged(&ping), now())
            .expect("ack clears the ping");
        let basic = job(&mut scheduler, now());
        scheduler
            .complete(acknowledged(&basic), now())
            .expect("ack clears the basic delivery");
        assert_eq!(pending_pings(&scheduler), 0);

        // Reopen the real scratch-backed service: the report must survive in
        // the durable audit, not in any in-memory state.
        drop(audit);
        let reopened =
            dbflux_audit::AuditService::new_sqlite(storage.dbflux_db_path()).expect("reopen");
        let report = last_report(&reopened)
            .expect("query")
            .expect("confirmed report");
        assert_eq!(report.payload, basic.payload());
        assert_eq!(report.payload_len, basic.payload().len());
        assert_eq!(report.level.as_deref(), Some("basic"));
        assert_eq!(report.correlation, Some(basic.correlation_id()));
    }

    #[test]
    fn telemetry_last_report_confirms_only_stored_or_duplicate_outcomes() {
        let (mut scheduler, audit, _) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");

        // A failed delivery leaves attempt + failure outcome events: no report.
        let failed = job(&mut scheduler, now());
        let completion = failed.attempt_with(now(), |dispatch| {
            assert!(dispatch());
            Err(TransportError::Timeout)
        });
        scheduler.complete(completion, now()).expect("recorded");
        assert_eq!(last_report(&audit).expect("query"), None);

        // A later confirmed delivery is the newest report.
        let delivered = job(&mut scheduler, now() + chrono::Duration::minutes(3));
        scheduler
            .complete(acknowledged(&delivered), now())
            .expect("ack");
        let report = last_report(&audit).expect("query").expect("report");
        assert_eq!(report.payload, delivered.payload());
        assert_eq!(report.correlation, Some(delivered.correlation_id()));
    }

    /// One confirmed delivery followed by four failed attempts (attempt +
    /// failure-outcome rows each) must still surface the confirmed report:
    /// the SQL filter selects success outcomes before the row bound.
    #[test]
    fn telemetry_last_report_survives_later_failure_and_pending_rows() {
        let (mut scheduler, audit, _) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        scheduler.set_basic(true, now()).expect("basic");

        let delivered = job(&mut scheduler, now());
        scheduler
            .complete(acknowledged(&delivered), now())
            .expect("ack");

        for attempt in [1_u32, 5, 10, 20, 40, 80, 160, 320] {
            let failed = job(
                &mut scheduler,
                now() + chrono::Duration::minutes(attempt as i64),
            );
            let completion = failed.attempt_with(
                now() + chrono::Duration::minutes(attempt as i64),
                |dispatch| {
                    assert!(dispatch());
                    Err(TransportError::Timeout)
                },
            );
            scheduler.complete(completion, now()).expect("recorded");
        }

        let report = last_report(&audit)
            .expect("query")
            .expect("confirmed report");
        assert_eq!(report.payload, delivered.payload());
        assert_eq!(report.correlation, Some(delivered.correlation_id()));
    }

    /// A success-labeled row whose payload is not a real closed-schema v1
    /// delivery is never a report; the older real confirmed payload wins.
    #[test]
    fn telemetry_last_report_skips_success_rows_with_fake_payloads() {
        let (mut scheduler, audit, _) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        let delivered = job(&mut scheduler, now());
        scheduler
            .complete(acknowledged(&delivered), now())
            .expect("ack");

        for fake in [
            "{\"event\":\"ping\"}",
            "any string is not a delivery",
            r#"{"schema":"dbflux.telemetry-event/v1","event":"ping"}"#,
        ] {
            audit
                .record(
                    EventRecord::new(
                        dbflux_core::chrono::Utc::now().timestamp_millis(),
                        EventSeverity::Info,
                        EventCategory::System,
                        EventOutcome::Success,
                    )
                    .with_action("telemetry.delivery")
                    .with_summary("Telemetry delivery")
                    .with_details_json(
                        serde_json::json!({
                            "payload": fake,
                            "level": "off",
                            "phase": "outcome",
                            "code": "stored"
                        })
                        .to_string(),
                    ),
                )
                .expect("fake-success row stored");
        }

        let report = last_report(&audit).expect("query").expect("real report");
        assert_eq!(report.payload, delivered.payload());
        assert_eq!(report.correlation, Some(delivered.correlation_id()));
    }

    /// The canonical `dbflux_telemetry::validate_payload` accepts exactly the
    /// production wire shapes: the real payloads the scheduler freezes, never
    /// arbitrary look-alikes.
    #[test]
    fn telemetry_report_payload_validation_matches_the_real_wire_schema() {
        let (mut scheduler, _, _) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        let ping = job(&mut scheduler, now());
        assert!(dbflux_telemetry::validate_payload(ping.payload()).is_ok());

        scheduler.set_basic(true, now()).expect("basic");
        scheduler
            .complete(acknowledged(&ping), now())
            .expect("ack ping");
        let basic = job(&mut scheduler, now());
        assert!(dbflux_telemetry::validate_payload(basic.payload()).is_ok());

        assert!(dbflux_telemetry::validate_payload("{\"event\":\"ping\"}").is_err());
        assert!(dbflux_telemetry::validate_payload("not json").is_err());
        // Double-encoded JSON is a string, not a delivery object.
        assert!(dbflux_telemetry::validate_payload(r#""{\"event\":\"ping\"}""#).is_err());
    }

    #[test]
    fn telemetry_last_report_skips_malformed_rows_and_bounds_the_query() {
        let (mut scheduler, audit, _) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        let delivered = job(&mut scheduler, now());
        scheduler
            .complete(acknowledged(&delivered), now())
            .expect("ack");

        audit
            .record(
                EventRecord::new(
                    now().timestamp_millis(),
                    EventSeverity::Info,
                    EventCategory::System,
                    EventOutcome::Success,
                )
                .with_action("telemetry.delivery")
                .with_summary("Telemetry delivery")
                .with_details_json(
                    serde_json::json!({"phase": "outcome", "code": "stored"}).to_string(),
                ),
            )
            .expect("row without payload stored");

        // The malformed newer row is skipped, not reported or fatal.
        let report = last_report(&audit).expect("query").expect("report");
        assert_eq!(report.payload, delivered.payload());
    }

    #[test]
    fn telemetry_report_source_is_send_for_background_queries() {
        fn require_send<T: Send>() {}
        require_send::<dbflux_audit::AuditService>();
    }

    #[test]
    fn telemetry_prepare_persists_frozen_job_only_after_disclosure() {
        let (mut scheduler, _, storage) = configured();
        assert!(!scheduler.is_disclosure_dismissed());
        assert!(matches!(
            scheduler.prepare(now(), "es_AR").expect("undisclosed"),
            Preparation::Skipped(SkipReason::NotDue)
        ));
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        assert!(scheduler.is_disclosure_dismissed());
        let job = job(&mut scheduler, now());
        assert_eq!(scheduler.state().pending()[0].json(), job.payload());
        let persisted = storage
            .telemetry()
            .update(now(), |_| Ok(()))
            .expect("reload current");
        assert!(persisted.disclosure_dismissed());
        assert_eq!(persisted.pending()[0].json(), job.payload());
        assert!(matches!(
            scheduler.prepare(now(), "es_AR").expect("single flight"),
            Preparation::Skipped(SkipReason::Busy)
        ));
    }
    #[test]
    fn telemetry_later_build_startup_preserves_disclosure_and_prepares_without_welcome() {
        let (mut scheduler, audit, storage) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        let first = job(&mut scheduler, now());
        scheduler
            .complete(acknowledged(&first), now())
            .expect("complete");
        drop(first);
        drop(scheduler);
        let mut restarted = TelemetryScheduler::new(
            storage.telemetry(),
            TelemetryConfig::from_values(Some("https://example.invalid/v1/events"), None, false)
                .expect("config"),
            BuildIdentity::published(
                "1.2.4",
                ReleaseChannel::Stable,
                Platform::Linux,
                Architecture::Amd64,
            ),
            Arc::new(audit.clone()),
            Arc::new(move || audit.is_enabled()),
            now(),
        )
        .expect("restart");
        assert!(restarted.is_disclosure_dismissed());
        // A Minimal queue holds the new build's ping and the weekly active, in
        // either order: walk it until the new build is the one prepared.
        let mut saw_new_build = false;
        for _ in 0..4 {
            let Ok(Preparation::Job(job)) = restarted.prepare(now(), "en") else {
                break;
            };
            if job.payload().contains("1.2.4") {
                saw_new_build = true;
                break;
            }
            restarted
                .complete(acknowledged(&job), now())
                .expect("clear the other delivery");
        }
        assert!(saw_new_build, "the new build must be prepared");
    }

    /// Capturing a usage ticket happens on the foreground for every counted
    /// action. With a stable identity there is no epoch to persist, so the
    /// capture must be a read: a write there put a SQLite `BEGIN IMMEDIATE` on
    /// the UI thread per action.
    #[test]
    fn capturing_a_usage_ticket_does_not_write_to_storage() {
        let (mut scheduler, _, storage) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        scheduler.set_basic(true, now()).expect("basic");
        fail_writes(&storage);
        assert!(
            scheduler
                .usage_source(now())
                .expect("capture must not need a writable store")
                .is_some()
        );
        restore_writes(&storage);
    }

    #[test]
    fn telemetry_dropping_owner_cancels_unstarted_job() {
        let (mut scheduler, _, _) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        let queued = job(&mut scheduler, now());
        drop(scheduler);
        let completion = queued.attempt_with(now(), |_| panic!("owner dropped before dispatch"));
        assert_eq!(completion.code(), "cancelled");
    }
    #[test]
    fn telemetry_owned_jobs_and_completions_are_send() {
        fn require_send<T: Send>() {}
        require_send::<DeliveryJob>();
        require_send::<DeliveryCompletion>();
    }
    #[test]
    fn telemetry_build_and_injected_configuration_are_strict() {
        assert!(
            BuildIdentity::published(
                "0.9.0-dev.0",
                ReleaseChannel::current(),
                Platform::Linux,
                Architecture::Amd64
            )
            .is_err()
        );
        if env!("CARGO_PKG_VERSION") == "0.9.0-dev.0" {
            assert!(BuildIdentity::compiled().is_err());
        }
        assert!(
            BuildIdentity::published(
                "1.2.3",
                ReleaseChannel::Rc,
                Platform::Linux,
                Architecture::Amd64
            )
            .is_err()
        );
        assert!(
            BuildIdentity::published(
                "1.2.3-rc.1",
                ReleaseChannel::Rc,
                Platform::Linux,
                Architecture::Amd64
            )
            .is_ok()
        );
        assert!(
            BuildIdentity::published(
                "1.2.3-nightly+abc1234",
                ReleaseChannel::Nightly,
                Platform::Linux,
                Architecture::Amd64
            )
            .is_ok()
        );
        let endpoint = Some("http://127.0.0.1:12345/v1/events");
        assert!(TelemetryConfig::from_values(endpoint, Some("1"), false).is_err());
        assert!(TelemetryConfig::from_values(endpoint, None, true).is_err());
        assert_eq!(
            TelemetryConfig::from_values(endpoint, Some("1"), true).is_ok(),
            cfg!(debug_assertions)
        );
        assert!(
            TelemetryConfig::from_values(Some("https://example.invalid"), None, false).is_err()
        );
        let (mut scheduler, _, _) = configured();
        scheduler.identity = BuildIdentity::compiled();
        if scheduler.identity.is_err() {
            scheduler.disclosure_dismissed(now()).expect("dismiss");
            assert!(matches!(
                scheduler.prepare(now(), "en"),
                Ok(Preparation::Skipped(SkipReason::UnsupportedBuild))
            ));
            assert_eq!(pending_pings(&scheduler), 0);
        }
    }
    #[test]
    fn telemetry_audit_roundtrip_preserves_full_ping_and_basic_payloads() {
        let (mut scheduler, audit, storage) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        scheduler.set_basic(true, now()).expect("basic");
        for feature in [
            Feature::QueryRun,
            Feature::VisualQueryBuilder,
            Feature::DashboardOpen,
            Feature::ScriptRun,
            Feature::McpTool,
            Feature::SchemaInspect,
        ] {
            scheduler.increment(feature, now()).expect("increment");
        }
        let mut payloads = Vec::new();
        for _ in 0..2 {
            let job = job(&mut scheduler, now());
            let completion = acknowledged(&job);
            assert_eq!(completion.code(), "stored");
            payloads.push((job.payload().to_owned(), job.correlation_id().to_string()));
            scheduler.complete(completion, now()).expect("complete");
        }
        // Reopen the real scratch-backed service, rather than trusting record's
        // return value or an in-memory recording fake.
        drop(audit);
        let reopened =
            dbflux_audit::AuditService::new_sqlite(storage.dbflux_db_path()).expect("reopen audit");
        assert!(reopened.redact_sensitive());
        let events = reopened
            .query_extended(&dbflux_audit::query::AuditQueryFilter::default())
            .expect("persisted events");
        assert_eq!(events.len(), 4);
        for (payload, correlation) in &payloads {
            let related: Vec<_> = events
                .iter()
                .filter(|event| event.correlation_id.as_ref() == Some(correlation))
                .collect();
            assert_eq!(related.len(), 2);
            let mut phases = HashSet::new();
            for event in related {
                assert_eq!(event.category.as_deref(), Some("system"));
                assert_eq!(event.action.as_deref(), Some("telemetry.delivery"));
                assert_eq!(event.summary.as_deref(), Some("Telemetry delivery"));
                let details_json = event.details_json.as_deref().expect("details");
                assert!(details_json.len() < 64 * 1024);
                assert!(!details_json.contains("example.invalid"));
                let details: serde_json::Value =
                    serde_json::from_str(details_json).expect("details JSON");
                assert_eq!(details["payload"].as_str(), Some(payload.as_str()));
                phases.insert(details["phase"].as_str().expect("phase").to_string());
            }
            assert_eq!(
                phases,
                HashSet::from(["attempt".to_string(), "outcome".to_string()])
            );
        }
        let basic: serde_json::Value = serde_json::from_str(&payloads[1].0).expect("basic JSON");
        assert_eq!(basic["system_language"], "es");
        assert_eq!(basic["country"], "ZZ");
        assert_eq!(basic["features"].as_object().expect("six counts").len(), 6);
        assert!(
            basic["features"]
                .as_object()
                .expect("counts")
                .values()
                .all(|value| value == 1)
        );
    }
    #[test]
    fn telemetry_retries_are_identical_and_current_counters_survive_ack() {
        let (mut scheduler, _, storage) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        scheduler.set_basic(true, now()).expect("basic");
        scheduler
            .increment(Feature::QueryRun, now())
            .expect("initial count");
        let ping = job(&mut scheduler, now());
        scheduler
            .complete(acknowledged(&ping), now())
            .expect("ping ack");
        let first = job(&mut scheduler, now());
        let payload = first.payload().to_owned();
        // Independent foreground transaction represents usage arriving after
        // queueing; completion must not write the scheduler's old snapshot.
        storage
            .telemetry()
            .update(now(), |state| {
                state.increment(Feature::QueryRun);
                Ok(())
            })
            .expect("concurrent count");
        let failed = first.attempt_with(now(), |dispatch| {
            assert!(dispatch());
            Err(TransportError::Timeout)
        });
        scheduler.complete(failed, now()).expect("backoff");
        assert!(matches!(
            scheduler.prepare(now(), "fr_FR"),
            Ok(Preparation::Skipped(SkipReason::NotDue))
        ));
        let later = now() + chrono::Duration::minutes(3);
        let retry = job(&mut scheduler, later);
        assert_eq!(retry.payload(), payload);
        assert_ne!(retry.correlation_id(), first.correlation_id());
        let duplicate = retry.attempt_with(later, |dispatch| {
            assert!(dispatch());
            Ok(Some(
                Acknowledgement::parse(
                    200,
                    br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"duplicate"}"#,
                )
                .expect("duplicate ack"),
            ))
        });
        scheduler
            .complete(duplicate, later)
            .expect("duplicate completion");
        let next = job(&mut scheduler, later + chrono::Duration::days(7));
        let next_payload: serde_json::Value =
            serde_json::from_str(next.payload()).expect("next basic");
        assert_eq!(next_payload["features"]["query_run"], 1);
    }
    #[test]
    fn telemetry_off_cancels_queued_basic_and_clears_counters() {
        let (mut scheduler, _, _) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        scheduler.set_basic(true, now()).expect("basic");
        let ping = job(&mut scheduler, now());
        scheduler
            .complete(acknowledged(&ping), now())
            .expect("ping");
        let queued = job(&mut scheduler, now());
        scheduler
            .increment(Feature::QueryRun, now())
            .expect("new count");
        scheduler.set_basic(false, now()).expect("off");
        let unchanged = scheduler.state().clone();
        let completion = queued.attempt_with(now(), |_| panic!("cancelled job must not dispatch"));
        assert_eq!(completion.code(), "cancelled");
        assert_eq!(
            scheduler.complete(completion, now()).expect("stale ack"),
            CompletionDisposition::Stale
        );
        assert_eq!(scheduler.state(), &unchanged);
        scheduler.set_basic(true, now()).expect("on again");
        let basic = job(&mut scheduler, now());
        let value: serde_json::Value = serde_json::from_str(basic.payload()).expect("basic");
        assert!(value["features"].as_object().expect("counts").is_empty());
    }
    #[test]
    fn telemetry_off_prepares_nothing_after_the_disclosure_is_dismissed() {
        let (mut scheduler, audit, _) = configured();
        scheduler.set_level(Level::Off, now()).expect("off");
        scheduler.disclosure_dismissed(now()).expect("dismiss");

        for time in [now(), now() + chrono::Duration::days(8)] {
            assert!(matches!(
                scheduler.prepare(time, "en").expect("prepare"),
                Preparation::Skipped(SkipReason::NotDue)
            ));
        }

        assert_eq!(pending_pings(&scheduler), 0);
        assert!(scheduler.usage_source(now()).expect("off").is_none());
        let events = audit
            .query_extended(&dbflux_audit::query::AuditQueryFilter::default())
            .expect("audit");
        assert!(events.is_empty(), "Off records no delivery attempt");
    }

    #[test]
    fn telemetry_minimal_level_sends_one_ping_and_never_counts_usage() {
        let (mut scheduler, audit, _) = configured();
        assert_eq!(
            scheduler.state().level(),
            Level::Basic,
            "Basic is the default"
        );
        scheduler.set_level(Level::Minimal, now()).expect("minimal");
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        assert!(scheduler.usage_source(now()).expect("minimal").is_none());

        let ping = job(&mut scheduler, now());
        let payload: serde_json::Value = serde_json::from_str(ping.payload()).expect("payload");
        assert_eq!(payload["event"], "ping");
        scheduler
            .complete(acknowledged(&ping), now())
            .expect("ping");

        // Eight days later the weekly signal is due, and at Minimal it is
        // `active`, never a Basic report.
        let weekly = scheduler
            .prepare(now() + chrono::Duration::days(8), "en")
            .expect("weekly");
        let Preparation::Job(weekly) = weekly else {
            panic!("the weekly signal is due at Minimal");
        };
        let wire: serde_json::Value = serde_json::from_str(weekly.payload()).expect("wire");
        assert_eq!(wire["event"], "active");
        let report = last_report(&audit).expect("lookup").expect("report");
        assert_eq!(report.level.as_deref(), Some("minimal"));
    }

    #[test]
    fn telemetry_switching_to_off_cancels_a_queued_ping() {
        let (mut scheduler, _, _) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        let queued = job(&mut scheduler, now());

        scheduler.set_level(Level::Off, now()).expect("off");

        assert_eq!(pending_pings(&scheduler), 0);
        let completion = queued.attempt_with(now(), |_| panic!("cancelled ping must not dispatch"));
        assert_eq!(completion.code(), "cancelled");
        assert_eq!(
            scheduler.complete(completion, now()).expect("stale"),
            CompletionDisposition::Stale
        );
        assert!(matches!(
            scheduler.prepare(now(), "en").expect("off"),
            Preparation::Skipped(SkipReason::NotDue)
        ));
    }

    #[test]
    fn telemetry_switching_basic_to_minimal_cancels_only_the_queued_report() {
        let (mut scheduler, _, _) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        scheduler.set_level(Level::Basic, now()).expect("basic");
        let ping = job(&mut scheduler, now());
        scheduler
            .complete(acknowledged(&ping), now())
            .expect("ping");
        let report = job(&mut scheduler, now());

        scheduler.set_level(Level::Minimal, now()).expect("minimal");

        assert_eq!(report.phase.load(Ordering::SeqCst), CANCELLED);
        assert_eq!(pending_pings(&scheduler), 0);
        assert!(scheduler.usage_source(now()).expect("minimal").is_none());
    }

    #[test]
    fn telemetry_off_and_reset_allow_bounded_inflight_ack_without_stale_write() {
        for reset in [false, true] {
            let (mut scheduler, _, _) = configured();
            scheduler.disclosure_dismissed(now()).expect("dismiss");
            scheduler.set_basic(true, now()).expect("basic");
            let ping = job(&mut scheduler, now());
            scheduler
                .complete(acknowledged(&ping), now())
                .expect("ping");
            let basic = job(&mut scheduler, now());
            let completion = basic.attempt_with(now(), |dispatch| {
                assert!(dispatch());
                if reset {
                    scheduler.reset(now()).expect("reset while dispatched");
                } else {
                    scheduler
                        .set_basic(false, now())
                        .expect("off while dispatched");
                }
                assert!(matches!(
                    scheduler.prepare(now(), "en"),
                    Ok(Preparation::Skipped(SkipReason::Busy))
                ));
                Ok(Some(
                    Acknowledgement::parse(
                        200,
                        br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored"}"#,
                    )
                    .expect("ack"),
                ))
            });
            let unchanged = scheduler.state().clone();
            assert_eq!(
                scheduler
                    .complete(completion, now())
                    .expect("stale completion"),
                CompletionDisposition::Stale
            );
            assert_eq!(scheduler.state(), &unchanged);
            let next = scheduler.prepare(now(), "en").expect("next");
            if reset {
                let Preparation::Job(job) = next else {
                    panic!("basic after reset");
                };
                let payload: serde_json::Value =
                    serde_json::from_str(job.payload()).expect("payload");
                assert_eq!(
                    payload["event"], "basic",
                    "reset must preserve acknowledged ping history"
                );
            } else {
                assert!(matches!(next, Preparation::Skipped(SkipReason::NotDue)));
            }
        }
    }
    #[test]
    fn telemetry_checks_lease_again_immediately_before_dispatch() {
        let (mut scheduler, audit, _) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        let queued = job(&mut scheduler, now());
        let completion = queued.attempt_with(now(), |dispatch| {
            scheduler.reset(now()).expect("reset during construction");
            assert!(!dispatch());
            Ok(None)
        });
        assert_eq!(completion.code(), "cancelled");
        let events = audit
            .query_extended(&dbflux_audit::query::AuditQueryFilter::default())
            .expect("audit");
        assert_eq!(events.len(), 2, "attempt plus cancelled outcome");
        assert_eq!(
            scheduler.complete(completion, now()).expect("cancelled"),
            CompletionDisposition::Stale
        );
    }
    #[test]
    fn telemetry_disabled_audit_is_fail_closed_before_io() {
        let (mut scheduler, audit, _) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        let queued = job(&mut scheduler, now());
        audit.set_enabled(false);
        let completion = queued.attempt_with(now(), |_| panic!("no durable attempt, no I/O"));
        assert_eq!(completion.code(), "audit_failure");
        assert_eq!(
            scheduler.complete(completion, now()),
            Err(TelemetryError::Audit)
        );
        assert_eq!(pending_pings(&scheduler), 1);
    }
    #[test]
    fn telemetry_failed_outcome_audit_does_not_acknowledge() {
        let (mut scheduler, audit, _) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        let queued = job(&mut scheduler, now());
        let completion = queued.attempt_with(now(), |dispatch| {
            assert!(dispatch());
            audit.set_enabled(false);
            Ok(Some(
                Acknowledgement::parse(
                    200,
                    br#"{"schema":"dbflux.telemetry-delivery/v1","decision":"stored"}"#,
                )
                .expect("ack"),
            ))
        });
        assert_eq!(completion.code(), "audit_failure");
        assert_eq!(
            scheduler.complete(completion, now()),
            Err(TelemetryError::Audit)
        );
        assert_eq!(scheduler.state().pending()[0].json(), queued.payload());
    }
    #[test]
    fn telemetry_background_unwind_releases_dispatch_lease() {
        let (mut scheduler, _, _) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        let queued = job(&mut scheduler, now());
        let payload = queued.payload().to_owned();
        let worker = std::thread::spawn(move || {
            queued.attempt_with(now(), |dispatch| {
                assert!(dispatch());
                panic!("injected executor unwind");
            })
        });
        assert!(worker.join().is_err());
        let replacement = job(&mut scheduler, now());
        assert_eq!(replacement.payload(), payload);
        // Successful owned execution also uses the actual scratch AuditService
        // from a background thread, returning no storage handle to the owner.
        let completion = std::thread::spawn(move || acknowledged(&replacement))
            .join()
            .expect("background job");
        scheduler
            .complete(completion, now())
            .expect("foreground completion");
        assert_eq!(pending_pings(&scheduler), 0);
    }
    #[test]
    fn telemetry_dropped_owner_update_releases_finished_completion_lease() {
        let (mut scheduler, _, _) = configured();
        scheduler.set_level(Level::Minimal, now()).expect("minimal");
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        let queued = job(&mut scheduler, now());
        let payload = queued.payload().to_owned();
        let id = queued.delivery_id();
        let phase = queued.phase.clone();
        let completion = std::thread::spawn(move || acknowledged(&queued))
            .join()
            .expect("background attempt");
        assert_eq!(phase.load(Ordering::SeqCst), FINISHED);
        // A failed weak-entity update drops its uncalled closure after await,
        // including the completion it would have passed to the surviving owner.
        let owner_update = move || completion;
        drop(owner_update);
        let retry = job(&mut scheduler, now());
        assert_eq!(retry.delivery_id(), id);
        assert_eq!(retry.payload(), payload);
        assert!(!Arc::ptr_eq(&phase, &retry.phase));
        assert_eq!(pending_pings(&scheduler), 1);
        scheduler
            .complete(acknowledged(&retry), now())
            .expect("retry completion");
        assert_eq!(pending_pings(&scheduler), 0);
        // The ping is settled; what remains due is the weekly active.
        match scheduler.prepare(now(), "en") {
            Ok(Preparation::Job(job)) => assert!(
                job.payload().contains("active"),
                "only the weekly signal may still be due"
            ),
            _ => panic!("unexpected preparation"),
        }
    }

    #[test]
    fn telemetry_old_completion_drop_cannot_cancel_replacement_after_reset() {
        let (mut scheduler, _, _) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        let old = job(&mut scheduler, now());
        scheduler.reset(now()).expect("reset old identity");
        let replacement = job(&mut scheduler, now());
        let stale = old.attempt_with(now(), |_| panic!("cancelled before transport"));
        assert!(!Arc::ptr_eq(&stale.phase, &replacement.phase));
        drop(stale);
        assert_eq!(replacement.phase.load(Ordering::SeqCst), QUEUED);
        assert!(matches!(
            scheduler.prepare(now(), "en"),
            Ok(Preparation::Skipped(SkipReason::Busy))
        ));
        scheduler
            .complete(acknowledged(&replacement), now())
            .expect("replacement completion");
        assert_eq!(pending_pings(&scheduler), 0);
    }

    #[test]
    fn telemetry_stale_finished_completion_drop_cannot_cancel_same_id_generation() {
        let (mut scheduler, _, _) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        let current = job(&mut scheduler, now());
        // Model a delayed finished result from a previous attempt of the same
        // immutable delivery. Only the per-attempt Arc distinguishes leases.
        let old_phase = Arc::new(AtomicU8::new(FINISHED));
        let stale = DeliveryCompletion {
            id: current.delivery_id(),
            correlation: Uuid::new_v4(),
            phase: old_phase.clone(),
            result: AttemptOutcome::Cancelled,
        };
        assert!(!Arc::ptr_eq(&stale.phase, &current.phase));
        drop(stale);
        assert_eq!(old_phase.load(Ordering::SeqCst), CANCELLED);
        assert_eq!(current.phase.load(Ordering::SeqCst), QUEUED);
        assert!(matches!(
            scheduler.prepare(now(), "en"),
            Ok(Preparation::Skipped(SkipReason::Busy))
        ));
        scheduler
            .complete(acknowledged(&current), now())
            .expect("current completion");
        assert_eq!(pending_pings(&scheduler), 0);
    }

    #[test]
    fn telemetry_successful_completion_does_not_cancel_next_delivery() {
        let (mut scheduler, _, _) = configured();
        scheduler.set_level(Level::Minimal, now()).expect("minimal");
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        let ping = job(&mut scheduler, now());
        let completion = acknowledged(&ping);
        let old_phase = completion.phase.clone();
        assert_eq!(
            scheduler
                .complete(completion, now())
                .expect("ping completion"),
            CompletionDisposition::Applied
        );
        assert_eq!(pending_pings(&scheduler), 0);
        // The weekly active queued at Minimal is unrelated to the next Basic
        // delivery, so clear it before switching levels.
        if let Ok(Preparation::Job(weekly)) = scheduler.prepare(now(), "en") {
            scheduler
                .complete(acknowledged(&weekly), now())
                .expect("weekly active");
        }
        scheduler.set_basic(true, now()).expect("basic");
        let basic = job(&mut scheduler, now());
        assert!(!Arc::ptr_eq(&old_phase, &basic.phase));
        drop(ping);
        assert_eq!(basic.phase.load(Ordering::SeqCst), QUEUED);
        scheduler
            .complete(acknowledged(&basic), now())
            .expect("basic completion");
        assert_eq!(pending_pings(&scheduler), 0);
    }

    #[test]
    fn telemetry_completion_storage_failure_releases_lease_without_acknowledging() {
        let (mut scheduler, _, storage) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        let queued = job(&mut scheduler, now());
        let completion = acknowledged(&queued);
        let connection = storage.dbflux_db();
        connection
            .execute_batch("BEGIN IMMEDIATE")
            .expect("injected transaction contention");
        assert_eq!(
            scheduler.complete(completion, now()),
            Err(TelemetryError::Storage)
        );
        connection
            .execute_batch("ROLLBACK")
            .expect("end injected contention");
        let retry = job(&mut scheduler, now());
        assert_eq!(retry.payload(), queued.payload());
    }
    #[test]
    fn telemetry_old_cancelled_completion_cannot_release_new_lease() {
        let (mut scheduler, _, _) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        let old = job(&mut scheduler, now());
        scheduler.reset(now()).expect("cancel old queue");
        let new = job(&mut scheduler, now());
        assert_ne!(old.delivery_id(), new.delivery_id());
        let stale = old.attempt_with(now(), |_| panic!("cancelled old job"));
        assert_eq!(
            scheduler.complete(stale, now()).expect("stale"),
            CompletionDisposition::Stale
        );
        assert!(matches!(
            scheduler.prepare(now(), "en"),
            Ok(Preparation::Skipped(SkipReason::Busy))
        ));
        scheduler
            .complete(acknowledged(&new), now())
            .expect("new completion");
    }
    #[test]
    fn telemetry_dropped_job_releases_lease_and_permanent_failure_is_not_retried() {
        let (mut scheduler, _, _) = configured();
        scheduler.set_level(Level::Minimal, now()).expect("minimal");
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        let queued = job(&mut scheduler, now());
        let payload = queued.payload().to_owned();
        drop(queued);
        let replacement = job(&mut scheduler, now());
        assert_eq!(replacement.payload(), payload);
        let completion = replacement.attempt_with(now(), |dispatch| {
            assert!(dispatch());
            Err(TransportError::Conflict)
        });
        assert_eq!(completion.code(), "conflict");
        scheduler
            .complete(completion, now())
            .expect("permanent rejection");
        // The rejected ping is not retried; the weekly active is unrelated and
        // may still be prepared.
        match scheduler.prepare(now() + chrono::Duration::days(1), "en") {
            Ok(Preparation::Job(job)) => assert!(
                job.payload().contains("active"),
                "a permanently rejected ping must not be prepared again"
            ),
            _ => panic!("unexpected preparation"),
        }
        assert_eq!(pending_pings(&scheduler), 1);
    }
    /// The 60 s tick runs on the UI thread: when nothing is due it must not
    /// open a SQLite write transaction. The injected trigger makes any write
    /// fail, so success proves the tick only read the cached state.
    #[test]
    fn telemetry_idle_prepare_does_not_write_storage() {
        let (mut scheduler, _, storage) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        while let Ok(Preparation::Job(due)) = scheduler.prepare(now(), "en") {
            scheduler
                .complete(acknowledged(&due), now())
                .expect("settle every due delivery");
        }
        fail_writes(&storage);
        let later = now() + chrono::Duration::minutes(1);
        assert!(matches!(
            scheduler
                .prepare(later, "en")
                .expect("idle tick needs no write"),
            Preparation::Skipped(SkipReason::NotDue)
        ));
        restore_writes(&storage);
    }
    #[test]
    fn telemetry_prepare_before_the_disclosure_does_not_write_storage() {
        let (mut scheduler, _, storage) = configured();
        fail_writes(&storage);
        assert!(matches!(
            scheduler.prepare(now(), "en").expect("nothing to schedule"),
            Preparation::Skipped(SkipReason::NotDue)
        ));
        restore_writes(&storage);
    }
    #[test]
    fn telemetry_prepare_still_writes_when_a_delivery_becomes_due() {
        let (mut scheduler, _, storage) = configured();
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        while let Ok(Preparation::Job(due)) = scheduler.prepare(now(), "en") {
            scheduler
                .complete(acknowledged(&due), now())
                .expect("settle every due delivery");
        }
        let week_later = now() + chrono::Duration::days(8);
        let Preparation::Job(weekly) = scheduler.prepare(week_later, "en").expect("weekly") else {
            panic!("the weekly report is due");
        };
        let persisted = storage
            .telemetry()
            .update(week_later, |_| Ok(()))
            .expect("reload");
        assert_eq!(persisted.pending()[0].json(), weekly.payload());
    }
    #[test]
    fn telemetry_compiled_endpoint_is_the_production_collector() {
        let config = TelemetryConfig::resolve(None, None, false, None).expect("default");
        assert_eq!(
            config.endpoint,
            Some(
                Endpoint::parse(Some("https://telemetry.dbflux.dev/v1/events"), false)
                    .expect("production endpoint")
            )
        );
        assert_eq!(
            DEFAULT_TELEMETRY_ENDPOINT,
            "https://telemetry.dbflux.dev/v1/events"
        );
    }
    #[test]
    fn telemetry_endpoint_override_replaces_the_default_under_the_transport_rules() {
        let custom = "https://collector.example.test/v1/events";
        let config = TelemetryConfig::resolve(Some(custom), None, false, None).expect("override");
        assert_eq!(
            config.endpoint,
            Some(Endpoint::parse(Some(custom), false).expect("custom"))
        );
        for rejected in [
            "http://collector.example.test/v1/events",
            "https://collector.example.test/other",
            "https://collector.example.test/v1/events?x=1",
            "",
        ] {
            assert!(
                TelemetryConfig::resolve(Some(rejected), None, false, None).is_err(),
                "{rejected:?} must be rejected, never fall back to the default"
            );
        }
    }
    #[test]
    fn telemetry_kill_switch_beats_the_compiled_default_and_any_override() {
        for switch in ["off", "OFF", " Off "] {
            let config = TelemetryConfig::resolve(None, None, false, Some(switch)).expect("off");
            assert!(config.endpoint.is_none(), "{switch:?}");
            let config = TelemetryConfig::resolve(
                Some("https://collector.example.test/v1/events"),
                None,
                false,
                Some(switch),
            )
            .expect("off with override");
            assert!(config.endpoint.is_none(), "{switch:?}");
        }
        for other in ["on", "1", ""] {
            let config = TelemetryConfig::resolve(None, None, false, Some(other)).expect("other");
            assert!(config.endpoint.is_some(), "{other:?} is not a kill switch");
        }
    }
    #[test]
    fn telemetry_kill_switch_prepares_nothing_even_after_the_disclosure() {
        let config = TelemetryConfig::resolve(None, None, false, Some("off")).expect("off");
        let (mut scheduler, _, _) = fixture(config);
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        assert!(!scheduler.endpoint_configured());
        assert!(matches!(
            scheduler.prepare(now(), "en").expect("no endpoint"),
            Preparation::Skipped(SkipReason::NoEndpoint)
        ));
    }
    #[test]
    fn telemetry_level_off_sends_nothing_with_the_compiled_default_endpoint() {
        let config = TelemetryConfig::resolve(None, None, false, None).expect("default");
        let (mut scheduler, _, _) = fixture(config);
        scheduler.set_level(Level::Off, now()).expect("off");
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        assert!(matches!(
            scheduler.prepare(now(), "en").expect("off"),
            Preparation::Skipped(SkipReason::NotDue)
        ));
        assert!(scheduler.state().pending().is_empty());
    }
    #[test]
    fn telemetry_a_panicking_request_becomes_a_retryable_failure() {
        let outcome = guarded_request(|| panic!("injected transport panic"));
        assert_eq!(outcome.err(), Some(TransportError::Network));
        assert!(TransportError::Network.retryable());
        let outcome = guarded_request(|| Err(TransportError::Conflict));
        assert_eq!(outcome.err(), Some(TransportError::Conflict));
    }
    #[test]
    fn telemetry_default_endpoint_is_unavailable() {
        let (mut scheduler, _, _) = fixture(TelemetryConfig::default());
        scheduler.disclosure_dismissed(now()).expect("dismiss");
        assert!(matches!(
            scheduler.prepare(now(), "en").expect("no endpoint"),
            Preparation::Skipped(SkipReason::NoEndpoint)
        ));
        assert_eq!(pending_pings(&scheduler), 0);
    }
}
