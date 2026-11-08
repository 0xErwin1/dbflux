//! Independent verification probes for the governed write seam, adopted
//! from the review that found the gaps they probe: the scrub's variant
//! coverage, the base64 echo of a binary payload, the store's write
//! capability, and an audit sink that refuses its event after the write
//! landed. They run beside the module's own tests and are kept as
//! regression probes for those findings.
//
// The probes hand `put_secret_value`-shaped closures to a stub store and
// mirror its `StoreSource::put_value`-shaped results, which trips lints
// written for boundary code; the assertions, not the shapes, are what these
// probes verify.
#![allow(clippy::result_large_err)]
#![allow(clippy::redundant_closure)]

use super::super::encode_base64;
use super::super::value::{RetrievalTarget, value_retrieval_event};
use super::super::write::*;
use dbflux_core::observability::{EventCategory, EventRecord, EventSink, EventSinkError};
use dbflux_core::secrecy::ExposeSecret;
use dbflux_core::{
    DbError, MutationPolicy, ReadOnlyReason, SecretCreateRequest, SecretCreated,
    SecretDeleteRequest, SecretDeletion, SecretDescriptionChange, SecretDescriptionUpdate,
    SecretDetails, SecretListPage, SecretListRequest, SecretPayload, SecretPutRequest,
    SecretRollback, SecretRollbackRequest, SecretRotationChange, SecretRotationStart,
    SecretRotationUpdate, SecretStoreCapabilities, SecretStoreDescriptor, SecretStoreSource,
    SecretTagsUpdate, SecretValue, SecretVersionStage,
};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use uuid::Uuid;

type Behaviour = Box<dyn Fn(&SecretPutRequest) -> Result<SecretValue, DbError> + Send + Sync>;

struct ProbeStore {
    descriptor: SecretStoreDescriptor,
    calls: AtomicUsize,
    behaviour: Behaviour,
}

impl ProbeStore {
    fn new(behaviour: Behaviour) -> Self {
        Self {
            descriptor: SecretStoreDescriptor::new(
                "probe",
                "Probe",
                SecretStoreCapabilities::WRITE,
            ),
            calls: AtomicUsize::new(0),
            behaviour,
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

impl SecretStoreSource for ProbeStore {
    fn descriptor(&self) -> &SecretStoreDescriptor {
        &self.descriptor
    }
    fn list(&self, _request: &SecretListRequest) -> Result<SecretListPage, DbError> {
        Err(DbError::NotSupported("unused".into()))
    }
    fn describe(&self, _name: &str) -> Result<SecretDetails, DbError> {
        Err(DbError::NotSupported("unused".into()))
    }
    fn get_value(&self, _name: &str, _version: Option<&str>) -> Result<SecretValue, DbError> {
        Err(DbError::NotSupported("unused".into()))
    }
    fn put_value(&self, request: &SecretPutRequest) -> Result<SecretValue, DbError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        (self.behaviour)(request)
    }
}

#[derive(Default)]
struct Sink(Mutex<Vec<EventRecord>>);

impl Sink {
    fn events(&self) -> Vec<EventRecord> {
        match self.0.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }
    fn actions(&self) -> Vec<String> {
        self.events()
            .into_iter()
            .map(|event| event.action)
            .collect()
    }
}

impl EventSink for Sink {
    fn record(&self, event: EventRecord) -> Result<EventRecord, EventSinkError> {
        match self.0.lock() {
            Ok(mut guard) => guard.push(event.clone()),
            Err(poisoned) => poisoned.into_inner().push(event.clone()),
        }
        Ok(event)
    }
}

const PROFILE: Uuid = Uuid::from_u128(0xfeed);

fn run(
    store: &ProbeStore,
    sink: &Sink,
    policy: MutationPolicy,
    reason: Option<ReadOnlyReason>,
    request: &SecretPutRequest,
) -> Result<SecretValue, SecretWriteFailure> {
    put_secret_value(store, PROFILE, "aws_secrets", policy, reason, sink, request)
}

fn succeed(request: &SecretPutRequest) -> Result<SecretValue, DbError> {
    Ok(SecretValue {
        version: Some("v-probe-1".to_string()),
        payload: request.payload.clone(),
    })
}

fn payload_text(request: &SecretPutRequest) -> String {
    match &request.payload {
        SecretPayload::Text(t) => t.expose_secret().to_owned(),
        SecretPayload::Binary(b) => String::from_utf8_lossy(b.expose_secret()).into_owned(),
    }
}

/// G1/S1: exactly one event per path, never zero, never two.
#[test]
fn probe_exactly_one_event_per_path() {
    let request = SecretPutRequest::text("prod/token", "probe-value-1");

    let store = ProbeStore::new(Box::new(|request| succeed(request)));
    let sink = Sink::default();
    run(
        &store,
        &sink,
        MutationPolicy::ReadOnly,
        Some(ReadOnlyReason::ProfileSetting),
        &request,
    )
    .expect_err("a read-only profile refuses");
    assert_eq!(
        sink.actions(),
        vec![SECRET_VALUE_WRITE_REFUSED_ACTION.to_string()]
    );
    assert_eq!(store.calls(), 0, "read-only reached the store");

    let store = ProbeStore::new(Box::new(|request| succeed(request)));
    let sink = Sink::default();
    run(
        &store,
        &sink,
        MutationPolicy::ApprovalRequired,
        None,
        &request,
    )
    .expect_err("an approval-required profile refuses");
    assert_eq!(
        sink.actions(),
        vec![SECRET_VALUE_WRITE_REFUSED_ACTION.to_string()]
    );
    assert_eq!(store.calls(), 0, "approval-required reached the store");

    let store = ProbeStore::new(Box::new(|request| succeed(request)));
    let sink = Sink::default();
    run(&store, &sink, MutationPolicy::Allowed, None, &request).expect("success");
    assert_eq!(
        sink.actions(),
        vec![SECRET_VALUE_WRITTEN_ACTION.to_string()]
    );
    assert_eq!(store.calls(), 1);

    let store = ProbeStore::new(Box::new(|r| {
        Err(DbError::permission_denied(format!("denied {}", r.name)))
    }));
    let sink = Sink::default();
    run(&store, &sink, MutationPolicy::Allowed, None, &request).expect_err("failure");
    assert_eq!(
        sink.actions(),
        vec![SECRET_VALUE_WRITE_FAILED_ACTION.to_string()]
    );
    assert_eq!(store.calls(), 1);
}

/// G2/S2: a refusal returns before the store is called, for both policies.
#[test]
fn probe_refusals_never_call_the_store() {
    let request = SecretPutRequest::text("prod/token", "probe-value-2");
    for policy in [MutationPolicy::ReadOnly, MutationPolicy::ApprovalRequired] {
        let store = ProbeStore::new(Box::new(|request| succeed(request)));
        let sink = Sink::default();
        let error = run(
            &store,
            &sink,
            policy,
            Some(ReadOnlyReason::ServerEnforced),
            &request,
        )
        .expect_err("refused");
        assert_eq!(store.calls(), 0, "{policy:?} called the store");
        assert!(
            matches!(error.error, DbError::PermissionDenied(_)),
            "{policy:?}: {error:?}"
        );
    }
}

/// G4: refusals are non-retriable permission denials, worded distinctly.
#[test]
fn probe_refusals_are_permission_denied_not_transient() {
    let request = SecretPutRequest::text("prod/token", "probe-value-3");
    let store = ProbeStore::new(Box::new(|request| succeed(request)));

    let sink = Sink::default();
    let read_only = run(
        &store,
        &sink,
        MutationPolicy::ReadOnly,
        Some(ReadOnlyReason::ProfileSetting),
        &request,
    )
    .expect_err("read-only");
    let approval = run(
        &store,
        &sink,
        MutationPolicy::ApprovalRequired,
        None,
        &request,
    )
    .expect_err("approval");

    let (DbError::PermissionDenied(read_only_refusal), DbError::PermissionDenied(approval_refusal)) =
        (&read_only.error, &approval.error)
    else {
        panic!("refusals are not PermissionDenied: {read_only:?} / {approval:?}");
    };
    assert!(
        !read_only_refusal.retriable,
        "read-only refusal marked retriable"
    );
    assert!(
        !approval_refusal.retriable,
        "approval refusal marked retriable"
    );
    assert!(
        read_only_refusal.message.contains("read-only"),
        "{}",
        read_only_refusal.message
    );
    assert!(
        approval_refusal.message.contains("approval"),
        "{}",
        approval_refusal.message
    );
    assert_ne!(
        read_only_refusal.message, approval_refusal.message,
        "refusals are not distinguishable"
    );
    // Refusals record no failure event, so no correlation id travels with
    // them.
    assert!(read_only.correlation_id.is_none(), "{read_only:?}");
    assert!(approval.correlation_id.is_none(), "{approval:?}");

    let events = sink.events();
    assert_eq!(events.len(), 2);
    for event in &events {
        assert_eq!(event.action, SECRET_VALUE_WRITE_REFUSED_ACTION);
    }
    let reasons: Vec<String> = events
        .iter()
        .map(|e| {
            let details: serde_json::Value =
                serde_json::from_str(e.details_json.as_deref().expect("details")).expect("json");
            details["reason"].as_str().unwrap_or_default().to_string()
        })
        .collect();
    assert_eq!(
        reasons,
        vec!["read_only".to_string(), "approval_required".to_string()]
    );
}

/// G3: a text failure echoing the value is scrubbed in the error and the audit.
#[test]
fn probe_text_failure_is_scrubbed() {
    let request = SecretPutRequest::text("prod/token", "probe-value-4");
    let store = ProbeStore::new(Box::new(|r| {
        Err(DbError::query_failed(format!(
            "the service rejected the value {}",
            payload_text(r)
        )))
    }));
    let sink = Sink::default();
    let error = run(&store, &sink, MutationPolicy::Allowed, None, &request).expect_err("failed");
    let message = error.to_string();
    assert!(!message.contains("probe-value-4"), "{message}");
    assert!(message.contains("[redacted]"), "{message}");
    assert_eq!(sink.events().len(), 1);
    assert!(!format!("{:?}", sink.events()).contains("probe-value-4"));
}

/// G3 boundary: an empty value scrubs nothing and does not mangle the message.
#[test]
fn probe_empty_value_keeps_the_message() {
    let request = SecretPutRequest::text("prod/token", "");
    let store = ProbeStore::new(Box::new(|_| {
        Err(DbError::NotSupported("the store refused the write".into()))
    }));
    let sink = Sink::default();
    let error = run(&store, &sink, MutationPolicy::Allowed, None, &request).expect_err("failed");
    assert_eq!(
        error.to_string(),
        "Operation not supported: the store refused the write"
    );
}

/// G3 gap probe: a binary value echoed as base64 by a store is not scrubbed.
#[test]
fn probe_binary_failure_echoed_as_base64_is_scrubbed() {
    let request = SecretPutRequest::binary("prod/blob", vec![0xff, 0x00, 0x41, 0x42, 0x43]);
    let encoded = encode_base64(match &request.payload {
        SecretPayload::Binary(b) => b.expose_secret(),
        _ => unreachable!(),
    });
    let echoed = encoded.clone();
    let store = ProbeStore::new(Box::new(move |_| {
        Err(DbError::query_failed(format!("bad blob {echoed}")))
    }));
    let sink = Sink::default();
    let error = run(&store, &sink, MutationPolicy::Allowed, None, &request).expect_err("failed");
    assert!(
        !error.to_string().contains(&encoded),
        "binary value reached the caller as base64: {error}"
    );
}

/// G3 gap probe: an `IoError` carrying the value is not scrubbed at all.
#[test]
fn probe_io_error_carrying_the_value_is_scrubbed() {
    let request = SecretPutRequest::text("prod/token", "probe-value-5");
    let store = ProbeStore::new(Box::new(|r| {
        Err(DbError::IoError(std::io::Error::other(format!(
            "socket write failed for {}",
            payload_text(r)
        ))))
    }));
    let sink = Sink::default();
    let error = run(&store, &sink, MutationPolicy::Allowed, None, &request).expect_err("failed");
    assert!(!error.to_string().contains("probe-value-5"), "{error}");
    let events = sink.events();
    assert_eq!(events.len(), 1);
    assert!(
        !events[0]
            .error_message
            .as_deref()
            .unwrap_or_default()
            .contains("probe-value-5"),
        "audit error_message leaked: {:?}",
        events[0].error_message
    );
}

/// G6: the write event matches the retrieval event's shape and validates.
#[test]
fn probe_write_event_matches_retrieval_shape_and_validates() {
    let write_target = WriteTarget {
        profile_id: PROFILE,
        driver_id: "aws_secrets",
        store_id: "aws-secrets-manager",
        store_label: "Secrets Manager",
        name: "prod/token",
    };
    let read_target = RetrievalTarget {
        profile_id: PROFILE,
        driver_id: "aws_secrets",
        store_id: "aws-secrets-manager",
        store_label: "Secrets Manager",
        name: "prod/token",
    };

    let written = secret_write_event(
        &write_target,
        SecretWriteOutcome::Written {
            version: Some("v-9"),
            stage: SecretVersionStage::Current,
        },
    );
    let read = value_retrieval_event(&read_target, Some("v-9"), None);

    assert_eq!(written.category, read.category);
    assert_eq!(written.actor_id, read.actor_id);
    assert_eq!(written.object_type, read.object_type);
    assert_eq!(written.object_id, read.object_id);
    assert_eq!(written.connection_id, read.connection_id);
    assert_eq!(written.driver_id, read.driver_id);
    assert_eq!(written.source_id, read.source_id);
    assert_eq!(written.actor_type, read.actor_type);

    let details: serde_json::Value =
        serde_json::from_str(written.details_json.as_deref().expect("details")).expect("json");
    assert_eq!(details["version"], serde_json::json!("v-9"));

    for outcome in [
        SecretWriteOutcome::Written {
            version: Some("v-9"),
            stage: SecretVersionStage::Pending,
        },
        SecretWriteOutcome::Refused {
            reason: "approval_required",
        },
        SecretWriteOutcome::Failed {
            message: "denied by the service",
        },
    ] {
        let event = secret_write_event(&write_target, outcome);
        dbflux_audit::AuditService::validate_event(&event).expect("valid event");
        assert_eq!(event.category, EventCategory::SecretStore);
        assert!(!event.summary.is_empty());
    }
}

/// G5: a store that advertises no `WRITE` capability is refused before any
/// call, audited once as a refusal for `store_not_writable` — the failed
/// action never appears, and the store's own forgetful gate is irrelevant.
#[test]
fn probe_a_store_without_write_capability_is_refused_before_any_call() {
    struct ReadOnlyStore(AtomicUsize);
    impl SecretStoreSource for ReadOnlyStore {
        fn descriptor(&self) -> &SecretStoreDescriptor {
            static DESCRIPTOR: std::sync::OnceLock<SecretStoreDescriptor> =
                std::sync::OnceLock::new();
            DESCRIPTOR.get_or_init(|| {
                SecretStoreDescriptor::new("ro", "RO", SecretStoreCapabilities::empty())
            })
        }
        fn list(&self, _request: &SecretListRequest) -> Result<SecretListPage, DbError> {
            Err(DbError::NotSupported("unused".into()))
        }
        fn describe(&self, _name: &str) -> Result<SecretDetails, DbError> {
            Err(DbError::NotSupported("unused".into()))
        }
        fn get_value(&self, _name: &str, _version: Option<&str>) -> Result<SecretValue, DbError> {
            Err(DbError::NotSupported("unused".into()))
        }
        fn put_value(&self, request: &SecretPutRequest) -> Result<SecretValue, DbError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            succeed(request)
        }
    }

    let store = ReadOnlyStore(AtomicUsize::new(0));
    let sink = Sink::default();
    let request = SecretPutRequest::text("prod/token", "probe-value-6");

    put_secret_value(
        &store,
        PROFILE,
        "probe",
        MutationPolicy::Allowed,
        None,
        &sink,
        &request,
    )
    .expect_err("a store that advertises no WRITE capability refuses");

    assert_eq!(
        store.0.load(Ordering::Relaxed),
        0,
        "the seam called put_value on a store that advertises no WRITE capability"
    );

    let events = sink.events();
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(
        events[0].action, SECRET_VALUE_WRITE_REFUSED_ACTION,
        "the capability refusal is audited as a refusal, not a failure: {events:?}"
    );
    assert_ne!(
        events[0].action, SECRET_VALUE_WRITE_FAILED_ACTION,
        "the capability refusal is audited as a refusal, not a failure: {events:?}"
    );
    let details: serde_json::Value =
        serde_json::from_str(events[0].details_json.as_deref().expect("details"))
            .expect("details_json is valid JSON");
    assert_eq!(
        details["reason"],
        serde_json::json!("store_not_writable"),
        "{details}"
    );
}

/// G1: a sink that refuses the event must not swallow a write the store
/// accepted, nor misreport it: the store was called once, the message says
/// the write landed, and the error is not retriable — a retry would only
/// write a second version.
#[test]
fn probe_a_refusing_sink_reports_that_the_write_landed() {
    struct RefusingSink;
    impl EventSink for RefusingSink {
        fn record(&self, _event: EventRecord) -> Result<EventRecord, EventSinkError> {
            Err(EventSinkError::Storage("audit store is closed".into()))
        }
    }

    let store = ProbeStore::new(Box::new(|request| succeed(request)));
    let request = SecretPutRequest::text("prod/token", "probe-value-7");

    let error = put_secret_value(
        &store,
        PROFILE,
        "aws_secrets",
        MutationPolicy::Allowed,
        None,
        &RefusingSink,
        &request,
    )
    .expect_err("the unaudited write must reach the caller");

    assert_eq!(
        store.calls(),
        1,
        "the write must not be aborted because the sink failed"
    );

    let message = error.to_string();
    assert!(message.contains("landed"), "{message}");
    assert!(!message.contains("probe-value-7"), "{message}");

    let DbError::QueryFailed(formatted) = &error.error else {
        panic!("the unaudited write is not a plain operation failure: {error:?}");
    };
    assert!(
        !formatted.retriable,
        "the audit-loss message presented itself as retryable"
    );
}

/// The value the sweep submits: stated in Rust source, the verbatim string
/// carries a backslash, a quote, a tab and a newline, so a JSON-escaped
/// echo differs from the verbatim form in four places.
const SWEEP_VALUE: &str = "sweep\\-value with\"quotes\" and\ttabs and\nnewlines";

/// The JSON-escaped form of a value, without its surrounding quotes: what a
/// serializer writes for it inside a JSON body.
fn json_escaped(value: &str) -> String {
    let quoted = serde_json::to_string(value).expect("a &str always serializes");
    quoted[1..quoted.len() - 1].to_string()
}

/// One sweep case: a store whose write fails with a `DbError` carrying the
/// sweep value in every field a rendered message can reach.
type SweepCase = (&'static str, Behaviour);

/// Builds one sweep case per message-carrying `DbError` variant.
fn sweep_cases() -> Vec<SweepCase> {
    // One formatted error carrying the value in every field a rendered
    // message can reach, including `location.schema`.
    let formatted = || {
        dbflux_core::FormattedError::new(format!("rejected {SWEEP_VALUE}"))
            .with_detail(format!("detail {SWEEP_VALUE}"))
            .with_hint(format!("hint {SWEEP_VALUE}"))
            .with_code(format!("SQLSTATE:{SWEEP_VALUE}"))
            .with_location(dbflux_core::ErrorLocation {
                schema: Some(format!("schema_{SWEEP_VALUE}")),
                table: Some(format!("table_{SWEEP_VALUE}")),
                column: Some(format!("column_{SWEEP_VALUE}")),
                constraint: Some(format!("constraint_{SWEEP_VALUE}")),
            })
    };

    vec![
        (
            "ConnectionFailed",
            Box::new(move |_| Err(DbError::ConnectionFailed(formatted()))),
        ),
        (
            "QueryFailed",
            Box::new(move |_| Err(DbError::QueryFailed(formatted()))),
        ),
        (
            "AuthFailed",
            Box::new(move |_| Err(DbError::AuthFailed(formatted()))),
        ),
        (
            "ConstraintViolation",
            Box::new(move |_| Err(DbError::ConstraintViolation(formatted()))),
        ),
        (
            "SyntaxError",
            Box::new(move |_| Err(DbError::SyntaxError(formatted()))),
        ),
        (
            "PermissionDenied",
            Box::new(move |_| Err(DbError::PermissionDenied(formatted()))),
        ),
        (
            "ObjectNotFound",
            Box::new(move |_| Err(DbError::ObjectNotFound(formatted()))),
        ),
        (
            "NotSupported",
            Box::new(move |_| Err(DbError::NotSupported(format!("refused {SWEEP_VALUE}")))),
        ),
        (
            "InvalidProfile",
            Box::new(move |_| Err(DbError::InvalidProfile(format!("malformed {SWEEP_VALUE}")))),
        ),
        (
            "ValueResolutionFailed",
            Box::new(move |_| {
                Err(DbError::ValueResolutionFailed(format!(
                    "unresolved {SWEEP_VALUE}"
                )))
            }),
        ),
        (
            "Parse",
            Box::new(move |_| Err(DbError::Parse(format!("unparsed {SWEEP_VALUE}")))),
        ),
        (
            "Unsupported",
            Box::new(move |_| Err(DbError::Unsupported(format!("unsupported {SWEEP_VALUE}")))),
        ),
        (
            "IoError",
            Box::new(move |_| {
                Err(DbError::IoError(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("io failed {SWEEP_VALUE}"),
                )))
            }),
        ),
    ]
}

/// Runs one sweep case through the seam and asserts what every variant
/// shares: the value is gone from the surfaced message, the marker is
/// there, and exactly one failure event with a scrubbed `error_message` was
/// recorded. Returns the failure for the per-kind checks.
fn assert_sweep_case(label: &'static str, behaviour: Behaviour) -> SecretWriteFailure {
    let store = ProbeStore::new(behaviour);
    let sink = Sink::default();
    let request = SecretPutRequest::text("prod/token", SWEEP_VALUE);
    let escaped = json_escaped(SWEEP_VALUE);

    let failure = run(&store, &sink, MutationPolicy::Allowed, None, &request).expect_err(label);

    // Neither the verbatim value nor the form a serializer would echo it in
    // reaches the caller, and the marker shows something was removed.
    let message = failure.to_string();
    assert!(!message.contains(SWEEP_VALUE), "{label}: {message}");
    assert!(!message.contains(&escaped), "{label}: {message}");
    assert!(message.contains("[redacted]"), "{label}: {message}");

    let events = sink.events();
    assert_eq!(events.len(), 1, "{label}");
    let recorded = events[0].error_message.as_deref().unwrap_or_default();
    assert!(!recorded.contains(SWEEP_VALUE), "{label}: {recorded}");
    assert!(!recorded.contains(&escaped), "{label}: {recorded}");

    failure
}

/// G3, exhaustive: every message-carrying `DbError` variant is scrubbed of
/// the submitted value in every field a rendered message reaches —
/// including `location.schema`, which `to_display_string` does not render —
/// an `io::Error` keeps its kind, and the message-less variants pass
/// through untouched.
#[test]
fn every_db_error_variant_is_scrubbed_and_io_keeps_its_kind() {
    for (label, behaviour) in sweep_cases() {
        let failure = assert_sweep_case(label, behaviour);

        // The `FormattedError` variants keep every field scrubbed in place,
        // even the ones `to_display_string` does not render.
        if let Some(scrubbed) = failure.error.formatted() {
            for field in [
                scrubbed.message.as_str(),
                scrubbed.detail.as_deref().unwrap_or_default(),
                scrubbed.hint.as_deref().unwrap_or_default(),
                scrubbed.code.as_deref().unwrap_or_default(),
            ] {
                assert!(!field.contains("sweep"), "{label}: {field}");
                assert!(
                    field.contains("[redacted]") || field.is_empty(),
                    "{label}: {field}"
                );
            }
            let location = scrubbed
                .location
                .as_ref()
                .expect("the echo filled the location");
            for field in [
                &location.schema,
                &location.table,
                &location.column,
                &location.constraint,
            ] {
                let field = field
                    .as_deref()
                    .expect("the echo filled every location field");
                assert!(!field.contains("sweep"), "{label}: {field}");
                assert!(field.contains("[redacted]"), "{label}: {field}");
            }
        }

        // The rebuilt `io::Error` keeps its kind.
        if let DbError::IoError(io) = &failure.error {
            assert_eq!(
                io.kind(),
                std::io::ErrorKind::PermissionDenied,
                "{label}: the io::Error kind was lost in the scrub"
            );
        }
    }

    // The message-less variants cannot carry a value and pass through
    // untouched.
    let timeout = ProbeStore::new(Box::new(|_request| Err(DbError::Timeout)));
    let error = run(
        &timeout,
        &Sink::default(),
        MutationPolicy::Allowed,
        None,
        &SecretPutRequest::text("prod/token", SWEEP_VALUE),
    )
    .expect_err("timeout");
    assert!(matches!(error.error, DbError::Timeout), "{error:?}");

    let cancelled = ProbeStore::new(Box::new(|_request| Err(DbError::Cancelled)));
    let error = run(
        &cancelled,
        &Sink::default(),
        MutationPolicy::Allowed,
        None,
        &SecretPutRequest::text("prod/token", SWEEP_VALUE),
    )
    .expect_err("cancelled");
    assert!(matches!(error.error, DbError::Cancelled), "{error:?}");
}

/// G1: a refusing sink must not distort the outcomes it did not prevent: a
/// policy refusal, a capability refusal and a store failure each return
/// their own operation error unchanged, none panics, and the store is
/// reached exactly when it may be.
#[test]
fn probe_a_refusing_sink_keeps_every_other_outcome_intact() {
    struct ReadOnlyStore(AtomicUsize);
    impl SecretStoreSource for ReadOnlyStore {
        fn descriptor(&self) -> &SecretStoreDescriptor {
            static DESCRIPTOR: std::sync::OnceLock<SecretStoreDescriptor> =
                std::sync::OnceLock::new();
            DESCRIPTOR.get_or_init(|| {
                SecretStoreDescriptor::new("ro", "RO", SecretStoreCapabilities::empty())
            })
        }
        fn list(&self, _request: &SecretListRequest) -> Result<SecretListPage, DbError> {
            Err(DbError::NotSupported("unused".into()))
        }
        fn describe(&self, _name: &str) -> Result<SecretDetails, DbError> {
            Err(DbError::NotSupported("unused".into()))
        }
        fn get_value(&self, _name: &str, _version: Option<&str>) -> Result<SecretValue, DbError> {
            Err(DbError::NotSupported("unused".into()))
        }
        fn put_value(&self, request: &SecretPutRequest) -> Result<SecretValue, DbError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            succeed(request)
        }
    }

    struct RefusingSink;
    impl EventSink for RefusingSink {
        fn record(&self, _event: EventRecord) -> Result<EventRecord, EventSinkError> {
            Err(EventSinkError::Storage("audit store is closed".into()))
        }
    }

    let request = SecretPutRequest::text("prod/token", "probe-value-8");

    // A policy refusal surfaces with its own message, before any call.
    let store = ProbeStore::new(Box::new(|request| succeed(request)));
    let refusal = put_secret_value(
        &store,
        PROFILE,
        "aws_secrets",
        MutationPolicy::ReadOnly,
        Some(ReadOnlyReason::ProfileSetting),
        &RefusingSink,
        &request,
    )
    .expect_err("the policy refusal surfaces");
    assert!(
        matches!(refusal.error, DbError::PermissionDenied(_)),
        "{refusal:?}"
    );
    assert!(refusal.to_string().contains("read-only"), "{refusal}");
    assert_eq!(store.calls(), 0);

    // A capability refusal surfaces with the store-level message.
    let read_only_store = ReadOnlyStore(AtomicUsize::new(0));
    let refusal = put_secret_value(
        &read_only_store,
        PROFILE,
        "probe",
        MutationPolicy::Allowed,
        None,
        &RefusingSink,
        &request,
    )
    .expect_err("the capability refusal surfaces");
    assert!(
        matches!(refusal.error, DbError::NotSupported(_)),
        "{refusal:?}"
    );
    assert!(
        refusal
            .to_string()
            .contains("does not support writing values"),
        "{refusal}"
    );
    assert_eq!(
        read_only_store.0.load(Ordering::Relaxed),
        0,
        "the capability refusal reached the store"
    );

    // A store failure surfaces with the store's own message, once.
    let store = ProbeStore::new(Box::new(|request| {
        Err(DbError::permission_denied(format!(
            "denied {}",
            request.name
        )))
    }));
    let failure = put_secret_value(
        &store,
        PROFILE,
        "aws_secrets",
        MutationPolicy::Allowed,
        None,
        &RefusingSink,
        &request,
    )
    .expect_err("the store failure surfaces");
    assert!(
        failure.to_string().contains("denied prod/token"),
        "{failure}"
    );
    assert_eq!(store.calls(), 1);
}

// --- Operations beyond a value put ---
//
// Every operation takes the same governed path, so one table of cases
// drives the same probes over all of them: a refusal records the refusal
// and makes no store call, a store failure is audited once with its
// correlation id, an audit loss after a landed operation says it landed, a
// read-only profile refuses, and no secret value reaches an audit field.

type OperationRun = Box<dyn Fn(&WriteContext<'_>) -> Result<(), SecretWriteFailure>>;

struct OperationCase {
    label: &'static str,
    operation: SecretOperation,
    name: &'static str,
    run: OperationRun,
    /// Secret values the case submits; none may appear in any audit field.
    values: Vec<&'static str>,
}

/// A store that advertises the capabilities it is given and answers every
/// operation with the behaviour it is given, counting the calls it gets.
struct OperationStore {
    descriptor: SecretStoreDescriptor,
    calls: AtomicUsize,
    failure: Option<Box<dyn Fn() -> DbError + Send + Sync>>,

    /// The store method each call reached and the request it carried.
    routes: Mutex<Vec<(&'static str, String)>>,
    unfinished_version: Option<String>,
}

impl OperationStore {
    fn new(capabilities: SecretStoreCapabilities) -> Self {
        Self {
            descriptor: SecretStoreDescriptor::new("probe", "Probe", capabilities),
            calls: AtomicUsize::new(0),
            failure: None,
            routes: Mutex::new(Vec::new()),
            unfinished_version: None,
        }
    }

    fn leaving_unfinished(mut self, version: &str) -> Self {
        self.unfinished_version = Some(version.to_string());
        self
    }

    fn routes(&self) -> Vec<(&'static str, String)> {
        match self.routes.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    fn failing(mut self, failure: impl Fn() -> DbError + Send + Sync + 'static) -> Self {
        self.failure = Some(Box::new(failure));
        self
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }

    fn answer(&self, method: &'static str, request: &dyn std::fmt::Debug) -> Result<(), DbError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut routes) = self.routes.lock() {
            routes.push((method, format!("{request:?}")));
        }

        match &self.failure {
            Some(failure) => Err(failure()),
            None => Ok(()),
        }
    }
}

impl SecretStoreSource for OperationStore {
    fn descriptor(&self) -> &SecretStoreDescriptor {
        &self.descriptor
    }
    fn list(&self, _request: &SecretListRequest) -> Result<SecretListPage, DbError> {
        Err(DbError::NotSupported("unused".into()))
    }
    fn describe(&self, _name: &str) -> Result<SecretDetails, DbError> {
        Err(DbError::NotSupported("unused".into()))
    }
    fn get_value(&self, _name: &str, _version: Option<&str>) -> Result<SecretValue, DbError> {
        Err(DbError::NotSupported("unused".into()))
    }
    fn roll_back(&self, request: &SecretRollbackRequest) -> Result<SecretRollback, DbError> {
        self.answer("roll_back", request)?;
        Ok(SecretRollback {
            current_version: request.version_id.clone(),
            previous_version: Some("v-before".to_string()),
        })
    }
    fn delete_secret(&self, request: &SecretDeleteRequest) -> Result<SecretDeletion, DbError> {
        self.answer("delete_secret", request)?;
        Ok(SecretDeletion::default())
    }
    fn restore_secret(&self, name: &str) -> Result<(), DbError> {
        self.answer("restore_secret", &name)
    }
    fn rotate_now(&self, name: &str) -> Result<SecretRotationStart, DbError> {
        self.answer("rotate_now", &name)?;
        Ok(SecretRotationStart {
            version: Some("v-rotated".to_string()),
        })
    }
    fn update_rotation(
        &self,
        update: &SecretRotationUpdate,
    ) -> Result<SecretRotationChange, DbError> {
        self.answer("update_rotation", update)?;
        Ok(SecretRotationChange {
            unfinished_version: self.unfinished_version.clone(),
        })
    }
    fn update_tags(&self, update: &SecretTagsUpdate) -> Result<(), DbError> {
        self.answer("update_tags", update)
    }
    fn create_secret(&self, request: &SecretCreateRequest) -> Result<SecretCreated, DbError> {
        self.answer("create_secret", request)?;
        Ok(SecretCreated {
            version: Some("v-created".to_string()),
            resource_id: Some("arn:probe:secret:created".to_string()),
        })
    }
    fn update_description(
        &self,
        update: &SecretDescriptionUpdate,
    ) -> Result<SecretDescriptionChange, DbError> {
        self.answer("update_description", update)?;
        Ok(SecretDescriptionChange {
            version: Some("4".to_string()),
        })
    }
}

fn operation_cases() -> Vec<OperationCase> {
    let mut cases = lifecycle_cases();
    cases.extend(rotation_cases());
    cases.extend(metadata_cases());
    cases
}

fn lifecycle_cases() -> Vec<OperationCase> {
    vec![
        OperationCase {
            label: "roll back",
            operation: SecretOperation::RollBack,
            name: "prod/api/credentials",
            run: Box::new(|context| {
                roll_back_secret(
                    context,
                    &SecretRollbackRequest::new("prod/api/credentials", "v-oldest"),
                )
                .map(|_| ())
            }),
            values: Vec::new(),
        },
        OperationCase {
            label: "scheduled delete",
            operation: SecretOperation::DeleteScheduled,
            name: "prod/api/credentials",
            run: Box::new(|context| {
                delete_secret(
                    context,
                    &SecretDeleteRequest::scheduled("prod/api/credentials", 30),
                )
                .map(|_| ())
            }),
            values: Vec::new(),
        },
        OperationCase {
            label: "irreversible delete",
            operation: SecretOperation::DeleteIrreversible,
            name: "/app/prod/db/host",
            run: Box::new(|context| {
                delete_secret(
                    context,
                    &SecretDeleteRequest::irreversible("/app/prod/db/host", "/app/prod/db/host"),
                )
                .map(|_| ())
            }),
            values: Vec::new(),
        },
        OperationCase {
            label: "restore",
            operation: SecretOperation::Restore,
            name: "prod/api/credentials",
            run: Box::new(|context| restore_secret(context, "prod/api/credentials")),
            values: Vec::new(),
        },
    ]
}

fn rotation_cases() -> Vec<OperationCase> {
    vec![
        OperationCase {
            label: "rotate now",
            operation: SecretOperation::RotateNow,
            name: "prod/api/credentials",
            run: Box::new(|context| rotate_secret_now(context, "prod/api/credentials").map(|_| ())),
            values: Vec::new(),
        },
        OperationCase {
            label: "edit rotation",
            operation: SecretOperation::EditRotation,
            name: "prod/api/credentials",
            run: Box::new(|context| {
                update_secret_rotation(
                    context,
                    &SecretRotationUpdate::enable("prod/api/credentials", "rate(30 days)")
                        .with_window("4h"),
                )
                .map(|_| ())
            }),
            values: Vec::new(),
        },
    ]
}

fn metadata_cases() -> Vec<OperationCase> {
    vec![
        OperationCase {
            label: "edit tags",
            operation: SecretOperation::EditTags,
            name: "prod/api/credentials",
            run: Box::new(|context| {
                update_secret_tags(
                    context,
                    &SecretTagsUpdate::new("prod/api/credentials")
                        .set("env", "tag-value-probe")
                        .remove("legacy"),
                )
            }),
            values: vec!["tag-value-probe"],
        },
        OperationCase {
            label: "edit description",
            operation: SecretOperation::EditDescription,
            name: "prod/api/credentials",
            run: Box::new(|context| {
                update_secret_description(
                    context,
                    &SecretDescriptionUpdate::new("prod/api/credentials", "description-probe"),
                )
                .map(|_| ())
            }),
            values: vec!["description-probe"],
        },
        OperationCase {
            label: "create",
            operation: SecretOperation::Create,
            name: "prod/billing/api-key",
            run: Box::new(|context| {
                create_secret(
                    context,
                    &SecretCreateRequest::text("prod/billing/api-key", "create-value-probe")
                        .with_description("create-description-probe")
                        .with_tag("env", "create-tag-probe"),
                )
                .map(|_| ())
            }),
            values: vec![
                "create-value-probe",
                "create-description-probe",
                "create-tag-probe",
            ],
        },
    ]
}

fn context<'a>(
    store: &'a OperationStore,
    sink: &'a dyn EventSink,
    policy: MutationPolicy,
    reason: Option<ReadOnlyReason>,
) -> WriteContext<'a> {
    WriteContext {
        store,
        profile_id: PROFILE,
        driver_id: "aws_secrets",
        policy,
        read_only_reason: reason,
        audit: sink,
    }
}

fn details_of(event: &EventRecord) -> serde_json::Value {
    serde_json::from_str(event.details_json.as_deref().expect("details")).expect("json")
}

#[test]
fn every_operation_pins_its_classification_and_capabilities() {
    let write = SecretStoreCapabilities::WRITE;

    for (operation, classification, capabilities) in [
        (
            SecretOperation::PutValue,
            dbflux_core::ExecutionClassification::Write,
            write,
        ),
        (
            SecretOperation::RollBack,
            dbflux_core::ExecutionClassification::Write,
            write | SecretStoreCapabilities::VERSION_LABELS,
        ),
        (
            SecretOperation::DeleteScheduled,
            dbflux_core::ExecutionClassification::Destructive,
            write | SecretStoreCapabilities::DELETE | SecretStoreCapabilities::SOFT_DELETE,
        ),
        (
            SecretOperation::DeleteIrreversible,
            dbflux_core::ExecutionClassification::AdminDestructive,
            write | SecretStoreCapabilities::DELETE,
        ),
        (
            SecretOperation::Restore,
            dbflux_core::ExecutionClassification::Write,
            write | SecretStoreCapabilities::SOFT_DELETE,
        ),
        (
            SecretOperation::Create,
            dbflux_core::ExecutionClassification::Write,
            write | SecretStoreCapabilities::CREATE,
        ),
        (
            SecretOperation::EditTags,
            dbflux_core::ExecutionClassification::Write,
            write | SecretStoreCapabilities::TAGS,
        ),
        (
            SecretOperation::EditDescription,
            dbflux_core::ExecutionClassification::Write,
            write | SecretStoreCapabilities::DESCRIPTION,
        ),
        (
            SecretOperation::RotateNow,
            dbflux_core::ExecutionClassification::Write,
            write | SecretStoreCapabilities::ROTATION,
        ),
        (
            SecretOperation::EditRotation,
            dbflux_core::ExecutionClassification::Write,
            write | SecretStoreCapabilities::ROTATION,
        ),
    ] {
        assert_eq!(operation.classification(), classification, "{operation:?}");
        assert_eq!(
            operation.required_capabilities(),
            capabilities,
            "{operation:?}"
        );
    }
}

#[test]
fn every_operation_refuses_a_gated_profile_before_any_call() {
    for case in operation_cases() {
        for (policy, reason, audited) in [
            (
                MutationPolicy::ReadOnly,
                Some(ReadOnlyReason::ProfileSetting),
                "read_only",
            ),
            (MutationPolicy::ApprovalRequired, None, "approval_required"),
        ] {
            let store = OperationStore::new(case.operation.required_capabilities());
            let sink = Sink::default();

            let failure =
                (case.run)(&context(&store, &sink, policy, reason)).expect_err(case.label);

            assert_eq!(
                failure.kind,
                SecretWriteFailureKind::Refused,
                "{}",
                case.label
            );
            assert!(
                matches!(failure.error, DbError::PermissionDenied(_)),
                "{}: {failure:?}",
                case.label
            );
            assert!(failure.correlation_id.is_none(), "{}", case.label);
            assert_eq!(
                store.calls(),
                0,
                "{}: {audited} reached the store",
                case.label
            );

            let events = sink.events();
            assert_eq!(events.len(), 1, "{}", case.label);
            assert_eq!(
                events[0].action,
                case.operation.actions().refused,
                "{}",
                case.label
            );
            assert_eq!(details_of(&events[0])["reason"], audited, "{}", case.label);
        }
    }
}

#[test]
fn every_operation_refuses_a_store_that_lacks_a_capability_before_any_call() {
    for case in operation_cases() {
        let required = case.operation.required_capabilities();

        // No WRITE at all, then WRITE without the operation's own
        // capability (skipped for an operation that needs only WRITE).
        let mut stores = vec![(
            OperationStore::new(SecretStoreCapabilities::empty()),
            "store_not_writable",
        )];
        if required != SecretStoreCapabilities::WRITE {
            stores.push((
                OperationStore::new(SecretStoreCapabilities::WRITE),
                "operation_not_supported",
            ));
        }
        if case.operation == SecretOperation::DeleteScheduled {
            for capability in [
                SecretStoreCapabilities::DELETE,
                SecretStoreCapabilities::SOFT_DELETE,
            ] {
                stores.push((
                    OperationStore::new(SecretStoreCapabilities::WRITE | capability),
                    "operation_not_supported",
                ));
            }
        }

        for (store, audited) in stores {
            let sink = Sink::default();

            let failure = (case.run)(&context(&store, &sink, MutationPolicy::Allowed, None))
                .expect_err(case.label);

            assert_eq!(
                failure.kind,
                SecretWriteFailureKind::Refused,
                "{}",
                case.label
            );
            assert!(
                matches!(failure.error, DbError::NotSupported(_)),
                "{}: {failure:?}",
                case.label
            );
            assert_eq!(store.calls(), 0, "{}", case.label);

            let events = sink.events();
            assert_eq!(events.len(), 1, "{}", case.label);
            assert_eq!(events[0].action, case.operation.actions().refused);
            assert_eq!(details_of(&events[0])["reason"], audited, "{}", case.label);
        }
    }
}

#[test]
fn every_operation_audits_a_store_failure_once_with_its_correlation_id() {
    for case in operation_cases() {
        let store = OperationStore::new(case.operation.required_capabilities())
            .failing(|| DbError::permission_denied("denied by the service"));
        let sink = Sink::default();

        let failure = (case.run)(&context(&store, &sink, MutationPolicy::Allowed, None))
            .expect_err(case.label);

        assert_eq!(
            failure.kind,
            SecretWriteFailureKind::StoreFailed,
            "{}",
            case.label
        );
        assert_eq!(store.calls(), 1, "{}", case.label);
        assert!(failure.to_string().contains("denied by the service"));

        let events = sink.events();
        assert_eq!(events.len(), 1, "{}", case.label);
        assert_eq!(events[0].action, case.operation.actions().failed);
        assert_eq!(
            events[0].correlation_id.as_deref(),
            failure.correlation_id.map(|id| id.to_string()).as_deref(),
            "{}",
            case.label
        );
        assert!(failure.correlation_id.is_some(), "{}", case.label);
    }
}

#[test]
fn every_operation_scrubs_the_values_it_submits_from_a_store_failure() {
    for case in operation_cases() {
        let echoed = case.values.join(" ");
        let store = OperationStore::new(case.operation.required_capabilities())
            .failing(move || DbError::query_failed(format!("service rejected {echoed}")));
        let sink = Sink::default();

        let failure = (case.run)(&context(&store, &sink, MutationPolicy::Allowed, None))
            .expect_err(case.label);

        for value in &case.values {
            assert!(!failure.to_string().contains(value), "{}", case.label);
            assert!(
                !format!("{:?}", sink.events()).contains(value),
                "{}",
                case.label
            );
        }
    }
}

#[test]
fn every_operation_reports_that_it_landed_when_the_audit_event_is_lost() {
    struct RefusingSink;
    impl EventSink for RefusingSink {
        fn record(&self, _event: EventRecord) -> Result<EventRecord, EventSinkError> {
            Err(EventSinkError::Storage("audit store is closed".into()))
        }
    }

    for case in operation_cases() {
        let store = OperationStore::new(case.operation.required_capabilities());

        let failure = (case.run)(&context(
            &store,
            &RefusingSink,
            MutationPolicy::Allowed,
            None,
        ))
        .expect_err(case.label);

        assert_eq!(
            failure.kind,
            SecretWriteFailureKind::AuditLost,
            "{}",
            case.label
        );
        assert_eq!(store.calls(), 1, "{}", case.label);
        assert!(failure.to_string().contains("landed"), "{failure}");
        let DbError::QueryFailed(formatted) = &failure.error else {
            panic!("{}: {failure:?}", case.label);
        };
        assert!(!formatted.retriable, "{}", case.label);
    }
}

#[test]
fn every_operation_records_one_event_shaped_like_a_retrieval() {
    let read_target = RetrievalTarget {
        profile_id: PROFILE,
        driver_id: "aws_secrets",
        store_id: "probe",
        store_label: "Probe",
        name: "prod/api/credentials",
    };

    for case in operation_cases() {
        let store = OperationStore::new(case.operation.required_capabilities());
        let sink = Sink::default();

        (case.run)(&context(&store, &sink, MutationPolicy::Allowed, None)).expect(case.label);

        let events = sink.events();
        assert_eq!(events.len(), 1, "{}", case.label);
        let event = &events[0];
        let read = value_retrieval_event(
            &RetrievalTarget {
                name: case.name,
                ..read_target
            },
            None,
            None,
        );

        assert_eq!(event.action, case.operation.actions().done);
        assert_eq!(event.category, read.category, "{}", case.label);
        assert_eq!(event.actor_id, read.actor_id, "{}", case.label);
        assert_eq!(event.actor_type, read.actor_type, "{}", case.label);
        assert_eq!(event.object_type, read.object_type, "{}", case.label);
        assert_eq!(event.object_id, read.object_id, "{}", case.label);
        assert_eq!(event.connection_id, read.connection_id, "{}", case.label);
        assert_eq!(event.driver_id, read.driver_id, "{}", case.label);
        assert_eq!(
            details_of(event)["classification"],
            serde_json::to_value(case.operation.classification()).expect("classification"),
            "{}",
            case.label
        );
        dbflux_audit::AuditService::validate_event(event).expect("valid event");
        for value in &case.values {
            assert!(!format!("{event:?}").contains(value), "{}", case.label);
        }
    }
}

#[test]
fn a_confirmation_that_is_not_the_exact_name_refuses_an_irreversible_delete_before_any_call() {
    let store =
        OperationStore::new(SecretStoreCapabilities::WRITE | SecretStoreCapabilities::DELETE);

    for typed in [
        "",
        "/app/prod/db/hos",
        "/APP/PROD/DB/HOST",
        "/app/prod/db/host ",
    ] {
        let sink = Sink::default();

        let failure = delete_secret(
            &context(&store, &sink, MutationPolicy::Allowed, None),
            &SecretDeleteRequest::irreversible("/app/prod/db/host", typed),
        )
        .expect_err("a mismatched confirmation refuses");

        assert_eq!(failure.kind, SecretWriteFailureKind::Refused, "{typed:?}");
        assert!(
            matches!(failure.error, DbError::ConstraintViolation(_)),
            "{typed:?}: {failure:?}"
        );
        assert_eq!(store.calls(), 0, "{typed:?} reached the store");

        let events = sink.events();
        assert_eq!(events.len(), 1, "{typed:?}");
        assert_eq!(
            events[0].action,
            SecretOperation::DeleteIrreversible.actions().refused
        );
        let details = details_of(&events[0]);
        assert_eq!(details["reason"], "confirmation_mismatch", "{typed:?}");
        // The typed text is not an audit field; the name is the object.
        assert!(
            details.get("confirmation").is_none(),
            "{typed:?}: {details}"
        );
    }
}

#[test]
fn a_window_outside_seven_to_thirty_days_refuses_a_scheduled_delete_before_any_call() {
    for days in [0, 6, 31] {
        let store = OperationStore::new(SecretOperation::DeleteScheduled.required_capabilities());
        let sink = Sink::default();

        let failure = delete_secret(
            &context(&store, &sink, MutationPolicy::Allowed, None),
            &SecretDeleteRequest::scheduled("prod/api/credentials", days),
        )
        .expect_err("a window outside the accepted days refuses");

        assert_eq!(failure.kind, SecretWriteFailureKind::Refused, "{days}");
        assert_eq!(store.calls(), 0, "{days}");
        assert_eq!(
            details_of(&sink.events()[0])["reason"],
            "invalid_request",
            "{days}"
        );
    }
}

#[test]
fn a_store_with_a_recovery_window_is_never_asked_to_delete_without_recovery() {
    let store = OperationStore::new(
        SecretStoreCapabilities::WRITE
            | SecretStoreCapabilities::DELETE
            | SecretStoreCapabilities::SOFT_DELETE,
    );
    let sink = Sink::default();

    let failure = delete_secret(
        &context(&store, &sink, MutationPolicy::Allowed, None),
        &SecretDeleteRequest::irreversible("prod/api/credentials", "prod/api/credentials"),
    )
    .expect_err("a store with a recovery window refuses an irreversible delete");

    assert_eq!(failure.kind, SecretWriteFailureKind::Refused);
    assert!(
        matches!(failure.error, DbError::NotSupported(_)),
        "{failure:?}"
    );
    assert_eq!(store.calls(), 0);
    assert_eq!(
        details_of(&sink.events()[0])["reason"],
        "recovery_window_available"
    );
}

#[test]
fn a_scheduled_delete_records_its_window_and_an_irreversible_one_says_so() {
    let store = OperationStore::new(SecretOperation::DeleteScheduled.required_capabilities());
    let sink = Sink::default();
    delete_secret(
        &context(&store, &sink, MutationPolicy::Allowed, None),
        &SecretDeleteRequest::scheduled("prod/api/credentials", 14),
    )
    .expect("scheduled");
    let details = details_of(&sink.events()[0]);
    assert_eq!(details["recovery_window_days"], 14);
    assert_eq!(details["classification"], "destructive");

    let store = OperationStore::new(SecretOperation::DeleteIrreversible.required_capabilities());
    let sink = Sink::default();
    delete_secret(
        &context(&store, &sink, MutationPolicy::Allowed, None),
        &SecretDeleteRequest::irreversible("/app/prod/db/host", "/app/prod/db/host"),
    )
    .expect("irreversible");
    let details = details_of(&sink.events()[0]);
    assert_eq!(details["irreversible"], true);
    assert_eq!(details["classification"], "admin_destructive");
}

#[test]
fn a_rotation_edit_the_service_would_reject_is_refused_before_any_call() {
    for update in [
        SecretRotationUpdate::enable("prod/api/credentials", "every month"),
        SecretRotationUpdate::enable("prod/api/credentials", "rate(30 days)").with_window("90m"),
        SecretRotationUpdate::enable("prod/api/credentials", "rate(30 days)")
            .with_function("rotate-fn"),
        SecretRotationUpdate {
            schedule: Some("rate(30 days)".to_string()),
            ..SecretRotationUpdate::disable("prod/api/credentials")
        },
    ] {
        let store = OperationStore::new(SecretOperation::EditRotation.required_capabilities());
        let sink = Sink::default();

        let failure = update_secret_rotation(
            &context(&store, &sink, MutationPolicy::Allowed, None),
            &update,
        )
        .expect_err("an invalid edit refuses");

        assert_eq!(failure.kind, SecretWriteFailureKind::Refused, "{update:?}");
        assert!(
            matches!(failure.error, DbError::ConstraintViolation(_)),
            "{failure:?}"
        );
        assert_eq!(store.calls(), 0, "{update:?}");
        let events = sink.events();
        assert_eq!(events.len(), 1);
        assert_eq!(details_of(&events[0])["reason"], "invalid_request");
    }
}

#[test]
fn a_rotation_edit_records_the_settings_it_applied() {
    let store = OperationStore::new(SecretOperation::EditRotation.required_capabilities());
    let sink = Sink::default();
    let lambda = "arn:aws:lambda:eu-west-1:441012345678:function:rotate";

    update_secret_rotation(
        &context(&store, &sink, MutationPolicy::Allowed, None),
        &SecretRotationUpdate::enable("prod/api/credentials", "rate(30 days)")
            .with_window("4h")
            .with_function(lambda),
    )
    .expect("applied");
    let details = details_of(&sink.events()[0]);
    assert_eq!(details["enabled"], true);
    assert_eq!(details["schedule"], "rate(30 days)");
    assert_eq!(details["window"], "4h");
    assert_eq!(details["function"], lambda);

    let sink = Sink::default();
    update_secret_rotation(
        &context(&store, &sink, MutationPolicy::Allowed, None),
        &SecretRotationUpdate::disable("prod/api/credentials"),
    )
    .expect("applied");
    let details = details_of(&sink.events()[0]);
    assert_eq!(details["enabled"], false);
    assert!(details.get("schedule").is_none());
}

#[test]
fn a_started_rotation_records_the_version_it_created() {
    let store = OperationStore::new(SecretOperation::RotateNow.required_capabilities());
    let sink = Sink::default();

    rotate_secret_now(
        &context(&store, &sink, MutationPolicy::Allowed, None),
        "prod/api/credentials",
    )
    .expect("started");

    assert_eq!(details_of(&sink.events()[0])["version"], "v-rotated");
}

#[test]
fn a_tag_edit_records_the_keys_it_changed_and_never_the_values() {
    let store = OperationStore::new(SecretOperation::EditTags.required_capabilities());
    let sink = Sink::default();

    update_secret_tags(
        &context(&store, &sink, MutationPolicy::Allowed, None),
        &SecretTagsUpdate::new("prod/api/credentials")
            .set("env", "tag-value-probe-1")
            .set("owner", "tag-value-probe-2")
            .remove("legacy"),
    )
    .expect("applied");

    let events = sink.events();
    let details = details_of(&events[0]);
    assert_eq!(details["set_keys"], serde_json::json!(["env", "owner"]));
    assert_eq!(details["removed_keys"], serde_json::json!(["legacy"]));
    assert!(!format!("{events:?}").contains("tag-value-probe"));
}

#[test]
fn a_tag_the_service_would_reject_refuses_before_any_call() {
    for update in [
        SecretTagsUpdate::new("prod/api/credentials"),
        SecretTagsUpdate::new("prod/api/credentials").set("aws:env", "x"),
        SecretTagsUpdate::new("prod/api/credentials").set("", "x"),
        SecretTagsUpdate::new("prod/api/credentials").set("k", "v".repeat(257)),
        SecretTagsUpdate::new("prod/api/credentials")
            .set("env", "a")
            .remove("env"),
    ] {
        let store = OperationStore::new(SecretOperation::EditTags.required_capabilities());
        let sink = Sink::default();

        let failure = update_secret_tags(
            &context(&store, &sink, MutationPolicy::Allowed, None),
            &update,
        )
        .expect_err("an invalid tag edit refuses");

        assert_eq!(failure.kind, SecretWriteFailureKind::Refused, "{update:?}");
        assert!(
            matches!(failure.error, DbError::ConstraintViolation(_)),
            "{failure:?}"
        );
        assert_eq!(store.calls(), 0, "{update:?}");
        assert_eq!(details_of(&sink.events()[0])["reason"], "invalid_request");
    }
}

#[test]
fn a_description_edit_never_records_its_text_and_names_a_version_it_created() {
    let store = OperationStore::new(SecretOperation::EditDescription.required_capabilities());
    let sink = Sink::default();

    update_secret_description(
        &context(&store, &sink, MutationPolicy::Allowed, None),
        &SecretDescriptionUpdate::new("prod/api/credentials", "description-probe-text"),
    )
    .expect("applied");

    let events = sink.events();
    let details = details_of(&events[0]);
    assert_eq!(details["cleared"], false);
    assert_eq!(details["version"], "4");
    assert!(!format!("{events:?}").contains("description-probe-text"));

    let sink = Sink::default();
    update_secret_description(
        &context(&store, &sink, MutationPolicy::Allowed, None),
        &SecretDescriptionUpdate::new("prod/api/credentials", ""),
    )
    .expect("cleared");
    assert_eq!(details_of(&sink.events()[0])["cleared"], true);
}

#[test]
fn a_create_the_service_would_reject_refuses_before_any_call_and_names_no_value() {
    for request in [
        SecretCreateRequest::text("", "create-value-probe"),
        SecretCreateRequest::text("prod/billing/api-key", ""),
        SecretCreateRequest::binary("prod/billing/api-key", b"create-value-probe".to_vec()),
        SecretCreateRequest::text("prod/billing/api-key", "create-value-probe")
            .with_tag("aws:env", "x"),
        SecretCreateRequest::text("prod/billing/api-key", "create-value-probe")
            .with_tag("env", "a")
            .with_tag("env", "b"),
    ] {
        let store = OperationStore::new(SecretOperation::Create.required_capabilities());
        let sink = Sink::default();

        let failure = create_secret(
            &context(&store, &sink, MutationPolicy::Allowed, None),
            &request,
        )
        .expect_err("an invalid create refuses");

        assert_eq!(failure.kind, SecretWriteFailureKind::Refused, "{request:?}");
        assert!(
            matches!(failure.error, DbError::ConstraintViolation(_)),
            "{failure:?}"
        );
        assert_eq!(store.calls(), 0, "{request:?}");
        let events = sink.events();
        assert_eq!(details_of(&events[0])["reason"], "invalid_request");
        assert!(!format!("{events:?}").contains("create-value-probe"));
        assert!(!failure.to_string().contains("create-value-probe"));
    }
}

#[test]
fn a_create_failure_that_echoes_the_value_is_scrubbed_in_every_form() {
    let value = "create\"value\\probe\nline";
    let escaped = json_escaped(value);

    for echoed in [value.to_string(), escaped.clone()] {
        let store = OperationStore::new(SecretOperation::Create.required_capabilities())
            .failing(move || DbError::query_failed(format!("service rejected {echoed}")));
        let sink = Sink::default();

        let failure = create_secret(
            &context(&store, &sink, MutationPolicy::Allowed, None),
            &SecretCreateRequest::text("prod/billing/api-key", value),
        )
        .expect_err("the store failed");

        assert_eq!(failure.kind, SecretWriteFailureKind::StoreFailed);
        let message = failure.to_string();
        assert!(!message.contains(value), "{message}");
        assert!(!message.contains(&escaped), "{message}");
        assert!(message.contains("[redacted]"), "{message}");
        let events = sink.events();
        assert_eq!(events.len(), 1);
        let recorded = events[0].error_message.as_deref().unwrap_or_default();
        assert!(!recorded.contains(value), "{recorded}");
        assert!(!recorded.contains(&escaped), "{recorded}");
    }
}

#[test]
fn a_taken_name_is_a_store_failure_audited_once_and_never_an_overwrite() {
    let store = OperationStore::new(SecretOperation::Create.required_capabilities())
        .failing(|| DbError::constraint_violation("Secret prod/billing/api-key already exists"));
    let sink = Sink::default();

    let failure = create_secret(
        &context(&store, &sink, MutationPolicy::Allowed, None),
        &SecretCreateRequest::text("prod/billing/api-key", "create-value-probe"),
    )
    .expect_err("the name is taken");

    assert_eq!(failure.kind, SecretWriteFailureKind::StoreFailed);
    assert!(failure.to_string().contains("already exists"), "{failure}");
    assert_eq!(store.calls(), 1);
    let events = sink.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].action, SecretOperation::Create.actions().failed);
    assert!(failure.correlation_id.is_some());
}

#[test]
fn a_create_records_the_settings_and_tag_keys_and_never_the_value_or_tag_values() {
    let store = OperationStore::new(SecretOperation::Create.required_capabilities());
    let sink = Sink::default();

    create_secret(
        &context(&store, &sink, MutationPolicy::Allowed, None),
        &SecretCreateRequest::text("/app/prod/db/replica", "create-value-probe")
            .with_value_type("SecureString")
            .with_tier("Advanced")
            .with_encryption_key("alias/app-prod")
            .with_description("description-probe-text")
            .with_tag("env", "tag-value-probe"),
    )
    .expect("created");

    let events = sink.events();
    let details = details_of(&events[0]);
    assert_eq!(details["value_type"], "SecureString");
    assert_eq!(details["tier"], "Advanced");
    assert_eq!(details["encryption_key"], "alias/app-prod");
    assert_eq!(details["has_description"], true);
    assert_eq!(details["tag_keys"], serde_json::json!(["env"]));
    assert_eq!(details["version"], "v-created");
    let rendered = format!("{events:?}");
    for leaked in [
        "create-value-probe",
        "tag-value-probe",
        "description-probe-text",
    ] {
        assert!(!rendered.contains(leaked), "{leaked}");
    }
}

#[test]
fn the_approval_refusal_keeps_its_wording_per_operation() {
    let put = put_secret_value(
        &OperationStore::new(SecretStoreCapabilities::WRITE),
        PROFILE,
        "aws_secrets",
        MutationPolicy::ApprovalRequired,
        None,
        &Sink::default(),
        &SecretPutRequest::text("prod/token", "probe-value-approval"),
    )
    .expect_err("approval-required refuses");
    assert_eq!(
        put.to_string(),
        "Permission denied: Writes on this connection require approval, and approving secret value writes \
         is not supported yet. The write to \"prod/token\" was refused."
    );

    let store = OperationStore::new(SecretOperation::RollBack.required_capabilities());
    let roll_back = roll_back_secret(
        &context(
            &store,
            &Sink::default(),
            MutationPolicy::ApprovalRequired,
            None,
        ),
        &SecretRollbackRequest::new("prod/token", "v-1"),
    )
    .expect_err("approval-required refuses");
    assert!(
        roll_back
            .to_string()
            .contains("approving secret writes is not supported yet"),
        "{roll_back}"
    );
}

#[test]
fn a_blank_name_refuses_restore_rotate_and_description_before_any_call() {
    let sink = Sink::default();
    let store = OperationStore::new(
        SecretStoreCapabilities::WRITE
            | SecretStoreCapabilities::SOFT_DELETE
            | SecretStoreCapabilities::ROTATION
            | SecretStoreCapabilities::DESCRIPTION,
    );
    let write_context = context(&store, &sink, MutationPolicy::Allowed, None);

    let failures = [
        restore_secret(&write_context, "  ").expect_err("blank"),
        rotate_secret_now(&write_context, "").expect_err("blank"),
        update_secret_description(&write_context, &SecretDescriptionUpdate::new(" ", "text"))
            .expect_err("blank"),
    ];

    for failure in failures {
        assert_eq!(failure.kind, SecretWriteFailureKind::Refused);
    }
    assert_eq!(store.calls(), 0);
    assert_eq!(sink.events().len(), 3);
}

// --- Review follow-ups ---

fn escape_json_text(text: &str) -> String {
    text.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[test]
fn a_put_failure_that_echoes_the_value_json_escaped_once_or_twice_is_scrubbed() {
    let value = "pa\"ss\\word\nline";
    let once = escape_json_text(value);
    let twice = escape_json_text(&once);

    for echoed in [value.to_string(), once.clone(), twice.clone()] {
        let sink = Sink::default();
        let store = ProbeStore::new(Box::new(move |_request| {
            Err(DbError::query_failed(format!(
                "rejected body {{\"Value\":\"{echoed}\"}}"
            )))
        }));

        let failure = run(
            &store,
            &sink,
            MutationPolicy::Allowed,
            None,
            &SecretPutRequest::text("prod/token", value),
        )
        .expect_err("the store fails");

        let recorded = format!("{:?}", sink.events());
        for form in [value, once.as_str(), twice.as_str()] {
            assert!(!failure.to_string().contains(form), "{form}: {failure}");
            assert!(!recorded.contains(form), "{form}: {recorded}");
        }
    }
}

#[test]
fn a_tag_or_description_echoed_by_a_failure_is_scrubbed_from_the_error_and_the_event() {
    let tag_value = "tag \"quoted\" value";
    let description = "describes \"things\"";

    for (label, echoed) in [
        ("plain", tag_value.to_string()),
        ("escaped", escape_json_text(tag_value)),
        ("twice", escape_json_text(&escape_json_text(tag_value))),
    ] {
        let sink = Sink::default();
        let store = OperationStore::new(SecretOperation::EditTags.required_capabilities())
            .failing(move || DbError::query_failed(format!("rejected tag {echoed}")));

        let failure = update_secret_tags(
            &context(&store, &sink, MutationPolicy::Allowed, None),
            &SecretTagsUpdate::new("prod/api/credentials").set("env", tag_value),
        )
        .expect_err("the store fails");

        let recorded = format!("{:?}", sink.events());
        assert!(
            !failure.to_string().contains("quoted"),
            "{label}: {failure}"
        );
        assert!(!recorded.contains("quoted"), "{label}: {recorded}");
    }

    let sink = Sink::default();
    let store = OperationStore::new(SecretOperation::EditDescription.required_capabilities())
        .failing(move || {
            DbError::query_failed(format!("rejected {}", escape_json_text(description)))
        });
    let failure = update_secret_description(
        &context(&store, &sink, MutationPolicy::Allowed, None),
        &SecretDescriptionUpdate::new("prod/api/credentials", description),
    )
    .expect_err("the store fails");
    assert!(!failure.to_string().contains("things"), "{failure}");
    assert!(!format!("{:?}", sink.events()).contains("things"));
}

#[test]
fn a_cancelled_rotation_reports_and_audits_the_version_it_left_unfinished() {
    let store = OperationStore::new(SecretOperation::EditRotation.required_capabilities())
        .leaving_unfinished("v-pending");
    let sink = Sink::default();

    let change = update_secret_rotation(
        &context(&store, &sink, MutationPolicy::Allowed, None),
        &SecretRotationUpdate::disable("prod/api/credentials"),
    )
    .expect("applied");

    assert_eq!(change.unfinished_version.as_deref(), Some("v-pending"));
    assert_eq!(
        details_of(&sink.events()[0])["unfinished_version"],
        "v-pending"
    );

    let clean = OperationStore::new(SecretOperation::EditRotation.required_capabilities());
    let sink = Sink::default();
    let change = update_secret_rotation(
        &context(&clean, &sink, MutationPolicy::Allowed, None),
        &SecretRotationUpdate::disable("prod/api/credentials"),
    )
    .expect("applied");
    assert_eq!(change.unfinished_version, None);
    assert!(
        details_of(&sink.events()[0])
            .get("unfinished_version")
            .is_none()
    );
}

#[test]
fn a_blank_name_is_refused_before_the_store_and_its_event_passes_audit_validation() {
    let sink = Sink::default();
    let store = ProbeStore::new(Box::new(|request| succeed(request)));

    let failure = run(
        &store,
        &sink,
        MutationPolicy::Allowed,
        None,
        &SecretPutRequest::text("  ", "blank-name-value"),
    )
    .expect_err("a blank name");

    assert_eq!(failure.kind, SecretWriteFailureKind::Refused);
    assert_eq!(store.calls(), 0);
    let events = sink.events();
    assert_eq!(events.len(), 1);
    dbflux_audit::AuditService::validate_event(&events[0]).expect("a recordable refusal");
    assert!(!format!("{events:?}").contains("blank-name-value"));

    let sink = Sink::default();
    let operations = OperationStore::new(
        SecretStoreCapabilities::WRITE
            | SecretStoreCapabilities::SOFT_DELETE
            | SecretStoreCapabilities::ROTATION
            | SecretStoreCapabilities::DESCRIPTION,
    );
    let write_context = context(&operations, &sink, MutationPolicy::Allowed, None);
    restore_secret(&write_context, "").expect_err("blank");
    rotate_secret_now(&write_context, " ").expect_err("blank");
    update_secret_description(&write_context, &SecretDescriptionUpdate::new("", "text"))
        .expect_err("blank");

    for event in sink.events() {
        dbflux_audit::AuditService::validate_event(&event)
            .unwrap_or_else(|error| panic!("{}: {error}", event.action));
    }
}

#[test]
fn every_operation_reaches_its_own_store_method_with_its_request() {
    let expected: [(SecretOperation, &str, &[&str]); 9] = [
        (SecretOperation::RollBack, "roll_back", &["v-oldest"]),
        (
            SecretOperation::DeleteScheduled,
            "delete_secret",
            &["Scheduled", "30"],
        ),
        (
            SecretOperation::DeleteIrreversible,
            "delete_secret",
            &["Irreversible"],
        ),
        (
            SecretOperation::Restore,
            "restore_secret",
            &["prod/api/credentials"],
        ),
        (
            SecretOperation::RotateNow,
            "rotate_now",
            &["prod/api/credentials"],
        ),
        (
            SecretOperation::EditRotation,
            "update_rotation",
            &["rate(30 days)", "4h"],
        ),
        (
            SecretOperation::EditTags,
            "update_tags",
            &["tag-value-probe", "legacy"],
        ),
        (
            SecretOperation::EditDescription,
            "update_description",
            &["description-probe"],
        ),
        (
            SecretOperation::Create,
            "create_secret",
            &["prod/billing/api-key", "create-tag-probe"],
        ),
    ];

    for case in operation_cases() {
        let store = OperationStore::new(case.operation.required_capabilities());

        (case.run)(&context(
            &store,
            &Sink::default(),
            MutationPolicy::Allowed,
            None,
        ))
        .expect_err_or_ok(case.label);

        let (_, method, fragments) = expected
            .iter()
            .find(|(operation, _, _)| *operation == case.operation)
            .unwrap_or_else(|| panic!("{}: no expectation", case.label));
        let routes = store.routes();
        assert_eq!(routes.len(), 1, "{}", case.label);
        assert_eq!(routes[0].0, *method, "{}", case.label);
        for fragment in *fragments {
            assert!(
                routes[0].1.contains(fragment),
                "{}: {fragment} in {}",
                case.label,
                routes[0].1
            );
        }
    }
}

trait ExpectOk {
    fn expect_err_or_ok(self, label: &str);
}

impl ExpectOk for Result<(), SecretWriteFailure> {
    fn expect_err_or_ok(self, label: &str) {
        if let Err(failure) = self {
            panic!("{label}: {failure}");
        }
    }
}

#[test]
fn a_failure_that_may_have_landed_is_indeterminate_and_says_to_check_before_retrying() {
    for error in [
        DbError::Timeout,
        DbError::IoError(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        )),
        DbError::QueryFailed(
            dbflux_core::FormattedError::new("AWS SDK dispatch failure").with_retriable(true),
        ),
    ] {
        let sink = Sink::default();
        let rendered = error.to_string();
        let store = OperationStore::new(SecretOperation::EditTags.required_capabilities()).failing(
            move || match &error {
                DbError::Timeout => DbError::Timeout,
                DbError::IoError(io) => {
                    DbError::IoError(std::io::Error::new(io.kind(), io.to_string()))
                }
                other => DbError::QueryFailed(other.formatted().cloned().unwrap_or_default()),
            },
        );

        let failure = update_secret_tags(
            &context(&store, &sink, MutationPolicy::Allowed, None),
            &SecretTagsUpdate::new("prod/api/credentials").set("env", "prod"),
        )
        .expect_err("the store fails");

        assert_eq!(
            failure.kind,
            SecretWriteFailureKind::Indeterminate,
            "{rendered}"
        );
        assert!(failure.correlation_id.is_some(), "{rendered}");
        assert!(!failure.audit_unrecorded, "{rendered}");
        assert_eq!(sink.events().len(), 1);
    }

    // An answer from the service says what it did: throttling is a plain
    // failure, even though it is retriable.
    let store =
        OperationStore::new(SecretOperation::EditTags.required_capabilities()).failing(|| {
            DbError::QueryFailed(
                dbflux_core::FormattedError::new("slow down")
                    .with_code("ThrottlingException")
                    .with_retriable(true),
            )
        });
    let failure = update_secret_tags(
        &context(&store, &Sink::default(), MutationPolicy::Allowed, None),
        &SecretTagsUpdate::new("prod/api/credentials").set("env", "prod"),
    )
    .expect_err("throttled");
    assert_eq!(failure.kind, SecretWriteFailureKind::StoreFailed);
    assert!(!failure.audit_unrecorded);
}

#[test]
fn a_store_failure_whose_audit_event_is_lost_says_so_and_offers_no_audit_row() {
    struct RefusingSink;
    impl EventSink for RefusingSink {
        fn record(&self, _event: EventRecord) -> Result<EventRecord, EventSinkError> {
            Err(EventSinkError::Storage("audit store is closed".into()))
        }
    }

    let store = OperationStore::new(SecretOperation::EditTags.required_capabilities())
        .failing(|| DbError::permission_denied("denied by the service"));

    let failure = update_secret_tags(
        &context(&store, &RefusingSink, MutationPolicy::Allowed, None),
        &SecretTagsUpdate::new("prod/api/credentials").set("env", "prod"),
    )
    .expect_err("the store fails");

    assert_eq!(failure.kind, SecretWriteFailureKind::StoreFailed);
    assert!(failure.correlation_id.is_none());
    assert!(matches!(failure.error, DbError::PermissionDenied(_)));
    assert!(
        failure.to_string().contains("denied by the service"),
        "{failure}"
    );
    assert!(failure.audit_unrecorded);
}
