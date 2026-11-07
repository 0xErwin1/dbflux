//! Independent verification probes for the governed write seam, adopted
//! from the review that found the gaps they probe: the scrub's variant
//! coverage, the base64 echo of a binary payload, the store's write
//! capability, and an audit sink that refuses its event after the write
//! landed. They run beside the module's own tests and are kept as
//! regression probes for those findings.
//
// The probes mirror the seam's `Result<SecretValue, DbError>` signature and
// hand `put_secret_value`-shaped closures to a stub store, which trips
// lints written for boundary code; the assertions, not the shapes, are what
// these probes verify.
#![allow(clippy::result_large_err)]
#![allow(clippy::redundant_closure)]

use super::encode_base64;
use super::value::{RetrievalTarget, value_retrieval_event};
use super::write::{
    SECRET_VALUE_WRITE_FAILED_ACTION, SECRET_VALUE_WRITE_REFUSED_ACTION,
    SECRET_VALUE_WRITTEN_ACTION, SecretWriteOutcome, WriteTarget, put_secret_value,
    secret_write_event,
};
use dbflux_core::observability::{EventCategory, EventRecord, EventSink, EventSinkError};
use dbflux_core::secrecy::ExposeSecret;
use dbflux_core::{
    DbError, MutationPolicy, ReadOnlyReason, SecretDetails, SecretListPage, SecretListRequest,
    SecretPayload, SecretPutRequest, SecretStoreCapabilities, SecretStoreDescriptor,
    SecretStoreSource, SecretValue, SecretVersionStage,
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
    fn list(&self, _r: &SecretListRequest) -> Result<SecretListPage, DbError> {
        Err(DbError::NotSupported("unused".into()))
    }
    fn describe(&self, _n: &str) -> Result<SecretDetails, DbError> {
        Err(DbError::NotSupported("unused".into()))
    }
    fn get_value(&self, _n: &str, _v: Option<&str>) -> Result<SecretValue, DbError> {
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
            Ok(g) => g.clone(),
            Err(p) => p.into_inner().clone(),
        }
    }
    fn actions(&self) -> Vec<String> {
        self.events().into_iter().map(|e| e.action).collect()
    }
}

impl EventSink for Sink {
    fn record(&self, event: EventRecord) -> Result<EventRecord, EventSinkError> {
        match self.0.lock() {
            Ok(mut g) => g.push(event.clone()),
            Err(p) => p.into_inner().push(event.clone()),
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
) -> Result<SecretValue, DbError> {
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

    let store = ProbeStore::new(Box::new(|r| succeed(r)));
    let sink = Sink::default();
    let _ = run(
        &store,
        &sink,
        MutationPolicy::ReadOnly,
        Some(ReadOnlyReason::ProfileSetting),
        &request,
    );
    assert_eq!(
        sink.actions(),
        vec![SECRET_VALUE_WRITE_REFUSED_ACTION.to_string()]
    );
    assert_eq!(store.calls(), 0, "read-only reached the store");

    let store = ProbeStore::new(Box::new(|r| succeed(r)));
    let sink = Sink::default();
    let _ = run(
        &store,
        &sink,
        MutationPolicy::ApprovalRequired,
        None,
        &request,
    );
    assert_eq!(
        sink.actions(),
        vec![SECRET_VALUE_WRITE_REFUSED_ACTION.to_string()]
    );
    assert_eq!(store.calls(), 0, "approval-required reached the store");

    let store = ProbeStore::new(Box::new(|r| succeed(r)));
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
        let store = ProbeStore::new(Box::new(|r| succeed(r)));
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
            matches!(error, DbError::PermissionDenied(_)),
            "{policy:?}: {error:?}"
        );
    }
}

/// G4: refusals are non-retriable permission denials, worded distinctly.
#[test]
fn probe_refusals_are_permission_denied_not_transient() {
    let request = SecretPutRequest::text("prod/token", "probe-value-3");
    let store = ProbeStore::new(Box::new(|r| succeed(r)));

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

    let (DbError::PermissionDenied(ro), DbError::PermissionDenied(ap)) = (&read_only, &approval)
    else {
        panic!("refusals are not PermissionDenied: {read_only:?} / {approval:?}");
    };
    assert!(!ro.retriable, "read-only refusal marked retriable");
    assert!(!ap.retriable, "approval refusal marked retriable");
    assert!(ro.message.contains("read-only"), "{}", ro.message);
    assert!(ap.message.contains("approval"), "{}", ap.message);
    assert_ne!(ro.message, ap.message, "refusals are not distinguishable");

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
            static D: std::sync::OnceLock<SecretStoreDescriptor> = std::sync::OnceLock::new();
            D.get_or_init(|| {
                SecretStoreDescriptor::new("ro", "RO", SecretStoreCapabilities::empty())
            })
        }
        fn list(&self, _r: &SecretListRequest) -> Result<SecretListPage, DbError> {
            Err(DbError::NotSupported("unused".into()))
        }
        fn describe(&self, _n: &str) -> Result<SecretDetails, DbError> {
            Err(DbError::NotSupported("unused".into()))
        }
        fn get_value(&self, _n: &str, _v: Option<&str>) -> Result<SecretValue, DbError> {
            Err(DbError::NotSupported("unused".into()))
        }
        fn put_value(&self, r: &SecretPutRequest) -> Result<SecretValue, DbError> {
            self.0.fetch_add(1, Ordering::Relaxed);
            succeed(r)
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

    let store = ProbeStore::new(Box::new(|r| succeed(r)));
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

    let DbError::QueryFailed(formatted) = &error else {
        panic!("the unaudited write is not a plain operation failure: {error:?}");
    };
    assert!(
        !formatted.retriable,
        "the audit-loss message presented itself as retryable"
    );
}

/// G3, exhaustive: every message-carrying `DbError` variant is scrubbed of
/// the submitted value in every field a rendered message reaches —
/// including `location.schema`, which `to_display_string` does not render —
/// an `io::Error` keeps its kind, and the message-less variants pass
/// through untouched.
#[test]
fn every_db_error_variant_is_scrubbed_and_io_keeps_its_kind() {
    const VALUE: &str = "sweep-value-8c31";

    // One formatted error carrying the value in every field a rendered
    // message can reach.
    let formatted = || {
        dbflux_core::FormattedError::new(format!("rejected {VALUE}"))
            .with_detail(format!("detail {VALUE}"))
            .with_hint(format!("hint {VALUE}"))
            .with_code(format!("SQLSTATE:{VALUE}"))
            .with_location(dbflux_core::ErrorLocation {
                schema: Some(format!("schema_{VALUE}")),
                table: Some(format!("table_{VALUE}")),
                column: Some(format!("column_{VALUE}")),
                constraint: Some(format!("constraint_{VALUE}")),
            })
    };

    let cases: Vec<(&'static str, Behaviour)> = vec![
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
            Box::new(move |_| Err(DbError::NotSupported(format!("refused {VALUE}")))),
        ),
        (
            "InvalidProfile",
            Box::new(move |_| Err(DbError::InvalidProfile(format!("malformed {VALUE}")))),
        ),
        (
            "ValueResolutionFailed",
            Box::new(move |_| {
                Err(DbError::ValueResolutionFailed(format!(
                    "unresolved {VALUE}"
                )))
            }),
        ),
        (
            "Parse",
            Box::new(move |_| Err(DbError::Parse(format!("unparsed {VALUE}")))),
        ),
        (
            "Unsupported",
            Box::new(move |_| Err(DbError::Unsupported(format!("unsupported {VALUE}")))),
        ),
        (
            "IoError",
            Box::new(move |_| {
                Err(DbError::IoError(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("io failed {VALUE}"),
                )))
            }),
        ),
    ];

    for (label, behaviour) in cases {
        let store = ProbeStore::new(behaviour);
        let sink = Sink::default();
        let request = SecretPutRequest::text("prod/token", VALUE);

        let error = run(&store, &sink, MutationPolicy::Allowed, None, &request).expect_err(label);

        let message = error.to_string();
        assert!(!message.contains(VALUE), "{label}: {message}");
        assert!(message.contains("[redacted]"), "{label}: {message}");

        let events = sink.events();
        assert_eq!(events.len(), 1, "{label}");
        assert!(
            !events[0]
                .error_message
                .as_deref()
                .unwrap_or_default()
                .contains(VALUE),
            "{label}: audit error_message leaked: {:?}",
            events[0].error_message
        );

        if let Some(scrubbed) = error.formatted() {
            for field in [
                scrubbed.message.as_str(),
                scrubbed.detail.as_deref().unwrap_or_default(),
                scrubbed.hint.as_deref().unwrap_or_default(),
                scrubbed.code.as_deref().unwrap_or_default(),
            ] {
                assert!(!field.contains(VALUE), "{label}: {field}");
            }
            let location = scrubbed
                .location
                .as_ref()
                .expect("the echo carried a location");
            for field in [
                &location.schema,
                &location.table,
                &location.column,
                &location.constraint,
            ] {
                let field = field
                    .as_deref()
                    .expect("the echo filled every location field");
                assert!(!field.contains(VALUE), "{label}: {field}");
                assert!(field.contains("[redacted]"), "{label}: {field}");
            }
        }

        if let DbError::IoError(io) = &error {
            assert_eq!(
                io.kind(),
                std::io::ErrorKind::PermissionDenied,
                "{label}: the io::Error kind was lost in the scrub"
            );
        }
    }

    // The message-less variants cannot carry a value and pass through
    // untouched.
    let timeout = ProbeStore::new(Box::new(|_| Err(DbError::Timeout)));
    let error = run(
        &timeout,
        &Sink::default(),
        MutationPolicy::Allowed,
        None,
        &SecretPutRequest::text("prod/token", VALUE),
    )
    .expect_err("timeout");
    assert!(matches!(error, DbError::Timeout), "{error:?}");

    let cancelled = ProbeStore::new(Box::new(|_| Err(DbError::Cancelled)));
    let error = run(
        &cancelled,
        &Sink::default(),
        MutationPolicy::Allowed,
        None,
        &SecretPutRequest::text("prod/token", VALUE),
    )
    .expect_err("cancelled");
    assert!(matches!(error, DbError::Cancelled), "{error:?}");
}
