use super::*;

#[test]
fn new_defaults_to_error_severity() {
    let err = UserFacingError::new(ErrorKind::Driver, "something failed");
    assert_eq!(err.severity, EventSeverity::Error);
    assert_eq!(err.kind, ErrorKind::Driver);
    assert_eq!(err.summary, "something failed");
    assert!(err.cause.is_none());
    assert!(err.suggested_action.is_none());
}

#[test]
fn with_correlation_id_overrides_generated_uuid() {
    let known = Uuid::nil();
    let err = UserFacingError::new(ErrorKind::Storage, "io error").with_correlation_id(known);
    assert_eq!(err.correlation_id, known);
}

#[test]
fn with_severity_round_trips() {
    let err =
        UserFacingError::new(ErrorKind::User, "mild problem").with_severity(EventSeverity::Warn);
    assert_eq!(err.severity, EventSeverity::Warn);
}

#[test]
fn from_formatted_sets_driver_kind_and_populates_cause() {
    let fe = FormattedError::new("connection refused")
        .with_detail("no route to host")
        .with_code("08006");
    let err = UserFacingError::from_formatted(ErrorKind::Driver, fe);
    assert_eq!(err.kind, ErrorKind::Driver);
    assert_eq!(err.summary, "connection refused");
    assert!(
        err.cause.is_some(),
        "cause must be populated from detail/code"
    );
}

#[test]
fn from_formatted_no_extras_has_no_cause() {
    let fe = FormattedError::new("simple error");
    let err = UserFacingError::from_formatted(ErrorKind::Network, fe);
    assert!(
        err.cause.is_none(),
        "cause must be None when no detail/hint/code"
    );
}

/// Collects the string fields of every event emitted while it is the
/// default subscriber.
#[derive(Default)]
struct FieldCapture {
    fields: std::sync::Mutex<std::collections::BTreeMap<String, String>>,
}

impl tracing::field::Visit for &FieldCapture {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if let Ok(mut fields) = self.fields.lock() {
            fields.insert(field.name().to_owned(), value.to_owned());
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if let Ok(mut fields) = self.fields.lock() {
            fields.insert(field.name().to_owned(), format!("{value:?}"));
        }
    }
}

impl tracing::Subscriber for FieldCapture {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut visitor = self;
        event.record(&mut visitor);
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

fn traced_fields(err: &UserFacingError) -> std::collections::BTreeMap<String, String> {
    let capture = std::sync::Arc::new(FieldCapture::default());

    tracing::subscriber::with_default(capture.clone(), || trace_user_error(err));

    capture
        .fields
        .lock()
        .map(|fields| fields.clone())
        .unwrap_or_default()
}

#[test]
fn traced_event_carries_the_cause_and_suggested_action() {
    let err = UserFacingError::new(ErrorKind::Network, "Failed to load the database list")
        .with_cause("Connection failed: error sending request")
        .with_suggested_action("Check that the server is reachable");

    let fields = traced_fields(&err);

    assert_eq!(
        fields.get("cause").map(String::as_str),
        Some("Connection failed: error sending request")
    );
    assert_eq!(
        fields.get("suggested_action").map(String::as_str),
        Some("Check that the server is reachable")
    );
    assert_eq!(fields.get("kind").map(String::as_str), Some("network"));
}

#[test]
fn traced_event_omits_an_unset_cause() {
    let err = UserFacingError::new(ErrorKind::Storage, "Export failed");

    let fields = traced_fields(&err);

    assert!(!fields.contains_key("cause"));
    assert!(!fields.contains_key("suggested_action"));
}

/// An app with the app-state global and a toast host installed, which is
/// all reporting touches.
fn reporting_app(
    cx: &mut gpui::TestAppContext,
) -> (
    gpui::Entity<crate::AppStateEntity>,
    gpui::Entity<bolt::toast::ToastHost>,
) {
    use gpui::AppContext as _;

    cx.update(|cx| {
        let app_state = cx.new(|_| {
            crate::AppStateEntity::new_with_storage_runtime(
                dbflux_storage::bootstrap::StorageRuntime::in_memory()
                    .expect("isolated storage runtime"),
            )
            .expect("test storage setup")
        });
        let host = cx.new(|_| bolt::toast::ToastHost::new());

        cx.set_global(crate::app_state_entity::AppStateGlobal {
            entity: app_state.clone(),
        });
        cx.set_global(bolt::toast::ToastGlobal { host: host.clone() });

        (app_state, host)
    })
}

fn recorded_summaries(
    app_state: &gpui::Entity<crate::AppStateEntity>,
    cx: &mut gpui::TestAppContext,
) -> Vec<String> {
    use crate::notifications::{NotificationKey, NotificationSources};

    cx.update(|cx| {
        let state = app_state.read(cx);

        state
            .notifications
            .snapshot(&NotificationSources::default())
            .entries
            .iter()
            .filter_map(|entry| match &entry.key {
                NotificationKey::Error(id) => state
                    .notifications
                    .error(*id)
                    .map(|error| error.summary.clone()),
                _ => None,
            })
            .collect()
    })
}

#[gpui::test]
fn record_error_keeps_the_notification_and_the_audit_event_but_queues_no_toast(
    cx: &mut gpui::TestAppContext,
) {
    let _keymap_state = crate::keymap::keymap_state_test_guard();
    let (app_state, host) = reporting_app(cx);
    let err =
        UserFacingError::new(ErrorKind::Driver, "history refused").with_cause("denied by policy");
    let capture = std::sync::Arc::new(FieldCapture::default());

    tracing::subscriber::with_default(capture.clone(), || {
        cx.update(|cx| record_error(err.clone(), cx));
    });

    let fields = capture
        .fields
        .lock()
        .map(|fields| fields.clone())
        .unwrap_or_default();
    assert_eq!(
        fields.get("correlation_id"),
        Some(&err.correlation_id.to_string())
    );
    assert_eq!(fields.get("action").map(String::as_str), Some("user_error"));
    assert_eq!(
        fields.get("cause").map(String::as_str),
        Some("denied by policy")
    );
    assert_eq!(recorded_summaries(&app_state, cx), ["history refused"]);
    assert_eq!(
        app_state.read_with(cx, |state, _| state.unread_error_count),
        1
    );
    assert_eq!(host.read_with(cx, |host, _| host.toast_count()), 0);
}

#[gpui::test]
fn report_error_still_queues_a_toast_next_to_the_same_record(cx: &mut gpui::TestAppContext) {
    let _keymap_state = crate::keymap::keymap_state_test_guard();
    let (app_state, host) = reporting_app(cx);
    let err = UserFacingError::new(ErrorKind::Driver, "save refused");
    let capture = std::sync::Arc::new(FieldCapture::default());

    tracing::subscriber::with_default(capture.clone(), || {
        cx.update(|cx| report_error(err.clone(), cx));
    });

    let fields = capture
        .fields
        .lock()
        .map(|fields| fields.clone())
        .unwrap_or_default();
    assert_eq!(
        fields.get("correlation_id"),
        Some(&err.correlation_id.to_string())
    );
    assert_eq!(recorded_summaries(&app_state, cx), ["save refused"]);
    assert_eq!(host.read_with(cx, |host, _| host.toast_count()), 1);
}

#[test]
fn error_kind_as_str_returns_lowercase() {
    assert_eq!(ErrorKind::Storage.as_str(), "storage");
    assert_eq!(ErrorKind::Network.as_str(), "network");
    assert_eq!(ErrorKind::Auth.as_str(), "auth");
    assert_eq!(ErrorKind::Hook.as_str(), "hook");
    assert_eq!(ErrorKind::Driver.as_str(), "driver");
    assert_eq!(ErrorKind::User.as_str(), "user");
    assert_eq!(ErrorKind::Config.as_str(), "config");
}
