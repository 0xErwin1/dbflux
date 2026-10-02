use dbflux_core::FormattedError;
use dbflux_core::observability::EventSeverity;
use gpui::{App, AsyncApp};
use uuid::Uuid;

pub mod throttle;

/// Coarse-grained taxonomy of user-visible failures.
///
/// Used for styling, audit `action` discrimination, and badge classification.
/// The free-form `summary` carries the actionable message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    Storage,
    Network,
    Auth,
    Hook,
    Driver,
    User,
    Config,
}

impl ErrorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Storage => "storage",
            Self::Network => "network",
            Self::Auth => "auth",
            Self::Hook => "hook",
            Self::Driver => "driver",
            Self::User => "user",
            Self::Config => "config",
        }
    }
}

/// A user-visible failure with enough context to render a toast and an audit
/// event that share a correlation id.
///
/// Construction is always at the *first catch site*. Propagators above MUST NOT
/// re-report — there is no runtime deduplication.
#[derive(Debug, Clone)]
pub struct UserFacingError {
    pub kind: ErrorKind,
    /// Severity drives toast styling and throttle eligibility.
    /// Default for `UserFacingError::new` is `EventSeverity::Error`.
    pub severity: EventSeverity,
    pub summary: String,
    pub cause: Option<String>,
    pub suggested_action: Option<String>,
    pub correlation_id: Uuid,
}

impl UserFacingError {
    /// Constructs a new error with `severity = Error` and a freshly generated
    /// UUID v7 correlation id.
    pub fn new(kind: ErrorKind, summary: impl Into<String>) -> Self {
        Self {
            kind,
            severity: EventSeverity::Error,
            summary: summary.into(),
            cause: None,
            suggested_action: None,
            correlation_id: Uuid::now_v7(),
        }
    }

    pub fn with_severity(mut self, severity: EventSeverity) -> Self {
        self.severity = severity;
        self
    }

    pub fn with_cause(mut self, cause: impl Into<String>) -> Self {
        self.cause = Some(cause.into());
        self
    }

    pub fn with_suggested_action(mut self, action: impl Into<String>) -> Self {
        self.suggested_action = Some(action.into());
        self
    }

    /// Test-only seam: override the auto-generated correlation id.
    pub fn with_correlation_id(mut self, id: Uuid) -> Self {
        self.correlation_id = id;
        self
    }

    /// Driver-agnostic constructor: takes the already-formatted output of a
    /// driver's error formatter. The driver-side formatter call lives in the
    /// call site (drivers, not UI).
    pub fn from_formatted(kind: ErrorKind, fe: FormattedError) -> Self {
        let cause = match (&fe.detail, &fe.hint, &fe.code) {
            (None, None, None) => None,
            _ => Some(fe.to_string()),
        };

        Self {
            kind,
            severity: EventSeverity::Error,
            summary: fe.message,
            cause,
            suggested_action: None,
            correlation_id: Uuid::now_v7(),
        }
    }
}

/// Foreground-only entry point.
///
/// MUST be called from the foreground (`&mut App`). Calling from a
/// `background_executor().spawn()` closure panics — the GPUI global pulled
/// in for `ToastHost` is foreground-only. Background callers MUST use
/// `report_error_async` instead.
///
/// | Caller context                          | Use                     |
/// | --------------------------------------- | ----------------------- |
/// | `&mut Context<T>`, `&mut App`           | `report_error`          |
/// | Inside `background_executor().spawn()`  | `report_error_async`    |
/// | Inside `cx.spawn(async ...)`            | `report_error_async`    |
/// | Inside a `cx.update(|cx| { ... })`      | `report_error`          |
pub fn report_error(err: UserFacingError, cx: &mut App) {
    use crate::app_state_entity::AppStateGlobal;
    use crate::toast::{Toast, ToastAction, copy_action, now_hms};

    let id_str = err.correlation_id.to_string();
    let summary = &err.summary;

    trace_user_error(&err);

    let mut toast = match err.severity {
        EventSeverity::Warn => Toast::warning(err.summary.clone()),
        _ => Toast::error(err.summary.clone()),
    };

    if let Some(c) = &err.cause {
        toast = toast.code_block(c.clone());
    }

    if let Some(a) = &err.suggested_action {
        toast = toast.body(a.clone());
    }

    let id_for_action = err.correlation_id;
    let view_in_audit = ToastAction::new(
        "view-in-audit",
        dbflux_i18n::t!("errors.action.view_in_audit"),
    )
    .on_click(move |cx: &mut App| {
        if let Some(g) = cx.try_global::<AppStateGlobal>() {
            let entity = g.entity.clone();
            entity.update(cx, |s, cx| s.request_open_audit(Some(id_for_action), cx));
        }
    });

    toast = toast
        .meta_right(now_hms())
        .details(dbflux_i18n::t!("errors.correlation.detail", id = id_str))
        .action(copy_action(dbflux_i18n::t!(
            "errors.correlation.clipboard",
            summary = summary,
            id = id_str
        )))
        .action(view_in_audit);

    toast.push(cx);

    if let Some(app_state_global) = cx.try_global::<AppStateGlobal>() {
        let entity = app_state_global.entity.clone();
        entity.update(cx, |s, cx| {
            s.notifications
                .record_error(crate::notifications::ErrorNotification::from_user_error(
                    &err,
                    dbflux_core::chrono::Utc::now(),
                ));
            s.note_user_error(err.correlation_id, err.severity, cx);
        });
    }
}

/// Emits the tracing event the audit bridge turns into the audit row.
///
/// `cause` and `suggested_action` are not typed audit fields, so the bridge
/// stores them in `details_json` next to `kind`. Both are skipped when unset.
fn trace_user_error(err: &UserFacingError) {
    let id_str = err.correlation_id.to_string();
    let kind_str = err.kind.as_str();
    let summary = &err.summary;
    let cause = err.cause.as_deref();
    let suggested_action = err.suggested_action.as_deref();

    match err.severity {
        EventSeverity::Warn => tracing::warn!(
            target: "dbflux_ui::user_error",
            correlation_id = %id_str,
            kind            = %kind_str,
            outcome         = "failure",
            action          = "user_error",
            cause,
            suggested_action,
            "{summary}",
        ),
        _ => tracing::error!(
            target: "dbflux_ui::user_error",
            correlation_id = %id_str,
            kind            = %kind_str,
            outcome         = "failure",
            action          = "user_error",
            cause,
            suggested_action,
            "{summary}",
        ),
    }
}

/// Background-safe entry point. Marshals to the foreground via `cx.update`.
///
/// Fire-and-forget: if the foreground has been dropped the call is silently
/// ignored.
///
/// | Caller context                          | Use                     |
/// | --------------------------------------- | ----------------------- |
/// | `&mut Context<T>`, `&mut App`           | `report_error`          |
/// | Inside `background_executor().spawn()`  | `report_error_async`    |
/// | Inside `cx.spawn(async ...)`            | `report_error_async`    |
/// | Inside a `cx.update(|cx| { ... })`      | `report_error`          |
pub fn report_error_async(err: UserFacingError, cx: &AsyncApp) {
    let cx = cx.clone();
    cx.update(move |cx| report_error(err, cx));
}

#[cfg(test)]
mod tests {
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
        let err = UserFacingError::new(ErrorKind::User, "mild problem")
            .with_severity(EventSeverity::Warn);
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
}

#[cfg(test)]
mod i18n_tests {
    const USER_ERROR_KEYS: &[&str] = &[
        "errors.action.view_in_audit",
        "errors.correlation.detail",
        "errors.correlation.clipboard",
    ];

    #[test]
    fn user_error_catalog_keys_resolve() {
        for key in USER_ERROR_KEYS {
            let english = dbflux_i18n::t!(key);
            let spanish = dbflux_i18n::t!(key, locale = "es");

            assert!(!english.is_empty(), "empty English translation for {key}");
            assert_ne!(english, *key, "missing English translation for {key}");
            assert!(!spanish.is_empty(), "empty Spanish translation for {key}");
            assert_ne!(spanish, *key, "missing Spanish translation for {key}");
        }
    }

    #[test]
    fn correlation_clipboard_ends_with_correlation_detail() {
        let clipboard =
            dbflux_i18n::t!("errors.correlation.clipboard", summary = "boom", id = "ID");
        let detail = dbflux_i18n::t!("errors.correlation.detail", id = "ID");

        assert!(clipboard.starts_with("boom\n"));
        assert!(clipboard.ends_with(&detail));
    }
}
