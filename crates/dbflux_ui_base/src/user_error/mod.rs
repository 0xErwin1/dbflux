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

    record(&err, cx);

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
}

/// Records a user-facing failure exactly as [`report_error`] does — the audit
/// row through the same tracing event, the correlation id, the
/// notification-center entry and the error badge — but queues no toast.
///
/// Use it for a failure the surface already shows in place, such as a section
/// that draws its own error banner: a toast on top would cover the controls
/// next to it, while the failure still belongs in the audit trail and the
/// notification center. A failure with no inline home keeps [`report_error`].
///
/// | Caller context                          | Use                     |
/// | --------------------------------------- | ----------------------- |
/// | `&mut Context<T>`, `&mut App`           | `record_error`          |
/// | Inside `cx.spawn(async ...)`            | `record_error_async`    |
pub fn record_error(err: UserFacingError, cx: &mut App) {
    record(&err, cx);
}

/// Background-safe variant of [`record_error`]. Marshals to the foreground
/// via `cx.update`.
pub fn record_error_async(err: UserFacingError, cx: &AsyncApp) {
    let cx = cx.clone();
    cx.update(move |cx| record_error(err, cx));
}

/// The part of reporting every failure shares, toast or not: the tracing
/// event the audit bridge turns into the audit row, then the
/// notification-center entry and the error badge.
fn record(err: &UserFacingError, cx: &mut App) {
    use crate::app_state_entity::AppStateGlobal;

    trace_user_error(err);

    if let Some(app_state_global) = cx.try_global::<AppStateGlobal>() {
        let entity = app_state_global.entity.clone();
        entity.update(cx, |s, cx| {
            s.notifications
                .record_error(crate::notifications::ErrorNotification::from_user_error(
                    err,
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

#[cfg(any(test, feature = "test-support"))]
pub mod capture;

#[cfg(test)]
mod tests;

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
