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
    let kind_str = err.kind.as_str();
    let summary = &err.summary;

    match err.severity {
        EventSeverity::Warn => tracing::warn!(
            target: "dbflux_ui::user_error",
            correlation_id = %id_str,
            kind            = %kind_str,
            outcome         = "failure",
            action          = "user_error",
            "{summary}",
        ),
        _ => tracing::error!(
            target: "dbflux_ui::user_error",
            correlation_id = %id_str,
            kind            = %kind_str,
            outcome         = "failure",
            action          = "user_error",
            "{summary}",
        ),
    }

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
    let view_in_audit =
        ToastAction::new("view-in-audit", "View in Audit").on_click(move |cx: &mut App| {
            if let Some(g) = cx.try_global::<AppStateGlobal>() {
                let entity = g.entity.clone();
                entity.update(cx, |s, cx| s.request_open_audit(Some(id_for_action), cx));
            }
        });

    toast = toast
        .meta_right(now_hms())
        .details(format!("Correlation: {id_str}"))
        .action(copy_action(format!("{summary}\nCorrelation: {id_str}")))
        .action(view_in_audit);

    toast.push(cx);

    if let Some(app_state_global) = cx.try_global::<AppStateGlobal>() {
        let entity = app_state_global.entity.clone();
        entity.update(cx, |s, cx| {
            s.note_user_error(err.correlation_id, err.severity, cx);
        });
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
    let _ = cx.update(move |cx| report_error(err, cx));
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
