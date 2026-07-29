//! Presigned-URL modal: method (GET/PUT) and expiry choice, the generated
//! URL, and a copy action.
//!
//! DEC-15 is absolute here: the URL is generated, shown, and copied, but it is
//! never logged, never persisted, and never reaches an audit field. Only the
//! generation event — bucket, key, method, expiry — is audited.

use super::ObjectBrowserDocument;
use super::data::db_error_to_user_facing;
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, SegmentedControl, SegmentedItem, Text};
use dbflux_components::tokens::{Heights, Radii, Spacing};
use dbflux_core::chrono::{DateTime, Duration as ChronoDuration, Utc};
use dbflux_core::{DbError, PresignMethod};
use dbflux_ui_base::modal_frame::ModalFrame;
use dbflux_ui_base::toast::{Toast, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error, report_error_async};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use std::time::Duration;
use uuid::Uuid;

/// The method segment of the modal. Wraps `PresignMethod` with the stable
/// segment id and the label the mock shows, so neither lives in the core type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PresignMethodChoice {
    Get,
    Put,
}

impl PresignMethodChoice {
    pub fn all() -> [PresignMethodChoice; 2] {
        [Self::Get, Self::Put]
    }

    pub fn from_method(method: PresignMethod) -> Self {
        match method {
            PresignMethod::Get => Self::Get,
            PresignMethod::Put => Self::Put,
        }
    }

    pub fn method(self) -> PresignMethod {
        match self {
            Self::Get => PresignMethod::Get,
            Self::Put => PresignMethod::Put,
        }
    }

    pub fn id(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Put => "put",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Get => "GET download",
            Self::Put => "PUT upload",
        }
    }

    pub fn from_id(id: &str) -> Option<Self> {
        Self::all().into_iter().find(|choice| choice.id() == id)
    }
}

/// Expiry choices offered by the modal (S3-3 / mock).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PresignExpiry {
    FifteenMinutes,
    OneHour,
    TwelveHours,
    SevenDays,
}

impl PresignExpiry {
    pub fn all() -> [PresignExpiry; 4] {
        [
            Self::FifteenMinutes,
            Self::OneHour,
            Self::TwelveHours,
            Self::SevenDays,
        ]
    }

    pub fn id(self) -> &'static str {
        match self {
            Self::FifteenMinutes => "15m",
            Self::OneHour => "1h",
            Self::TwelveHours => "12h",
            Self::SevenDays => "7d",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::FifteenMinutes => "15 min",
            Self::OneHour => "1 hour",
            Self::TwelveHours => "12 hours",
            Self::SevenDays => "7 days",
        }
    }

    pub fn from_id(id: &str) -> Option<Self> {
        Self::all().into_iter().find(|choice| choice.id() == id)
    }

    pub fn seconds(self) -> i64 {
        match self {
            Self::FifteenMinutes => 15 * 60,
            Self::OneHour => 60 * 60,
            Self::TwelveHours => 12 * 60 * 60,
            Self::SevenDays => 7 * 24 * 60 * 60,
        }
    }

    pub fn duration(self) -> Duration {
        Duration::from_secs(self.seconds() as u64)
    }
}

/// Generation state of the URL shown by the modal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PresignUrlState {
    Generating,
    Ready(String),
    Error(String),
}

/// Everything the presign modal renders. `url` holds the signed URL in memory
/// for the lifetime of the modal only.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PresignState {
    pub key: String,
    pub method: PresignMethod,
    pub expiry: PresignExpiry,
    pub url: PresignUrlState,
    /// Wall-clock instant the shown URL stops working, computed when the URL
    /// was generated rather than when the modal was opened.
    pub expires_at: Option<DateTime<Utc>>,
    generation: u64,
}

/// Warning copy under the URL: when it dies, and whose credentials signed it.
pub(super) fn presign_warning(
    method: PresignMethod,
    expires_at: Option<DateTime<Utc>>,
    signed_by: &str,
) -> String {
    let capability = match method {
        PresignMethod::Get => "download this object",
        PresignMethod::Put => "overwrite this object",
    };

    let expiry = match expires_at {
        Some(at) => format!("until {}", at.format("%Y-%m-%d %H:%M UTC")),
        None => "until it expires".to_string(),
    };

    format!(
        "Anyone holding this URL can {capability} {expiry}. It is signed with the credentials of \"{signed_by}\" and carries their permissions."
    )
}

impl ObjectBrowserDocument {
    pub fn presign(&self) -> Option<&PresignState> {
        self.presign.as_ref()
    }

    /// Opens the modal for `key` and immediately generates a URL with the
    /// default method/expiry, so the modal is never shown empty.
    pub(super) fn open_presign(&mut self, key: String, cx: &mut Context<Self>) {
        self.presign = Some(PresignState {
            key,
            method: PresignMethod::Get,
            expiry: PresignExpiry::OneHour,
            url: PresignUrlState::Generating,
            expires_at: None,
            generation: 0,
        });

        self.generate_presigned_url(cx);
    }

    pub(super) fn close_presign(&mut self, cx: &mut Context<Self>) {
        self.presign = None;
        cx.notify();
    }

    pub(super) fn set_presign_method(&mut self, method: PresignMethod, cx: &mut Context<Self>) {
        let Some(presign) = self.presign.as_mut() else {
            return;
        };

        if presign.method == method {
            return;
        }

        presign.method = method;
        self.generate_presigned_url(cx);
    }

    pub(super) fn set_presign_expiry(&mut self, expiry: PresignExpiry, cx: &mut Context<Self>) {
        let Some(presign) = self.presign.as_mut() else {
            return;
        };

        if presign.expiry == expiry {
            return;
        }

        presign.expiry = expiry;
        self.generate_presigned_url(cx);
    }

    /// Signs a fresh URL for the current method/expiry. Every change of either
    /// option re-signs, because the signature covers both.
    fn generate_presigned_url(&mut self, cx: &mut Context<Self>) {
        let Some(presign) = self.presign.as_mut() else {
            return;
        };

        presign.generation += 1;
        presign.url = PresignUrlState::Generating;
        presign.expires_at = None;

        let generation = presign.generation;
        let key = presign.key.clone();
        let method = presign.method;
        let expiry = presign.expiry;
        cx.notify();

        let Some(connection) = self.get_connection(cx) else {
            report_error(
                UserFacingError::new(ErrorKind::Storage, "Connection is no longer active"),
                cx,
            );
            self.apply_presigned_url(
                generation,
                Err("Connection is no longer active".to_string()),
                cx,
            );
            return;
        };

        let audit_service = self.app_state.read(cx).audit_service().clone();
        let bucket = self.bucket.clone();
        let profile_id = self.profile_id;
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn({
                    let bucket = bucket.clone();
                    let key = key.clone();
                    async move {
                        let api = connection.object_store_api().ok_or_else(|| {
                            DbError::NotSupported("Object-store API unavailable".to_string())
                        })?;
                        api.presign(&bucket, &key, method, expiry.duration())
                    }
                })
                .await;

            record_presign_audit(
                &audit_service,
                profile_id,
                &bucket,
                &key,
                method,
                expiry,
                result.as_ref().err().map(ToString::to_string).as_deref(),
            );

            if let Err(err) = &result {
                report_error_async(db_error_to_user_facing(err), cx);
            }

            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    doc.apply_presigned_url(generation, result.map_err(|err| err.to_string()), cx);
                });
            })
            .ok();
        })
        .detach();
    }

    fn apply_presigned_url(
        &mut self,
        generation: u64,
        result: Result<String, String>,
        cx: &mut Context<Self>,
    ) {
        let Some(presign) = self.presign.as_mut() else {
            return;
        };

        // A URL signed for options the user has since changed must never be
        // shown next to the new options.
        if presign.generation != generation {
            return;
        }

        match result {
            Ok(url) => {
                presign.expires_at =
                    Some(Utc::now() + ChronoDuration::seconds(presign.expiry.seconds()));
                presign.url = PresignUrlState::Ready(url);
            }
            Err(message) => {
                presign.expires_at = None;
                presign.url = PresignUrlState::Error(message);
            }
        }

        cx.notify();
    }

    /// Copies the generated URL. The toast deliberately reports only that a
    /// URL was copied — printing it would defeat DEC-15.
    pub(super) fn copy_presigned_url(&mut self, cx: &mut Context<Self>) {
        let Some(PresignUrlState::Ready(url)) =
            self.presign.as_ref().map(|presign| presign.url.clone())
        else {
            return;
        };

        cx.write_to_clipboard(ClipboardItem::new_string(url));
        Toast::success("Copied presigned URL to clipboard")
            .meta_right(now_hms())
            .push(cx);
    }

    /// Name the URL is signed as, shown in the warning line.
    fn presign_signing_identity(&self, cx: &Context<Self>) -> String {
        self.app_state
            .read(cx)
            .connections()
            .get(&self.profile_id)
            .map(|connected| connected.profile.name.clone())
            .unwrap_or_else(|| "this connection".to_string())
    }

    pub(super) fn render_presign_modal(
        &self,
        presign: &PresignState,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let theme = cx.theme().clone();
        let entity = cx.entity().downgrade();

        let close = {
            let entity = entity.clone();
            move |_window: &mut Window, cx: &mut App| {
                entity.update(cx, |this, cx| this.close_presign(cx)).ok();
            }
        };

        let method_control = SegmentedControl::new(
            PresignMethodChoice::all()
                .iter()
                .map(|choice| SegmentedItem::new(choice.id(), choice.label()))
                .collect(),
            PresignMethodChoice::from_method(presign.method).id(),
            {
                let entity = entity.clone();
                move |selected, _window, cx| {
                    let Some(choice) = PresignMethodChoice::from_id(selected.as_ref()) else {
                        return;
                    };

                    entity
                        .update(cx, |this, cx| {
                            this.set_presign_method(choice.method(), cx);
                        })
                        .ok();
                }
            },
        );

        let expiry_control = SegmentedControl::new(
            PresignExpiry::all()
                .iter()
                .map(|choice| SegmentedItem::new(choice.id(), choice.label()))
                .collect(),
            presign.expiry.id(),
            {
                let entity = entity.clone();
                move |selected, _window, cx| {
                    let Some(choice) = PresignExpiry::from_id(selected.as_ref()) else {
                        return;
                    };

                    entity
                        .update(cx, |this, cx| {
                            this.set_presign_expiry(choice, cx);
                        })
                        .ok();
                }
            },
        );

        let url_area = match &presign.url {
            PresignUrlState::Generating => div()
                .flex()
                .items_center()
                .gap(Spacing::SM)
                .child(Icon::new(AppIcon::Loader).small().muted())
                .child(Text::caption("Signing URL…"))
                .into_any_element(),
            PresignUrlState::Ready(url) => Text::code(url.clone())
                .muted_foreground()
                .into_any_element(),
            PresignUrlState::Error(message) => {
                Text::caption(message.clone()).danger().into_any_element()
            }
        };

        let can_copy = matches!(presign.url, PresignUrlState::Ready(_));

        let body = div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .p(Spacing::LG)
            .child(Text::code(format!("s3://{}/{}", self.bucket, presign.key)).primary())
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .gap(Spacing::MD)
                    .child(Text::caption("Method"))
                    .child(method_control),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .gap(Spacing::MD)
                    .child(Text::caption("Expires in"))
                    .child(expiry_control),
            )
            .child(
                div()
                    .w_full()
                    .p(Spacing::SM)
                    .rounded(Radii::SM)
                    .border_1()
                    .border_color(theme.border)
                    .bg(theme.secondary)
                    .child(url_area),
            )
            .child(
                div()
                    .flex()
                    .items_start()
                    .gap(Spacing::SM)
                    .child(Icon::new(AppIcon::TriangleAlert).small().warning())
                    .child(
                        Text::caption(presign_warning(
                            presign.method,
                            presign.expires_at,
                            &self.presign_signing_identity(cx),
                        ))
                        .warning(),
                    ),
            )
            .child(
                div()
                    .flex()
                    .justify_end()
                    .gap(Spacing::SM)
                    .child(
                        div()
                            .id("object-browser-presign-close")
                            .flex()
                            .items_center()
                            .h(Heights::CONTROL)
                            .px(Spacing::SM)
                            .rounded(Radii::SM)
                            .cursor_pointer()
                            .bg(theme.secondary)
                            .hover(|d| d.bg(theme.muted))
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.close_presign(cx);
                            }))
                            .child(Text::caption("Close")),
                    )
                    .child(
                        div()
                            .id("object-browser-presign-copy")
                            .flex()
                            .items_center()
                            .gap(Spacing::XS)
                            .h(Heights::CONTROL)
                            .px(Spacing::SM)
                            .rounded(Radii::SM)
                            .bg(theme.primary)
                            .when(!can_copy, |d| d.opacity(0.5))
                            .when(can_copy, |d| d.cursor_pointer().hover(|d| d.opacity(0.9)))
                            .on_click(cx.listener(|this, _, _, cx| {
                                this.copy_presigned_url(cx);
                            }))
                            .child(
                                Icon::new(AppIcon::Copy)
                                    .size(Heights::ICON_SM)
                                    .color(theme.primary_foreground),
                            )
                            .child(Text::caption("Copy URL").color(theme.primary_foreground)),
                    ),
            );

        ModalFrame::new("object-browser-presign-modal", &self.focus_handle, close)
            .title("Presigned URL")
            .icon(AppIcon::Link2)
            .width(px(560.0))
            .max_height(px(460.0))
            .center_vertically()
            .child(body.into_any_element())
            .render(cx)
    }
}

/// Builds the presigned-URL generation event. Pure and independently
/// testable so a no-leak test can prove — by construction, not just by
/// inspecting the resulting summary — that the generated URL never reaches
/// this function's parameters in the first place: only the bucket, key,
/// method, expiry, and optional driver error string are accepted.
fn build_presign_audit_event(
    profile_id: Uuid,
    bucket: &str,
    key: &str,
    method: PresignMethod,
    expiry: PresignExpiry,
    error: Option<&str>,
) -> dbflux_core::observability::EventRecord {
    use dbflux_core::chrono::Utc;
    use dbflux_core::observability::{EventCategory, EventOutcome, EventRecord, EventSeverity};

    let (severity, outcome, action) = match error {
        Some(_) => (
            EventSeverity::Error,
            EventOutcome::Failure,
            "object_presign_failed",
        ),
        None => (EventSeverity::Info, EventOutcome::Success, "object_presign"),
    };

    let method_label = PresignMethodChoice::from_method(method).label();

    let mut summary = format!(
        "Presigned {method_label} URL for s3://{bucket}/{key} ({})",
        expiry.label()
    );
    if let Some(error) = error {
        summary.push_str(&format!(": {error}"));
    }

    EventRecord::new(
        Utc::now().timestamp_millis(),
        severity,
        EventCategory::ObjectStorage,
        outcome,
    )
    .with_action(action.to_string())
    .with_summary(summary)
    .with_actor_id("ui:user")
    .with_object_ref("object", format!("{bucket}/{key}"))
    .with_connection_context(profile_id.to_string(), bucket.to_string(), String::new())
}

/// Audits a presigned-URL generation. The URL itself is never part of the
/// event — only the object it points at and the terms it was signed under.
fn record_presign_audit(
    audit_service: &dbflux_audit::AuditService,
    profile_id: Uuid,
    bucket: &str,
    key: &str,
    method: PresignMethod,
    expiry: PresignExpiry,
    error: Option<&str>,
) {
    use dbflux_core::observability::EventSink;

    let event = build_presign_audit_event(profile_id, bucket, key, method, expiry, error);

    if let Err(e) = audit_service.record(event) {
        log::warn!("[object browser] failed to record presign audit event: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::{PresignExpiry, build_presign_audit_event, presign_warning};
    use dbflux_core::PresignMethod;
    use dbflux_core::chrono::{TimeZone, Utc};
    use uuid::Uuid;

    /// T41: every expiry choice round-trips through its stable segment id.
    #[test]
    fn expiry_choices_round_trip_through_their_ids() {
        for choice in PresignExpiry::all() {
            assert_eq!(PresignExpiry::from_id(choice.id()), Some(choice));
        }

        assert_eq!(PresignExpiry::from_id("30m"), None);
    }

    /// T41: the durations are the four the spec names, in seconds.
    #[test]
    fn expiry_choices_carry_the_specified_durations() {
        assert_eq!(PresignExpiry::FifteenMinutes.seconds(), 900);
        assert_eq!(PresignExpiry::OneHour.seconds(), 3600);
        assert_eq!(PresignExpiry::TwelveHours.seconds(), 43_200);
        assert_eq!(PresignExpiry::SevenDays.seconds(), 604_800);
    }

    /// T41: the warning names the concrete expiry instant, the capability the
    /// URL grants, and the identity that signed it.
    #[test]
    fn warning_states_capability_expiry_instant_and_signing_identity() {
        let expires_at = Utc.with_ymd_and_hms(2026, 7, 28, 18, 30, 0).unwrap();

        let get = presign_warning(PresignMethod::Get, Some(expires_at), "prod-s3");
        assert!(get.contains("download this object"));
        assert!(get.contains("2026-07-28 18:30 UTC"));
        assert!(get.contains("prod-s3"));

        let put = presign_warning(PresignMethod::Put, Some(expires_at), "prod-s3");
        assert!(put.contains("overwrite this object"));
    }

    /// T41: a URL that has not been signed yet still warns, without inventing
    /// an expiry instant.
    #[test]
    fn warning_without_a_generated_url_omits_the_instant() {
        let warning = presign_warning(PresignMethod::Get, None, "prod-s3");

        assert!(warning.contains("until it expires"));
        assert!(!warning.contains("UTC"));
    }

    /// T44 no-leak: the audit event never contains a URL, for either a
    /// success or a failure outcome. `build_presign_audit_event` structurally
    /// cannot leak the signed URL — its signature has no parameter for it —
    /// but this also guards the summary/object-ref text against regressions.
    #[test]
    fn presign_audit_event_never_contains_a_url() {
        let success = build_presign_audit_event(
            Uuid::nil(),
            "prod-bucket",
            "reports/q3.csv",
            PresignMethod::Get,
            PresignExpiry::OneHour,
            None,
        );

        assert!(!success.summary.to_lowercase().contains("http"));
        assert!(
            !success
                .object_id
                .as_deref()
                .unwrap_or_default()
                .to_lowercase()
                .contains("http")
        );

        let failure = build_presign_audit_event(
            Uuid::nil(),
            "prod-bucket",
            "reports/q3.csv",
            PresignMethod::Put,
            PresignExpiry::SevenDays,
            Some("AccessDenied: insufficient permissions"),
        );

        assert!(!failure.summary.to_lowercase().contains("http"));
        assert!(failure.summary.contains("AccessDenied"));
    }
}
