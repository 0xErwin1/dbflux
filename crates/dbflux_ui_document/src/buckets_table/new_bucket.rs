//! New Bucket modal: name validation, region, bucket-level options, default
//! encryption, and the `create_bucket` submission behind them.
//!
//! Every option is always offered; endpoints that do not support one report it
//! back through `BucketCreateOutcome::warnings` and the user sees it as a
//! non-blocking warning instead of a failed creation (DEC-20). No client-side
//! vendor feature matrix.

use super::BucketsTableDocument;
use dbflux_components::controls::{Checkbox, Input, InputEvent, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, SegmentedControl, SegmentedItem, Text};
use dbflux_components::tokens::{Heights, Radii, Spacing};
use dbflux_core::{BucketCreateOptions, BucketCreateOutcome, BucketEncryption, DbError};
use dbflux_ui_base::modal_frame::ModalFrame;
use dbflux_ui_base::toast::{Toast, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error, report_error_async};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use uuid::Uuid;

/// Bucket names are 3-63 characters of lowercase letters, digits, dots and
/// hyphens (the subset of the S3 rules the spec pins down).
pub fn bucket_name_error(name: &str) -> Option<String> {
    if name.len() < 3 || name.len() > 63 {
        return Some(dbflux_i18n::t!(
            "document.buckets_table.new_bucket.error.length"
        ));
    }

    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '-')
    {
        return Some(dbflux_i18n::t!(
            "document.buckets_table.new_bucket.error.charset"
        ));
    }

    None
}

/// Default-encryption segment. Wraps `BucketEncryption` with the stable
/// segment id and the label, so neither lives in the core type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BucketEncryptionChoice {
    SseS3,
    SseKms,
    None,
}

impl BucketEncryptionChoice {
    pub fn all() -> [BucketEncryptionChoice; 3] {
        [Self::SseS3, Self::SseKms, Self::None]
    }

    pub fn id(self) -> &'static str {
        match self {
            Self::SseS3 => "sse-s3",
            Self::SseKms => "sse-kms",
            Self::None => "none",
        }
    }

    pub fn label(self) -> String {
        crate::labels::bucket_encryption_choice_label(self)
    }

    pub fn from_id(id: &str) -> Option<Self> {
        Self::all().into_iter().find(|choice| choice.id() == id)
    }

    pub fn encryption(self) -> BucketEncryption {
        match self {
            Self::SseS3 => BucketEncryption::SseS3,
            Self::SseKms => BucketEncryption::SseKms { key_id: None },
            Self::None => BucketEncryption::None,
        }
    }
}

/// Everything the New Bucket modal edits. Built on the render pass that
/// consumes the toolbar's intent, because its inputs need a `Window`.
pub struct NewBucketState {
    pub name_input: Entity<InputState>,
    pub region_input: Entity<InputState>,
    pub versioning: bool,
    pub block_public_access: bool,
    pub object_lock: bool,
    pub encryption: BucketEncryptionChoice,
    pub submitting: bool,
    pub error: Option<String>,
    _subscriptions: Vec<Subscription>,
}

impl BucketsTableDocument {
    pub fn new_bucket(&self) -> Option<&NewBucketState> {
        self.new_bucket.as_ref()
    }

    /// Consumes the toolbar's "New bucket" intent on the next render pass and
    /// builds the modal's inputs, pre-filling the region from the connection's
    /// own configuration.
    pub(super) fn drain_pending_new_bucket(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.take_pending_new_bucket() {
            return;
        }

        let region = self
            .app_state
            .read(cx)
            .connections()
            .get(&self.profile_id)
            .and_then(|connected| connected.profile.config.region().map(str::to_string))
            .unwrap_or_default();

        let name_input = cx.new(|cx| InputState::new(window, cx).placeholder("my-bucket-name"));
        let region_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder("us-east-1");
            state.set_value(&region, window, cx);
            state
        });

        // Both inputs feed validation and the submitted options, so every
        // keystroke has to reach the next render.
        let subscriptions = vec![
            cx.subscribe(
                &name_input,
                |this, _input, event: &InputEvent, cx| match event {
                    InputEvent::Change => cx.notify(),
                    InputEvent::PressEnter { secondary: false } => this.submit_new_bucket(cx),
                    _ => {}
                },
            ),
            cx.subscribe(
                &region_input,
                |this, _input, event: &InputEvent, cx| match event {
                    InputEvent::Change => cx.notify(),
                    InputEvent::PressEnter { secondary: false } => this.submit_new_bucket(cx),
                    _ => {}
                },
            ),
        ];

        self.new_bucket = Some(NewBucketState {
            name_input,
            region_input,
            versioning: false,
            block_public_access: true,
            object_lock: false,
            encryption: BucketEncryptionChoice::SseS3,
            submitting: false,
            error: None,
            _subscriptions: subscriptions,
        });

        cx.notify();
    }

    pub(super) fn close_new_bucket(&mut self, cx: &mut Context<Self>) {
        self.new_bucket = None;
        cx.notify();
    }

    fn new_bucket_name(&self, cx: &Context<Self>) -> String {
        self.new_bucket
            .as_ref()
            .map(|state| state.name_input.read(cx).value().to_string())
            .unwrap_or_default()
    }

    fn new_bucket_region(&self, cx: &Context<Self>) -> String {
        self.new_bucket
            .as_ref()
            .map(|state| state.region_input.read(cx).value().trim().to_string())
            .unwrap_or_default()
    }

    /// The Create button is live only for a name that satisfies the naming
    /// rule, a non-empty region, and no submission already in flight.
    fn can_create_bucket(&self, cx: &Context<Self>) -> bool {
        let Some(state) = self.new_bucket.as_ref() else {
            return false;
        };

        !state.submitting
            && bucket_name_error(&self.new_bucket_name(cx)).is_none()
            && !self.new_bucket_region(cx).is_empty()
    }

    pub(super) fn submit_new_bucket(&mut self, cx: &mut Context<Self>) {
        if !self.can_create_bucket(cx) {
            return;
        }

        let name = self.new_bucket_name(cx);
        let region = self.new_bucket_region(cx);

        let Some(state) = self.new_bucket.as_mut() else {
            return;
        };

        let options = BucketCreateOptions {
            region,
            versioning: state.versioning,
            block_public_access: state.block_public_access,
            object_lock: state.object_lock,
            encryption: state.encryption.encryption(),
        };

        state.submitting = true;
        state.error = None;
        cx.notify();

        let Some(connection) = self.get_connection(cx) else {
            let message = dbflux_i18n::t!("document.object_browser.error.connection_unavailable");
            report_error(UserFacingError::new(ErrorKind::User, message.clone()), cx);
            self.apply_bucket_created(name, Err(message), cx);
            return;
        };

        let audit_service = self.app_state.read(cx).audit_service().clone();
        let profile_id = self.profile_id;
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn({
                    let name = name.clone();
                    async move {
                        let api = connection.object_store_api().ok_or_else(|| {
                            DbError::NotSupported(dbflux_i18n::t!(
                                "document.object_browser.error.api_unavailable"
                            ))
                        })?;
                        api.create_bucket(&name, options)
                    }
                })
                .await;

            record_bucket_create_audit(
                &audit_service,
                profile_id,
                &name,
                result.as_ref().err().map(ToString::to_string).as_deref(),
            );

            if let Err(err) = &result {
                report_error_async(db_error_to_user_facing(err), cx);
            }

            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    doc.apply_bucket_created(name, result.map_err(|err| err.to_string()), cx);
                });
            })
            .ok();
        })
        .detach();
    }

    /// Closes the modal and refreshes the table on success, keeping it open
    /// with the message when creation failed so the user can fix the input.
    fn apply_bucket_created(
        &mut self,
        name: String,
        result: Result<BucketCreateOutcome, String>,
        cx: &mut Context<Self>,
    ) {
        match result {
            Ok(outcome) => {
                self.new_bucket = None;

                if outcome.warnings.is_empty() {
                    Toast::success(dbflux_i18n::t!(
                        "document.buckets_table.new_bucket.toast.created",
                        name = name.as_str()
                    ))
                    .meta_right(now_hms())
                    .push(cx);
                } else {
                    Toast::warning(dbflux_i18n::t!(
                        "document.buckets_table.new_bucket.toast.created_with_limitations",
                        name = name.as_str()
                    ))
                    .body(outcome.warnings.join("\n"))
                    .meta_right(now_hms())
                    .push(cx);
                }

                self.load_buckets(cx);
            }
            Err(message) => {
                if let Some(state) = self.new_bucket.as_mut() {
                    state.submitting = false;
                    state.error = Some(message);
                }

                cx.notify();
            }
        }
    }

    /// Renders the modal, or nothing when it is closed.
    pub(super) fn render_new_bucket_modal(&self, cx: &mut Context<Self>) -> AnyElement {
        let Some(state) = self.new_bucket.as_ref() else {
            return div().into_any_element();
        };

        let theme = cx.theme().clone();
        let entity = cx.entity().downgrade();

        let close = {
            let entity = entity.clone();
            move |_window: &mut Window, cx: &mut App| {
                entity.update(cx, |this, cx| this.close_new_bucket(cx)).ok();
            }
        };

        let name = self.new_bucket_name(cx);
        let name_error = (!name.is_empty())
            .then(|| bucket_name_error(&name))
            .flatten();
        let can_create = self.can_create_bucket(cx);

        let encryption_control = SegmentedControl::new(
            BucketEncryptionChoice::all()
                .iter()
                .map(|choice| SegmentedItem::new(choice.id(), choice.label()))
                .collect(),
            state.encryption.id(),
            {
                let entity = entity.clone();
                move |selected, _window, cx| {
                    let Some(choice) = BucketEncryptionChoice::from_id(selected.as_ref()) else {
                        return;
                    };

                    entity
                        .update(cx, |this, cx| {
                            if let Some(state) = this.new_bucket.as_mut() {
                                state.encryption = choice;
                                cx.notify();
                            }
                        })
                        .ok();
                }
            },
        );

        let mut body = div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .p(Spacing::LG)
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XS)
                    .child(Text::caption(dbflux_i18n::t!(
                        "document.buckets_table.new_bucket.field.name"
                    )))
                    .child(Input::new(&state.name_input).small().w_full())
                    .child(match &name_error {
                        Some(error) => Text::caption(error.clone()).danger(),
                        None => Text::caption(dbflux_i18n::t!(
                            "document.buckets_table.new_bucket.field.name_hint"
                        ))
                        .muted_foreground(),
                    }),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XS)
                    .child(Text::caption(dbflux_i18n::t!(
                        "document.buckets_table.new_bucket.field.region"
                    )))
                    .child(Input::new(&state.region_input).small().w_full()),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::SM)
                    .child(Text::caption(dbflux_i18n::t!(
                        "document.buckets_table.new_bucket.section.options"
                    )))
                    .child(
                        Checkbox::new("new-bucket-versioning")
                            .label(dbflux_i18n::t!("document.buckets_table.new_bucket.option.versioning"))
                            .checked(state.versioning)
                            .on_click(cx.listener(|this, checked: &bool, _window, cx| {
                                if let Some(state) = this.new_bucket.as_mut() {
                                    state.versioning = *checked;
                                    cx.notify();
                                }
                            })),
                    )
                    .child(
                        Checkbox::new("new-bucket-block-public")
                            .label(dbflux_i18n::t!(
                                "document.buckets_table.new_bucket.option.block_public_access"
                            ))
                            .checked(state.block_public_access)
                            .on_click(cx.listener(|this, checked: &bool, _window, cx| {
                                if let Some(state) = this.new_bucket.as_mut() {
                                    state.block_public_access = *checked;
                                    cx.notify();
                                }
                            })),
                    )
                    .child(
                        Checkbox::new("new-bucket-object-lock")
                            .label(dbflux_i18n::t!("document.buckets_table.new_bucket.option.object_lock"))
                            .checked(state.object_lock)
                            .on_click(cx.listener(|this, checked: &bool, _window, cx| {
                                if let Some(state) = this.new_bucket.as_mut() {
                                    state.object_lock = *checked;
                                    cx.notify();
                                }
                            })),
                    )
                    .when(state.object_lock, |this| {
                        this.child(
                            div()
                                .flex()
                                .items_start()
                                .gap(Spacing::SM)
                                .child(Icon::new(AppIcon::TriangleAlert).small().warning())
                                .child(
                                    Text::caption(dbflux_i18n::t!(
                                        "document.buckets_table.new_bucket.option.object_lock_warning"
                                    ))
                                    .warning(),
                                ),
                        )
                    }),
            )
            .child(
                div()
                    .flex()
                    .items_center()
                    .justify_between()
                    .gap(Spacing::MD)
                    .child(Text::caption(dbflux_i18n::t!(
                        "document.buckets_table.new_bucket.field.encryption"
                    )))
                    .child(encryption_control),
            );

        if let Some(error) = state.error.as_ref() {
            body = body.child(Text::caption(error.clone()).danger());
        }

        body = body.child(
            div()
                .flex()
                .items_center()
                .justify_between()
                .gap(Spacing::MD)
                .child(
                    Text::caption(format!(
                        "CreateBucket · {}",
                        dbflux_i18n::t!("document.buckets_table.new_bucket.applied_immediately")
                    ))
                    .muted_foreground(),
                )
                .child(
                    div()
                        .flex()
                        .justify_end()
                        .gap(Spacing::SM)
                        .child(
                            div()
                                .id("new-bucket-cancel")
                                .flex()
                                .items_center()
                                .h(Heights::CONTROL)
                                .px(Spacing::SM)
                                .rounded(Radii::SM)
                                .cursor_pointer()
                                .bg(theme.secondary)
                                .hover(|d| d.bg(theme.muted))
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.close_new_bucket(cx);
                                }))
                                .child(Text::caption(dbflux_i18n::t!(
                                    "document.buckets_table.new_bucket.cancel"
                                ))),
                        )
                        .child(
                            div()
                                .id("new-bucket-create")
                                .flex()
                                .items_center()
                                .gap(Spacing::XS)
                                .h(Heights::CONTROL)
                                .px(Spacing::SM)
                                .rounded(Radii::SM)
                                .bg(theme.primary)
                                .when(!can_create, |d| d.opacity(0.5))
                                .when(can_create, |d| d.cursor_pointer().hover(|d| d.opacity(0.9)))
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.submit_new_bucket(cx);
                                }))
                                .child(
                                    Icon::new(if state.submitting {
                                        AppIcon::Loader
                                    } else {
                                        AppIcon::Plus
                                    })
                                    .size(Heights::ICON_SM)
                                    .color(theme.primary_foreground),
                                )
                                .child(
                                    Text::caption(if state.submitting {
                                        dbflux_i18n::t!(
                                            "document.buckets_table.new_bucket.creating"
                                        )
                                    } else {
                                        dbflux_i18n::t!("document.buckets_table.new_bucket.create")
                                    })
                                    .color(theme.primary_foreground),
                                ),
                        ),
                ),
        );

        ModalFrame::new("buckets-new-bucket-modal", &self.focus_handle, close)
            .title(dbflux_i18n::t!("document.buckets_table.new_bucket.title"))
            .icon(AppIcon::Plus)
            .width(px(560.0))
            .max_height(px(620.0))
            .center_vertically()
            .child(body.into_any_element())
            .render(cx)
    }
}

/// Converts a driver error into a `UserFacingError` of kind `Driver`, using
/// the driver's structured `FormattedError` when the variant carries one.
fn db_error_to_user_facing(err: &DbError) -> UserFacingError {
    match err.formatted() {
        Some(fe) => UserFacingError::from_formatted(ErrorKind::Driver, fe.clone()),
        None => UserFacingError::new(ErrorKind::Driver, err.to_string()),
    }
}

/// Audits a bucket creation. Records the bucket and the outcome, never the
/// endpoint credentials behind it.
fn record_bucket_create_audit(
    audit_service: &dbflux_audit::AuditService,
    profile_id: Uuid,
    bucket: &str,
    error: Option<&str>,
) {
    use dbflux_core::chrono::Utc;
    use dbflux_core::observability::{
        EventCategory, EventOutcome, EventRecord, EventSeverity, EventSink,
    };

    let (severity, outcome, action) = match error {
        Some(_) => (
            EventSeverity::Error,
            EventOutcome::Failure,
            "bucket_create_failed",
        ),
        None => (EventSeverity::Info, EventOutcome::Success, "bucket_create"),
    };

    let mut summary = format!("Created bucket {bucket}");
    if let Some(error) = error {
        summary.push_str(&format!(": {error}"));
    }

    let event = EventRecord::new(
        Utc::now().timestamp_millis(),
        severity,
        EventCategory::ObjectStorage,
        outcome,
    )
    .with_action(action.to_string())
    .with_summary(summary)
    .with_actor_id("ui:user")
    .with_object_ref("bucket", bucket.to_string())
    .with_connection_context(profile_id.to_string(), bucket.to_string(), String::new());

    if let Err(e) = audit_service.record(event) {
        log::warn!("[buckets table] failed to record bucket-create audit event: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::{BucketEncryptionChoice, bucket_name_error};
    use crate::buckets_table::data::tests::new_test_entity;
    use dbflux_core::BucketEncryption;

    /// T39: the toolbar's intent turns into an open modal on the next render
    /// pass, with the defaults the mock specifies.
    #[gpui::test]
    fn new_bucket_intent_opens_the_modal_with_its_defaults(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, cx| doc.request_new_bucket(cx));
        });

        cx.update(|cx| {
            let doc = doc.read(cx);
            let state = doc.new_bucket().expect("the modal must be open");

            assert!(!state.versioning);
            assert!(state.block_public_access);
            assert!(!state.object_lock);
            assert_eq!(state.encryption, BucketEncryptionChoice::SseS3);
            assert!(!state.submitting);
        });
    }

    /// T39: closing the modal drops its state, so the next open starts clean.
    #[gpui::test]
    fn closing_the_modal_drops_its_state(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, cx| doc.request_new_bucket(cx));
        });

        cx.update(|cx| {
            doc.update(cx, |doc, cx| doc.close_new_bucket(cx));
        });

        cx.update(|cx| assert!(doc.read(cx).new_bucket().is_none()));
    }

    /// T39: the naming rule the spec pins down — length first, character set
    /// second, with a message that says which one failed.
    #[test]
    fn bucket_name_validation_rejects_length_and_character_violations() {
        assert_eq!(bucket_name_error("logs-archive-2026"), None);
        assert_eq!(bucket_name_error("a.b-c.9"), None);

        assert!(bucket_name_error("ab").is_some_and(|error| error.contains("3-63")));
        assert!(bucket_name_error(&"a".repeat(64)).is_some_and(|error| error.contains("3-63")));
        assert!(bucket_name_error("My-Bucket").is_some_and(|error| error.contains("lowercase")));
        assert!(bucket_name_error("my_bucket").is_some_and(|error| error.contains("lowercase")));
    }

    /// T39: every encryption segment round-trips through its stable id and
    /// maps onto the core option the driver receives.
    #[test]
    fn encryption_choices_round_trip_and_map_to_core_options() {
        for choice in BucketEncryptionChoice::all() {
            assert_eq!(BucketEncryptionChoice::from_id(choice.id()), Some(choice));
        }

        assert_eq!(
            BucketEncryptionChoice::SseS3.encryption(),
            BucketEncryption::SseS3
        );
        assert_eq!(
            BucketEncryptionChoice::SseKms.encryption(),
            BucketEncryption::SseKms { key_id: None }
        );
        assert_eq!(
            BucketEncryptionChoice::None.encryption(),
            BucketEncryption::None
        );
    }
}
