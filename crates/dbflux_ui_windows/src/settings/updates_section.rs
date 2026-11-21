use super::section_trait::SectionFocusEvent;
use super::{SettingsSection, SettingsSectionId};
use dbflux_app::keymap::Modifiers;
#[cfg(feature = "telemetry")]
use dbflux_app::telemetry::TelemetryLevel;
use dbflux_app::updates::{CheckedAgo, UpdateCheckOutcome, UpdateCheckState, UpdateSettings};
use dbflux_components::controls::{Button, Checkbox};
use dbflux_components::icons::AppIcon;
#[cfg(feature = "telemetry")]
use dbflux_components::modals::{
    ConfirmOutcome, ConfirmRequest, JsonReportPreviewRequest, ModalConfirm, ModalJsonReportPreview,
    ModalVariant,
};
#[cfg(feature = "telemetry")]
use dbflux_components::primitives::Chamfer;
use dbflux_components::primitives::{Icon, Text};
#[cfg(feature = "telemetry")]
use dbflux_components::primitives::{SegmentedControl, SegmentedItem};
#[cfg(feature = "telemetry")]
use dbflux_components::tokens::{ChamferCut, Fields, Heights};
use dbflux_components::tokens::{ChromeColors, FontSizes, Spacing, ui};
use dbflux_core::ReleaseChannel;
use dbflux_core::chrono::Utc;
use dbflux_ui_base::keymap::key_chord_from_gpui;
use dbflux_ui_base::toast::{Toast, now_hms};
use dbflux_ui_base::updates::{
    UpdateDialogRequest, channel_label, run_update_check, running_version_label,
};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use dbflux_ui_base::{AppStateChanged, AppStateEntity};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use gpui_component::scroll::ScrollableElement;

/// Width of the label column in a settings row.
const ROW_LABEL_WIDTH: Rems = ui(200.0);

/// Vertical padding of a settings row.
const ROW_PADDING_Y: Pixels = px(7.0);

/// Width of one telemetry-level column in the "what is sent" table.
#[cfg(feature = "telemetry")]
const USAGE_COLUMN_WIDTH: Pixels = px(72.0);

/// Telemetry levels in the order of the segmented control.
#[cfg(feature = "telemetry")]
const TELEMETRY_LEVELS: [TelemetryLevel; 3] = [
    TelemetryLevel::Off,
    TelemetryLevel::Minimal,
    TelemetryLevel::Basic,
];

/// The level staged when the saved one is unavailable, and by Reset.
#[cfg(feature = "telemetry")]
const DEFAULT_TELEMETRY_LEVEL: TelemetryLevel = TelemetryLevel::Basic;

/// Keyboard-navigable controls of the section, in visual order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UpdatesRow {
    CheckOnStartup,
    CheckNow,
    ShowWhatsNew,
    OpenWhatsNew,
    ShowWelcome,
    /// The staged Off/Minimal/Basic telemetry choice (a segmented field).
    #[cfg(feature = "telemetry")]
    TelemetryLevel,
    /// Opens the confirm dialog that resets the installation id.
    #[cfg(feature = "telemetry")]
    InstallReset,
    /// Opens the read-only viewer for the last confirmed report.
    #[cfg(feature = "telemetry")]
    ViewReport,
    /// Opens the audit viewer at the last report's correlation id.
    #[cfg(feature = "telemetry")]
    OpenInAudit,
    Reset,
    Save,
}

#[cfg(not(feature = "telemetry"))]
const ROWS: [UpdatesRow; 7] = [
    UpdatesRow::CheckOnStartup,
    UpdatesRow::CheckNow,
    UpdatesRow::ShowWhatsNew,
    UpdatesRow::OpenWhatsNew,
    UpdatesRow::ShowWelcome,
    UpdatesRow::Reset,
    UpdatesRow::Save,
];

#[cfg(feature = "telemetry")]
const ROWS: [UpdatesRow; 11] = [
    UpdatesRow::CheckOnStartup,
    UpdatesRow::CheckNow,
    UpdatesRow::ShowWhatsNew,
    UpdatesRow::OpenWhatsNew,
    UpdatesRow::ShowWelcome,
    UpdatesRow::TelemetryLevel,
    UpdatesRow::InstallReset,
    UpdatesRow::ViewReport,
    UpdatesRow::OpenInAudit,
    UpdatesRow::Reset,
    UpdatesRow::Save,
];

/// Where the cached last-report lookup stands. The lookup itself is cached
/// outside render and queried on a background executor.
#[cfg(feature = "telemetry")]
#[derive(Clone, Default)]
enum ReportState {
    #[default]
    Loading,
    /// No confirmed report in the durable audit (nothing sent yet). Only a
    /// successful `Ok(None)` lookup may produce this state.
    None,
    /// The audit lookup failed; the truth is unknown, not "nothing sent".
    Failed,
    Ready(dbflux_app::telemetry::TelemetryLastReport),
}

/// Settings › Updates: the update-check and changelog preferences, the
/// result of the latest check, and entry points to the two dialogs.
pub(super) struct UpdatesSection {
    app_state: Entity<AppStateEntity>,
    check_on_startup: bool,
    show_whats_new: bool,
    /// Staged telemetry level, applied only by Save. `None` while the
    /// telemetry module is unavailable: there is nothing to stage or write.
    #[cfg(feature = "telemetry")]
    telemetry_level: Option<TelemetryLevel>,
    /// Whether the user changed the level here. Save writes a level only
    /// when this is set, so a draft that went stale (Welcome stored Off
    /// while this section stayed open) can never overwrite the stored one.
    #[cfg(feature = "telemetry")]
    telemetry_edited: bool,
    /// Confirmation of the install-id reset, opened by `InstallReset`.
    #[cfg(feature = "telemetry")]
    install_reset_confirm: Entity<ModalConfirm>,
    /// Cached newest confirmed report, refreshed outside render.
    #[cfg(feature = "telemetry")]
    report_state: ReportState,
    /// Advances on every lookup, so a slower older lookup cannot replace the
    /// result of a newer one.
    #[cfg(feature = "telemetry")]
    report_generation: u64,
    /// A failed lookup has already been reported to the user in this streak.
    #[cfg(feature = "telemetry")]
    report_failure_reported: bool,
    #[cfg(feature = "telemetry")]
    report_preview: Entity<ModalJsonReportPreview>,
    content_focused: bool,
    cursor: usize,
    _subscriptions: Vec<Subscription>,
}

impl UpdatesSection {
    pub(super) fn new(
        app_state: Entity<AppStateEntity>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let saved = app_state.read(cx).update_settings().clone();
        let subscription = cx.subscribe(&app_state, |_this, _, _: &AppStateChanged, cx| {
            cx.notify();
        });

        #[cfg(feature = "telemetry")]
        let telemetry_subscription =
            cx.subscribe(&app_state, |this, _, _: &AppStateChanged, cx| {
                this.sync_unedited_telemetry_level(cx);
                this.refresh_last_report(cx);
            });

        #[cfg_attr(not(feature = "telemetry"), allow(unused_mut))]
        let mut section = Self {
            app_state,
            check_on_startup: saved.check_for_updates_on_startup,
            show_whats_new: saved.show_whats_new_after_update,
            #[cfg(feature = "telemetry")]
            telemetry_level: None,
            #[cfg(feature = "telemetry")]
            telemetry_edited: false,
            #[cfg(feature = "telemetry")]
            install_reset_confirm: cx.new(ModalConfirm::new),
            #[cfg(feature = "telemetry")]
            report_state: ReportState::Loading,
            #[cfg(feature = "telemetry")]
            report_generation: 0,
            #[cfg(feature = "telemetry")]
            report_failure_reported: false,
            #[cfg(feature = "telemetry")]
            report_preview: cx.new(ModalJsonReportPreview::new),
            content_focused: false,
            cursor: 0,
            _subscriptions: vec![subscription],
        };

        #[cfg(feature = "telemetry")]
        {
            section.telemetry_level = section.app_state.read(cx).telemetry_level();
            section.refresh_last_report(cx);
            section._subscriptions.push(telemetry_subscription);

            let reset_subscription = cx.subscribe(
                &section.install_reset_confirm,
                |this, _, outcome: &ConfirmOutcome, cx| match outcome {
                    ConfirmOutcome::Confirmed => this.confirm_install_reset(cx),
                    ConfirmOutcome::Cancelled => {}
                },
            );
            section._subscriptions.push(reset_subscription);
        }

        section
    }

    /// Queries the newest confirmed report from the durable audit on the
    /// background executor and caches the metadata outside render. No query
    /// runs while the optional telemetry module is unavailable.
    #[cfg(feature = "telemetry")]
    fn refresh_last_report(&mut self, cx: &mut Context<Self>) {
        self.report_generation += 1;
        let generation = self.report_generation;
        if self.app_state.read(cx).telemetry().is_none() {
            self.report_state = ReportState::Failed;
            cx.notify();
            return;
        }
        self.report_state = ReportState::Loading;
        let audit = self.app_state.read(cx).audit_service().clone();
        let task = cx
            .background_executor()
            .spawn(async move { dbflux_app::telemetry::last_report(&audit) });
        cx.spawn(async move |section, cx| {
            let result = task.await;
            let applied = section.update(cx, |section, cx| {
                section.apply_report_lookup(generation, result, cx);
            });
            if applied.is_err() {
                // The section (or its window) closed while the query ran:
                // nothing to update, nothing to report to the user.
                log::debug!("updates.report_query_dropped: section gone");
            }
        })
        .detach();
    }

    /// Applies a finished lookup unless a newer one started meanwhile, so a
    /// slow older lookup can never replace a newer result.
    #[cfg(feature = "telemetry")]
    fn apply_report_lookup(
        &mut self,
        generation: u64,
        result: Result<
            Option<dbflux_app::telemetry::TelemetryLastReport>,
            dbflux_app::telemetry::TelemetryError,
        >,
        cx: &mut Context<Self>,
    ) {
        if self.report_generation != generation {
            return;
        }
        self.report_state = match result {
            Ok(Some(report)) => {
                self.report_failure_reported = false;
                ReportState::Ready(report)
            }
            Ok(None) => {
                self.report_failure_reported = false;
                ReportState::None
            }
            Err(_) => {
                // First catch site: one finite user-facing error; the
                // payload or raw audit text never reaches the toast. Reporting
                // emits `AppStateChanged`, which starts another lookup, so a
                // failure streak reports once or the section would query and
                // notify forever.
                if !self.report_failure_reported {
                    self.report_failure_reported = true;
                    report_error(
                        UserFacingError::new(
                            ErrorKind::Storage,
                            dbflux_i18n::t!("updates.settings.report_load_failed"),
                        ),
                        cx,
                    );
                }
                ReportState::Failed
            }
        };
        cx.notify();
    }

    /// Row label of the cached report state, for rendering and regressions.
    #[cfg(feature = "telemetry")]
    fn report_meta_label(&self) -> String {
        report_meta_label(&self.report_state)
    }

    /// Follows the stored level while the user has not edited it here, so an
    /// Off chosen in Welcome is what this section shows and what Save keeps.
    #[cfg(feature = "telemetry")]
    fn sync_unedited_telemetry_level(&mut self, cx: &App) {
        if !self.telemetry_edited {
            self.telemetry_level = self.app_state.read(cx).telemetry_level();
        }
    }

    fn saved_settings(&self, cx: &App) -> UpdateSettings {
        self.app_state.read(cx).update_settings().clone()
    }

    fn has_unsaved_changes(&self, cx: &App) -> bool {
        let saved = self.saved_settings(cx);

        let update_changes = saved.check_for_updates_on_startup != self.check_on_startup
            || saved.show_whats_new_after_update != self.show_whats_new;

        if update_changes {
            return true;
        }

        #[cfg(feature = "telemetry")]
        if self.telemetry_edited
            && self
                .telemetry_level
                .is_some_and(|draft| self.app_state.read(cx).telemetry_level() != Some(draft))
        {
            return true;
        }

        false
    }

    fn save(&mut self, cx: &mut Context<Self>) {
        let settings = UpdateSettings {
            check_for_updates_on_startup: self.check_on_startup,
            show_whats_new_after_update: self.show_whats_new,
            ..self.saved_settings(cx)
        };

        let result = self.app_state.update(cx, |state, cx| {
            let result = state.set_update_settings(settings);
            cx.emit(AppStateChanged);
            result
        });

        if let Err(error) = result {
            report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!("updates.settings.save_error", error = error),
                ),
                cx,
            );
            return;
        }

        // First catch site: persist the staged level on the storage owner, in
        // the foreground. On failure the staged value stays dirty and the
        // saved state stays truthful; no success toast is shown.
        #[cfg(feature = "telemetry")]
        if let Some(level) = self.telemetry_level.filter(|_| self.telemetry_edited) {
            let telemetry_result = self
                .app_state
                .update(cx, |state, _| state.set_telemetry_level(level, Utc::now()));
            if let Err(_error) = telemetry_result {
                report_error(
                    UserFacingError::new(
                        ErrorKind::Storage,
                        dbflux_i18n::t!("updates.settings.telemetry_save_failed"),
                    ),
                    cx,
                );
                self.refresh_last_report(cx);
                cx.notify();
                return;
            }
            self.telemetry_edited = false;
            self.refresh_last_report(cx);
        }

        Toast::success(dbflux_i18n::t!("updates.settings.saved"))
            .meta_right(now_hms())
            .push(cx);
        cx.notify();
    }

    fn reset_to_defaults(&mut self, cx: &mut Context<Self>) {
        let defaults = UpdateSettings::default();
        self.check_on_startup = defaults.check_for_updates_on_startup;
        self.show_whats_new = defaults.show_whats_new_after_update;
        // Defaults stage the default level only: the install id and the
        // saved level are untouched until Save, and no telemetry write
        // happens here.
        #[cfg(feature = "telemetry")]
        if self.telemetry_level.is_some() {
            self.telemetry_level = Some(DEFAULT_TELEMETRY_LEVEL);
            self.telemetry_edited = true;
        }
        cx.notify();
    }

    fn request_dialog(&self, request: UpdateDialogRequest, cx: &mut Context<Self>) {
        self.app_state.update(cx, |state, cx| {
            state.request_update_dialog(request, cx);
        });
    }

    fn activate(&mut self, row: UpdatesRow, cx: &mut Context<Self>) {
        match row {
            UpdatesRow::CheckOnStartup => self.check_on_startup = !self.check_on_startup,
            UpdatesRow::ShowWhatsNew => self.show_whats_new = !self.show_whats_new,
            UpdatesRow::CheckNow => run_update_check(&self.app_state, cx),
            UpdatesRow::OpenWhatsNew => {
                self.request_dialog(UpdateDialogRequest::WhatsNew { since: None }, cx)
            }
            UpdatesRow::ShowWelcome => self.request_dialog(UpdateDialogRequest::Welcome, cx),
            #[cfg(feature = "telemetry")]
            UpdatesRow::TelemetryLevel => {
                if let Some(current) = self.telemetry_level {
                    let next = (telemetry_level_index(current) + 1) % TELEMETRY_LEVELS.len();
                    self.stage_telemetry_level_at(next);
                }
            }
            #[cfg(feature = "telemetry")]
            UpdatesRow::InstallReset => self.open_install_reset_confirm(cx),
            #[cfg(feature = "telemetry")]
            UpdatesRow::ViewReport => self.open_report_preview(cx),
            #[cfg(feature = "telemetry")]
            UpdatesRow::OpenInAudit => self.open_last_report_in_audit(cx),
            UpdatesRow::Reset => self.reset_to_defaults(cx),
            UpdatesRow::Save => self.save(cx),
        }
        cx.notify();
    }

    /// Stages the level at `index` of the segmented control. An index outside
    /// the control, or an unavailable telemetry module, leaves the staged
    /// level unchanged.
    #[cfg(feature = "telemetry")]
    fn stage_telemetry_level_at(&mut self, index: usize) {
        if self.telemetry_level.is_none() {
            return;
        }
        if let Some(level) = TELEMETRY_LEVELS.get(index).copied() {
            self.telemetry_level = Some(level);
            self.telemetry_edited = true;
        }
    }

    /// Opens the read-only viewer over the cached report; nothing renders for
    /// an empty cache, so no fake empty report can appear.
    #[cfg(feature = "telemetry")]
    fn open_report_preview(&mut self, cx: &mut Context<Self>) {
        let ReportState::Ready(report) = &self.report_state else {
            return;
        };
        let when = report_time_label(report.ack_at_epoch_ms);
        let level = report_level_label(report.level.as_deref());
        let request = JsonReportPreviewRequest {
            title: dbflux_i18n::t!("updates.settings.reports").into(),
            meta: Some(
                dbflux_i18n::t!(
                    "updates.settings.last_sent",
                    when = when,
                    level = level,
                    size = report_size_label(report.payload_len)
                )
                .into(),
            ),
            raw: report.payload.clone(),
        };
        self.report_preview
            .update(cx, |preview, cx| preview.open(request, cx));
    }

    /// Opens the audit viewer at the last report's real correlation id.
    #[cfg(feature = "telemetry")]
    fn open_last_report_in_audit(&mut self, cx: &mut Context<Self>) {
        // The rendered button is disabled without a correlation id; the
        // keyboard path must not open an unfiltered audit view instead.
        let ReportState::Ready(report) = &self.report_state else {
            return;
        };
        let Some(correlation) = report.correlation else {
            return;
        };
        self.app_state.update(cx, |state, cx| {
            state.request_open_audit(Some(correlation), cx);
        });
    }

    fn select(&mut self, row: UpdatesRow) {
        self.content_focused = true;
        if let Some(position) = ROWS.iter().position(|candidate| *candidate == row) {
            self.cursor = position;
        }
    }

    fn is_at(&self, row: UpdatesRow) -> bool {
        self.content_focused && ROWS.get(self.cursor).copied() == Some(row)
    }

    fn focus_frame(&self, row: UpdatesRow, cx: &App, child: impl IntoElement) -> Div {
        super::layout::cursor_ring(self.is_at(row), child, cx)
    }

    fn render_group_header(&self, cx: &App) -> AnyElement {
        dbflux_components::composites::section_header(
            dbflux_i18n::t!("updates.settings.group"),
            Some(AppIcon::Download.into()),
            cx,
        )
        .into_any_element()
    }

    fn render_row(&self, label: String, content: impl IntoElement) -> AnyElement {
        div()
            .flex()
            .items_start()
            .gap(Spacing::LG)
            .py(ROW_PADDING_Y)
            .child(
                div()
                    .w(ROW_LABEL_WIDTH)
                    .flex_shrink_0()
                    .pt(ROW_PADDING_Y)
                    .child(Text::body(label)),
            )
            .child(div().flex_1().min_w_0().child(content))
            .into_any_element()
    }

    fn render_checkbox(
        &self,
        id: &'static str,
        row: UpdatesRow,
        label: String,
        checked: bool,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        self.focus_frame(
            row,
            cx,
            div()
                .id(id)
                .flex()
                .items_center()
                .gap(Spacing::SM)
                .py(Spacing::XXS)
                .on_mouse_down(
                    MouseButton::Left,
                    cx.listener(move |this, _, _, cx| {
                        this.select(row);
                        cx.notify();
                    }),
                )
                .child(
                    Checkbox::new(id)
                        .checked(checked)
                        .aria_label(label.clone())
                        .on_click(cx.listener(move |this, value: &bool, _, cx| {
                            this.select(row);
                            match row {
                                UpdatesRow::CheckOnStartup => this.check_on_startup = *value,
                                UpdatesRow::ShowWhatsNew => this.show_whats_new = *value,
                                _ => {}
                            }
                            cx.notify();
                        })),
                )
                .child(Text::body(label)),
        )
        .into_any_element()
    }

    fn render_button(
        &self,
        id: &'static str,
        row: UpdatesRow,
        label: String,
        icon: AppIcon,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        Button::new(id, label)
            .secondary()
            .icon(icon)
            .focused(self.is_at(row))
            .on_click(cx.listener(move |this, _, _, cx| {
                this.select(row);
                this.activate(row, cx);
            }))
            .into_any_element()
    }

    fn render_status(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let check = self.app_state.read(cx).update_check().clone();
        let (icon, color, headline) = status_headline(&check, theme);

        let detail = status_detail(&check, Utc::now());

        div()
            .flex()
            .items_center()
            .gap(Spacing::MD)
            .child(Icon::new(icon).size(ui(15.0)).color(color))
            .child(Text::body(headline).text_color(theme.accent_foreground))
            .child(
                Text::code(detail)
                    .font_size(FontSizes::XS)
                    .text_color(theme.muted_foreground),
            )
            .child(div().flex_1())
            .child(self.render_button(
                "updates-check-now",
                UpdatesRow::CheckNow,
                dbflux_i18n::t!("updates.settings.check_now"),
                AppIcon::RefreshCcw,
                cx,
            ))
            .into_any_element()
    }

    fn render_body(&self, cx: &mut Context<Self>) -> AnyElement {
        let new_versions = self.render_checkbox(
            "updates-check-on-startup",
            UpdatesRow::CheckOnStartup,
            dbflux_i18n::t!("updates.settings.check_on_startup"),
            self.check_on_startup,
            cx,
        );

        let status = self.render_status(cx);

        let changelog = div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(self.render_checkbox(
                "updates-show-whats-new",
                UpdatesRow::ShowWhatsNew,
                dbflux_i18n::t!("updates.settings.show_whats_new"),
                self.show_whats_new,
                cx,
            ))
            .child(div().flex_1())
            .child(self.render_button(
                "updates-open-whats-new",
                UpdatesRow::OpenWhatsNew,
                dbflux_i18n::t!("updates.settings.open_whats_new"),
                AppIcon::History,
                cx,
            ))
            .child(self.render_button(
                "updates-show-welcome",
                UpdatesRow::ShowWelcome,
                dbflux_i18n::t!("updates.settings.show_welcome"),
                AppIcon::Layers,
                cx,
            ));

        let group_header = self.render_group_header(cx);

        #[cfg_attr(not(feature = "telemetry"), allow(unused_mut))]
        let mut body = div()
            .flex()
            .flex_col()
            .child(group_header)
            .child(self.render_row(
                dbflux_i18n::t!("updates.settings.new_versions"),
                new_versions,
            ))
            .child(self.render_row(dbflux_i18n::t!("updates.settings.status"), status))
            .child(self.render_row(dbflux_i18n::t!("updates.settings.changelog"), changelog));

        #[cfg(feature = "telemetry")]
        {
            body = body.child(self.render_usage_group(cx));
        }

        body.into_any_element()
    }

    /// The staged-level segmented control inside its focus frame.
    #[cfg(feature = "telemetry")]
    fn render_level_selector(&self, cx: &mut Context<Self>) -> Div {
        let level = SegmentedControl::new(
            vec![
                SegmentedItem::new("off", dbflux_i18n::t!("updates.settings.level_off"))
                    .icon(AppIcon::EyeOff),
                SegmentedItem::new("minimal", dbflux_i18n::t!("updates.settings.level_minimal"))
                    .icon(AppIcon::Tag),
                SegmentedItem::new("basic", dbflux_i18n::t!("updates.settings.level_basic"))
                    .icon(AppIcon::Columns),
            ],
            telemetry_level_segment(self.telemetry_level.unwrap_or(DEFAULT_TELEMETRY_LEVEL)),
            {
                let state = cx.entity();
                move |id, _, cx| {
                    state.update(cx, |section, cx| {
                        let index = TELEMETRY_LEVELS
                            .iter()
                            .position(|level| telemetry_level_segment(*level) == id.as_ref());

                        if let Some(index) = index {
                            section.stage_telemetry_level_at(index);
                            cx.notify();
                        }
                    });
                }
            },
        )
        .group("telemetry-level")
        .focused(self.content_focused && self.is_at(UpdatesRow::TelemetryLevel));

        // A flex row keeps the segmented track hugging its segments; a plain
        // child of the column would stretch it across the whole row.
        self.focus_frame(UpdatesRow::TelemetryLevel, cx, div().flex().child(level))
    }

    /// The "what is sent" table: a caption over one row per collected item.
    #[cfg(feature = "telemetry")]
    fn render_collected_table(&self, cx: &mut Context<Self>) -> Div {
        let theme = cx.theme();
        let muted = theme.muted_foreground;
        div()
            .flex()
            .flex_col()
            .gap(Spacing::SM)
            .child(
                Text::caption(dbflux_i18n::t!("updates.settings.counts_not_instrumented"))
                    .text_color(muted),
            )
            .child(
                div()
                    .relative()
                    .flex()
                    .flex_col()
                    .child(
                        Chamfer::new(ChamferCut::CONTROL)
                            .fill(theme.background)
                            .border(theme.border),
                    )
                    .child(self.usage_table_header(cx))
                    .child(self.collected_row(
                        dbflux_i18n::t!("updates.settings.collected_build"),
                        true,
                        true,
                        true,
                        cx,
                    ))
                    .child(self.collected_row(
                        dbflux_i18n::t!("updates.settings.collected_language"),
                        false,
                        true,
                        true,
                        cx,
                    ))
                    .child(self.collected_row(
                        dbflux_i18n::t!("updates.settings.collected_counts"),
                        false,
                        true,
                        false,
                        cx,
                    )),
            )
    }

    /// The masked installation id with its reset button.
    #[cfg(feature = "telemetry")]
    fn render_install_id_row(&self, cx: &mut Context<Self>) -> Div {
        let theme = cx.theme();
        div()
            .flex()
            .items_center()
            .gap(Spacing::SM)
            .child(
                div()
                    .relative()
                    .flex()
                    .items_center()
                    .h(Fields::HEIGHT)
                    .px(Fields::PADDING_X)
                    .child(
                        Chamfer::new(ChamferCut::CONTROL)
                            .fill(theme.background)
                            .border(theme.border),
                    )
                    .child(
                        Text::code(
                            self.app_state
                                .read(cx)
                                .telemetry_install_id_masked()
                                .unwrap_or_else(|| "—".to_owned()),
                        )
                        .font_size(FontSizes::SM)
                        .text_color(ChromeColors::strong(theme)),
                    ),
            )
            .child(self.render_button(
                "updates-install-reset",
                UpdatesRow::InstallReset,
                dbflux_i18n::t!("updates.settings.reset_install_id"),
                AppIcon::RotateCcw,
                cx,
            ))
    }

    /// Usage statistics: the staged Off/Minimal/Basic level, what is sent, the
    /// install id and the last confirmed report.
    #[cfg(feature = "telemetry")]
    fn render_usage_group(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = cx.theme();
        let muted = theme.muted_foreground;
        let tint = ChromeColors::tint(theme);

        let level_row = self.render_level_selector(cx);
        let what_is_sent = self.render_collected_table(cx);
        let install_id = self.render_install_id_row(cx);

        let delivery_configured = self.app_state.read(cx).telemetry_delivery_configured();
        let reports = self.render_reports_controls(cx);

        div()
            .flex()
            .flex_col()
            .pt(Spacing::MD)
            .child(dbflux_components::composites::section_header(
                dbflux_i18n::t!("updates.settings.usage_heading"),
                Some(AppIcon::Activity.into()),
                cx,
            ))
            .child(
                self.render_row(
                    dbflux_i18n::t!("updates.settings.usage_level"),
                    div()
                        .flex()
                        .flex_col()
                        .gap(Spacing::XXS)
                        .child(level_row)
                        .child(
                            Text::caption(dbflux_i18n::t!("updates.settings.level_hint"))
                                .text_color(muted),
                        )
                        .children((delivery_configured == Some(false)).then(|| {
                            Text::caption(dbflux_i18n::t!("updates.settings.delivery_unavailable"))
                                .text_color(tint)
                        })),
                ),
            )
            .child(self.render_row(
                dbflux_i18n::t!("updates.settings.what_is_sent"),
                what_is_sent,
            ))
            .child(
                self.render_row(
                    dbflux_i18n::t!("updates.settings.install_id"),
                    div()
                        .flex()
                        .flex_col()
                        .gap(Spacing::XXS)
                        .child(install_id)
                        .child(
                            Text::caption(dbflux_i18n::t!("updates.settings.install_id_hint"))
                                .text_color(muted),
                        ),
                ),
            )
            .child(self.render_row(dbflux_i18n::t!("updates.settings.reports"), reports))
            .into_any_element()
    }

    /// Header of the "what is sent" table: the collected item column and one
    /// column per telemetry level, aligned with the marks of every row.
    #[cfg(feature = "telemetry")]
    fn usage_table_header(&self, cx: &App) -> Div {
        let theme = cx.theme();
        let head = |label: String| {
            div()
                .w(USAGE_COLUMN_WIDTH)
                .flex_shrink_0()
                .flex()
                .justify_end()
                .child(
                    Text::label(label)
                        .font_size(FontSizes::XS)
                        .text_color(theme.muted_foreground),
                )
        };

        div()
            .flex()
            .items_center()
            .gap(Spacing::LG)
            .px(Spacing::MD)
            .py(Spacing::SM)
            .border_b_1()
            .border_color(theme.border)
            .child(
                Text::label(dbflux_i18n::t!("updates.settings.collected_header"))
                    .font_size(FontSizes::XS)
                    .text_color(theme.muted_foreground),
            )
            .child(div().flex_1())
            .child(head(dbflux_i18n::t!("updates.settings.collected_minimal")))
            .child(head(dbflux_i18n::t!("updates.settings.collected_basic")))
    }

    /// One honest row of the "what is sent" table: the collected item and
    /// whether the Minimal / Basic level includes it.
    #[cfg(feature = "telemetry")]
    fn collected_row(
        &self,
        label: String,
        in_minimal: bool,
        in_basic: bool,
        separator: bool,
        cx: &App,
    ) -> Div {
        let theme = cx.theme();
        let mark = |included: bool| {
            let color = if included {
                theme.success
            } else {
                theme.muted_foreground
            };
            div()
                .w(USAGE_COLUMN_WIDTH)
                .flex_shrink_0()
                .flex()
                .justify_end()
                .child(
                    Text::caption(if included {
                        dbflux_i18n::t!("updates.settings.collected_yes")
                    } else {
                        dbflux_i18n::t!("updates.settings.collected_no")
                    })
                    .text_color(color),
                )
        };

        div()
            .flex()
            .items_center()
            .gap(Spacing::LG)
            .px(Spacing::MD)
            .py(Spacing::SM)
            .when(separator, |row| row.border_b_1().border_color(theme.border))
            .child(Text::caption(label))
            .child(div().flex_1())
            .child(mark(in_minimal))
            .child(mark(in_basic))
    }

    /// Last-sent line plus the two report actions, both disabled without a
    /// confirmed report so no fake empty report can be opened.
    #[cfg(feature = "telemetry")]
    fn render_reports_controls(&self, cx: &mut Context<Self>) -> Div {
        let theme = cx.theme();
        let muted = theme.muted_foreground;

        let meta = Text::caption(self.report_meta_label()).text_color(muted);

        let has_report = matches!(self.report_state, ReportState::Ready(_));
        let view = Button::new(
            "updates-view-report",
            dbflux_i18n::t!("updates.settings.view_report"),
        )
        .secondary()
        .icon(AppIcon::Braces)
        .disabled(!has_report)
        .focused(self.is_at(UpdatesRow::ViewReport))
        .on_click(cx.listener(move |this, _, _, cx| {
            this.select(UpdatesRow::ViewReport);
            this.activate(UpdatesRow::ViewReport, cx);
        }));

        let can_open_audit = matches!(&self.report_state, ReportState::Ready(report) if report.correlation.is_some());
        let open = Button::new(
            "updates-open-in-audit",
            dbflux_i18n::t!("updates.settings.open_in_audit"),
        )
        .secondary()
        .icon(AppIcon::ExternalLink)
        .disabled(!can_open_audit)
        .focused(self.is_at(UpdatesRow::OpenInAudit))
        .on_click(cx.listener(move |this, _, _, cx| {
            this.select(UpdatesRow::OpenInAudit);
            this.activate(UpdatesRow::OpenInAudit, cx);
        }));

        div()
            .flex()
            .flex_col()
            .gap(Spacing::XXS)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(
                        Icon::new(AppIcon::Activity)
                            .size(Heights::ICON_SM)
                            .color(muted),
                    )
                    .child(meta)
                    .child(div().flex_1())
                    .child(view)
                    .child(open),
            )
            .child(
                Text::caption(dbflux_i18n::t!("updates.settings.reports_audit_note"))
                    .text_color(muted),
            )
    }

    fn handle_key(&mut self, event: &KeyDownEvent, cx: &mut Context<Self>) {
        let chord = key_chord_from_gpui(&event.keystroke);

        match (chord.key.as_str(), chord.modifiers) {
            ("s", modifiers) if modifiers == Modifiers::ctrl() => {
                self.save(cx);
            }
            ("j", modifiers) | ("down", modifiers) | ("tab", modifiers)
                if modifiers == Modifiers::none() =>
            {
                self.cursor = (self.cursor + 1).min(ROWS.len() - 1);
                cx.notify();
            }
            ("k", modifiers) | ("up", modifiers) if modifiers == Modifiers::none() => {
                self.cursor = self.cursor.saturating_sub(1);
                cx.notify();
            }
            ("tab", modifiers) if modifiers == Modifiers::shift() => {
                self.cursor = self.cursor.saturating_sub(1);
                cx.notify();
            }
            ("enter", modifiers) | ("space", modifiers) if modifiers == Modifiers::none() => {
                if let Some(row) = ROWS.get(self.cursor).copied() {
                    self.activate(row, cx);
                }
            }
            #[cfg(feature = "telemetry")]
            ("h", modifiers) | ("left", modifiers)
                if modifiers == Modifiers::none()
                    && ROWS.get(self.cursor).copied() == Some(UpdatesRow::TelemetryLevel) =>
            {
                if let Some(current) = self.telemetry_level {
                    self.stage_telemetry_level_at(telemetry_level_index(current).saturating_sub(1));
                }
                cx.notify();
            }
            #[cfg(feature = "telemetry")]
            ("l", modifiers) | ("right", modifiers)
                if modifiers == Modifiers::none()
                    && ROWS.get(self.cursor).copied() == Some(UpdatesRow::TelemetryLevel) =>
            {
                if let Some(current) = self.telemetry_level {
                    self.stage_telemetry_level_at(telemetry_level_index(current) + 1);
                }
                cx.notify();
            }
            ("escape", modifiers) if modifiers == Modifiers::none() => {
                cx.emit(SectionFocusEvent::RequestFocusReturn);
            }
            _ => {}
        }
    }
}

/// Icon, icon color and headline for the latest check.
fn status_headline(
    check: &UpdateCheckState,
    theme: &gpui_component::Theme,
) -> (AppIcon, Hsla, String) {
    if check.in_progress {
        return (
            AppIcon::Loader,
            theme.muted_foreground,
            dbflux_i18n::t!("updates.settings.checking"),
        );
    }

    match &check.outcome {
        None => (
            AppIcon::Info,
            theme.muted_foreground,
            dbflux_i18n::t!("updates.settings.not_checked"),
        ),
        Some(UpdateCheckOutcome::UpToDate) => (
            AppIcon::CircleCheck,
            theme.success,
            dbflux_i18n::t!("updates.settings.up_to_date"),
        ),
        Some(UpdateCheckOutcome::Available(update)) => (
            AppIcon::ArrowUp,
            ChromeColors::tint(theme),
            dbflux_i18n::t!("updates.settings.available", version = update.label),
        ),
        Some(UpdateCheckOutcome::Failed) => (
            AppIcon::CircleAlert,
            theme.warning,
            dbflux_i18n::t!("updates.settings.failed"),
        ),
    }
}

/// `<version> · <channel> · checked <when>`, without the last part before the
/// first check of the session.
fn status_detail(check: &UpdateCheckState, now: dbflux_core::chrono::DateTime<Utc>) -> String {
    let version = running_version_label();
    let channel = channel_label(ReleaseChannel::current());

    match check.checked_at {
        Some(checked_at) => dbflux_i18n::t!(
            "updates.settings.detail",
            version = version,
            channel = channel,
            ago = checked_ago_label(CheckedAgo::between(checked_at, now))
        ),
        None => dbflux_i18n::t!(
            "updates.settings.detail_unchecked",
            version = version,
            channel = channel
        ),
    }
}

fn checked_ago_label(ago: CheckedAgo) -> String {
    match ago {
        CheckedAgo::JustNow => dbflux_i18n::t!("updates.ago.just_now"),
        CheckedAgo::Minutes(count) => dbflux_i18n::t!("updates.ago.minutes", count = count),
        CheckedAgo::Hours(count) => dbflux_i18n::t!("updates.ago.hours", count = count),
        CheckedAgo::Days(count) => dbflux_i18n::t!("updates.ago.days", count = count),
    }
}

impl EventEmitter<SectionFocusEvent> for UpdatesSection {}

impl SettingsSection for UpdatesSection {
    fn section_id(&self) -> SettingsSectionId {
        SettingsSectionId::Updates
    }

    fn handle_key_event(
        &mut self,
        event: &KeyDownEvent,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.handle_key(event, cx);
    }

    fn focus_in(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = true;
        cx.notify();
    }

    fn focus_out(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.content_focused = false;
        cx.notify();
    }

    fn is_dirty(&self, cx: &App) -> bool {
        self.has_unsaved_changes(cx)
    }

    fn save_from_shortcut(&mut self, _window: &mut Window, cx: &mut Context<Self>) {
        self.save(cx);
    }

    fn render_footer_leading_actions(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        Some(self.render_button(
            "updates-reset",
            UpdatesRow::Reset,
            dbflux_i18n::t!("updates.settings.reset"),
            AppIcon::RotateCcw,
            cx,
        ))
    }

    fn render_footer_actions(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<AnyElement> {
        Some(
            Button::new("updates-save", dbflux_i18n::t!("updates.settings.save"))
                .primary()
                .icon(AppIcon::Save)
                .when_some(crate::settings::save_shortcut(), Button::kbd)
                .focused(self.is_at(UpdatesRow::Save))
                .on_click(cx.listener(|this, _, _, cx| {
                    this.select(UpdatesRow::Save);
                    this.save(cx);
                }))
                .into_any_element(),
        )
    }
}

impl Render for UpdatesSection {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let body = self.render_body(cx);

        #[cfg_attr(not(feature = "telemetry"), allow(unused_mut))]
        let mut shell = div()
            .flex_1()
            .min_h_0()
            .flex()
            .flex_col()
            .overflow_hidden()
            .child(
                div()
                    .flex_shrink_0()
                    .pb(crate::tokens::SettingsMetrics::PAGE_HEAD_PADDING_BOTTOM - Spacing::SM)
                    .border_b_1()
                    .border_color(cx.theme().border)
                    .child(dbflux_components::composites::page_header(
                        dbflux_i18n::t!("updates.settings.title"),
                        dbflux_i18n::t!("updates.settings.subtitle"),
                        cx,
                    )),
            )
            .child(
                div()
                    .id("updates-settings-body")
                    .flex_1()
                    .min_h_0()
                    .overflow_y_scrollbar()
                    .px(crate::tokens::SettingsMetrics::BODY_PADDING_X)
                    .pb(Spacing::XL)
                    .child(body),
            );

        #[cfg(feature = "telemetry")]
        {
            shell = shell
                .child(self.install_reset_confirm.clone())
                .child(self.report_preview.clone());
        }

        shell
    }
}

impl UpdatesSection {
    /// Opens the safe confirmation of the install-id reset. The reusable
    /// `ModalConfirm` carries the section's own copy; confirming resolves
    /// through its `Confirmed` outcome, cancelling through `Cancelled`.
    #[cfg(feature = "telemetry")]
    fn open_install_reset_confirm(&mut self, cx: &mut Context<Self>) {
        self.install_reset_confirm.update(cx, |confirm, cx| {
            confirm.open(
                ConfirmRequest {
                    title: dbflux_i18n::t!("updates.settings.reset_install_id_title"),
                    body: dbflux_i18n::t!("updates.settings.reset_install_id_body"),
                    variant: ModalVariant::Danger,
                    confirm_label: dbflux_i18n::t!("updates.settings.reset_install_id"),
                },
                cx,
            );
        });
    }

    /// Rotates the installation identity immediately, in the foreground.
    /// Cancels old-identity pending work; acknowledged history survives. The
    /// app-state change also wakes the retained telemetry worker.
    #[cfg(feature = "telemetry")]
    fn confirm_install_reset(&mut self, cx: &mut Context<Self>) {
        let result = self.app_state.update(cx, |state, cx| {
            let result = state.reset_telemetry_install_id(Utc::now());
            cx.emit(AppStateChanged);
            result
        });
        if let Err(_error) = result {
            report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!("updates.settings.telemetry_reset_failed"),
                ),
                cx,
            );
        }
        cx.notify();
    }
}

/// Row label of the cached lookup state. Each state tells the truth: a
/// running lookup is checking, a failed lookup says so, and only a successful
/// empty lookup may claim that no report was ever sent.
#[cfg(feature = "telemetry")]
fn report_meta_label(state: &ReportState) -> String {
    match state {
        ReportState::Ready(report) => dbflux_i18n::t!(
            "updates.settings.last_sent",
            when = report_time_label(report.ack_at_epoch_ms),
            level = report_level_label(report.level.as_deref()),
            size = report_size_label(report.payload_len)
        ),
        ReportState::Loading => {
            dbflux_i18n::t!("updates.settings.checking_last_report")
        }
        ReportState::Failed => dbflux_i18n::t!("updates.settings.report_load_failed"),
        ReportState::None => dbflux_i18n::t!("updates.settings.no_report_sent_yet"),
    }
}

/// Calendar time label of an epoch-millisecond value, in the local zone.
#[cfg(feature = "telemetry")]
fn report_time_label(epoch_ms: i64) -> String {
    dbflux_core::chrono::DateTime::<dbflux_core::chrono::Utc>::from_timestamp_millis(epoch_ms)
        .map(|utc| {
            utc.with_timezone(&dbflux_core::chrono::Local)
                .format("%Y-%m-%d %H:%M")
                .to_string()
        })
        .unwrap_or_default()
}

/// Segment id of a telemetry level in the Settings control.
#[cfg(feature = "telemetry")]
fn telemetry_level_segment(level: TelemetryLevel) -> &'static str {
    match level {
        TelemetryLevel::Off => "off",
        TelemetryLevel::Minimal => "minimal",
        TelemetryLevel::Basic => "basic",
    }
}

/// Position of a telemetry level in the segmented control.
#[cfg(feature = "telemetry")]
fn telemetry_level_index(level: TelemetryLevel) -> usize {
    match level {
        TelemetryLevel::Off => 0,
        TelemetryLevel::Minimal => 1,
        TelemetryLevel::Basic => 2,
    }
}

/// Readable level label of the marker stored with a delivered report. Rows
/// written before the Minimal level existed marked the per-build ping
/// `"off"`; a delivered report is never Off, so those read as Minimal.
#[cfg(feature = "telemetry")]
fn report_level_label(level: Option<&str>) -> String {
    match level {
        Some("basic") => dbflux_i18n::t!("updates.settings.level_basic_display"),
        Some("minimal") | Some("off") | None => {
            dbflux_i18n::t!("updates.settings.level_minimal_display")
        }
        Some(other) => other.to_owned(),
    }
}

/// Readable size of the stored payload bytes.
#[cfg(feature = "telemetry")]
fn report_size_label(len: usize) -> String {
    if len >= 1024 {
        format!("{:.1} KiB", len as f64 / 1024.0)
    } else {
        format!("{len} B")
    }
}

#[cfg(test)]
mod tests {
    // Explicit imports: the parent's `gpui::*` glob would make `#[test]`
    // resolve to `gpui::test`, whose expansion recurses without bound.
    use super::{
        ReleaseChannel, UpdateCheckOutcome, UpdateCheckState, Utc, channel_label,
        running_version_label, status_detail, status_headline,
    };
    use dbflux_app::updates::AvailableUpdate;
    use dbflux_core::chrono::TimeDelta;

    #[cfg(feature = "telemetry")]
    mod telemetry;

    #[test]
    fn detail_mentions_the_check_time_only_after_a_check() {
        let now = Utc::now();
        let unchecked = UpdateCheckState::default();
        let checked = UpdateCheckState {
            outcome: Some(UpdateCheckOutcome::UpToDate),
            checked_at: Some(now - TimeDelta::minutes(2)),
            in_progress: false,
        };

        let version = running_version_label();
        let channel = channel_label(ReleaseChannel::current());

        assert_eq!(
            status_detail(&unchecked, now),
            dbflux_i18n::t!(
                "updates.settings.detail_unchecked",
                version = version,
                channel = channel
            )
        );
        assert_eq!(
            status_detail(&checked, now),
            dbflux_i18n::t!(
                "updates.settings.detail",
                version = version,
                channel = channel,
                ago = dbflux_i18n::t!("updates.ago.minutes", count = 2)
            )
        );
    }

    #[test]
    fn available_status_names_the_version() {
        let theme = gpui_component::Theme::default();
        let check = UpdateCheckState {
            outcome: Some(UpdateCheckOutcome::Available(AvailableUpdate {
                label: "0.8.1".to_string(),
                download_url: String::new(),
                notes_url: String::new(),
            })),
            checked_at: None,
            in_progress: false,
        };

        let (_, _, headline) = status_headline(&check, &theme);
        assert_eq!(
            headline,
            dbflux_i18n::t!("updates.settings.available", version = "0.8.1")
        );

        let running = UpdateCheckState {
            in_progress: true,
            ..check
        };
        assert_eq!(
            status_headline(&running, &theme).2,
            dbflux_i18n::t!("updates.settings.checking")
        );
    }

    #[test]
    fn updates_settings_copy_resolves_in_every_locale() {
        for key in [
            "settings.nav.updates",
            "updates.settings.title",
            "updates.settings.subtitle",
            "updates.settings.group",
            "updates.settings.new_versions",
            "updates.settings.check_on_startup",
            "updates.settings.status",
            "updates.settings.up_to_date",
            "updates.settings.available",
            "updates.settings.failed",
            "updates.settings.checking",
            "updates.settings.not_checked",
            "updates.settings.detail",
            "updates.settings.detail_unchecked",
            "updates.settings.check_now",
            "updates.settings.changelog",
            "updates.settings.show_whats_new",
            "updates.settings.open_whats_new",
            "updates.settings.show_welcome",
            "updates.settings.reset",
            "updates.settings.save",
            "updates.settings.saved",
            "updates.settings.save_error",
            "updates.settings.usage_heading",
            "updates.settings.usage_level",
            "updates.settings.level_off",
            "updates.settings.level_minimal",
            "updates.settings.level_basic",
            "updates.settings.level_off_display",
            "updates.settings.level_minimal_display",
            "updates.settings.level_basic_display",
            "updates.settings.level_hint",
            "updates.settings.what_is_sent",
            "updates.settings.collected_build",
            "updates.settings.collected_language",
            "updates.settings.collected_counts",
            "updates.settings.collected_header",
            "updates.settings.collected_minimal",
            "updates.settings.collected_basic",
            "updates.settings.counts_not_instrumented",
            "updates.settings.install_id",
            "updates.settings.install_id_hint",
            "updates.settings.reset_install_id",
            "updates.settings.reset_install_id_title",
            "updates.settings.reset_install_id_body",
            "updates.settings.reports",
            "updates.settings.no_report_sent_yet",
            "updates.settings.checking_last_report",
            "updates.settings.delivery_unavailable",
            "updates.settings.report_load_failed",
            "updates.settings.last_sent",
            "updates.settings.view_report",
            "updates.settings.open_in_audit",
            "updates.settings.reports_audit_note",
            "updates.settings.telemetry_save_failed",
            "updates.settings.telemetry_reset_failed",
            "updates.ago.just_now",
            "updates.ago.minutes",
            "updates.ago.hours",
            "updates.ago.days",
        ] {
            for locale in ["en", "es", "ko", "zh_Hans"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(!value.is_empty(), "{key} resolved empty for {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "{key} missing in {locale}"
                );
                // Version-format keys are shared across locales; prose keys
                // must be translated.
                let is_prose = key.contains("usage_")
                    || key.contains("level_")
                    || key.contains("collected_")
                    || key.contains("install_")
                    || key.contains("reset_install")
                    || key.contains("report")
                    || key.contains("telemetry_")
                    || key.contains("delivery_")
                    || key.starts_with("updates.settings.what_is_sent");
                if locale != "en" && is_prose {
                    assert_ne!(
                        value,
                        dbflux_i18n::t!(key, locale = "en"),
                        "English fallback for {key} in {locale}"
                    );
                }
            }
        }
    }
}
