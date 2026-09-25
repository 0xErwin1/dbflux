//! Expiry editor for the open key: Never, In (a duration) or At (a local
//! date and time), with the resulting absolute time shown before applying.
//!
//! The parsing and the date math are pure functions over an explicit `now`
//! so they can be tested without a clock or a window.

use super::metadata::{KeyExpiry, format_ttl};
use chrono::{DateTime, Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
use dbflux_components::controls::{InputEvent, InputState};
use dbflux_core::{DbError, KeyExpireRequest, KeyPersistRequest, TaskKind};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error_async};
use gpui::*;
use std::time::Instant;

/// Duration presets offered next to the duration field.
pub(super) const EXPIRY_PRESETS: [&str; 4] = ["1h", "24h", "7d", "30d"];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ExpiryMode {
    Never,
    In,
    At,
}

impl ExpiryMode {
    pub(super) const ALL: [ExpiryMode; 3] = [ExpiryMode::Never, ExpiryMode::In, ExpiryMode::At];

    pub(super) fn id(self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::In => "in",
            Self::At => "at",
        }
    }

    pub(super) fn from_id(id: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|mode| mode.id() == id)
    }

    pub(super) fn label(self) -> String {
        match self {
            Self::Never => dbflux_i18n::t!("document.key_value.expiry.mode.never"),
            Self::In => dbflux_i18n::t!("document.key_value.expiry.mode.in"),
            Self::At => dbflux_i18n::t!("document.key_value.expiry.mode.at"),
        }
    }
}

/// What applying the editor does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ExpiryPlan {
    /// Remove the expiry (`PERSIST`).
    Persist,
    /// Expire this many seconds from now (`EXPIRE`).
    ExpireIn(u64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ExpiryInputError {
    EmptyDuration,
    InvalidDuration,
    InvalidDateTime,
    NotInFuture,
}

impl ExpiryInputError {
    pub(super) fn message(&self) -> String {
        match self {
            Self::EmptyDuration => dbflux_i18n::t!("document.key_value.expiry.error.empty"),
            Self::InvalidDuration => dbflux_i18n::t!("document.key_value.expiry.error.duration"),
            Self::InvalidDateTime => dbflux_i18n::t!("document.key_value.expiry.error.date_time"),
            Self::NotInFuture => dbflux_i18n::t!("document.key_value.expiry.error.past"),
        }
    }
}

/// Parses a duration such as `7d`, `24h`, `90m`, `1h30m`, `2d 4h` or a bare
/// number of seconds. Units: `s`, `m`, `h`, `d`, `w`. Returns `None` for
/// anything else and for a zero duration.
pub(super) fn parse_duration(text: &str) -> Option<u64> {
    let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();

    if compact.is_empty() {
        return None;
    }

    if let Ok(seconds) = compact.parse::<u64>() {
        return (seconds > 0).then_some(seconds);
    }

    let mut total: u64 = 0;
    let mut number = String::new();

    for character in compact.chars() {
        if character.is_ascii_digit() {
            number.push(character);
            continue;
        }

        let unit_seconds: u64 = match character.to_ascii_lowercase() {
            's' => 1,
            'm' => 60,
            'h' => 3_600,
            'd' => 86_400,
            'w' => 604_800,
            _ => return None,
        };

        let amount: u64 = number.parse().ok()?;
        total = total.checked_add(amount.checked_mul(unit_seconds)?)?;
        number.clear();
    }

    if !number.is_empty() || total == 0 {
        return None;
    }

    Some(total)
}

/// Compact duration for the field (`3h12m`, `7d`, `45s`), the inverse of
/// [`parse_duration`] up to a minute of precision.
pub(super) fn compact_duration(seconds: u64) -> String {
    if seconds < 60 {
        return format!("{seconds}s");
    }

    let days = seconds / 86_400;
    let hours = (seconds % 86_400) / 3_600;
    let minutes = (seconds % 3_600) / 60;

    let mut text = String::new();
    if days > 0 {
        text.push_str(&format!("{days}d"));
    }
    if hours > 0 {
        text.push_str(&format!("{hours}h"));
    }
    if minutes > 0 {
        text.push_str(&format!("{minutes}m"));
    }
    text
}

/// Parses a local date and time: `2026-09-30 17:31`, `2026-09-30T17:31:05`
/// or a bare date (midnight).
pub(super) fn parse_local_date_time<Tz: TimeZone>(text: &str, zone: &Tz) -> Option<DateTime<Tz>> {
    let trimmed = text.trim();

    let naive = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    ]
    .iter()
    .find_map(|format| NaiveDateTime::parse_from_str(trimmed, format).ok())
    .or_else(|| {
        NaiveDate::parse_from_str(trimmed, "%Y-%m-%d")
            .ok()
            .map(|date| date.and_time(NaiveTime::MIN))
    })?;

    zone.from_local_datetime(&naive).earliest()
}

/// Resolves the editor's fields into what applying it would do.
pub(super) fn plan_expiry<Tz: TimeZone>(
    mode: ExpiryMode,
    duration_text: &str,
    at_text: &str,
    now: &DateTime<Tz>,
) -> Result<ExpiryPlan, ExpiryInputError> {
    match mode {
        ExpiryMode::Never => Ok(ExpiryPlan::Persist),
        ExpiryMode::In => {
            if duration_text.trim().is_empty() {
                return Err(ExpiryInputError::EmptyDuration);
            }

            parse_duration(duration_text)
                .map(ExpiryPlan::ExpireIn)
                .ok_or(ExpiryInputError::InvalidDuration)
        }
        ExpiryMode::At => {
            let at = parse_local_date_time(at_text, &now.timezone())
                .ok_or(ExpiryInputError::InvalidDateTime)?;

            let seconds = (at.timestamp() - now.timestamp()).max(0) as u64;
            if seconds == 0 {
                return Err(ExpiryInputError::NotInFuture);
            }

            Ok(ExpiryPlan::ExpireIn(seconds))
        }
    }
}

/// The confirmation line under the fields: when the key will expire, in
/// local time, and the command that will run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ExpiryPreview {
    /// `Wed 30 Sep, 17:31`; `None` for "never expires".
    pub when: Option<String>,
    /// `EXPIRE 604800` or `PERSIST`.
    pub command: String,
}

pub(super) fn expiry_preview<Tz: TimeZone>(plan: ExpiryPlan, now: &DateTime<Tz>) -> ExpiryPreview
where
    Tz::Offset: std::fmt::Display,
{
    match plan {
        ExpiryPlan::Persist => ExpiryPreview {
            when: None,
            command: "PERSIST".to_string(),
        },
        ExpiryPlan::ExpireIn(seconds) => {
            let at = now.clone() + chrono::Duration::seconds(seconds.min(i64::MAX as u64) as i64);

            ExpiryPreview {
                when: Some(at.format("%a %-d %b, %H:%M").to_string()),
                command: format!("EXPIRE {seconds}"),
            }
        }
    }
}

/// State of the open expiry popover.
pub(super) struct ExpiryEditor {
    pub key: String,
    pub mode: ExpiryMode,
    pub duration_input: Entity<InputState>,
    pub at_input: Entity<InputState>,
    pub error: Option<String>,
    _subscriptions: Vec<Subscription>,
}

impl ExpiryEditor {
    /// Current plan and preview, or the input error to show instead.
    pub(super) fn resolve(
        &self,
        cx: &App,
    ) -> Result<(ExpiryPlan, ExpiryPreview), ExpiryInputError> {
        let now = Local::now();
        let duration_text = self.duration_input.read(cx).value().to_string();
        let at_text = self.at_input.read(cx).value().to_string();

        let plan = plan_expiry(self.mode, &duration_text, &at_text, &now)?;
        Ok((plan, expiry_preview(plan, &now)))
    }
}

impl super::KeyValueDocument {
    /// Opens the expiry editor for the selected key (`t`).
    pub(super) fn open_expiry_editor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.supports_expiry(cx) {
            return;
        }

        let Some(key) = self.selected_key() else {
            return;
        };

        let remaining = self.selected_ttl_remaining();
        let initial_duration = remaining.map(compact_duration).unwrap_or_default();

        let duration_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "document.key_value.expiry.duration_placeholder"
            ));
            state.set_value(initial_duration, window, cx);
            state
        });

        let at_input = cx.new(|cx| {
            InputState::new(window, cx).placeholder(
                (Local::now() + chrono::Duration::days(1))
                    .format("%Y-%m-%d %H:%M")
                    .to_string(),
            )
        });

        let mut subscriptions = Vec::new();

        for input in [&duration_input, &at_input] {
            subscriptions.push(cx.subscribe_in(
                input,
                window,
                |this, _, event: &InputEvent, window, cx| match event {
                    InputEvent::PressEnter { .. } => this.apply_expiry_editor(window, cx),
                    InputEvent::Change => {
                        if let Some(editor) = this.expiry_editor.as_mut() {
                            editor.error = None;
                        }
                        cx.notify();
                    }
                    _ => {}
                },
            ));
        }

        duration_input.update(cx, |state, cx| state.focus(window, cx));

        self.expiry_editor = Some(ExpiryEditor {
            key,
            mode: ExpiryMode::In,
            duration_input,
            at_input,
            error: None,
            _subscriptions: subscriptions,
        });
        self.focus_mode = super::KeyValueFocusMode::TextInput;
        cx.notify();
    }

    pub(super) fn close_expiry_editor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.expiry_editor = None;
        self.focus_mode = super::KeyValueFocusMode::ValuePanel;
        self.focus_handle.focus(window, cx);
        cx.notify();
    }

    pub(super) fn set_expiry_mode(
        &mut self,
        mode: ExpiryMode,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(editor) = self.expiry_editor.as_mut() else {
            return;
        };

        editor.mode = mode;
        editor.error = None;

        match mode {
            ExpiryMode::In => editor
                .duration_input
                .update(cx, |state, cx| state.focus(window, cx)),
            ExpiryMode::At => editor
                .at_input
                .update(cx, |state, cx| state.focus(window, cx)),
            ExpiryMode::Never => self.focus_handle.focus(window, cx),
        }

        cx.notify();
    }

    pub(super) fn apply_expiry_preset(
        &mut self,
        preset: &'static str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(editor) = self.expiry_editor.as_mut() else {
            return;
        };

        editor.mode = ExpiryMode::In;
        editor.error = None;
        editor.duration_input.update(cx, |state, cx| {
            state.set_value(preset, window, cx);
            state.focus(window, cx);
        });
        cx.notify();
    }

    /// Runs `EXPIRE` or `PERSIST` for the editor's key and closes it.
    pub(super) fn apply_expiry_editor(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(editor) = self.expiry_editor.as_mut() else {
            return;
        };

        let plan = match editor.resolve(cx) {
            Ok((plan, _)) => plan,
            Err(error) => {
                editor.error = Some(error.message());
                cx.notify();
                return;
            }
        };

        let key = editor.key.clone();
        self.close_expiry_editor(window, cx);

        let Some(connection) = self.get_connection(cx) else {
            self.report_connection_inactive(cx);
            return;
        };

        let description = match plan {
            ExpiryPlan::Persist => {
                format!("PERSIST {}", dbflux_core::truncate_string_safe(&key, 60))
            }
            ExpiryPlan::ExpireIn(seconds) => format!(
                "EXPIRE {} {seconds}",
                dbflux_core::truncate_string_safe(&key, 60)
            ),
        };
        let (task_id, _cancel_token) =
            self.runner
                .start_mutation(TaskKind::KeyMutation, description, cx);

        let keyspace = self.keyspace_index();
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn({
                    let key = key.clone();
                    async move {
                        let api = connection.key_value_api().ok_or_else(|| {
                            DbError::NotSupported("Key-value API unavailable".to_string())
                        })?;

                        match plan {
                            ExpiryPlan::Persist => {
                                api.persist_key(&KeyPersistRequest { key, keyspace })
                            }
                            ExpiryPlan::ExpireIn(ttl_seconds) => {
                                api.expire_key(&KeyExpireRequest {
                                    key,
                                    ttl_seconds,
                                    keyspace,
                                })
                            }
                        }
                    }
                })
                .await;

            if let Err(error) = &result {
                report_error_async(
                    UserFacingError::new(ErrorKind::Driver, error.to_string()),
                    cx,
                );
            }

            cx.update(|cx| {
                entity.update(cx, |this, cx| {
                    match result {
                        Ok(_) => {
                            this.runner.complete_mutation(task_id, cx);
                            this.record_applied_expiry(&key, plan, cx);
                        }
                        Err(error) => {
                            this.runner.fail_mutation(task_id, error.to_string(), cx);
                        }
                    }

                    cx.notify();
                });
            });
        })
        .detach();
    }

    /// Reflects an applied expiry in the value header and the key list
    /// without another round trip.
    fn record_applied_expiry(&mut self, key: &str, plan: ExpiryPlan, cx: &mut Context<Self>) {
        let now = Instant::now();
        let ttl_seconds = match plan {
            ExpiryPlan::Persist => None,
            ExpiryPlan::ExpireIn(seconds) => Some(seconds as i64),
        };

        if let Some(cached) = self.key_metadata.get_mut(key) {
            cached.expiry = KeyExpiry::from_ttl_seconds(ttl_seconds, now);
        }

        if self.selected_key().as_deref() == Some(key)
            && let Some(value) = self.selected_value.as_mut()
        {
            value.entry.ttl_seconds = ttl_seconds;
            let entry = value.entry.clone();
            self.apply_ttl_from_entry(&entry, cx);
        }
    }

    /// Remaining TTL of the selected key in seconds, when it has one.
    pub(super) fn selected_ttl_remaining(&self) -> Option<u64> {
        match self.ttl_state {
            super::TtlState::Remaining { deadline } => {
                Some(deadline.saturating_duration_since(Instant::now()).as_secs())
            }
            _ => None,
        }
    }

    /// Relative TTL for the value header (`3h 12m`), or `None` when the key
    /// never expires.
    pub(super) fn selected_ttl_label(&self) -> Option<String> {
        self.selected_ttl_remaining()
            .map(|seconds| format_ttl(Some(seconds)))
    }

    pub(super) fn supports_expiry(&self, cx: &App) -> bool {
        self.get_connection_metadata_capabilities(cx)
            .is_some_and(|capabilities| {
                capabilities.contains(dbflux_core::DriverCapabilities::KV_TTL)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ExpiryInputError, ExpiryMode, ExpiryPlan, compact_duration, expiry_preview, parse_duration,
        plan_expiry,
    };
    use chrono::{DateTime, FixedOffset, TimeZone};

    fn fixed_now() -> DateTime<FixedOffset> {
        let zone = FixedOffset::east_opt(2 * 3600).expect("offset");
        zone.with_ymd_and_hms(2026, 9, 23, 17, 31, 0)
            .single()
            .expect("valid time")
    }

    #[test]
    fn durations_accept_units_combinations_and_bare_seconds() {
        assert_eq!(parse_duration("1h"), Some(3_600));
        assert_eq!(parse_duration("24h"), Some(86_400));
        assert_eq!(parse_duration("7d"), Some(604_800));
        assert_eq!(parse_duration("30d"), Some(2_592_000));
        assert_eq!(parse_duration("1h30m"), Some(5_400));
        assert_eq!(parse_duration("2d 4h"), Some(187_200));
        assert_eq!(parse_duration("90"), Some(90));
        assert_eq!(parse_duration("1w"), Some(604_800));
    }

    #[test]
    fn durations_reject_garbage_and_zero() {
        assert_eq!(parse_duration(""), None);
        assert_eq!(parse_duration("0"), None);
        assert_eq!(parse_duration("0h"), None);
        assert_eq!(parse_duration("5x"), None);
        assert_eq!(parse_duration("h"), None);
        assert_eq!(parse_duration("10m5"), None);
    }

    #[test]
    fn compact_duration_round_trips_through_the_parser() {
        for seconds in [45, 600, 3_600 + 12 * 60, 604_800, 90_061] {
            let compact = compact_duration(seconds);
            let parsed = parse_duration(&compact).expect("compact duration parses");

            assert_eq!(parsed / 60, seconds / 60, "{compact}");
        }

        assert_eq!(compact_duration(3 * 3_600 + 12 * 60), "3h12m");
    }

    #[test]
    fn plan_for_never_persists() {
        assert_eq!(
            plan_expiry(ExpiryMode::Never, "", "", &fixed_now()),
            Ok(ExpiryPlan::Persist)
        );
    }

    #[test]
    fn plan_for_in_uses_the_duration() {
        assert_eq!(
            plan_expiry(ExpiryMode::In, "7d", "", &fixed_now()),
            Ok(ExpiryPlan::ExpireIn(604_800))
        );
        assert_eq!(
            plan_expiry(ExpiryMode::In, " ", "", &fixed_now()),
            Err(ExpiryInputError::EmptyDuration)
        );
        assert_eq!(
            plan_expiry(ExpiryMode::In, "soon", "", &fixed_now()),
            Err(ExpiryInputError::InvalidDuration)
        );
    }

    #[test]
    fn plan_for_at_counts_seconds_from_now_in_local_time() {
        assert_eq!(
            plan_expiry(ExpiryMode::At, "", "2026-09-30 17:31", &fixed_now()),
            Ok(ExpiryPlan::ExpireIn(7 * 86_400))
        );
        assert_eq!(
            plan_expiry(ExpiryMode::At, "", "2026-09-24", &fixed_now()),
            Ok(ExpiryPlan::ExpireIn(6 * 3_600 + 29 * 60))
        );
    }

    #[test]
    fn plan_for_at_rejects_the_past_and_bad_input() {
        assert_eq!(
            plan_expiry(ExpiryMode::At, "", "2026-09-01 10:00", &fixed_now()),
            Err(ExpiryInputError::NotInFuture)
        );
        assert_eq!(
            plan_expiry(ExpiryMode::At, "", "next tuesday", &fixed_now()),
            Err(ExpiryInputError::InvalidDateTime)
        );
    }

    #[test]
    fn preview_shows_the_absolute_time_and_the_command() {
        let preview = expiry_preview(ExpiryPlan::ExpireIn(604_800), &fixed_now());

        assert_eq!(preview.when.as_deref(), Some("Wed 30 Sep, 17:31"));
        assert_eq!(preview.command, "EXPIRE 604800");

        let never = expiry_preview(ExpiryPlan::Persist, &fixed_now());
        assert_eq!(never.when, None);
        assert_eq!(never.command, "PERSIST");
    }

    #[test]
    fn expiry_modes_round_trip_their_ids() {
        for mode in ExpiryMode::ALL {
            assert_eq!(ExpiryMode::from_id(mode.id()), Some(mode));
        }
    }
}
