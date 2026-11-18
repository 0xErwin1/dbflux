//! What the listing says about its rows and its footer: the rotation chip, the
//! short change time and the counts of what is loaded.

use super::listing::PathMode;
use dbflux_components::primitives::BadgeTone;
use dbflux_core::SecretRotation;
use dbflux_core::chrono::{DateTime, Datelike as _, Local, Timelike as _, Utc};

const MONTH_KEYS: [&str; 12] = [
    "document.secret_store.date.months.jan",
    "document.secret_store.date.months.feb",
    "document.secret_store.date.months.mar",
    "document.secret_store.date.months.apr",
    "document.secret_store.date.months.may",
    "document.secret_store.date.months.jun",
    "document.secret_store.date.months.jul",
    "document.secret_store.date.months.aug",
    "document.secret_store.date.months.sep",
    "document.secret_store.date.months.oct",
    "document.secret_store.date.months.nov",
    "document.secret_store.date.months.dec",
];

/// The text and tone of a rotation cell.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct RotationChip {
    pub label: String,
    pub tone: BadgeTone,
}

/// Enabled rotation is a success chip with the schedule in words ("Every 30
/// days"; a cron expression is too long for the column and reads "On a
/// schedule"); anything else is the neutral "Off".
pub(super) fn rotation_chip(rotation: Option<&SecretRotation>) -> RotationChip {
    let Some(rotation) = rotation.filter(|rotation| rotation.enabled) else {
        return RotationChip {
            label: dbflux_i18n::t!("document.secret_store.rotation.off"),
            tone: BadgeTone::Neutral,
        };
    };

    let is_cron = rotation
        .schedule
        .as_deref()
        .is_some_and(|schedule| schedule.trim().starts_with("cron("));

    let label = if is_cron {
        dbflux_i18n::t!("document.secret_store.rotation.on_schedule")
    } else {
        super::render::rotation_label(Some(rotation))
    };

    RotationChip {
        label,
        tone: BadgeTone::Success,
    }
}

/// "Oct 1, 03:12", with the year ("Oct 1, 2025, 03:12") when `time` is not in
/// `current_year`.
pub(super) fn short_time_label(time: DateTime<Local>, current_year: i32) -> String {
    let month = MONTH_KEYS
        .get(time.month0() as usize)
        .map(|key| dbflux_i18n::t!(key))
        .unwrap_or_default();
    let day = time.day();
    let clock = format!("{:02}:{:02}", time.hour(), time.minute());

    if time.year() == current_year {
        dbflux_i18n::t!(
            "document.secret_store.date.short",
            month = month.as_str(),
            day = day,
            time = clock.as_str()
        )
    } else {
        dbflux_i18n::t!(
            "document.secret_store.date.short_with_year",
            month = month.as_str(),
            day = day,
            year = time.year(),
            time = clock.as_str()
        )
    }
}

/// The listing's change time in local time, or the placeholder.
pub(super) fn format_short_time(time: Option<DateTime<Utc>>) -> String {
    let Some(time) = time else {
        return super::render::UNKNOWN.to_string();
    };

    let local = time.with_timezone(&Local);
    short_time_label(local, Local::now().year())
}

/// What a footer says about the loaded rows. A hierarchical store counts
/// paths and parameters under the listed path ("2 paths · 7 parameters under
/// /app/prod/shop/"; a flat listing has no paths to count); any other store
/// counts secrets.
pub(super) fn loaded_summary(
    hierarchical: bool,
    mode: PathMode,
    path: Option<&str>,
    path_count: usize,
    entry_count: usize,
) -> String {
    if !hierarchical {
        return super::render::loaded_label(entry_count);
    }

    let plural = |count: usize, one: &str, many: &str| {
        dbflux_i18n::t!(if count == 1 { one } else { many }, count = count)
    };

    let parameters = plural(
        entry_count,
        "document.secret_store.footer.parameters.one",
        "document.secret_store.footer.parameters.many",
    );
    let path = path.unwrap_or("/");

    match mode {
        PathMode::Flat => dbflux_i18n::t!(
            "document.secret_store.footer.flat_summary",
            parameters = parameters.as_str(),
            path = path
        ),
        PathMode::Tree => {
            let paths = plural(
                path_count,
                "document.secret_store.footer.paths.one",
                "document.secret_store.footer.paths.many",
            );

            dbflux_i18n::t!(
                "document.secret_store.footer.tree_summary",
                paths = paths.as_str(),
                parameters = parameters.as_str(),
                path = path
            )
        }
    }
}
