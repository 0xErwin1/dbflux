//! The network side of the update check.
//!
//! One unauthenticated `GET` to the public GitHub releases API. The request
//! carries only a `User-Agent` naming the app and its version, which GitHub
//! requires; nothing about the user, their machine or their data is sent.
//! Callers run [`fetch_available_update`] on a background executor: it
//! blocks for at most [`REQUEST_TIMEOUT`].

use std::time::Duration;

use chrono::{DateTime, Utc};
use dbflux_core::ReleaseChannel;

use super::release::{
    AvailableUpdate, parse_release_list, parse_single_release, select_nightly_update,
    select_versioned_update,
};
use super::{REPOSITORY, parse_version};

pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// How many releases the stable and rc checks inspect. Newer releases come
/// first in the API response, so a page this size always holds the newest
/// stable and rc tags.
const RELEASES_PAGE_SIZE: u32 = 30;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateCheckOutcome {
    UpToDate,
    Available(AvailableUpdate),
    Failed,
}

/// The result of the most recent check in this session. Not persisted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UpdateCheckState {
    pub outcome: Option<UpdateCheckOutcome>,
    pub checked_at: Option<DateTime<Utc>>,
    pub in_progress: bool,
}

impl UpdateCheckState {
    pub fn available_update(&self) -> Option<&AvailableUpdate> {
        match &self.outcome {
            Some(UpdateCheckOutcome::Available(update)) => Some(update),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum UpdateCheckError {
    /// The running version is not semver, so nothing can be compared.
    UnparseableVersion(String),
    Request(reqwest::Error),
    Status(reqwest::StatusCode),
    Parse(String),
}

impl std::fmt::Display for UpdateCheckError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnparseableVersion(version) => {
                write!(formatter, "running version {version:?} is not semver")
            }
            Self::Request(error) => write!(formatter, "request failed: {error}"),
            Self::Status(status) => write!(formatter, "GitHub answered {status}"),
            Self::Parse(error) => write!(formatter, "unexpected response: {error}"),
        }
    }
}

impl std::error::Error for UpdateCheckError {}

/// Asks GitHub for the newest release on `channel` and compares it with
/// `current_version`. `Ok(None)` means the running build is the newest.
pub fn fetch_available_update(
    channel: ReleaseChannel,
    current_version: &str,
) -> Result<Option<AvailableUpdate>, UpdateCheckError> {
    let current = parse_version(current_version)
        .ok_or_else(|| UpdateCheckError::UnparseableVersion(current_version.to_string()))?;

    let client = reqwest::blocking::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .user_agent(concat!("DBFlux/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(UpdateCheckError::Request)?;

    match channel {
        ReleaseChannel::Nightly => {
            let url = format!("https://api.github.com/repos/{REPOSITORY}/releases/tags/nightly");
            let body = get(&client, &url)?;
            let release = parse_single_release(&body).map_err(UpdateCheckError::Parse)?;

            Ok(select_nightly_update(&current, &release))
        }
        ReleaseChannel::Stable | ReleaseChannel::Rc => {
            let url = format!(
                "https://api.github.com/repos/{REPOSITORY}/releases?per_page={RELEASES_PAGE_SIZE}"
            );
            let body = get(&client, &url)?;
            let releases = parse_release_list(&body).map_err(UpdateCheckError::Parse)?;

            Ok(select_versioned_update(channel, &current, &releases))
        }
    }
}

fn get(client: &reqwest::blocking::Client, url: &str) -> Result<String, UpdateCheckError> {
    let response = client
        .get(url)
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .send()
        .map_err(UpdateCheckError::Request)?;

    let status = response.status();
    if !status.is_success() {
        return Err(UpdateCheckError::Status(status));
    }

    response.text().map_err(UpdateCheckError::Request)
}

/// How long ago a check ran, in the unit the settings status line shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckedAgo {
    JustNow,
    Minutes(i64),
    Hours(i64),
    Days(i64),
}

impl CheckedAgo {
    pub fn between(checked_at: DateTime<Utc>, now: DateTime<Utc>) -> Self {
        let elapsed = now.signed_duration_since(checked_at);

        if elapsed.num_minutes() < 1 {
            Self::JustNow
        } else if elapsed.num_hours() < 1 {
            Self::Minutes(elapsed.num_minutes())
        } else if elapsed.num_days() < 1 {
            Self::Hours(elapsed.num_hours())
        } else {
            Self::Days(elapsed.num_days())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;

    #[test]
    fn checked_ago_buckets() {
        let now = Utc::now();

        assert_eq!(CheckedAgo::between(now, now), CheckedAgo::JustNow);
        assert_eq!(
            CheckedAgo::between(now + TimeDelta::seconds(30), now),
            CheckedAgo::JustNow
        );
        assert_eq!(
            CheckedAgo::between(now - TimeDelta::minutes(2), now),
            CheckedAgo::Minutes(2)
        );
        assert_eq!(
            CheckedAgo::between(now - TimeDelta::minutes(125), now),
            CheckedAgo::Hours(2)
        );
        assert_eq!(
            CheckedAgo::between(now - TimeDelta::hours(49), now),
            CheckedAgo::Days(2)
        );
    }

    #[test]
    fn unparseable_running_version_fails_before_any_request() {
        let result = fetch_available_update(ReleaseChannel::Stable, "not-a-version");

        assert!(matches!(
            result,
            Err(UpdateCheckError::UnparseableVersion(_))
        ));
    }

    #[test]
    fn available_update_reads_only_the_available_outcome() {
        let update = AvailableUpdate {
            label: "0.8.1".to_string(),
            download_url: "https://example.com/d".to_string(),
            notes_url: "https://example.com/n".to_string(),
        };
        let state = UpdateCheckState {
            outcome: Some(UpdateCheckOutcome::Available(update.clone())),
            checked_at: None,
            in_progress: false,
        };

        assert_eq!(state.available_update(), Some(&update));
        assert_eq!(UpdateCheckState::default().available_update(), None);
    }
}
