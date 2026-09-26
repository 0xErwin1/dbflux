//! GitHub release payloads and the per-channel "is there a newer build" rules.
//!
//! The rules follow the tag scheme in `docs/RELEASE.md`:
//!
//! - stable (`vX.Y.Z`, published): the newest published release whose tag is a
//!   plain `vX.Y.Z`.
//! - rc (`vX.Y.Z-rc.N`, prerelease): the newest of every `vX.Y.Z` and
//!   `vX.Y.Z-rc.N` tag, so an rc build also learns about the stable release it
//!   leads to. `nightly` and the retired `-dev.N` tags are ignored.
//! - nightly (rolling `nightly` tag, prerelease replaced on every run): the tag
//!   never changes, so the build is compared by commit. The nightly version is
//!   `X.Y.Z-nightly+<short-sha>` and the release body carries
//!   ``Commit: `<short-sha>` ``; a different commit means a newer nightly.

use std::cmp::Ordering;

use dbflux_core::ReleaseChannel;
use semver::Version;
use serde_json::Value;

use super::{REPOSITORY, parse_version};

/// The subset of a GitHub release object the update check reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GithubRelease {
    pub tag_name: String,
    pub html_url: String,
    pub prerelease: bool,
    pub draft: bool,
    pub body: Option<String>,
}

/// A release newer than the running build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvailableUpdate {
    /// What the UI shows and what "Skip" stores: the version (`0.8.1`,
    /// `0.8.1-rc.2`) or, for nightly, `nightly <short-sha>`.
    pub label: String,
    /// Release page with the downloadable assets.
    pub download_url: String,
    /// Where the notes for this release are read.
    pub notes_url: String,
}

impl GithubRelease {
    fn from_value(value: &Value) -> Option<Self> {
        let tag_name = value.get("tag_name")?.as_str()?.to_string();
        let html_url = value.get("html_url")?.as_str()?.to_string();

        Some(Self {
            tag_name,
            html_url,
            prerelease: value
                .get("prerelease")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            draft: value.get("draft").and_then(Value::as_bool).unwrap_or(false),
            body: value
                .get("body")
                .and_then(Value::as_str)
                .map(str::to_string),
        })
    }
}

/// Parses the array returned by `GET /repos/{repo}/releases`. Entries missing
/// a tag or page URL are skipped.
pub fn parse_release_list(json: &str) -> Result<Vec<GithubRelease>, String> {
    let value: Value = serde_json::from_str(json).map_err(|error| error.to_string())?;
    let items = value
        .as_array()
        .ok_or_else(|| "release list is not a JSON array".to_string())?;

    Ok(items.iter().filter_map(GithubRelease::from_value).collect())
}

/// Parses the object returned by `GET /repos/{repo}/releases/tags/{tag}`.
pub fn parse_single_release(json: &str) -> Result<GithubRelease, String> {
    let value: Value = serde_json::from_str(json).map_err(|error| error.to_string())?;

    GithubRelease::from_value(&value)
        .ok_or_else(|| "release object has no tag_name or html_url".to_string())
}

/// Picks the newest stable or rc release that is newer than `current`, or
/// `None` when the running build is already the newest one.
pub fn select_versioned_update(
    channel: ReleaseChannel,
    current: &Version,
    releases: &[GithubRelease],
) -> Option<AvailableUpdate> {
    let newest = releases
        .iter()
        .filter(|release| !release.draft)
        .filter_map(|release| {
            let version = parse_version(&release.tag_name)?;
            accepts_version(channel, &version, release.prerelease).then_some((version, release))
        })
        .max_by(|(left, _), (right, _)| left.cmp_precedence(right))?;

    let (version, release) = newest;
    if version.cmp_precedence(current) != Ordering::Greater {
        return None;
    }

    Some(AvailableUpdate {
        label: version.to_string(),
        download_url: release.html_url.clone(),
        notes_url: tagged_changelog_url(&release.tag_name),
    })
}

/// Whether a release tag is a candidate on `channel`.
fn accepts_version(channel: ReleaseChannel, version: &Version, prerelease: bool) -> bool {
    if !version.build.is_empty() {
        return false;
    }

    match channel {
        ReleaseChannel::Stable => version.pre.is_empty() && !prerelease,
        ReleaseChannel::Rc => version.pre.is_empty() || version.pre.as_str().starts_with("rc."),
        ReleaseChannel::Nightly => false,
    }
}

/// Compares the rolling nightly release against the running nightly build.
///
/// Returns `None` when either commit is unknown (a local build without the
/// `+<sha>` suffix, or a release body without the commit line) or when both
/// name the same commit. Short hashes of different lengths match by prefix.
pub fn select_nightly_update(
    current: &Version,
    release: &GithubRelease,
) -> Option<AvailableUpdate> {
    if release.draft {
        return None;
    }

    let running_commit = current.build.as_str();
    if running_commit.is_empty() {
        return None;
    }

    let published_commit = nightly_commit(release.body.as_deref()?)?;
    if running_commit.starts_with(published_commit) || published_commit.starts_with(running_commit)
    {
        return None;
    }

    Some(AvailableUpdate {
        label: format!("nightly {published_commit}"),
        download_url: release.html_url.clone(),
        notes_url: release.html_url.clone(),
    })
}

/// Extracts the commit from the nightly body line ``Commit: `abc1234` ``.
pub fn nightly_commit(body: &str) -> Option<&str> {
    let after_label = &body[body.find("Commit:")? + "Commit:".len()..];
    let after_tick = after_label.trim_start().strip_prefix('`')?;
    let commit = &after_tick[..after_tick.find('`')?];

    let is_hash = !commit.is_empty()
        && commit
            .chars()
            .all(|character| character.is_ascii_hexdigit());
    is_hash.then_some(commit)
}

/// The repository changelog as of `tag`, where each stable release has its
/// own section.
fn tagged_changelog_url(tag: &str) -> String {
    format!("https://github.com/{REPOSITORY}/blob/{tag}/CHANGELOG.md")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn release(tag: &str, prerelease: bool) -> GithubRelease {
        GithubRelease {
            tag_name: tag.to_string(),
            html_url: format!("https://github.com/{REPOSITORY}/releases/tag/{tag}"),
            prerelease,
            draft: false,
            body: None,
        }
    }

    fn version(text: &str) -> Version {
        Version::parse(text).expect("valid version")
    }

    fn published() -> Vec<GithubRelease> {
        vec![
            release("nightly", true),
            release("v0.9.0-rc.1", true),
            release("v0.8.1", false),
            release("v0.8.0", false),
            release("v0.8.0-rc.3", true),
            release("v0.6.0-dev.10", true),
        ]
    }

    #[test]
    fn stable_picks_the_newest_published_release() {
        let update =
            select_versioned_update(ReleaseChannel::Stable, &version("0.8.0"), &published())
                .expect("0.8.1 is newer");

        assert_eq!(update.label, "0.8.1");
        assert_eq!(
            update.download_url,
            format!("https://github.com/{REPOSITORY}/releases/tag/v0.8.1")
        );
        assert_eq!(
            update.notes_url,
            format!("https://github.com/{REPOSITORY}/blob/v0.8.1/CHANGELOG.md")
        );
    }

    #[test]
    fn stable_is_up_to_date_on_the_newest_release_and_ignores_prereleases() {
        assert_eq!(
            select_versioned_update(ReleaseChannel::Stable, &version("0.8.1"), &published()),
            None
        );

        let mislabelled = vec![release("v0.8.2", true)];
        assert_eq!(
            select_versioned_update(ReleaseChannel::Stable, &version("0.8.1"), &mislabelled),
            None
        );
    }

    #[test]
    fn rc_sees_newer_rcs_and_stable_but_not_nightly_or_dev() {
        let from_rc =
            select_versioned_update(ReleaseChannel::Rc, &version("0.9.0-rc.0"), &published())
                .expect("rc.1 is newer");
        assert_eq!(from_rc.label, "0.9.0-rc.1");

        let stable_after_rc = vec![release("v0.9.0", false), release("v0.9.0-rc.1", true)];
        let to_stable =
            select_versioned_update(ReleaseChannel::Rc, &version("0.9.0-rc.1"), &stable_after_rc)
                .expect("stable is newer than its rc");
        assert_eq!(to_stable.label, "0.9.0");

        let only_dev = vec![release("v0.9.0-dev.4", true), release("nightly", true)];
        assert_eq!(
            select_versioned_update(ReleaseChannel::Rc, &version("0.9.0-rc.0"), &only_dev),
            None
        );
    }

    #[test]
    fn drafts_and_build_metadata_tags_are_ignored() {
        let mut draft = release("v1.0.0", false);
        draft.draft = true;
        let with_build = release("v1.0.0+local", false);

        assert_eq!(
            select_versioned_update(
                ReleaseChannel::Stable,
                &version("0.8.0"),
                &[draft, with_build]
            ),
            None
        );
    }

    #[test]
    fn nightly_compares_commits_by_prefix() {
        let mut rolling = release("nightly", true);
        rolling.body = Some(
            "> **Nightly build** — built automatically.\n>\n> Commit: `abc1234` — 2026-09-20\n"
                .to_string(),
        );

        assert_eq!(
            select_nightly_update(&version("0.9.0-nightly+abc1234"), &rolling),
            None
        );
        assert_eq!(
            select_nightly_update(&version("0.9.0-nightly+abc12"), &rolling),
            None
        );

        let update = select_nightly_update(&version("0.9.0-nightly+def5678"), &rolling)
            .expect("different commit");
        assert_eq!(update.label, "nightly abc1234");
        assert_eq!(update.notes_url, rolling.html_url);

        assert_eq!(
            select_nightly_update(&version("0.9.0-nightly"), &rolling),
            None
        );

        rolling.body = Some("no commit line".to_string());
        assert_eq!(
            select_nightly_update(&version("0.9.0-nightly+def5678"), &rolling),
            None
        );
    }

    #[test]
    fn nightly_commit_rejects_non_hash_values() {
        assert_eq!(nightly_commit("Commit: `abc1234`"), Some("abc1234"));
        assert_eq!(nightly_commit("Commit: `not-a-hash`"), None);
        assert_eq!(nightly_commit("Commit: abc1234"), None);
        assert_eq!(nightly_commit("Commit: ``"), None);
    }

    #[test]
    fn parses_github_payloads() {
        let list = r#"[
            {"tag_name": "v0.8.1", "html_url": "https://example.com/a", "prerelease": false, "draft": false, "body": "notes"},
            {"tag_name": "v0.8.0", "prerelease": false},
            {"tag_name": "nightly", "html_url": "https://example.com/n", "prerelease": true}
        ]"#;

        let releases = parse_release_list(list).expect("valid list");
        assert_eq!(releases.len(), 2);
        assert_eq!(releases[0].body.as_deref(), Some("notes"));
        assert!(releases[1].prerelease);
        assert!(!releases[1].draft);

        assert!(parse_release_list("{}").is_err());
        assert!(parse_release_list("not json").is_err());

        let single = parse_single_release(
            r#"{"tag_name": "nightly", "html_url": "https://example.com/n", "prerelease": true, "body": "Commit: `abc1234`"}"#,
        )
        .expect("valid release");
        assert_eq!(single.tag_name, "nightly");
        assert!(parse_single_release(r#"{"message": "Not Found"}"#).is_err());
    }
}
