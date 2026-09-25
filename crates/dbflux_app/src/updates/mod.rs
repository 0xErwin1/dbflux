//! Update checks, the bundled changelog, and first-run bookkeeping.
//!
//! Everything here is GPUI-free: the parsing, version selection and startup
//! decisions are pure functions, and the network request lives in
//! [`check`] so the UI layer only decides when to run it and how to show the
//! outcome.

pub mod changelog;
pub mod check;
pub mod install_source;
pub mod release;
pub mod settings;
pub mod startup;

pub use changelog::{
    ChangelogEntry, ChangelogRelease, ChangelogSection, ReleaseHeading, SectionKind,
};
pub use check::{
    CheckedAgo, UpdateCheckError, UpdateCheckOutcome, UpdateCheckState, fetch_available_update,
};
pub use install_source::{InstallSource, PackageManager};
pub use release::{AvailableUpdate, GithubRelease};
pub use settings::{UpdateSettings, load_update_settings, save_update_settings};
pub use startup::{StartupDialog, record_startup, startup_dialog};

/// `owner/name` of the GitHub repository releases are published to.
pub const REPOSITORY: &str = "0xErwin1/dbflux";

/// Human-facing page listing every release in the repository changelog.
pub const FULL_CHANGELOG_URL: &str = "https://github.com/0xErwin1/dbflux/blob/main/CHANGELOG.md";

/// Version of the running build, as stamped by CI before compiling.
pub fn current_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Parses a version string, tolerating a leading `v` as used by release tags.
pub fn parse_version(value: &str) -> Option<semver::Version> {
    let trimmed = value.trim();
    let without_prefix = trimmed.strip_prefix('v').unwrap_or(trimmed);

    semver::Version::parse(without_prefix).ok()
}

/// The version as users read it: `MAJOR.MINOR.PATCH` plus any pre-release
/// label, without build metadata such as the nightly commit hash.
pub fn display_version(value: &str) -> String {
    match parse_version(value) {
        Some(version) if version.pre.is_empty() => {
            format!("{}.{}.{}", version.major, version.minor, version.patch)
        }
        Some(version) => format!(
            "{}.{}.{}-{}",
            version.major, version.minor, version.patch, version.pre
        ),
        None => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_version_accepts_tags_and_rejects_rolling_names() {
        assert_eq!(parse_version("v0.8.1"), Some(semver::Version::new(0, 8, 1)));
        assert_eq!(
            parse_version(" 0.8.1 "),
            Some(semver::Version::new(0, 8, 1))
        );
        assert!(parse_version("0.8.0-nightly+abc1234").is_some());
        assert_eq!(parse_version("nightly"), None);
        assert_eq!(parse_version(""), None);
    }

    #[test]
    fn display_version_drops_build_metadata_only() {
        assert_eq!(display_version("0.8.0"), "0.8.0");
        assert_eq!(display_version("0.8.0-rc.2"), "0.8.0-rc.2");
        assert_eq!(display_version("0.8.0-nightly+abc1234"), "0.8.0-nightly");
        assert_eq!(display_version("garbage"), "garbage");
    }
}
