//! Release channel detected from the compiled crate version.
//!
//! The CI release pipeline stamps the workspace version before building, so the
//! version embedded in the binary already encodes the channel: `-nightly` for
//! rolling nightly builds, `-rc.N` for release candidates, and a plain
//! `MAJOR.MINOR.PATCH` for stable. This module turns that single signal into the
//! channel-specific identity the runtime needs (window/app identifiers, display
//! name, and the on-disk database file).

/// The release channel a running build belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseChannel {
    Stable,
    Rc,
    Nightly,
}

impl ReleaseChannel {
    /// The channel of the currently running build, derived from its version.
    pub fn current() -> Self {
        Self::from_version(env!("CARGO_PKG_VERSION"))
    }

    /// Classifies a semver string into a channel.
    ///
    /// Nightly takes precedence over rc so a hypothetical `-rc.N-nightly+sha`
    /// still resolves to nightly. Anything without a recognized pre-release
    /// marker is treated as stable.
    pub fn from_version(version: &str) -> Self {
        if version.contains("-nightly") {
            Self::Nightly
        } else if version.contains("-rc") {
            Self::Rc
        } else {
            Self::Stable
        }
    }

    /// Platform application identifier (GPUI `app_id`: Wayland app id / X11
    /// `WM_CLASS`). Nightly gets a distinct id so it coexists with stable
    /// instead of sharing its taskbar entry and icon association.
    pub fn app_id(self) -> &'static str {
        match self {
            Self::Nightly => "dbflux-nightly",
            Self::Stable | Self::Rc => "dbflux",
        }
    }

    /// Human-facing application name used for window titles and bundle metadata.
    pub fn display_name(self) -> &'static str {
        match self {
            Self::Nightly => "DBFlux Nightly",
            Self::Stable | Self::Rc => "DBFlux",
        }
    }

    /// File name of the unified SQLite database inside the data directory.
    ///
    /// Nightly uses a separate database so a migration that breaks on a
    /// pre-release build cannot corrupt the stable database of a user who runs
    /// both channels side by side.
    pub fn db_file_name(self) -> &'static str {
        match self {
            Self::Nightly => "dbflux-nightly.db",
            Self::Stable | Self::Rc => "dbflux.db",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_versions() {
        assert_eq!(
            ReleaseChannel::from_version("0.7.0"),
            ReleaseChannel::Stable
        );
        assert_eq!(
            ReleaseChannel::from_version("0.7.0-rc.2"),
            ReleaseChannel::Rc
        );
        assert_eq!(
            ReleaseChannel::from_version("0.7.0-nightly+abc1234"),
            ReleaseChannel::Nightly
        );
    }

    #[test]
    fn nightly_has_distinct_identity() {
        let nightly = ReleaseChannel::Nightly;
        assert_ne!(nightly.app_id(), ReleaseChannel::Stable.app_id());
        assert_ne!(
            nightly.db_file_name(),
            ReleaseChannel::Stable.db_file_name()
        );
    }

    #[test]
    fn rc_shares_stable_identity() {
        assert_eq!(ReleaseChannel::Rc.app_id(), ReleaseChannel::Stable.app_id());
        assert_eq!(
            ReleaseChannel::Rc.db_file_name(),
            ReleaseChannel::Stable.db_file_name()
        );
    }
}
