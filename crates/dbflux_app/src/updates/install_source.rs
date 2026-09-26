//! How the running binary was installed.
//!
//! Package-manager installs must be updated through that package manager, so
//! the update notice shows a rebuild hint instead of a download button. The
//! build-time `DBFLUX_INSTALL_SOURCE` variable, set by the packaging recipes,
//! is authoritative. Without it, only signals that cannot be mistaken are
//! used: the Flatpak sandbox sets `FLATPAK_ID`, and a Nix install runs from
//! `/nix/store`. An AUR install cannot be proven at runtime, so it is only
//! recognized through the build-time variable.

use std::path::Path;

/// Package managers whose installs are updated outside DBFlux.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackageManager {
    Nix,
    Aur,
    Flatpak,
}

impl PackageManager {
    /// Parses a `DBFLUX_INSTALL_SOURCE` value.
    pub fn from_build_value(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "nix" => Some(Self::Nix),
            "aur" => Some(Self::Aur),
            "flatpak" => Some(Self::Flatpak),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallSource {
    /// Downloaded from the release page or built from source by hand.
    Direct,
    PackageManager(PackageManager),
}

/// Decides the install source from the build-time value and runtime hints.
pub fn detect_install_source(
    build_value: Option<&str>,
    flatpak_id: Option<&str>,
    executable: Option<&Path>,
) -> InstallSource {
    if let Some(manager) = build_value.and_then(PackageManager::from_build_value) {
        return InstallSource::PackageManager(manager);
    }

    if flatpak_id.is_some_and(|id| !id.trim().is_empty()) {
        return InstallSource::PackageManager(PackageManager::Flatpak);
    }

    if executable.is_some_and(|path| path.starts_with("/nix/store")) {
        return InstallSource::PackageManager(PackageManager::Nix);
    }

    InstallSource::Direct
}

/// The install source of the running process.
pub fn current_install_source() -> InstallSource {
    let flatpak_id = std::env::var("FLATPAK_ID").ok();
    let executable = match std::env::current_exe() {
        Ok(path) => Some(path),
        Err(error) => {
            log::debug!("Could not resolve the executable path: {error}");
            None
        }
    };

    detect_install_source(
        option_env!("DBFLUX_INSTALL_SOURCE"),
        flatpak_id.as_deref(),
        executable.as_deref(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_time_value_wins_over_runtime_hints() {
        assert_eq!(
            detect_install_source(Some("aur"), Some("dev.dbflux.DBFlux"), None),
            InstallSource::PackageManager(PackageManager::Aur)
        );
        assert_eq!(
            detect_install_source(Some(" NIX "), None, Some(Path::new("/usr/bin/dbflux"))),
            InstallSource::PackageManager(PackageManager::Nix)
        );
        assert_eq!(
            detect_install_source(Some("flatpak"), None, None),
            InstallSource::PackageManager(PackageManager::Flatpak)
        );
    }

    #[test]
    fn unknown_build_values_fall_through_to_runtime_hints() {
        assert_eq!(
            detect_install_source(Some("homebrew"), Some("dev.dbflux.DBFlux"), None),
            InstallSource::PackageManager(PackageManager::Flatpak)
        );
        assert_eq!(
            detect_install_source(Some(""), None, None),
            InstallSource::Direct
        );
    }

    #[test]
    fn runtime_hints_detect_flatpak_and_nix_only() {
        assert_eq!(
            detect_install_source(None, Some(""), None),
            InstallSource::Direct
        );
        assert_eq!(
            detect_install_source(
                None,
                None,
                Some(Path::new("/nix/store/abc-dbflux-0.8.0/bin/dbflux"))
            ),
            InstallSource::PackageManager(PackageManager::Nix)
        );
        assert_eq!(
            detect_install_source(None, None, Some(Path::new("/usr/bin/dbflux"))),
            InstallSource::Direct
        );
        assert_eq!(
            detect_install_source(None, None, Some(Path::new("/home/user/nix/store/dbflux"))),
            InstallSource::Direct
        );
    }
}
