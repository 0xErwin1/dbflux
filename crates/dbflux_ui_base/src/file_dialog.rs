//! File-dialog availability probe and export fallback path.
//!
//! `rfd::AsyncFileDialog::save_file()` returns `Option<FileHandle>` and cannot
//! distinguish "user cancelled" from "backend failed" (no portal, no zenity/
//! kdialog). To avoid silent failures on Linux when the host has neither a
//! working XDG desktop portal nor a zenity/kdialog fallback installed, callers
//! probe `is_native_file_dialog_available()` before invoking rfd and route to
//! `fallback_export_dir()` when the probe fails.
//!
//! Open pickers have no fallback location, so `pick_existing_file()` and
//! `pick_existing_folder()` report the missing picker to the user instead.

use std::path::PathBuf;

use gpui::AsyncApp;

use crate::app_state_entity::{SaveTargetOutcome, SaveTargetProvider, SaveTargetRequest};
use crate::user_error::{ErrorKind, UserFacingError, report_error_async};

/// Returns `true` when a native file picker is expected to work on this host.
///
/// On Windows and macOS this is unconditionally `true` — the native pickers
/// are part of the OS.
///
/// On Linux this is a heuristic that succeeds when any of these holds:
/// - the `org.freedesktop.portal.Desktop` session service is installed (the
///   XDG portal backend rfd prefers)
/// - `zenity` is on `PATH` (rfd's fallback when the portal call fails)
/// - `kdialog` is on `PATH` (KDE's equivalent fallback)
///
/// The heuristic is intentionally lenient: rfd may still fail at runtime when
/// the portal is installed but the FileChooser interface is unimplemented by
/// the active desktop. The Linux user pain we're solving — a missing portal AND
/// missing zenity, which makes rfd return `None` silently — is reliably caught
/// by this probe.
pub fn is_native_file_dialog_available() -> bool {
    #[cfg(not(target_os = "linux"))]
    {
        true
    }

    #[cfg(target_os = "linux")]
    {
        binary_on_path("zenity")
            || binary_on_path("kdialog")
            || portal_service_installed_in(&session_service_dirs())
    }
}

/// The portal binary usually lives outside `PATH` (`/usr/lib` on Arch, the
/// Nix store on NixOS) and is started on demand by D-Bus activation, so
/// neither a `PATH` lookup nor a process scan finds it reliably. The session
/// service file that D-Bus activation reads is present whenever the portal can
/// be started, running or not.
#[cfg(target_os = "linux")]
const PORTAL_SERVICE_FILE: &str = "org.freedesktop.portal.Desktop.service";

#[cfg(target_os = "linux")]
fn portal_service_installed_in(service_dirs: &[PathBuf]) -> bool {
    service_dirs
        .iter()
        .any(|dir| dir.join(PORTAL_SERVICE_FILE).is_file())
}

#[cfg(target_os = "linux")]
fn session_service_dirs() -> Vec<PathBuf> {
    session_service_dirs_from(
        std::env::var_os("XDG_DATA_HOME"),
        std::env::var_os("HOME"),
        std::env::var_os("XDG_DATA_DIRS"),
    )
}

/// The session bus searches `dbus-1/services` under `XDG_DATA_HOME` and each
/// `XDG_DATA_DIRS` entry, with the XDG Base Directory defaults when a variable
/// is unset or empty, plus the compiled-in `/usr/share` location.
#[cfg(target_os = "linux")]
fn session_service_dirs_from(
    data_home: Option<std::ffi::OsString>,
    home: Option<std::ffi::OsString>,
    data_dirs: Option<std::ffi::OsString>,
) -> Vec<PathBuf> {
    let data_home = data_home
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            home.filter(|value| !value.is_empty())
                .map(|home| PathBuf::from(home).join(".local").join("share"))
        });

    let data_dirs: Vec<PathBuf> = match data_dirs.filter(|value| !value.is_empty()) {
        Some(value) => std::env::split_paths(&value)
            .filter(|dir| !dir.as_os_str().is_empty())
            .collect(),
        None => vec![
            PathBuf::from("/usr/local/share"),
            PathBuf::from("/usr/share"),
        ],
    };

    let mut service_dirs: Vec<PathBuf> = data_home
        .into_iter()
        .chain(data_dirs)
        .map(|dir| dir.join("dbus-1").join("services"))
        .collect();

    let system_default = PathBuf::from("/usr/share/dbus-1/services");
    if !service_dirs.contains(&system_default) {
        service_dirs.push(system_default);
    }

    service_dirs
}

#[cfg(target_os = "linux")]
fn binary_on_path(bin: &str) -> bool {
    let Some(paths) = std::env::var_os("PATH") else {
        return false;
    };

    std::env::split_paths(&paths).any(|dir| {
        let candidate = dir.join(bin);
        candidate.is_file()
    })
}

/// Returns the fallback directory for exports written when no native file
/// picker is available, creating it if missing.
///
/// Resolves to `~/.local/share/dbflux/exports/` via
/// `dbflux_storage::paths::data_dir()`.
pub fn fallback_export_dir() -> Result<PathBuf, String> {
    let data_dir = dbflux_storage::paths::data_dir()
        .map_err(|e| format!("Failed to resolve data directory: {}", e))?;

    let exports = data_dir.join("exports");

    std::fs::create_dir_all(&exports).map_err(|e| {
        format!(
            "Failed to create exports directory {}: {}",
            exports.display(),
            e
        )
    })?;

    Ok(exports)
}

/// Builds a non-clobbering path inside `dir` for `filename`. If the file
/// already exists, suffixes `-2`, `-3`, ... before the extension until a free
/// slot is found.
pub fn unique_path_in(dir: &std::path::Path, filename: &str) -> PathBuf {
    let initial = dir.join(filename);
    if !initial.exists() {
        return initial;
    }

    let (stem, ext) = match filename.rfind('.') {
        Some(i) if i > 0 => (&filename[..i], Some(&filename[i + 1..])),
        _ => (filename, None),
    };

    for n in 2..u32::MAX {
        let candidate_name = match ext {
            Some(ext) => format!("{}-{}.{}", stem, n, ext),
            None => format!("{}-{}", stem, n),
        };
        let candidate = dir.join(candidate_name);
        if !candidate.exists() {
            return candidate;
        }
    }

    initial
}

/// Resolves a save destination through a per-entity test/embedding provider if
/// one is installed, otherwise through the normal native-dialog or fallback
/// export path.
///
/// The native future is lazy: when a provider is present, or when no native
/// dialog is available, it is never polled. That keeps dialog construction at
/// the call site while centralizing cancellation and fallback semantics here.
pub async fn resolve_save_target<'a>(
    provider: Option<SaveTargetProvider>,
    request: SaveTargetRequest<'a>,
    native: impl std::future::Future<Output = Option<PathBuf>>,
) -> SaveTargetOutcome {
    if let Some(provider) = provider {
        return provider(request).await;
    }

    if !is_native_file_dialog_available() {
        return match fallback_export_dir() {
            Ok(dir) => SaveTargetOutcome::Selected {
                path: unique_path_in(&dir, request.suggested_name),
                used_fallback: true,
            },
            Err(err) => SaveTargetOutcome::Failed(err),
        };
    }

    match native.await {
        Some(path) => SaveTargetOutcome::Selected {
            path,
            used_fallback: false,
        },
        None => SaveTargetOutcome::Cancelled,
    }
}

/// Result of asking the user for an existing file or folder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenTargetOutcome {
    /// The user picked this path.
    Selected(PathBuf),
    /// The user dismissed the picker.
    Cancelled,
    /// No native picker is available on this host, so none was shown.
    Unavailable,
}

/// Resolves an open-file or open-folder request through the native picker.
///
/// The native future is lazy: when no native dialog is available it is never
/// polled. Dialog construction stays at the call site, the same split as
/// [`resolve_save_target`].
pub async fn resolve_open_target(
    native: impl std::future::Future<Output = Option<PathBuf>>,
) -> OpenTargetOutcome {
    resolve_open_target_with(is_native_file_dialog_available(), native).await
}

async fn resolve_open_target_with(
    dialog_available: bool,
    native: impl std::future::Future<Output = Option<PathBuf>>,
) -> OpenTargetOutcome {
    if !dialog_available {
        return OpenTargetOutcome::Unavailable;
    }

    match native.await {
        Some(path) => OpenTargetOutcome::Selected(path),
        None => OpenTargetOutcome::Cancelled,
    }
}

/// Asks the user for an existing file through `native`.
///
/// Returns `None` when the user cancels. When no native picker is available
/// the user gets an error toast and `None` is returned.
pub async fn pick_existing_file(
    cx: &AsyncApp,
    native: impl std::future::Future<Output = Option<PathBuf>>,
) -> Option<PathBuf> {
    picked_path_or_report(
        resolve_open_target(native).await,
        cx,
        file_picker_unavailable_error,
    )
}

/// Asks the user for an existing folder through `native`.
///
/// Returns `None` when the user cancels. When no native picker is available
/// the user gets an error toast and `None` is returned.
pub async fn pick_existing_folder(
    cx: &AsyncApp,
    native: impl std::future::Future<Output = Option<PathBuf>>,
) -> Option<PathBuf> {
    picked_path_or_report(
        resolve_open_target(native).await,
        cx,
        folder_picker_unavailable_error,
    )
}

fn picked_path_or_report(
    outcome: OpenTargetOutcome,
    cx: &AsyncApp,
    unavailable_error: fn() -> UserFacingError,
) -> Option<PathBuf> {
    match outcome {
        OpenTargetOutcome::Selected(path) => Some(path),
        OpenTargetOutcome::Cancelled => None,
        OpenTargetOutcome::Unavailable => {
            report_error_async(unavailable_error(), cx);
            None
        }
    }
}

fn file_picker_unavailable_error() -> UserFacingError {
    UserFacingError::new(
        ErrorKind::Config,
        dbflux_i18n::t!("document.object_browser.upload.error.no_file_picker"),
    )
}

fn folder_picker_unavailable_error() -> UserFacingError {
    UserFacingError::new(
        ErrorKind::Config,
        dbflux_i18n::t!("document.import_wizard.pick_folder.error.no_dialog"),
    )
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::path::PathBuf;

    use super::{
        OpenTargetOutcome, file_picker_unavailable_error, folder_picker_unavailable_error,
        resolve_open_target_with,
    };
    use crate::user_error::ErrorKind;

    #[gpui::test]
    async fn open_target_returns_the_picked_path() {
        let outcome =
            resolve_open_target_with(true, async { Some(PathBuf::from("/tmp/script.sql")) }).await;

        assert_eq!(
            outcome,
            OpenTargetOutcome::Selected(PathBuf::from("/tmp/script.sql"))
        );
    }

    #[gpui::test]
    async fn open_target_reports_a_dismissed_picker_as_cancelled() {
        let outcome = resolve_open_target_with(true, async { None }).await;

        assert_eq!(outcome, OpenTargetOutcome::Cancelled);
    }

    #[gpui::test]
    async fn open_target_never_shows_the_picker_when_none_is_available() {
        let polled = Cell::new(false);

        let outcome = resolve_open_target_with(false, async {
            polled.set(true);
            Some(PathBuf::from("/tmp/ignored"))
        })
        .await;

        assert_eq!(outcome, OpenTargetOutcome::Unavailable);
        assert!(!polled.get());
    }

    #[cfg(target_os = "linux")]
    mod portal_service {
        use std::ffi::OsString;
        use std::path::PathBuf;

        use super::super::{portal_service_installed_in, session_service_dirs_from};

        fn install_portal_service(data_dir: &std::path::Path) {
            let services = data_dir.join("dbus-1").join("services");
            std::fs::create_dir_all(&services).expect("create services dir");
            std::fs::write(
                services.join("org.freedesktop.portal.Desktop.service"),
                "[D-BUS Service]\nName=org.freedesktop.portal.Desktop\n",
            )
            .expect("write service file");
        }

        #[test]
        fn portal_is_detected_from_its_session_service_file() {
            let missing = tempfile::tempdir().expect("tempdir");
            let installed = tempfile::tempdir().expect("tempdir");
            install_portal_service(installed.path());

            let dirs = session_service_dirs_from(
                None,
                None,
                Some(OsString::from(format!(
                    "{}:{}",
                    missing.path().display(),
                    installed.path().display()
                ))),
            );

            assert!(portal_service_installed_in(&dirs));
        }

        #[test]
        fn portal_is_not_detected_without_a_service_file() {
            let empty = tempfile::tempdir().expect("tempdir");
            let dirs = vec![empty.path().join("dbus-1").join("services")];

            assert!(!portal_service_installed_in(&dirs));
        }

        #[test]
        fn service_dirs_follow_the_xdg_base_directory_defaults() {
            let dirs = session_service_dirs_from(None, Some(OsString::from("/home/user")), None);

            assert_eq!(
                dirs,
                vec![
                    PathBuf::from("/home/user/.local/share/dbus-1/services"),
                    PathBuf::from("/usr/local/share/dbus-1/services"),
                    PathBuf::from("/usr/share/dbus-1/services"),
                ]
            );
        }

        #[test]
        fn service_dirs_prefer_explicit_xdg_variables() {
            let dirs = session_service_dirs_from(
                Some(OsString::from("/data/home")),
                Some(OsString::from("/home/user")),
                Some(OsString::from("/run/current-system/sw/share")),
            );

            assert_eq!(
                dirs,
                vec![
                    PathBuf::from("/data/home/dbus-1/services"),
                    PathBuf::from("/run/current-system/sw/share/dbus-1/services"),
                    PathBuf::from("/usr/share/dbus-1/services"),
                ]
            );
        }
    }

    #[test]
    fn unavailable_picker_errors_carry_a_message() {
        let file_error = file_picker_unavailable_error();
        let folder_error = folder_picker_unavailable_error();

        assert_eq!(file_error.kind, ErrorKind::Config);
        assert!(!file_error.summary.is_empty());
        assert_eq!(folder_error.kind, ErrorKind::Config);
        assert!(!folder_error.summary.is_empty());
    }
}
