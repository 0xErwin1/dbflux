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

use std::sync::Arc;

use gpui::{App, AsyncApp, Global};

use crate::app_state_entity::{SaveTargetOutcome, SaveTargetProvider, SaveTargetRequest};
use crate::user_error::{ErrorKind, UserFacingError, report_error_async};

/// Returns `true` when a native file picker is expected to work on this host.
///
/// On Windows and macOS this is unconditionally `true` — the native pickers
/// are part of the OS.
///
/// On Linux this is a filesystem-only heuristic that succeeds when any of
/// these holds:
/// - the XDG portal is usable: the `org.freedesktop.portal.Desktop` session
///   service file is installed (runtime dir, `XDG_DATA_HOME`, `XDG_DATA_DIRS`
///   or `/usr/share`) AND at least one backend `*.portal` file under an
///   `xdg-desktop-portal/portals` data directory lists
///   `org.freedesktop.impl.portal.FileChooser` in its `Interfaces=` key
/// - `zenity` is on `PATH` (rfd's fallback when the portal call fails)
/// - `kdialog` is on `PATH` (KDE's equivalent fallback)
///
/// A portal frontend without a FileChooser backend (for example only
/// `xdg-desktop-portal-hyprland`) makes rfd return `None`, which callers would
/// misread as a cancellation, so it does not count. Remaining limitations:
/// `UseIn` and `portals.conf` backend routing are ignored, and rfd still cannot
/// tell a backend failure from a user cancellation.
pub fn is_native_file_dialog_available() -> bool {
    #[cfg(not(target_os = "linux"))]
    {
        true
    }

    #[cfg(target_os = "linux")]
    {
        binary_on_path("zenity") || binary_on_path("kdialog") || portal_file_chooser_available()
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
const FILE_CHOOSER_INTERFACE: &str = "org.freedesktop.impl.portal.FileChooser";

/// The environment inputs that locate D-Bus service files and portal backend
/// declarations, captured once so the directory computation stays pure.
#[cfg(target_os = "linux")]
struct XdgDirectories {
    data_home: Option<std::ffi::OsString>,
    home: Option<std::ffi::OsString>,
    data_dirs: Option<std::ffi::OsString>,
    runtime_dir: Option<std::ffi::OsString>,
}

#[cfg(target_os = "linux")]
impl XdgDirectories {
    fn from_env() -> Self {
        Self {
            data_home: std::env::var_os("XDG_DATA_HOME"),
            home: std::env::var_os("HOME"),
            data_dirs: std::env::var_os("XDG_DATA_DIRS"),
            runtime_dir: std::env::var_os("XDG_RUNTIME_DIR"),
        }
    }

    /// `XDG_DATA_HOME` then each `XDG_DATA_DIRS` entry, with the XDG Base
    /// Directory defaults when a variable is unset or empty, then the
    /// compiled-in `/usr/share` when absent.
    fn data_directories(&self) -> Vec<PathBuf> {
        let data_home = self
            .data_home
            .clone()
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .or_else(|| {
                self.home
                    .clone()
                    .filter(|value| !value.is_empty())
                    .map(|home| PathBuf::from(home).join(".local").join("share"))
            });

        let data_dirs: Vec<PathBuf> =
            match self.data_dirs.as_ref().filter(|value| !value.is_empty()) {
                Some(value) => std::env::split_paths(value)
                    .filter(|dir| !dir.as_os_str().is_empty())
                    .collect(),
                None => vec![
                    PathBuf::from("/usr/local/share"),
                    PathBuf::from("/usr/share"),
                ],
            };

        let mut directories: Vec<PathBuf> = data_home.into_iter().chain(data_dirs).collect();

        let system_default = PathBuf::from("/usr/share");
        if !directories.contains(&system_default) {
            directories.push(system_default);
        }

        directories
    }

    /// Mirrors the session bus search order: `XDG_RUNTIME_DIR`, then the data
    /// directories.
    fn session_service_dirs(&self) -> Vec<PathBuf> {
        let runtime_dir = self
            .runtime_dir
            .iter()
            .filter(|value| !value.is_empty())
            .map(PathBuf::from);

        runtime_dir
            .chain(self.data_directories())
            .map(|dir| dir.join("dbus-1").join("services"))
            .collect()
    }

    fn portal_backend_dirs(&self) -> Vec<PathBuf> {
        self.data_directories()
            .into_iter()
            .map(|dir| dir.join("xdg-desktop-portal").join("portals"))
            .collect()
    }
}

#[cfg(target_os = "linux")]
fn portal_file_chooser_available() -> bool {
    let directories = XdgDirectories::from_env();

    portal_service_installed_in(&directories.session_service_dirs())
        && file_chooser_backend_installed_in(&directories.portal_backend_dirs())
}

#[cfg(target_os = "linux")]
fn portal_service_installed_in(service_dirs: &[PathBuf]) -> bool {
    service_dirs
        .iter()
        .any(|dir| dir.join(PORTAL_SERVICE_FILE).is_file())
}

/// Unreadable directories and files are skipped.
#[cfg(target_os = "linux")]
fn file_chooser_backend_installed_in(backend_dirs: &[PathBuf]) -> bool {
    backend_dirs.iter().any(|dir| {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return false;
        };

        entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "portal"))
            .filter_map(|path| std::fs::read_to_string(path).ok())
            .any(|contents| declares_file_chooser(&contents))
    })
}

#[cfg(target_os = "linux")]
fn declares_file_chooser(portal_file: &str) -> bool {
    portal_file
        .lines()
        .filter_map(|line| line.split_once('='))
        .filter(|(key, _)| key.trim() == "Interfaces")
        .any(|(_, value)| {
            value
                .split(';')
                .any(|interface| interface.trim() == FILE_CHOOSER_INTERFACE)
        })
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

/// Answers every open-file and open-folder request in place of the native
/// picker: `Some` is a picked path, `None` a dismissed picker.
pub type OpenTargetProvider = Arc<dyn Fn() -> Option<PathBuf> + Send + Sync>;

struct OpenTargetOverride(OpenTargetProvider);

impl Global for OpenTargetOverride {}

/// Installs an app-wide replacement for the native open pickers. Production
/// never calls this; tests that dispatch a command reaching a picker install
/// one so no OS dialog opens.
pub fn install_open_target_override(cx: &mut App, provider: OpenTargetProvider) {
    cx.set_global(OpenTargetOverride(provider));
}

fn open_target_override(cx: &AsyncApp) -> Option<OpenTargetProvider> {
    cx.update(|cx| {
        cx.try_global::<OpenTargetOverride>()
            .map(|installed| installed.0.clone())
    })
}

/// Asks the user for an existing file through `native`.
///
/// Returns `None` when the user cancels. When no native picker is available
/// the user gets an error toast and `None` is returned. An installed
/// [`OpenTargetProvider`] answers instead and `native` is never polled.
pub async fn pick_existing_file(
    cx: &AsyncApp,
    native: impl std::future::Future<Output = Option<PathBuf>>,
) -> Option<PathBuf> {
    if let Some(provider) = open_target_override(cx) {
        return provider();
    }
    picked_path_or_report(
        resolve_open_target(native).await,
        cx,
        file_picker_unavailable_error,
    )
}

/// Asks the user for an existing folder through `native`.
///
/// Returns `None` when the user cancels. When no native picker is available
/// the user gets an error toast and `None` is returned. An installed
/// [`OpenTargetProvider`] answers instead and `native` is never polled.
pub async fn pick_existing_folder(
    cx: &AsyncApp,
    native: impl std::future::Future<Output = Option<PathBuf>>,
) -> Option<PathBuf> {
    if let Some(provider) = open_target_override(cx) {
        return provider();
    }
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
    async fn an_installed_override_answers_both_pickers_without_polling_native(
        cx: &mut gpui::TestAppContext,
    ) {
        let asked = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        cx.update(|cx| {
            let asked = asked.clone();
            super::install_open_target_override(
                cx,
                std::sync::Arc::new(move || {
                    asked.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Some(PathBuf::from("/tmp/fake-pick"))
                }),
            );
        });
        let async_cx = cx.to_async();
        let polled = Cell::new(false);

        let folder = super::pick_existing_folder(&async_cx, async {
            polled.set(true);
            None
        })
        .await;
        let file = super::pick_existing_file(&async_cx, async {
            polled.set(true);
            None
        })
        .await;

        assert_eq!(folder, Some(PathBuf::from("/tmp/fake-pick")));
        assert_eq!(file, Some(PathBuf::from("/tmp/fake-pick")));
        assert_eq!(asked.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(!polled.get(), "the native picker must not be polled");
    }

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
        use std::path::{Path, PathBuf};

        use super::super::{
            XdgDirectories, file_chooser_backend_installed_in, portal_service_installed_in,
        };

        fn directories(
            data_home: Option<&str>,
            home: Option<&str>,
            data_dirs: Option<OsString>,
            runtime_dir: Option<&str>,
        ) -> XdgDirectories {
            XdgDirectories {
                data_home: data_home.map(OsString::from),
                home: home.map(OsString::from),
                data_dirs,
                runtime_dir: runtime_dir.map(OsString::from),
            }
        }

        fn install_portal_service(data_dir: &Path) {
            let services = data_dir.join("dbus-1").join("services");
            std::fs::create_dir_all(&services).expect("create services dir");
            std::fs::write(
                services.join("org.freedesktop.portal.Desktop.service"),
                "[D-BUS Service]\nName=org.freedesktop.portal.Desktop\n",
            )
            .expect("write service file");
        }

        fn install_backend(data_dir: &Path, file_name: &str, interfaces: &str) {
            let portals = data_dir.join("xdg-desktop-portal").join("portals");
            std::fs::create_dir_all(&portals).expect("create portals dir");
            std::fs::write(
                portals.join(file_name),
                format!("[portal]\nDBusName=org.example\nInterfaces={interfaces}\nUseIn=gnome\n"),
            )
            .expect("write backend file");
        }

        fn backend_dirs_of(data_dir: &Path) -> Vec<PathBuf> {
            vec![data_dir.join("xdg-desktop-portal").join("portals")]
        }

        #[test]
        fn portal_is_detected_from_its_session_service_file() {
            let missing = tempfile::tempdir().expect("tempdir");
            let installed = tempfile::tempdir().expect("tempdir");
            install_portal_service(installed.path());

            let dirs = directories(
                None,
                None,
                Some(OsString::from(format!(
                    "{}:{}",
                    missing.path().display(),
                    installed.path().display()
                ))),
                None,
            )
            .session_service_dirs();

            assert!(portal_service_installed_in(&dirs));
        }

        #[test]
        fn portal_service_is_detected_in_the_runtime_dir() {
            let runtime = tempfile::tempdir().expect("tempdir");
            install_portal_service(runtime.path());

            let dirs = directories(None, Some("/home/user"), None, runtime.path().to_str())
                .session_service_dirs();

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
            let dirs = directories(None, Some("/home/user"), None, None).session_service_dirs();

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
            let dirs = directories(
                Some("/data/home"),
                Some("/home/user"),
                Some(OsString::from("/run/current-system/sw/share")),
                None,
            )
            .session_service_dirs();

            assert_eq!(
                dirs,
                vec![
                    PathBuf::from("/data/home/dbus-1/services"),
                    PathBuf::from("/run/current-system/sw/share/dbus-1/services"),
                    PathBuf::from("/usr/share/dbus-1/services"),
                ]
            );
        }

        #[test]
        fn service_dirs_search_the_runtime_dir_first_when_set() {
            let dirs = directories(None, Some("/home/user"), None, Some("/run/user/1000"))
                .session_service_dirs();

            assert_eq!(
                dirs.first(),
                Some(&PathBuf::from("/run/user/1000/dbus-1/services"))
            );
            assert_eq!(dirs.len(), 4);
        }

        #[test]
        fn service_dirs_omit_the_runtime_dir_when_unset_or_empty() {
            let unset = directories(None, Some("/home/user"), None, None).session_service_dirs();
            let empty =
                directories(None, Some("/home/user"), None, Some("")).session_service_dirs();

            assert_eq!(unset, empty);
            assert_eq!(unset.len(), 3);
        }

        #[test]
        fn backend_dirs_follow_the_data_dirs_without_the_runtime_dir() {
            let dirs = directories(None, Some("/home/user"), None, Some("/run/user/1000"))
                .portal_backend_dirs();

            assert_eq!(
                dirs,
                vec![
                    PathBuf::from("/home/user/.local/share/xdg-desktop-portal/portals"),
                    PathBuf::from("/usr/local/share/xdg-desktop-portal/portals"),
                    PathBuf::from("/usr/share/xdg-desktop-portal/portals"),
                ]
            );
        }

        #[test]
        fn a_file_chooser_backend_is_detected() {
            let data = tempfile::tempdir().expect("tempdir");
            install_backend(
                data.path(),
                "gtk.portal",
                "org.freedesktop.impl.portal.FileChooser;org.freedesktop.impl.portal.AppChooser;org.freedesktop.impl.portal.Print;",
            );

            assert!(file_chooser_backend_installed_in(&backend_dirs_of(
                data.path()
            )));
        }

        #[test]
        fn a_backend_without_file_chooser_is_not_detected() {
            let data = tempfile::tempdir().expect("tempdir");
            install_backend(
                data.path(),
                "hyprland.portal",
                "org.freedesktop.impl.portal.Screenshot;org.freedesktop.impl.portal.ScreenCast;",
            );

            assert!(!file_chooser_backend_installed_in(&backend_dirs_of(
                data.path()
            )));
        }

        #[test]
        fn an_interface_that_only_contains_the_name_is_not_detected() {
            let data = tempfile::tempdir().expect("tempdir");
            install_backend(
                data.path(),
                "extra.portal",
                "org.freedesktop.impl.portal.FileChooserExtra;x.org.freedesktop.impl.portal.FileChooser;",
            );

            assert!(!file_chooser_backend_installed_in(&backend_dirs_of(
                data.path()
            )));
        }

        #[test]
        fn missing_backend_dirs_are_skipped() {
            let data = tempfile::tempdir().expect("tempdir");

            assert!(!file_chooser_backend_installed_in(&backend_dirs_of(
                &data.path().join("absent")
            )));
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
