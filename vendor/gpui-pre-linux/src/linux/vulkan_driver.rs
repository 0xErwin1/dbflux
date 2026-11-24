//! Selects the Vulkan driver of the boot GPU by re-executing the process.
//!
//! The Vulkan loader reads its driver variables once, when the process starts, and probes every
//! installed ICD in turn. On a hybrid machine whose boot GPU driver is listed after another
//! driver, that probe is most of the time to the first frame, and `VK_LOADER_DRIVERS_SELECT`
//! narrows it. The loader only reads that variable from the environment, which cannot be changed
//! safely once threads exist, so the choice is made before any thread starts and applied by
//! replacing the process with itself, the same `argv` and one extra variable.
//!
//! Call [`select_vulkan_driver`] as the first statement of `main`. It is a no-op where the
//! firmware-booted GPU cannot be read (`/sys/class/drm`), so it does nothing outside Linux.
//!
//! The selection is for this process only. While it is in effect, this process carries
//! `VK_LOADER_DRIVERS_SELECT` and the marker in its environment, and a child process inherits
//! both: a child would then be pinned to the boot GPU's driver, so an application launched with
//! PRIME offload would silently lose its own GPU. Every app must therefore call
//! [`restore_child_environment`] on each `Command` it prepares, rather than on the child after it
//! started. See `odd/tasks/gpui-cold-start.md`, S4 and S4b.

use std::io;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};

/// The loader variable that narrows the probe to the given ICD manifest patterns.
const LOADER_SELECTION_VARIABLE: &str = "VK_LOADER_DRIVERS_SELECT";

/// Driver variables a user sets to choose the Vulkan driver themselves; any of them wins.
const USER_DRIVER_VARIABLES: [&str; 5] = [
    "VK_ICD_FILENAMES",
    "VK_DRIVER_FILES",
    "VK_ADD_DRIVER_FILES",
    "VK_LOADER_DRIVERS_SELECT",
    "VK_LOADER_DRIVERS_DISABLE",
];

/// Records which driver this module selected, as the patterns it passed to the loader.
pub const SELECTION_VARIABLE: &str = "GPUI_VULKAN_DRIVER_SELECT";

/// Value of [`SELECTION_VARIABLE`] meaning the selected driver did not work, so every driver was
/// tried instead. A process that carries it never selects a driver again.
pub const FALLBACK_VALUE: &str = "fallback";

/// Value of [`SELECTION_VARIABLE`] that turns the selection off.
pub const OPT_OUT_VALUE: &str = "0";

/// Subdirectory of each XDG directory that the loader reads ICD manifests from.
const MANIFEST_SUBDIRECTORY: &str = "vulkan/icd.d";

/// Where sysfs exposes the DRM devices.
const DRM_ROOT: &str = "/sys/class/drm";

/// The process environment, injected so the decision can be tested without changing it.
pub(crate) trait Environment {
    fn get(&self, name: &str) -> Option<String>;
}

/// The real process environment.
pub(crate) struct ProcessEnvironment;

impl Environment for ProcessEnvironment {
    fn get(&self, name: &str) -> Option<String> {
        std::env::var_os(name).map(|value| value.to_string_lossy().into_owned())
    }
}

/// What [`select_vulkan_driver`] should do for this process.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Decision {
    /// Replace the process with itself, selecting these ICD manifest patterns.
    Reexec { patterns: String },
    /// Leave the environment alone.
    Skip(SkipReason),
}

/// Why no driver is selected.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SkipReason {
    /// The user set a driver variable, which is an explicit choice.
    UserDriverVariable(&'static str),
    /// The user set [`SELECTION_VARIABLE`] to [`OPT_OUT_VALUE`].
    OptedOut,
    /// [`SELECTION_VARIABLE`] is already present, so a selection already ran in this start.
    AlreadySelected,
    /// No DRM card reports itself as the one the firmware booted from.
    NoBootVgaGpu,
    /// The boot GPU's PCI vendor has no ICD manifest patterns known here.
    UnknownVendor(String),
    /// The boot GPU's vendor matched, but none of its manifests is installed.
    NoMatchingManifest,
}

/// The filesystem state [`decide`] reads, injected so tests can use scratch directories.
pub(crate) struct Locations {
    /// Directory holding the `card*` entries, `/sys/class/drm` in a real process.
    pub drm_root: PathBuf,
    /// Directories the loader reads ICD manifests from.
    pub manifest_directories: Vec<PathBuf>,
}

/// Reads the process environment and, when this module may choose, re-executes with the boot
/// GPU's Vulkan driver selected.
///
/// Call this as the first statement of `main`, before any thread exists: the Vulkan loader reads
/// its driver variables when it initialises, and the selected driver is applied by replacing the
/// process image, never by mutating this process's environment.
pub fn select_vulkan_driver() {
    let environment = ProcessEnvironment;
    let locations = Locations {
        drm_root: PathBuf::from(DRM_ROOT),
        manifest_directories: loader_manifest_directories(&environment),
    };

    match decide(&environment, &locations) {
        Decision::Skip(reason) => log::debug!("vulkan driver selection skipped: {reason:?}"),
        Decision::Reexec { patterns } => {
            log::info!(
                "selecting the boot GPU's vulkan driver: {LOADER_SELECTION_VARIABLE}={patterns}"
            );
            let error = select_driver(&patterns);
            log::warn!("could not re-execute with the selected vulkan driver: {error}");
        }
    }
}

/// Whether any window of this process created its GPU renderer.
///
/// A failure after that point must not replace the process: the app is already on screen with the
/// user's pending work in it.
static RENDERER_CREATED: AtomicBool = AtomicBool::new(false);

/// Records that a window created its GPU renderer.
///
/// Call this on the success path of renderer creation in every window backend.
pub(crate) fn renderer_created() {
    RENDERER_CREATED.store(true, Ordering::Relaxed);
}

/// Whether a window of this process already created its GPU renderer.
fn renderer_exists() -> bool {
    RENDERER_CREATED.load(Ordering::Relaxed)
}

/// Retries this start without the driver [`select_vulkan_driver`] chose, once, and only while no
/// window has a working renderer yet.
///
/// Call this when creating the first window's renderer fails: a driver this module selected can
/// turn out to be unusable, and the process is then replaced with itself, running with every
/// driver probed as before and carrying [`FALLBACK_VALUE`], so a driver that fails again does not
/// loop. A renderer failure after any window drew is reported and logged instead of replacing the
/// process, which would lose pending edits and tasks.
pub fn retry_after_renderer_failure() {
    let renderer_exists = renderer_exists();
    if !should_retry_without_selection(&ProcessEnvironment, renderer_exists) {
        if renderer_exists {
            log::error!(
                "the window could not create its GPU renderer; an earlier window already drew, so this process keeps running"
            );
        }
        return;
    }

    log::warn!(
        "the window could not use the selected vulkan driver; retrying with every installed driver"
    );
    let error = retry_without_selection();
    log::error!("could not re-execute without the selected vulkan driver: {error}");
}

/// The decision for one process, from the given environment and filesystem state.
pub(crate) fn decide(environment: &dyn Environment, locations: &Locations) -> Decision {
    if let Some(marker) = variable(environment, SELECTION_VARIABLE) {
        return Decision::Skip(if marker == OPT_OUT_VALUE {
            SkipReason::OptedOut
        } else {
            SkipReason::AlreadySelected
        });
    }

    if let Some(name) = USER_DRIVER_VARIABLES
        .into_iter()
        .find(|name| variable(environment, name).is_some())
    {
        return Decision::Skip(SkipReason::UserDriverVariable(name));
    }

    let Some(vendor) = boot_vga_vendor(&locations.drm_root) else {
        return Decision::Skip(SkipReason::NoBootVgaGpu);
    };
    let Some(patterns) = vendor_patterns(&vendor) else {
        return Decision::Skip(SkipReason::UnknownVendor(vendor));
    };
    if !manifest_is_installed(&locations.manifest_directories, patterns) {
        return Decision::Skip(SkipReason::NoMatchingManifest);
    }

    Decision::Reexec {
        patterns: patterns.to_owned(),
    }
}

/// Whether a child process inherits this process's Vulkan driver variables.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ChildEnvironment {
    /// The child inherits the environment as it is.
    Unchanged,
    /// The child must not inherit these variables.
    Remove(&'static [&'static str]),
}

/// Removes the driver variables this module set, so a child process starts with the environment
/// the user gave this process.
///
/// Call this on every `Command` a GPUI app prepares: `VK_LOADER_DRIVERS_SELECT` and
/// [`SELECTION_VARIABLE`] narrow this process to the boot GPU's driver, and a child spawned with
/// them keeps that pin (a game launched with PRIME offload would run on the wrong GPU). A child
/// is left alone when the driver variables came from the user, and when this module selected
/// nothing, so only a selection this module made is undone.
pub fn restore_child_environment(command: &mut Command) {
    restore_child_environment_from(&ProcessEnvironment, command);
}

/// Applies [`child_environment`] to a command, for the given process environment.
pub(crate) fn restore_child_environment_from(
    environment: &dyn Environment,
    command: &mut Command,
) {
    match child_environment(environment) {
        ChildEnvironment::Unchanged => {}
        ChildEnvironment::Remove(names) => {
            for name in names {
                command.env_remove(name);
            }
        }
    }
}

/// Which variables a child of this process must not inherit.
pub(crate) fn child_environment(environment: &dyn Environment) -> ChildEnvironment {
    match variable(environment, SELECTION_VARIABLE).as_deref() {
        None | Some(OPT_OUT_VALUE) => ChildEnvironment::Unchanged,
        Some(FALLBACK_VALUE) => ChildEnvironment::Remove(&[SELECTION_VARIABLE]),
        Some(_) => ChildEnvironment::Remove(&[LOADER_SELECTION_VARIABLE, SELECTION_VARIABLE]),
    }
}

/// Whether a failed renderer should retry without the driver this module chose.
///
/// `renderer_exists` says whether a window of this process already created its renderer; once one
/// did, no failure may replace the process.
pub(crate) fn should_retry_without_selection(
    environment: &dyn Environment,
    renderer_exists: bool,
) -> bool {
    if renderer_exists {
        return false;
    }

    match variable(environment, SELECTION_VARIABLE) {
        Some(marker) => marker != OPT_OUT_VALUE && marker != FALLBACK_VALUE,
        None => false,
    }
}

/// The value of `name`, treating an empty value as unset, as the Vulkan loader does.
fn variable(environment: &dyn Environment, name: &str) -> Option<String> {
    environment.get(name).filter(|value| !value.is_empty())
}

/// Directories the loader reads ICD manifests from, in its own order.
pub(crate) fn loader_manifest_directories(environment: &dyn Environment) -> Vec<PathBuf> {
    let home = variable(environment, "HOME");
    let home_config = home
        .as_ref()
        .map(|home| PathBuf::from(home).join(".config"));
    let home_data = home
        .as_ref()
        .map(|home| PathBuf::from(home).join(".local/share"));

    let mut directories = Vec::new();
    let mut push = |directory: Option<PathBuf>| {
        let Some(directory) = directory else {
            return;
        };
        let directory = directory.join(MANIFEST_SUBDIRECTORY);
        if !directories.contains(&directory) {
            directories.push(directory);
        }
    };

    push(
        variable(environment, "XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or(home_config),
    );
    for directory in xdg_directories(environment, "XDG_CONFIG_DIRS", "/etc/xdg") {
        push(Some(directory));
    }
    // The loader always reads its built-in directories as well, so a manifest there counts even
    // when the XDG variable names others.
    push(Some(PathBuf::from("/etc/xdg")));
    push(Some(PathBuf::from("/etc")));

    push(
        variable(environment, "XDG_DATA_HOME")
            .map(PathBuf::from)
            .or(home_data),
    );
    push(Some(PathBuf::from("/run/opengl-driver/share")));
    for directory in xdg_directories(environment, "XDG_DATA_DIRS", "/usr/local/share:/usr/share") {
        push(Some(directory));
    }
    push(Some(PathBuf::from("/usr/local/share")));
    push(Some(PathBuf::from("/usr/share")));

    directories
}

/// The directories `name` lists, or its XDG default when it is unset.
fn xdg_directories(environment: &dyn Environment, name: &str, default: &str) -> Vec<PathBuf> {
    let value = variable(environment, name).unwrap_or_else(|| default.to_owned());
    value
        .split(':')
        .filter(|directory| !directory.is_empty())
        .map(PathBuf::from)
        .collect()
}

/// The PCI vendor of the GPU the firmware booted from.
fn boot_vga_vendor(drm_root: &Path) -> Option<String> {
    let entries = std::fs::read_dir(drm_root).ok()?;

    for entry in entries.flatten() {
        if !entry.file_name().to_string_lossy().starts_with("card") {
            continue;
        }

        let device = entry.path().join("device");
        let booted = std::fs::read_to_string(device.join("boot_vga")).unwrap_or_default();
        if booted.trim() != "1" {
            continue;
        }

        let Ok(vendor) = std::fs::read_to_string(device.join("vendor")) else {
            continue;
        };
        return Some(vendor.trim().to_ascii_lowercase());
    }

    None
}

/// The ICD manifest patterns of a PCI vendor, or `None` when this code has none for it.
fn vendor_patterns(vendor: &str) -> Option<&'static str> {
    match vendor {
        "0x8086" => Some("*intel_icd*,*intel_hasvk_icd*"),
        "0x1002" => Some("*radeon_icd*,*amd_icd*"),
        "0x10de" => Some("*nvidia_icd*,*nouveau_icd*"),
        _ => None,
    }
}

/// Whether any manifest matching `patterns` is installed in `directories`.
fn manifest_is_installed(directories: &[PathBuf], patterns: &str) -> bool {
    let patterns: Vec<&str> = patterns.split(',').collect();

    directories.iter().any(|directory| {
        let Ok(entries) = std::fs::read_dir(directory) else {
            return false;
        };

        entries.flatten().any(|entry| {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                return false;
            };

            name.ends_with(".json")
                && patterns
                    .iter()
                    .any(|pattern| matches_pattern(pattern, name))
        })
    })
}

/// Whether `name` matches `pattern`, which supports the `*` wildcard the loader uses.
fn matches_pattern(pattern: &str, name: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    let [first, rest @ ..] = parts.as_slice() else {
        return false;
    };
    if rest.is_empty() {
        return *first == name;
    }

    let Some(mut remaining) = name.strip_prefix(first) else {
        return false;
    };
    let Some((last, middle)) = rest.split_last() else {
        return false;
    };
    for part in middle {
        match remaining.find(part) {
            Some(index) => remaining = &remaining[index + part.len()..],
            None => return false,
        }
    }

    remaining.ends_with(last)
}

/// Replaces this process with itself, selecting `patterns` in the new process's environment.
fn select_driver(patterns: &str) -> io::Error {
    replace_process(|command| {
        command
            .env(LOADER_SELECTION_VARIABLE, patterns)
            .env(SELECTION_VARIABLE, patterns);
    })
}

/// Replaces this process with itself, probing every installed driver again.
fn retry_without_selection() -> io::Error {
    replace_process(|command| {
        command
            .env_remove(LOADER_SELECTION_VARIABLE)
            .env(SELECTION_VARIABLE, FALLBACK_VALUE);
    })
}

/// Replaces this process image with the same executable, `argv` and updated environment.
///
/// `exec` only returns on failure, so a successful replacement never comes back here.
fn replace_process(update_environment: impl FnOnce(&mut Command)) -> io::Error {
    let mut arguments = std::env::args_os();
    let program = arguments.next();
    let executable = match std::env::current_exe() {
        Ok(executable) => executable,
        Err(error) => return error,
    };

    let mut command = Command::new(executable);
    if let Some(program) = program {
        command.arg0(program);
    }
    command.args(arguments);
    update_environment(&mut command);

    command.exec()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    /// An environment the test fills in.
    #[derive(Default)]
    struct MapEnvironment(HashMap<String, String>);

    impl Environment for MapEnvironment {
        fn get(&self, name: &str) -> Option<String> {
            self.0.get(name).cloned()
        }
    }

    static SCRATCH_COUNTER: AtomicU32 = AtomicU32::new(0);

    /// A directory under the system temp directory, removed when the test ends.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new() -> Self {
            let unique = format!(
                "gpui-vulkan-driver-{}-{}",
                std::process::id(),
                SCRATCH_COUNTER.fetch_add(1, Ordering::Relaxed)
            );
            let path = std::env::temp_dir().join(unique);
            std::fs::create_dir_all(&path).expect("scratch directory");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            if let Err(error) = std::fs::remove_dir_all(&self.0) {
                eprintln!(
                    "could not remove scratch directory {}: {error}",
                    self.0.display()
                );
            }
        }
    }

    /// One scratch filesystem plus one fake environment.
    struct Scenario {
        scratch: Scratch,
        environment: MapEnvironment,
    }

    impl Scenario {
        fn new() -> Self {
            Self {
                scratch: Scratch::new(),
                environment: MapEnvironment::default(),
            }
        }

        fn with_variable(mut self, name: &str, value: &str) -> Self {
            self.environment.0.insert(name.to_owned(), value.to_owned());
            self
        }

        fn drm_root(&self) -> PathBuf {
            self.scratch.path().join("drm")
        }

        fn manifests(&self) -> PathBuf {
            self.scratch.path().join("manifests")
        }

        fn card(self, name: &str, boot_vga: &str, vendor: &str) -> Self {
            let device = self.drm_root().join(name).join("device");
            std::fs::create_dir_all(&device).expect("scratch card directory");
            std::fs::write(device.join("boot_vga"), format!("{boot_vga}\n"))
                .expect("scratch boot_vga");
            std::fs::write(device.join("vendor"), format!("{vendor}\n")).expect("scratch vendor");
            self
        }

        fn manifest(self, name: &str) -> Self {
            let directory = self.manifests();
            std::fs::create_dir_all(&directory).expect("scratch manifest directory");
            std::fs::write(directory.join(name), "{}").expect("scratch manifest");
            self
        }

        fn locations(&self) -> Locations {
            Locations {
                drm_root: self.drm_root(),
                manifest_directories: vec![self.manifests()],
            }
        }

        fn decide(&self) -> Decision {
            decide(&self.environment, &self.locations())
        }
    }

    #[test]
    fn every_user_driver_variable_leaves_the_environment_alone() {
        for name in USER_DRIVER_VARIABLES {
            let scenario = Scenario::new()
                .card("card0", "1", "0x8086")
                .manifest("intel_icd.x86_64.json")
                .with_variable(name, "*any*");

            assert_eq!(
                scenario.decide(),
                Decision::Skip(SkipReason::UserDriverVariable(name)),
                "{name} is the user's choice"
            );
        }
    }

    #[test]
    fn an_empty_user_driver_variable_is_unset_and_still_selects() {
        for name in USER_DRIVER_VARIABLES {
            let scenario = Scenario::new()
                .card("card0", "1", "0x8086")
                .manifest("intel_icd.x86_64.json")
                .with_variable(name, "");

            assert_eq!(
                scenario.decide(),
                Decision::Reexec {
                    patterns: "*intel_icd*,*intel_hasvk_icd*".to_owned(),
                },
                "an empty {name} is unset to the loader"
            );
        }
    }

    #[test]
    fn the_opt_out_value_leaves_the_environment_alone() {
        let scenario = Scenario::new()
            .card("card0", "1", "0x8086")
            .manifest("intel_icd.x86_64.json")
            .with_variable(SELECTION_VARIABLE, OPT_OUT_VALUE);

        assert_eq!(scenario.decide(), Decision::Skip(SkipReason::OptedOut));
    }

    #[test]
    fn a_process_that_already_selected_a_driver_does_not_execute_again() {
        for marker in ["*intel_icd*", FALLBACK_VALUE] {
            let scenario = Scenario::new()
                .card("card0", "1", "0x8086")
                .manifest("intel_icd.x86_64.json")
                .with_variable(SELECTION_VARIABLE, marker);

            assert_eq!(
                scenario.decide(),
                Decision::Skip(SkipReason::AlreadySelected),
                "marker {marker} ends the chain"
            );
        }
    }

    #[test]
    fn the_boot_gpu_is_the_card_the_firmware_booted_from() {
        let scenario = Scenario::new()
            .card("card0", "0", "0x10de")
            .card("card1", "1", "0x8086")
            .manifest("nvidia_icd.json")
            .manifest("intel_icd.x86_64.json");

        assert_eq!(
            scenario.decide(),
            Decision::Reexec {
                patterns: "*intel_icd*,*intel_hasvk_icd*".to_owned(),
            },
            "card1 is the boot GPU even though card0 comes first"
        );
    }

    #[test]
    fn no_boot_gpu_selects_nothing() {
        let scenario = Scenario::new()
            .card("card0", "0", "0x8086")
            .card("card1", "0", "0x10de")
            .manifest("intel_icd.x86_64.json")
            .manifest("nvidia_icd.json");

        assert_eq!(scenario.decide(), Decision::Skip(SkipReason::NoBootVgaGpu));
    }

    #[test]
    fn a_machine_without_drm_selects_nothing() {
        let scenario = Scenario::new().manifest("intel_icd.x86_64.json");

        assert_eq!(scenario.decide(), Decision::Skip(SkipReason::NoBootVgaGpu));
    }

    #[test]
    fn each_vendor_selects_its_own_manifests() {
        for (vendor, patterns, manifest) in [
            (
                "0x8086",
                "*intel_icd*,*intel_hasvk_icd*",
                "intel_hasvk_icd.x86_64.json",
            ),
            ("0x1002", "*radeon_icd*,*amd_icd*", "amd_icd64.json"),
            (
                "0x10de",
                "*nvidia_icd*,*nouveau_icd*",
                "nouveau_icd.x86_64.json",
            ),
        ] {
            let scenario = Scenario::new()
                .card("card0", "1", vendor)
                .manifest(manifest);

            assert_eq!(
                scenario.decide(),
                Decision::Reexec {
                    patterns: patterns.to_owned(),
                },
                "vendor {vendor}"
            );
        }
    }

    #[test]
    fn an_unknown_vendor_selects_nothing() {
        let scenario = Scenario::new()
            .card("card0", "1", "0x1234")
            .manifest("intel_icd.x86_64.json");

        assert_eq!(
            scenario.decide(),
            Decision::Skip(SkipReason::UnknownVendor("0x1234".to_owned()))
        );
    }

    #[test]
    fn a_vendor_without_an_installed_manifest_selects_nothing() {
        let scenario = Scenario::new()
            .card("card0", "1", "0x8086")
            .manifest("radeon_icd.x86_64.json");

        assert_eq!(
            scenario.decide(),
            Decision::Skip(SkipReason::NoMatchingManifest)
        );
    }

    #[test]
    fn a_manifest_in_any_search_directory_is_found() {
        let scenario = Scenario::new().card("card0", "1", "0x8086");
        let second = scenario.scratch.path().join("second/vulkan/icd.d");
        std::fs::create_dir_all(&second).expect("scratch second manifest directory");
        std::fs::write(second.join("intel_icd.x86_64.json"), "{}").expect("scratch manifest");

        let locations = Locations {
            drm_root: scenario.drm_root(),
            manifest_directories: vec![scenario.manifests(), second],
        };

        assert_eq!(
            decide(&scenario.environment, &locations),
            Decision::Reexec {
                patterns: "*intel_icd*,*intel_hasvk_icd*".to_owned(),
            }
        );
    }

    #[test]
    fn the_patterns_are_a_comma_list_of_globs() {
        for (vendor, patterns) in [
            ("0x8086", "*intel_icd*,*intel_hasvk_icd*"),
            ("0x1002", "*radeon_icd*,*amd_icd*"),
            ("0x10de", "*nvidia_icd*,*nouveau_icd*"),
        ] {
            assert_eq!(vendor_patterns(vendor), Some(patterns), "vendor {vendor}");
            let parts: Vec<&str> = patterns.split(',').collect();
            assert!(parts.len() > 1, "{patterns} is a list");
            for part in parts {
                assert!(!part.is_empty(), "{patterns} has no empty member");
                assert!(
                    part.starts_with('*') && part.ends_with('*'),
                    "{part} is a glob"
                );
                assert!(!part.contains(' '), "{part} has no space");
            }
        }
    }

    #[test]
    fn a_pattern_matches_only_the_manifest_names_it_should() {
        assert!(matches_pattern("*intel_icd*", "intel_icd.x86_64.json"));
        assert!(matches_pattern("*intel_icd*", "intel_icd.json"));
        assert!(matches_pattern("*intel_icd*", "intel_icd"));
        assert!(!matches_pattern(
            "*intel_icd*",
            "intel_hasvk_icd.x86_64.json"
        ));
        assert!(!matches_pattern("*intel_icd*", "nvidia_icd.json"));
        assert!(matches_pattern("*radeon_icd*", "radeon_icd.x86_64.json"));
        assert!(matches_pattern("*amd_icd*", "amd_icd64.json"));
        assert!(!matches_pattern("*amd_icd*", "intel_icd.x86_64.json"));
        assert!(matches_pattern("*nvidia_icd*", "nvidia_icd.json"));
        assert!(matches_pattern("*nouveau_icd*", "nouveau_icd.x86_64.json"));
        assert!(matches_pattern("nvidia_icd.json", "nvidia_icd.json"));
        assert!(!matches_pattern("nvidia_icd.json", "nvidia_icd.json.bak"));
    }

    #[test]
    fn the_loader_directories_include_every_manifest_location() {
        let environment = MapEnvironment(HashMap::from([
            ("HOME".to_owned(), "/home/user".to_owned()),
            ("XDG_CONFIG_DIRS".to_owned(), "/xdg/config".to_owned()),
            ("XDG_DATA_HOME".to_owned(), "/xdg/data/home".to_owned()),
            (
                "XDG_DATA_DIRS".to_owned(),
                "/nix/store/share:/xdg/data".to_owned(),
            ),
        ]));

        let directories = loader_manifest_directories(&environment);
        let expected = [
            "/home/user/.config/vulkan/icd.d",
            "/xdg/config/vulkan/icd.d",
            "/etc/xdg/vulkan/icd.d",
            "/etc/vulkan/icd.d",
            "/xdg/data/home/vulkan/icd.d",
            "/nix/store/share/vulkan/icd.d",
            "/xdg/data/vulkan/icd.d",
            "/run/opengl-driver/share/vulkan/icd.d",
            "/usr/local/share/vulkan/icd.d",
            "/usr/share/vulkan/icd.d",
        ];

        for path in expected {
            assert!(
                directories.contains(&PathBuf::from(path)),
                "{path} is a loader directory, got {directories:?}"
            );
        }
        let mut unique = directories.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), directories.len(), "no directory repeats");
    }

    #[test]
    fn the_loader_directories_default_to_the_xdg_ones() {
        let environment = MapEnvironment(HashMap::from([(
            "HOME".to_owned(),
            "/home/user".to_owned(),
        )]));

        assert_eq!(
            loader_manifest_directories(&environment),
            vec![
                PathBuf::from("/home/user/.config/vulkan/icd.d"),
                PathBuf::from("/etc/xdg/vulkan/icd.d"),
                PathBuf::from("/etc/vulkan/icd.d"),
                PathBuf::from("/home/user/.local/share/vulkan/icd.d"),
                PathBuf::from("/run/opengl-driver/share/vulkan/icd.d"),
                PathBuf::from("/usr/local/share/vulkan/icd.d"),
                PathBuf::from("/usr/share/vulkan/icd.d"),
            ]
        );
    }

    #[test]
    fn the_fallback_runs_only_after_this_module_selected_a_driver() {
        let cases = [
            (None, false),
            (Some(OPT_OUT_VALUE), false),
            (Some(FALLBACK_VALUE), false),
            (Some("*intel_icd*,*intel_hasvk_icd*"), true),
        ];

        for (marker, expected) in cases {
            let mut environment = MapEnvironment::default();
            if let Some(marker) = marker {
                environment
                    .0
                    .insert(SELECTION_VARIABLE.to_owned(), marker.to_owned());
            }

            assert_eq!(
                should_retry_without_selection(&environment, false),
                expected,
                "marker {marker:?}"
            );
        }
    }

    #[test]
    fn a_failure_after_a_renderer_exists_keeps_the_process() {
        let scenario =
            Scenario::new().with_variable(SELECTION_VARIABLE, "*intel_icd*,*intel_hasvk_icd*");

        assert!(
            should_retry_without_selection(&scenario.environment, false),
            "the first window still retries with every driver"
        );
        assert!(
            !should_retry_without_selection(&scenario.environment, true),
            "a window opened after another one drew must not be replaced: its edits and tasks would be lost"
        );
    }

    /// A command as [`restore_child_environment`] leaves it, for a process with `environment`.
    fn restored_child_of(environment: &MapEnvironment) -> Command {
        let mut command = Command::new("dbflux-child");
        restore_child_environment_from(environment, &mut command);
        command
    }

    /// Every environment update the command records, as `(name, value)` pairs.
    fn recorded_updates(command: &Command) -> Vec<(String, Option<String>)> {
        command
            .get_envs()
            .map(|(name, value)| {
                (
                    name.to_string_lossy().into_owned(),
                    value.map(|value| value.to_string_lossy().into_owned()),
                )
            })
            .collect()
    }

    #[test]
    fn a_child_of_a_process_that_selected_a_driver_drops_the_selection_and_the_marker() {
        let scenario =
            Scenario::new().with_variable(SELECTION_VARIABLE, "*intel_icd*,*intel_hasvk_icd*");

        assert_eq!(
            child_environment(&scenario.environment),
            ChildEnvironment::Remove(&[LOADER_SELECTION_VARIABLE, SELECTION_VARIABLE])
        );
        let updates = recorded_updates(&restored_child_of(&scenario.environment));
        assert_eq!(
            updates.len(),
            2,
            "the marker holds this module's patterns, so the child gets neither variable: {updates:?}"
        );
        for name in [LOADER_SELECTION_VARIABLE, SELECTION_VARIABLE] {
            assert!(
                updates.contains(&(name.to_owned(), None)),
                "{name} is removed from the child, got {updates:?}"
            );
        }
    }

    #[test]
    fn a_child_of_a_fallback_process_drops_only_the_marker() {
        let scenario = Scenario::new().with_variable(SELECTION_VARIABLE, FALLBACK_VALUE);

        assert_eq!(
            child_environment(&scenario.environment),
            ChildEnvironment::Remove(&[SELECTION_VARIABLE])
        );
        assert_eq!(
            recorded_updates(&restored_child_of(&scenario.environment)),
            vec![(SELECTION_VARIABLE.to_owned(), None)],
            "the fallback process set no selection, so only the marker is this module's"
        );
    }

    #[test]
    fn a_child_of_a_process_without_a_marker_inherits_the_environment() {
        let scenario = Scenario::new();

        assert_eq!(
            child_environment(&scenario.environment),
            ChildEnvironment::Unchanged
        );
        assert_eq!(
            recorded_updates(&restored_child_of(&scenario.environment)),
            Vec::new(),
            "this module changed nothing, so nothing is restored"
        );
    }

    #[test]
    fn a_child_of_an_opted_out_process_inherits_the_environment() {
        let scenario = Scenario::new().with_variable(SELECTION_VARIABLE, OPT_OUT_VALUE);

        assert_eq!(
            child_environment(&scenario.environment),
            ChildEnvironment::Unchanged
        );
        assert_eq!(
            recorded_updates(&restored_child_of(&scenario.environment)),
            Vec::new(),
            "the opt-out marker is the user's, not this module's"
        );
    }

    #[test]
    fn a_user_driver_selection_without_a_marker_is_kept() {
        let scenario = Scenario::new()
            .with_variable(LOADER_SELECTION_VARIABLE, "*radeon_icd*")
            .with_variable("VK_ICD_FILENAMES", "/home/user/icd.json");

        assert_eq!(
            child_environment(&scenario.environment),
            ChildEnvironment::Unchanged
        );
        assert_eq!(
            recorded_updates(&restored_child_of(&scenario.environment)),
            Vec::new(),
            "without this module's marker, the driver variables are the user's choice"
        );
    }
}
