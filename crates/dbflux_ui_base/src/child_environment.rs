//! The environment a child process may inherit from DBFlux.
//!
//! `gpui_linux::select_vulkan_driver` pins this process to the firmware-booted GPU's Vulkan
//! driver before `main` runs, so a program DBFlux opens for the user — a system handler, a file
//! manager — would inherit that pin and run on the boot GPU. The rule that decides which
//! variables a child must not inherit belongs to `gpui-pre-linux`, which owns the selection; this
//! module only applies it where a GPUI crate spawns a child, without repeating the platform split
//! at every call site.

use std::process::Command;

/// Removes the Vulkan driver selection this process may carry, so a child process starts with the
/// environment the user gave DBFlux.
///
/// Call this on every `Command` that runs a program the user chose.
#[cfg(target_os = "linux")]
pub fn restore_child_environment(command: &mut Command) {
    gpui_linux::restore_child_environment(command);
}

/// Nothing selects a Vulkan driver outside Linux, so a child inherits the environment unchanged.
#[cfg(not(target_os = "linux"))]
pub fn restore_child_environment(_command: &mut Command) {}

#[cfg(test)]
#[cfg(target_os = "linux")]
mod tests {
    use std::process::Command;

    use super::restore_child_environment;

    /// Set for the child run of this module alone, so the selection marker reaches the helper as
    /// a real process environment without this process changing its own.
    const CHILD_FIXTURE_VARIABLE: &str = "DBFLUX_CHILD_ENVIRONMENT_FIXTURE";

    /// The marker `gpui_linux::select_vulkan_driver` sets to the patterns it chose.
    const SELECTION_VARIABLE: &str = "GPUI_VULKAN_DRIVER_SELECT";

    /// The loader variable the selection narrows.
    const LOADER_SELECTION_VARIABLE: &str = "VK_LOADER_DRIVERS_SELECT";

    /// The test the parent re-executes, which reports what the helper removed.
    const CHILD_TEST: &str = "child_environment::tests::child_reports_what_it_removed";

    /// Prefix of the report the child prints for the parent.
    const REPORT_PREFIX: &str = "removals=";

    /// The environment updates `command` records, sorted and joined: a name alone for a removal,
    /// `name=value` for a value.
    fn recorded_updates(command: &Command) -> String {
        let mut updates: Vec<String> = command
            .get_envs()
            .map(|(name, value)| match value {
                Some(value) => format!("{}={}", name.to_string_lossy(), value.to_string_lossy()),
                None => name.to_string_lossy().into_owned(),
            })
            .collect();
        updates.sort();
        updates.join(",")
    }

    /// Runs the report in a child copy of this test binary whose environment carries `marker`.
    ///
    /// The marker is injected by the parent's spawn, so neither process calls `set_var`.
    fn removals_in_child_carrying(marker: Option<&str>) -> String {
        let mut command = Command::new(std::env::current_exe().expect("the test binary path"));
        command
            .args(["--exact", CHILD_TEST, "--nocapture", "--test-threads=1"])
            .env(CHILD_FIXTURE_VARIABLE, "1")
            .env_remove(SELECTION_VARIABLE);
        if let Some(marker) = marker {
            command.env(SELECTION_VARIABLE, marker);
        }

        let output = command
            .output()
            .expect("the child test process must be spawnable");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "the child test must pass, status {:?}: {stdout}",
            output.status
        );

        stdout
            .lines()
            .find_map(|line| line.split_once(REPORT_PREFIX).map(|(_, report)| report))
            .unwrap_or_else(|| panic!("the child reported no removals: {stdout}"))
            .to_owned()
    }

    #[test]
    fn child_reports_what_it_removed() {
        if std::env::var_os(CHILD_FIXTURE_VARIABLE).is_none() {
            return;
        }

        let mut command = Command::new("dbflux-child");
        restore_child_environment(&mut command);
        println!("{REPORT_PREFIX}{}", recorded_updates(&command));
    }

    #[test]
    fn a_child_of_a_process_that_selected_a_driver_drops_the_selection_and_the_marker() {
        let mut expected = [SELECTION_VARIABLE, LOADER_SELECTION_VARIABLE];
        expected.sort();

        assert_eq!(
            removals_in_child_carrying(Some("*intel_icd*,*intel_hasvk_icd*")),
            expected.join(","),
            "the marker holds the patterns GPUI chose, so the child gets neither variable"
        );
    }

    #[test]
    fn a_child_of_a_process_without_a_selection_inherits_the_environment() {
        assert_eq!(
            removals_in_child_carrying(None),
            "",
            "no selection ran in that child, so it inherits the environment"
        );
    }
}
