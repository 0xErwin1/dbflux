//! The environment a child process may inherit from DBFlux.
//!
//! `gpui_linux::select_vulkan_driver` pins this process to the firmware-booted GPU's Vulkan
//! driver by setting `VK_LOADER_DRIVERS_SELECT` and a marker before `main` runs. A child inherits
//! both, so a program DBFlux starts for the user would be pinned to that driver too — a game
//! launched from DBFlux with PRIME offload would silently run on the boot GPU.
//!
//! The rule and the variable names mirror `gpui-pre-linux`'s `child_environment`, which owns them.
//! It is repeated here because the crates that run user programs (hooks, Lua scripts, SSM
//! sessions, the AWS CLI) must not depend on GPUI. `dbflux_ui_base::child_environment` applies
//! the owning rule directly, in the crates that do depend on GPUI.

use std::process::Command;

/// Records which Vulkan driver GPUI's startup selected, as the ICD manifest patterns it chose.
const SELECTION_VARIABLE: &str = "GPUI_VULKAN_DRIVER_SELECT";

/// The loader variable that selection narrows.
const LOADER_SELECTION_VARIABLE: &str = "VK_LOADER_DRIVERS_SELECT";

/// Marker value meaning no driver is selected, because every driver was tried instead.
const FALLBACK_VALUE: &str = "fallback";

/// Marker value that turns the selection off.
const OPT_OUT_VALUE: &str = "0";

/// Removes the Vulkan driver selection GPUI's startup may have set, so a child process starts
/// with the environment the user gave DBFlux.
///
/// Call this on every `Command` that runs a program the user chose.
pub fn restore_child_environment(command: &mut Command) {
    restore_child_environment_from(selection_marker().as_deref(), command);
}

/// Applies the rule for a given marker, so the removals can be asserted without changing this
/// process's environment.
pub(crate) fn restore_child_environment_from(marker: Option<&str>, command: &mut Command) {
    match marker.filter(|marker| !marker.is_empty()) {
        None | Some(OPT_OUT_VALUE) => {}
        Some(FALLBACK_VALUE) => {
            command.env_remove(SELECTION_VARIABLE);
        }
        Some(_) => {
            command.env_remove(LOADER_SELECTION_VARIABLE);
            command.env_remove(SELECTION_VARIABLE);
        }
    }
}

/// The selection marker as this process carries it.
fn selection_marker() -> Option<String> {
    std::env::var_os(SELECTION_VARIABLE).map(|value| value.to_string_lossy().into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A command as [`restore_child_environment_from`] leaves it, for a process carrying
    /// `marker`.
    fn restored_child_of(marker: Option<&str>) -> Command {
        let mut command = Command::new("dbflux-child");
        restore_child_environment_from(marker, &mut command);
        command
    }

    /// Every environment update the command records, sorted, as `(name, value)` pairs.
    fn recorded_updates(command: &Command) -> Vec<(String, Option<String>)> {
        let mut updates: Vec<(String, Option<String>)> = command
            .get_envs()
            .map(|(name, value)| {
                (
                    name.to_string_lossy().into_owned(),
                    value.map(|value| value.to_string_lossy().into_owned()),
                )
            })
            .collect();
        updates.sort();
        updates
    }

    #[test]
    fn a_child_of_a_process_that_selected_a_driver_drops_the_selection_and_the_marker() {
        let updates = recorded_updates(&restored_child_of(Some("*intel_icd*,*intel_hasvk_icd*")));

        assert_eq!(
            updates,
            vec![
                (SELECTION_VARIABLE.to_owned(), None),
                (LOADER_SELECTION_VARIABLE.to_owned(), None),
            ],
            "the marker holds the patterns this module chose, so the child gets neither variable"
        );
    }

    #[test]
    fn a_child_of_a_fallback_process_drops_only_the_marker() {
        assert_eq!(
            recorded_updates(&restored_child_of(Some(FALLBACK_VALUE))),
            vec![(SELECTION_VARIABLE.to_owned(), None)],
            "the fallback process set no selection, so only the marker is GPUI's"
        );
    }

    #[test]
    fn a_process_without_a_marker_leaves_the_child_environment_alone() {
        for marker in [None, Some(OPT_OUT_VALUE), Some("")] {
            assert_eq!(
                recorded_updates(&restored_child_of(marker)),
                Vec::new(),
                "marker {marker:?} is not a selection this module made"
            );
        }
    }

    #[test]
    fn the_rule_names_the_variables_the_vulkan_selection_uses() {
        assert_eq!(SELECTION_VARIABLE, "GPUI_VULKAN_DRIVER_SELECT");
        assert_eq!(LOADER_SELECTION_VARIABLE, "VK_LOADER_DRIVERS_SELECT");
        assert_eq!(FALLBACK_VALUE, "fallback");
        assert_eq!(OPT_OUT_VALUE, "0");
    }
}
