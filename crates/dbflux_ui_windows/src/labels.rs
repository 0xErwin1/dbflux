//! Translation helpers shared across settings-window sections.
//!
//! These wrap [`dbflux_i18n::t!`] calls that need named arguments so
//! per-row rendering code can build the label once instead of repeating
//! the placeholder substitution inline on every render pass.

/// Formats the "(N bindings)" count shown next to a keybindings context header.
pub(crate) fn keybindings_binding_count(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!("settings.keybindings.binding_count.one")
    } else {
        dbflux_i18n::t!("settings.keybindings.binding_count.many", count = count)
    }
}

/// Formats the "inherits from <parent context>" hint on a context header.
pub(crate) fn keybindings_inherits_from(parent: &str) -> String {
    dbflux_i18n::t!("settings.keybindings.inherits_from", parent = parent)
}

/// Formats the conflict banner title for a chord shared by multiple commands.
pub(crate) fn keybindings_conflict_title(chord: &str, others: &str) -> String {
    dbflux_i18n::t!(
        "settings.keybindings.conflict.title",
        chord = chord,
        others = others
    )
}

/// Formats the About section copyright line with the resolved author name.
pub(crate) fn about_copyright(author: &str) -> String {
    dbflux_i18n::t!("settings.about.copyright", author = author)
}

/// Formats the About section license line with the resolved license identifier.
pub(crate) fn about_license(license: &str) -> String {
    dbflux_i18n::t!("settings.about.license", license = license)
}

/// Formats the confirmation prompt for deleting a named hook.
pub(crate) fn hooks_delete_message(name: &str) -> String {
    dbflux_i18n::t!("hooks.delete.message", name = name)
}

/// Formats the confirmation prompt for deleting an unreadable hook row.
pub(crate) fn hooks_delete_unreadable_message(name: &str) -> String {
    dbflux_i18n::t!("hooks.delete_unreadable.message", name = name)
}

/// Formats the "interpreter not found in PATH" warning shown in the hook form.
pub(crate) fn hooks_interpreter_missing(interpreter: &str) -> String {
    dbflux_i18n::t!(
        "settings.hooks.status.interpreter_missing",
        interpreter = interpreter
    )
}

/// Formats the toast shown when opening a hook's script in the OS default editor fails.
pub(crate) fn hooks_open_script_failed(error: &str) -> String {
    dbflux_i18n::t!("settings.hooks.error.open_script", error = error)
}

/// Formats the toast shown when writing a hook's script file to disk fails.
pub(crate) fn hooks_write_script_failed(error: &str) -> String {
    dbflux_i18n::t!("settings.hooks.error.write_script", error = error)
}

/// Formats the toast shown when creating the hooks scripts directory fails.
pub(crate) fn hooks_create_dir_failed(error: &str) -> String {
    dbflux_i18n::t!("settings.hooks.error.create_dir", error = error)
}

/// Formats the validation error shown when saving a hook with an ID already in use.
pub(crate) fn hooks_duplicate_id(id: &str) -> String {
    dbflux_i18n::t!("settings.hooks.validation.duplicate_id", id = id)
}

/// Formats the validation error shown for a malformed `KEY=value` environment pair.
pub(crate) fn hooks_env_pair_invalid(pair: &str) -> String {
    dbflux_i18n::t!("settings.hooks.validation.env_pair", pair = pair)
}

#[cfg(test)]
mod tests {
    use super::{
        hooks_create_dir_failed, hooks_delete_message, hooks_delete_unreadable_message,
        hooks_duplicate_id, hooks_env_pair_invalid, hooks_interpreter_missing,
        hooks_open_script_failed, hooks_write_script_failed,
    };

    #[test]
    fn hooks_delete_message_embeds_hook_name() {
        let message = hooks_delete_message("nightly-backup");

        assert_eq!(
            message,
            "Are you sure you want to delete hook \"nightly-backup\"?"
        );
    }

    #[test]
    fn hooks_delete_unreadable_message_embeds_row_name() {
        let message = hooks_delete_unreadable_message("legacy-row");

        assert_eq!(
            message,
            "Permanently delete the unreadable hook row \"legacy-row\"? Its stored data cannot be recovered, but its name becomes reusable afterwards."
        );
    }

    #[test]
    fn hooks_interpreter_missing_embeds_interpreter_name() {
        let message = hooks_interpreter_missing("python3");

        assert_eq!(message, "Interpreter 'python3' was not found in PATH");
    }

    #[test]
    fn hooks_open_script_failed_embeds_error_cause() {
        let message = hooks_open_script_failed("no application registered");

        assert_eq!(message, "Failed to open script: no application registered");
    }

    #[test]
    fn hooks_write_script_failed_embeds_error_cause() {
        let message = hooks_write_script_failed("permission denied");

        assert_eq!(message, "Failed to write script file: permission denied");
    }

    #[test]
    fn hooks_create_dir_failed_embeds_error_cause() {
        let message = hooks_create_dir_failed("disk full");

        assert_eq!(message, "Failed to create hooks directory: disk full");
    }

    #[test]
    fn hooks_duplicate_id_embeds_id() {
        let message = hooks_duplicate_id("nightly-backup");

        assert_eq!(message, "A hook with ID 'nightly-backup' already exists");
    }

    #[test]
    fn hooks_env_pair_invalid_embeds_pair() {
        let message = hooks_env_pair_invalid("FOO");

        assert_eq!(message, "Invalid env pair 'FOO'. Expected KEY=value format");
    }
}
