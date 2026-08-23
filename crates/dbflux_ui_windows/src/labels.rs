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

#[cfg(test)]
mod tests {
    use super::{hooks_delete_message, hooks_delete_unreadable_message};

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
}
