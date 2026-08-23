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

/// Formats the "auto (<interpreter>)" placeholder shown when the interpreter field is empty.
pub(crate) fn hooks_interpreter_auto_label(value: &str) -> String {
    dbflux_i18n::t!("settings.hooks.form.interpreter_auto", value = value)
}

/// Formats the "Leave empty for <default interpreter>" hint under the interpreter field.
pub(crate) fn hooks_form_interpreter_hint(default_interpreter: &str) -> String {
    dbflux_i18n::t!(
        "settings.hooks.form.interpreter_hint",
        default_interpreter = default_interpreter
    )
}

/// Formats the "Default: <value>" caption under a connection override control.
pub(crate) fn override_default_caption(value: &str) -> String {
    dbflux_i18n::t!(
        "connection_manager.overrides.default_caption",
        value = value
    )
}

/// Formats the "Default: <seconds>s" caption under the refresh interval override.
pub(crate) fn override_default_seconds_caption(seconds: u32) -> String {
    dbflux_i18n::t!(
        "connection_manager.overrides.default_seconds_caption",
        value = seconds
    )
}

/// Formats the "<name> (disabled)" label shown for a disabled proxy in the proxy dropdown.
pub(crate) fn access_proxy_disabled_label(name: &str) -> String {
    dbflux_i18n::t!("access.proxy_disabled_label", name = name)
}

/// Formats the "Private Key (<path>)" label shown for a saved SSH tunnel's private key auth.
pub(crate) fn ssh_private_key_with_path(path: &str) -> String {
    dbflux_i18n::t!("ssh.private_key_with_path", path = path)
}

/// Formats the toast shown when the selected auth profile's provider is not registered.
pub(crate) fn auth_provider_unavailable(provider_id: &str) -> String {
    dbflux_i18n::t!(
        "connection_manager.auth.provider_unavailable",
        provider_id = provider_id
    )
}

/// Formats the status message shown while an auth-provider login is starting.
pub(crate) fn auth_login_starting(name: &str) -> String {
    dbflux_i18n::t!("connection_manager.auth.login_starting", name = name)
}

/// Formats the status message shown when an auth-provider login fails.
pub(crate) fn auth_login_failed(error: &str) -> String {
    dbflux_i18n::t!("connection_manager.auth.login_failed", error = error)
}

/// Formats the "Session status: valid (expires at <timestamp>)" caption.
pub(crate) fn auth_session_status_valid_expires(expires_at: &str) -> String {
    dbflux_i18n::t!(
        "connection_manager.auth.session_status_valid_expires",
        expires_at = expires_at
    )
}

/// Formats the MCP tab's "Actor 'x' | role: y | policy: z" scope/policy preview line.
pub(crate) fn mcp_preview_summary(actor: &str, role: &str, policy: &str) -> String {
    dbflux_i18n::t!(
        "connection_manager.mcp_preview_summary",
        actor = actor,
        role = role,
        policy = policy
    )
}

/// Formats the "Configure <driver name>" call-to-action label shown in the driver picker
/// footer once a driver card is focused.
pub(crate) fn driver_select_configure(name: &str) -> String {
    dbflux_i18n::t!(
        "connection_manager.driver_select.configure_named",
        name = name
    )
}

/// Builds the delete-confirmation body for an auth profile, embedding the
/// profile name and pluralizing the affected-connections count.
pub(crate) fn auth_profiles_delete_body(name: &str, affected_connections: usize) -> String {
    match affected_connections {
        0 => dbflux_i18n::t!(
            "settings.auth_profiles.delete_dialog.body_none",
            name = name
        ),
        1 => dbflux_i18n::t!("settings.auth_profiles.delete_dialog.body_one", name = name),
        count => dbflux_i18n::t!(
            "settings.auth_profiles.delete_dialog.body_many",
            name = name,
            count = count
        ),
    }
}

/// Formats the "Inherited from <field> ['<name>']." hint shown under a
/// disabled auth profile field whose value is inherited from another field.
pub(crate) fn auth_profiles_inherited_hint(
    trigger_id: &str,
    referenced_name: Option<&str>,
) -> String {
    match referenced_name {
        Some(name) => dbflux_i18n::t!(
            "settings.auth_profiles.inherited_from_named",
            trigger = trigger_id,
            name = name
        ),
        None => dbflux_i18n::t!(
            "settings.auth_profiles.inherited_from",
            trigger = trigger_id
        ),
    }
}

/// Builds the user-visible conflict message for a `Conflict` save outcome,
/// naming the target's label and pointing to the Reload affordance.
pub(crate) fn auth_profiles_conflict_message(label: &str) -> String {
    dbflux_i18n::t!("settings.auth_profiles.conflict_message", label = label)
}

/// Builds the user-visible message for a `PartialSaved` save outcome,
/// naming both the written and conflicted targets.
pub(crate) fn auth_profiles_partial_saved_message(
    written_label: &str,
    conflicted_label: &str,
) -> String {
    dbflux_i18n::t!(
        "settings.auth_profiles.partial_saved_message",
        written_label = written_label,
        conflicted_label = conflicted_label
    )
}

/// Formats the status message shown while the Settings-window auth-profile
/// login is starting.
pub(crate) fn auth_profiles_starting_login(name: &str) -> String {
    dbflux_i18n::t!("settings.auth_profiles.starting_login", name = name)
}

/// Formats the status message shown when the Settings-window auth-profile
/// login completes successfully.
pub(crate) fn auth_profiles_login_completed(name: &str) -> String {
    dbflux_i18n::t!("settings.auth_profiles.login_completed", name = name)
}

/// Formats the status message shown when the Settings-window auth-profile
/// login fails.
pub(crate) fn auth_profiles_login_failed(name: &str, error: &str) -> String {
    dbflux_i18n::t!(
        "settings.auth_profiles.login_failed",
        name = name,
        error = error
    )
}

/// Formats the "N policies" caption shown next to an MCP role row.
#[cfg(feature = "mcp")]
pub(crate) fn mcp_role_policy_count(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!("settings.mcp.field.policy_count.one")
    } else {
        dbflux_i18n::t!("settings.mcp.field.policy_count.many", count = count)
    }
}

/// Formats the "N tools · M classes" caption shown next to an MCP policy row.
#[cfg(feature = "mcp")]
pub(crate) fn mcp_policy_tools_classes_summary(tools: usize, classes: usize) -> String {
    dbflux_i18n::t!(
        "settings.mcp.field.tools_classes_summary",
        tools = tools,
        classes = classes
    )
}

/// Formats the validation error shown when saving an RPC service with a
/// socket ID already used by another configured service.
pub(crate) fn rpc_services_duplicate_socket_id(id: &str) -> String {
    dbflux_i18n::t!("settings.rpc_services.error.duplicate_socket_id", id = id)
}

/// Formats the delete-confirmation body for a named SSH tunnel.
pub(crate) fn ssh_tunnels_delete_body(name: &str) -> String {
    dbflux_i18n::t!("settings.ssh_tunnels.delete_dialog.body", name = name)
}

/// Builds the delete-confirmation body for a proxy profile, embedding the
/// proxy name and pluralizing the affected-connections count.
pub(crate) fn proxies_delete_body(name: &str, affected_connections: usize) -> String {
    match affected_connections {
        0 => dbflux_i18n::t!("settings.proxies.delete_dialog.body_none", name = name),
        1 => dbflux_i18n::t!("settings.proxies.delete_dialog.body_one", name = name),
        count => dbflux_i18n::t!(
            "settings.proxies.delete_dialog.body_many",
            name = name,
            count = count
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        access_proxy_disabled_label, auth_login_failed, auth_login_starting,
        auth_profiles_conflict_message, auth_profiles_login_completed, auth_profiles_login_failed,
        auth_profiles_partial_saved_message, auth_profiles_starting_login,
        auth_provider_unavailable, auth_session_status_valid_expires, hooks_create_dir_failed,
        hooks_delete_message, hooks_delete_unreadable_message, hooks_duplicate_id,
        hooks_env_pair_invalid, hooks_form_interpreter_hint, hooks_interpreter_auto_label,
        hooks_interpreter_missing, hooks_open_script_failed, hooks_write_script_failed,
        mcp_preview_summary, proxies_delete_body, ssh_private_key_with_path,
        ssh_tunnels_delete_body,
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

    #[test]
    fn hooks_interpreter_auto_label_embeds_interpreter() {
        let message = hooks_interpreter_auto_label("python3");

        assert_eq!(message, "auto (python3)");
    }

    #[test]
    fn hooks_form_interpreter_hint_embeds_default_interpreter() {
        let message = hooks_form_interpreter_hint("auto (python3)");

        assert_eq!(message, "Leave empty for auto (python3)");
    }

    #[test]
    fn access_proxy_disabled_label_embeds_proxy_name() {
        let message = access_proxy_disabled_label("corporate-proxy");

        assert_eq!(message, "corporate-proxy (disabled)");
    }

    #[test]
    fn ssh_private_key_with_path_embeds_key_path() {
        let message = ssh_private_key_with_path("~/.ssh/id_ed25519");

        assert_eq!(message, "Private Key (~/.ssh/id_ed25519)");
    }

    #[test]
    fn auth_provider_unavailable_embeds_provider_id_untouched() {
        let message = auth_provider_unavailable("acme-mongo");

        assert!(message.contains("acme-mongo"));
    }

    #[test]
    fn auth_login_starting_embeds_profile_name() {
        let message = auth_login_starting("prod-mongo");

        assert_eq!(message, "Starting auth-provider login for 'prod-mongo'...");
    }

    #[test]
    fn auth_login_failed_embeds_error_cause() {
        let message = auth_login_failed("token expired");

        assert_eq!(message, "Auth-provider login failed: token expired");
    }

    #[test]
    fn auth_session_status_valid_expires_embeds_timestamp() {
        let message = auth_session_status_valid_expires("2026-08-22 00:00:00 UTC");

        assert_eq!(
            message,
            "Session status: valid (expires at 2026-08-22 00:00:00 UTC)"
        );
    }

    #[test]
    fn mcp_preview_summary_embeds_actor_role_policy() {
        let message = mcp_preview_summary("prod-agent", "read-only", "strict");

        assert_eq!(
            message,
            "Actor 'prod-agent' | role: read-only | policy: strict"
        );
    }

    #[test]
    fn driver_select_configure_embeds_driver_name_untouched() {
        let message = super::driver_select_configure("MongoDB");

        assert!(message.contains("MongoDB"));
    }

    #[test]
    fn override_default_captions_embed_value() {
        assert!(super::override_default_caption("On").contains("On"));
        assert!(super::override_default_seconds_caption(30).contains("30"));
        assert_ne!(
            dbflux_i18n::t!(
                "connection_manager.overrides.default_caption",
                locale = "en"
            ),
            dbflux_i18n::t!(
                "connection_manager.overrides.default_caption",
                locale = "es"
            )
        );
    }

    #[test]
    fn auth_profiles_conflict_message_names_target_label() {
        let message = auth_profiles_conflict_message("FAKE_TARGET_A");

        assert!(message.contains("FAKE_TARGET_A"));
        assert!(!message.contains("settings.auth_profiles.conflict_message"));
    }

    #[test]
    fn auth_profiles_partial_saved_message_names_both_targets() {
        let message = auth_profiles_partial_saved_message("FAKE_TARGET_A", "FAKE_TARGET_B");

        assert!(message.contains("FAKE_TARGET_A"));
        assert!(message.contains("FAKE_TARGET_B"));
    }

    #[test]
    fn auth_profiles_starting_login_embeds_profile_name() {
        let message = auth_profiles_starting_login("prod-mongo");

        assert!(message.contains("prod-mongo"));
    }

    #[test]
    fn auth_profiles_login_completed_embeds_profile_name() {
        let message = auth_profiles_login_completed("prod-mongo");

        assert!(message.contains("prod-mongo"));
    }

    #[test]
    fn auth_profiles_login_failed_embeds_profile_name_and_error() {
        let message = auth_profiles_login_failed("prod-mongo", "token expired");

        assert!(message.contains("prod-mongo"));
        assert!(message.contains("token expired"));
    }

    #[cfg(feature = "mcp")]
    #[test]
    fn mcp_role_policy_count_uses_singular_form_for_one() {
        use super::mcp_role_policy_count;

        let message = mcp_role_policy_count(1);

        assert_eq!(message, "1 policy");
    }

    #[cfg(feature = "mcp")]
    #[test]
    fn mcp_role_policy_count_embeds_count_for_many() {
        use super::mcp_role_policy_count;

        let message = mcp_role_policy_count(3);

        assert!(message.contains('3'));
        assert_ne!(message, "1 policy");
    }

    #[cfg(feature = "mcp")]
    #[test]
    fn mcp_policy_tools_classes_summary_embeds_both_counts() {
        use super::mcp_policy_tools_classes_summary;

        let message = mcp_policy_tools_classes_summary(4, 2);

        assert!(message.contains('4'));
        assert!(message.contains('2'));
    }

    #[test]
    fn ssh_tunnels_delete_body_embeds_tunnel_name() {
        let message = ssh_tunnels_delete_body("bastion-prod");

        assert!(message.contains("bastion-prod"));
    }

    #[test]
    fn proxies_delete_body_uses_singular_form_for_one_connection() {
        let message = proxies_delete_body("corporate-proxy", 1);

        assert!(message.contains("corporate-proxy"));
        assert!(message.contains('1'));
    }

    #[test]
    fn proxies_delete_body_embeds_name_and_count_for_many_connections() {
        let message = proxies_delete_body("corporate-proxy", 3);

        assert!(message.contains("corporate-proxy"));
        assert!(message.contains('3'));
    }

    #[test]
    fn proxies_delete_body_omits_count_when_no_connections_affected() {
        let message = proxies_delete_body("corporate-proxy", 0);

        assert!(message.contains("corporate-proxy"));
        assert!(!message.contains('0'));
    }
}
