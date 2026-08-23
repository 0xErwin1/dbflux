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

/// Formats the copy-action text for the audit "Failed to save" toast,
/// embedding the storage error cause.
pub(crate) fn audit_save_failed_copy(error: &str) -> String {
    dbflux_i18n::t!("settings.audit.error.save_failed_copy", error = error)
}

/// Formats the "Cannot read file: <error>" import-panel error.
pub(crate) fn import_error_cannot_read_file(error: &str) -> String {
    dbflux_i18n::t!(
        "connection_manager.import.error.cannot_read_file",
        error = error
    )
}

/// Formats the "Parse error: <error>" import-panel error.
pub(crate) fn import_error_parse_error(error: &str) -> String {
    dbflux_i18n::t!("connection_manager.import.error.parse_error", error = error)
}

/// Formats the "Decryption error: <error>" import-panel error.
pub(crate) fn import_error_decryption_error(error: &str) -> String {
    dbflux_i18n::t!(
        "connection_manager.import.error.decryption_error",
        error = error
    )
}

/// Formats the "Import failed: <error>" import-panel error.
pub(crate) fn import_error_import_failed(error: &str) -> String {
    dbflux_i18n::t!(
        "connection_manager.import.error.import_failed",
        error = error
    )
}

/// Formats the pluralized "N secret(s) could not be written to the keyring during import..."
/// toast raised from the import apply pipeline.
pub(crate) fn import_error_secret_failures_toast(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!(
            "connection_manager.import.error.secret_failures_toast.one",
            count = count
        )
    } else {
        dbflux_i18n::t!(
            "connection_manager.import.error.secret_failures_toast.many",
            count = count
        )
    }
}

/// Formats the pluralized "N connection(s)" import-preview count line.
pub(crate) fn import_preview_count_connections(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!(
            "connection_manager.import.preview.count.connections.one",
            count = count
        )
    } else {
        dbflux_i18n::t!(
            "connection_manager.import.preview.count.connections.many",
            count = count
        )
    }
}

/// Formats the pluralized "N auth profile(s)" import-preview count line.
pub(crate) fn import_preview_count_auth_profiles(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!(
            "connection_manager.import.preview.count.auth_profiles.one",
            count = count
        )
    } else {
        dbflux_i18n::t!(
            "connection_manager.import.preview.count.auth_profiles.many",
            count = count
        )
    }
}

/// Formats the pluralized "N SSH tunnel(s)" import-preview count line.
pub(crate) fn import_preview_count_ssh_tunnels(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!(
            "connection_manager.import.preview.count.ssh_tunnels.one",
            count = count
        )
    } else {
        dbflux_i18n::t!(
            "connection_manager.import.preview.count.ssh_tunnels.many",
            count = count
        )
    }
}

/// Formats the pluralized "N proxy profile(s)" import-preview count line.
pub(crate) fn import_preview_count_proxies(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!(
            "connection_manager.import.preview.count.proxies.one",
            count = count
        )
    } else {
        dbflux_i18n::t!(
            "connection_manager.import.preview.count.proxies.many",
            count = count
        )
    }
}

/// Formats the pluralized import-preview "already exist at the destination" conflicts banner.
pub(crate) fn import_preview_conflicts_banner(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!(
            "connection_manager.import.preview.conflicts_banner.one",
            count = count
        )
    } else {
        dbflux_i18n::t!(
            "connection_manager.import.preview.conflicts_banner.many",
            count = count
        )
    }
}

/// Formats the pluralized import-preview "may be required after import" banner.
pub(crate) fn import_preview_required_banner(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!(
            "connection_manager.import.preview.required_banner.one",
            count = count
        )
    } else {
        dbflux_i18n::t!(
            "connection_manager.import.preview.required_banner.many",
            count = count
        )
    }
}

/// Resolves the translated label for an import conflict's entity kind.
pub(crate) fn import_conflict_kind_label(kind: dbflux_portability::ConflictKind) -> String {
    use dbflux_portability::ConflictKind;

    match kind {
        ConflictKind::AuthProfile => {
            dbflux_i18n::t!("connection_manager.import.conflict_kind.auth_profile")
        }
        ConflictKind::SshTunnel => {
            dbflux_i18n::t!("connection_manager.import.conflict_kind.ssh_tunnel")
        }
        ConflictKind::Proxy => dbflux_i18n::t!("connection_manager.import.conflict_kind.proxy"),
        ConflictKind::Connection => {
            dbflux_i18n::t!("connection_manager.import.conflict_kind.connection")
        }
    }
}

/// Formats the "<kind>: "<bundle name>" conflicts with "<existing name>"" import conflict row label.
pub(crate) fn import_conflicts_row_label(
    kind_label: &str,
    bundle_name: &str,
    existing_name: &str,
) -> String {
    dbflux_i18n::t!(
        "connection_manager.import.conflicts.row_label",
        kind = kind_label,
        bundle_name = bundle_name,
        existing_name = existing_name
    )
}

/// Formats the "Secret for "<owner>": <field>" import required-resolution label.
pub(crate) fn import_required_secret_label(owner: &str, field: &str) -> String {
    dbflux_i18n::t!(
        "connection_manager.import.required.secret_label",
        owner = owner,
        field = field
    )
}

/// Formats the "AWS auth profile "<name>" (<provider>) for "<owner>"" import required-resolution label.
pub(crate) fn import_required_aws_reference_label(
    name: &str,
    provider_id: &str,
    owner: &str,
) -> String {
    dbflux_i18n::t!(
        "connection_manager.import.required.aws_reference_label",
        name = name,
        provider_id = provider_id,
        owner = owner
    )
}

/// Formats the "Auth profile for "<owner>": <field>" import required-resolution label.
pub(crate) fn import_required_auth_profile_ref_label(owner: &str, field: &str) -> String {
    dbflux_i18n::t!(
        "connection_manager.import.required.auth_profile_ref_label",
        owner = owner,
        field = field
    )
}

/// Formats the pluralized "Imported N entity/entities." import success toast.
pub(crate) fn import_status_imported_toast(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!(
            "connection_manager.import.status.imported_toast.one",
            count = count
        )
    } else {
        dbflux_i18n::t!(
            "connection_manager.import.status.imported_toast.many",
            count = count
        )
    }
}

/// Formats the pluralized "N entity/entities imported." import-outcome success banner.
pub(crate) fn import_outcome_succeeded(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!(
            "connection_manager.import.outcome.succeeded.one",
            count = count
        )
    } else {
        dbflux_i18n::t!(
            "connection_manager.import.outcome.succeeded.many",
            count = count
        )
    }
}

/// Formats the pluralized "N connection(s) skipped — driver not installed." import-outcome banner.
pub(crate) fn import_outcome_needs_driver(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!(
            "connection_manager.import.outcome.needs_driver.one",
            count = count
        )
    } else {
        dbflux_i18n::t!(
            "connection_manager.import.outcome.needs_driver.many",
            count = count
        )
    }
}

/// Formats the pluralized "N connection(s) could not be configured." import-outcome banner.
pub(crate) fn import_outcome_config_failures(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!(
            "connection_manager.import.outcome.config_failures.one",
            count = count
        )
    } else {
        dbflux_i18n::t!(
            "connection_manager.import.outcome.config_failures.many",
            count = count
        )
    }
}

/// Formats the pluralized "N connection(s) had unresolvable references..." import-outcome banner.
pub(crate) fn import_outcome_unresolved_refs(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!(
            "connection_manager.import.outcome.unresolved_refs.one",
            count = count
        )
    } else {
        dbflux_i18n::t!(
            "connection_manager.import.outcome.unresolved_refs.many",
            count = count
        )
    }
}

/// Formats the pluralized "N secret(s) could not be written to the keyring..." import-outcome banner.
pub(crate) fn import_outcome_secret_failures(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!(
            "connection_manager.import.outcome.secret_failures.one",
            count = count
        )
    } else {
        dbflux_i18n::t!(
            "connection_manager.import.outcome.secret_failures.many",
            count = count
        )
    }
}

/// Formats the "Map to: <candidate name>" import conflict-resolution segment label.
pub(crate) fn import_action_map_to(name: &str) -> String {
    dbflux_i18n::t!("connection_manager.import.action.map_to", name = name)
}

/// Formats the "Use: <profile name>" import required-reference segment label.
pub(crate) fn import_action_use_profile(name: &str) -> String {
    dbflux_i18n::t!("connection_manager.import.action.use_profile", name = name)
}

#[cfg(test)]
mod tests {
    use super::{
        access_proxy_disabled_label, audit_save_failed_copy, auth_login_failed,
        auth_login_starting, auth_profiles_conflict_message, auth_profiles_login_completed,
        auth_profiles_login_failed, auth_profiles_partial_saved_message,
        auth_profiles_starting_login, auth_provider_unavailable, auth_session_status_valid_expires,
        hooks_create_dir_failed, hooks_delete_message, hooks_delete_unreadable_message,
        hooks_duplicate_id, hooks_env_pair_invalid, hooks_form_interpreter_hint,
        hooks_interpreter_auto_label, hooks_interpreter_missing, hooks_open_script_failed,
        hooks_write_script_failed, import_action_map_to, import_action_use_profile,
        import_conflict_kind_label, import_conflicts_row_label, import_error_cannot_read_file,
        import_error_decryption_error, import_error_import_failed, import_error_parse_error,
        import_error_secret_failures_toast, import_outcome_config_failures,
        import_outcome_needs_driver, import_outcome_secret_failures, import_outcome_succeeded,
        import_outcome_unresolved_refs, import_preview_conflicts_banner,
        import_preview_count_auth_profiles, import_preview_count_connections,
        import_preview_count_proxies, import_preview_count_ssh_tunnels,
        import_preview_required_banner, import_required_auth_profile_ref_label,
        import_required_aws_reference_label, import_required_secret_label,
        import_status_imported_toast, mcp_preview_summary, proxies_delete_body,
        ssh_private_key_with_path, ssh_tunnels_delete_body,
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

    #[test]
    fn audit_save_failed_copy_embeds_error_cause() {
        let message = audit_save_failed_copy("disk full");

        assert!(message.contains("disk full"));
    }

    #[test]
    fn import_error_helpers_embed_error_cause() {
        assert!(import_error_cannot_read_file("permission denied").contains("permission denied"));
        assert!(import_error_parse_error("unexpected token").contains("unexpected token"));
        assert!(import_error_decryption_error("bad tag").contains("bad tag"));
        assert!(import_error_import_failed("disk full").contains("disk full"));
    }

    #[test]
    fn import_preview_count_helpers_use_singular_for_one() {
        assert_eq!(import_preview_count_connections(1), "1 connection");
        assert_eq!(import_preview_count_auth_profiles(1), "1 auth profile");
        assert_eq!(import_preview_count_ssh_tunnels(1), "1 SSH tunnel");
        assert_eq!(import_preview_count_proxies(1), "1 proxy profile");
    }

    #[test]
    fn import_preview_count_helpers_use_plural_for_many() {
        assert_eq!(import_preview_count_connections(3), "3 connections");
        assert_eq!(import_preview_count_auth_profiles(2), "2 auth profiles");
        assert_eq!(import_preview_count_ssh_tunnels(2), "2 SSH tunnels");
        assert_eq!(import_preview_count_proxies(2), "2 proxy profiles");
    }

    #[test]
    fn import_preview_banners_pluralize_by_count() {
        let one = import_preview_conflicts_banner(1);
        let many = import_preview_conflicts_banner(3);
        assert!(one.contains('1'));
        assert!(many.contains('3'));
        assert_ne!(one, many);

        let required_one = import_preview_required_banner(1);
        let required_many = import_preview_required_banner(2);
        assert!(required_one.contains('1'));
        assert!(required_many.contains('2'));
        assert_ne!(required_one, required_many);
    }

    #[test]
    fn import_conflict_kind_label_is_exhaustive_over_the_enum() {
        use dbflux_portability::ConflictKind;

        for kind in [
            ConflictKind::AuthProfile,
            ConflictKind::SshTunnel,
            ConflictKind::Proxy,
            ConflictKind::Connection,
        ] {
            let label = import_conflict_kind_label(kind);
            assert!(!label.is_empty(), "{kind:?} resolved an empty label");
        }
    }

    #[test]
    fn import_conflicts_row_label_embeds_kind_and_names() {
        let message = import_conflicts_row_label("SSH tunnel", "bastion-a", "bastion-b");

        assert!(message.contains("SSH tunnel"));
        assert!(message.contains("bastion-a"));
        assert!(message.contains("bastion-b"));
    }

    #[test]
    fn import_required_label_helpers_embed_their_arguments() {
        assert!(import_required_secret_label("prod-mongo", "password").contains("prod-mongo"));
        assert!(import_required_secret_label("prod-mongo", "password").contains("password"));

        let aws = import_required_aws_reference_label(
            "prod-sso",
            "aws-iam-identity-center",
            "prod-mongo",
        );
        assert!(aws.contains("prod-sso"));
        assert!(aws.contains("aws-iam-identity-center"));
        assert!(aws.contains("prod-mongo"));

        let profile_ref = import_required_auth_profile_ref_label("prod-mongo", "auth_profile_id");
        assert!(profile_ref.contains("prod-mongo"));
        assert!(profile_ref.contains("auth_profile_id"));
    }

    #[test]
    fn import_status_imported_toast_uses_singular_form_for_one() {
        assert_eq!(import_status_imported_toast(1), "Imported 1 entity.");
        assert_eq!(import_status_imported_toast(3), "Imported 3 entities.");
    }

    #[test]
    fn import_outcome_banners_pluralize_by_count() {
        assert_eq!(import_outcome_succeeded(1), "1 entity imported.");
        assert_eq!(import_outcome_succeeded(2), "2 entities imported.");

        assert!(import_outcome_needs_driver(1).contains('1'));
        assert!(import_outcome_needs_driver(2).contains('2'));

        assert!(import_outcome_config_failures(1).contains('1'));
        assert!(import_outcome_config_failures(2).contains('2'));

        assert!(import_outcome_unresolved_refs(1).contains('1'));
        assert!(import_outcome_unresolved_refs(2).contains('2'));

        assert!(import_outcome_secret_failures(1).contains('1'));
        assert!(import_outcome_secret_failures(2).contains('2'));
    }

    #[test]
    fn import_error_secret_failures_toast_pluralizes_by_count() {
        assert!(import_error_secret_failures_toast(1).contains('1'));
        assert!(import_error_secret_failures_toast(2).contains('2'));
    }

    #[test]
    fn import_action_segment_labels_embed_their_names() {
        assert!(import_action_map_to("staging-db").contains("staging-db"));
        assert!(import_action_use_profile("prod-sso").contains("prod-sso"));
    }
}
