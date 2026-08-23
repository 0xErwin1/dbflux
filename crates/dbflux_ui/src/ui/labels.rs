//! Translation helpers shared across `dbflux_ui` shell chrome.
//!
//! These wrap [`dbflux_i18n::t!`] calls that need named arguments, plural
//! selection, or an exhaustive match so render code can build the label
//! once instead of repeating the substitution inline on every render pass.

use dbflux_core::ShutdownPhase;

/// Formats the "N running" status-bar label for the current task count.
pub(crate) fn tasks_running_label(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!("status_bar.tasks_running.one")
    } else {
        dbflux_i18n::t!("status_bar.tasks_running.many", count = count)
    }
}

/// Formats the shutdown overlay message for the given phase.
///
/// `NotStarted` has no visible message — the overlay only renders while
/// `ShutdownPhase::is_active()` is true — so it resolves to an empty string
/// like the phase it mirrors.
pub(crate) fn shutdown_phase_label(phase: ShutdownPhase) -> String {
    match phase {
        ShutdownPhase::NotStarted => String::new(),
        ShutdownPhase::SignalSent => dbflux_i18n::t!("shutdown.signal_sent"),
        ShutdownPhase::CancellingTasks => dbflux_i18n::t!("shutdown.cancelling_tasks"),
        ShutdownPhase::ClosingConnections => dbflux_i18n::t!("shutdown.closing_connections"),
        ShutdownPhase::FlushingLogs => dbflux_i18n::t!("shutdown.flushing_logs"),
        ShutdownPhase::Complete => dbflux_i18n::t!("shutdown.complete"),
        ShutdownPhase::Failed => dbflux_i18n::t!("shutdown.failed"),
    }
}

/// Formats the confirmation prompt for deleting several selected items.
pub(crate) fn workspace_delete_selected_message(count: usize) -> String {
    dbflux_i18n::t!("workspace.confirm.delete_selected", count = count)
}

/// Formats the confirmation prompt for a DDL drop, falling back to a
/// generic object-type label when the sidebar didn't supply one.
pub(crate) fn workspace_drop_object_message(object_type: Option<&str>, name: &str) -> String {
    let object_type = object_type
        .map(str::to_string)
        .unwrap_or_else(|| dbflux_i18n::t!("workspace.default_object_type"));

    dbflux_i18n::t!(
        "workspace.confirm.drop_object",
        object_type = object_type,
        name = name
    )
}

/// Formats the confirmation prompt for deleting a sidebar folder.
pub(crate) fn workspace_delete_folder_message(name: &str) -> String {
    dbflux_i18n::t!("workspace.confirm.delete_folder", name = name)
}

/// Formats the confirmation prompt for deleting a connection.
pub(crate) fn workspace_delete_connection_message(name: &str) -> String {
    dbflux_i18n::t!("workspace.confirm.delete_connection", name = name)
}

/// Formats the login modal's "Sign in with X to continue connecting Y" prompt.
pub(crate) fn login_sign_in_prompt(provider_name: &str, profile_name: &str) -> String {
    dbflux_i18n::t!(
        "login.body.sign_in_prompt",
        provider = provider_name,
        profile = profile_name
    )
}

/// Formats the "Elapsed: Ns" caption shown while waiting for login to complete.
pub(crate) fn login_elapsed_message(elapsed_secs: u64) -> String {
    dbflux_i18n::t!("login.body.elapsed", seconds = elapsed_secs)
}

/// Formats the fallback message shown when the login browser could not be launched.
pub(crate) fn login_browser_open_failed_message(
    error: impl std::fmt::Display,
    fallback_error: impl std::fmt::Display,
) -> String {
    dbflux_i18n::t!(
        "login.body.browser_open_failed",
        error = error,
        fallback_error = fallback_error
    )
}

/// Formats the error shown when a saved chart fails source validation on open.
pub(crate) fn charts_cannot_open_chart_message(error: impl std::fmt::Display) -> String {
    dbflux_i18n::t!("charts.error.cannot_open_chart", error = error)
}

/// Formats the error shown when a dashboard import parse/build step fails.
pub(crate) fn charts_import_failed_message(error: impl std::fmt::Display) -> String {
    dbflux_i18n::t!("charts.error.import_failed", error = error)
}

/// Formats the error shown when an imported chart fails to persist.
pub(crate) fn charts_persist_import_chart_failed_message(name: &str) -> String {
    dbflux_i18n::t!("charts.error.persist_import_chart_failed", name = name)
}

/// Formats the error shown when an imported dashboard fails to persist.
pub(crate) fn charts_persist_import_dashboard_failed_message(name: &str) -> String {
    dbflux_i18n::t!("charts.error.persist_import_dashboard_failed", name = name)
}

/// Formats the error shown when a dashboard's charts/panels fail to save.
pub(crate) fn charts_save_dashboard_failed_message(name: &str, message: &str) -> String {
    dbflux_i18n::t!(
        "charts.error.save_dashboard_failed",
        name = name,
        message = message
    )
}

/// Formats the toast shown after a successful dashboard import.
pub(crate) fn charts_import_success_message(count: usize) -> String {
    dbflux_i18n::t!("charts.toast.import_success", count = count)
}

/// Formats the error shown when a remote dashboard fails to parse.
pub(crate) fn charts_remote_parse_failed_message(error: impl std::fmt::Display) -> String {
    dbflux_i18n::t!("charts.error.remote_parse_failed", error = error)
}

/// Formats the error shown when creating a dashboard fails.
pub(crate) fn charts_create_dashboard_failed_message(error: impl std::fmt::Display) -> String {
    dbflux_i18n::t!("charts.error.create_dashboard_failed", error = error)
}

/// Formats the error shown when deleting a dashboard fails.
pub(crate) fn charts_delete_dashboard_failed_message(error: impl std::fmt::Display) -> String {
    dbflux_i18n::t!("charts.error.delete_dashboard_failed", error = error)
}

/// Formats the error shown when duplicating a dashboard fails.
pub(crate) fn charts_duplicate_dashboard_failed_message(error: impl std::fmt::Display) -> String {
    dbflux_i18n::t!("charts.error.duplicate_dashboard_failed", error = error)
}

/// Formats the error shown when renaming a dashboard or saved chart fails.
pub(crate) fn charts_rename_failed_message(error: impl std::fmt::Display) -> String {
    dbflux_i18n::t!("charts.error.rename_failed", error = error)
}

/// Formats the error shown when deleting a saved chart fails.
pub(crate) fn charts_delete_chart_failed_message(error: impl std::fmt::Display) -> String {
    dbflux_i18n::t!("charts.error.delete_chart_failed", error = error)
}

/// Formats the error shown when duplicating a saved chart fails.
pub(crate) fn charts_duplicate_chart_failed_message(error: impl std::fmt::Display) -> String {
    dbflux_i18n::t!("charts.error.duplicate_chart_failed", error = error)
}

/// Formats the error shown when loading metric namespaces fails.
pub(crate) fn charts_load_namespaces_failed_message(error: impl std::fmt::Display) -> String {
    dbflux_i18n::t!("charts.error.load_namespaces_failed", error = error)
}

/// Formats the background-task label shown while loading metrics for a namespace.
pub(crate) fn charts_loading_metrics_label(namespace: &str) -> String {
    dbflux_i18n::t!("charts.status.loading_metrics", namespace = namespace)
}

/// Formats the error shown when loading metrics for a namespace fails.
pub(crate) fn charts_load_metrics_failed_message(error: impl std::fmt::Display) -> String {
    dbflux_i18n::t!("charts.error.load_metrics_failed", error = error)
}

/// Formats the error shown when a query-backed chart fails to persist for a new panel.
pub(crate) fn charts_persist_chart_for_panel_failed_message(name: &str) -> String {
    dbflux_i18n::t!("charts.error.persist_chart_for_panel_failed", name = name)
}

/// Formats the error shown when adding a panel to a dashboard fails.
pub(crate) fn charts_add_panel_failed_message(error: impl std::fmt::Display) -> String {
    dbflux_i18n::t!("charts.error.add_panel_failed", error = error)
}

/// Formats the error shown when a metric-backed chart fails to persist for a new panel.
pub(crate) fn charts_persist_metric_chart_for_panel_failed_message(name: &str) -> String {
    dbflux_i18n::t!(
        "charts.error.persist_metric_chart_for_panel_failed",
        name = name
    )
}

/// Formats the toast shown when a panel for the same metric already exists.
pub(crate) fn charts_panel_already_exists_message(namespace: &str, metric: &str) -> String {
    dbflux_i18n::t!(
        "charts.toast.panel_already_exists",
        namespace = namespace,
        metric = metric
    )
}

/// Formats the error shown when adding several panels to a dashboard fails.
pub(crate) fn charts_add_panels_failed_message(error: impl std::fmt::Display) -> String {
    dbflux_i18n::t!("charts.error.add_panels_failed", error = error)
}

/// Formats the toast shown when the driver has no Instance Overview dashboard.
pub(crate) fn charts_instance_overview_no_dashboard_message() -> String {
    dbflux_i18n::t!("charts.instance_overview.toast.no_dashboard_defined")
}

/// Formats the toast shown after cloning a read-only overview into an editable dashboard.
pub(crate) fn charts_instance_overview_created_editable_message() -> String {
    dbflux_i18n::t!("charts.instance_overview.toast.created_editable")
}

/// Formats the persisted display name for a dashboard cloned from a read-only
/// Instance Overview into an editable copy.
pub(crate) fn charts_instance_overview_editable_name(source_title: &str) -> String {
    dbflux_i18n::t!(
        "charts.instance_overview.editable_name",
        name = source_title
    )
}

/// Formats the error shown when cloning an overview into an editable dashboard fails.
pub(crate) fn charts_instance_overview_create_editable_failed_message(
    error: impl std::fmt::Display,
) -> String {
    dbflux_i18n::t!(
        "charts.instance_overview.error.create_editable_failed",
        error = error
    )
}

/// Formats the "Connection Manager" window title.
pub(crate) fn connections_manager_window_title() -> String {
    dbflux_i18n::t!("connections.window.manager_title")
}

/// Formats the "Edit Connection" window title.
pub(crate) fn connections_edit_window_title() -> String {
    dbflux_i18n::t!("connections.window.edit_title")
}

/// Formats the "Disconnecting from X..." toast shown while tearing down a connection.
pub(crate) fn connections_disconnecting_message(name: &str) -> String {
    dbflux_i18n::t!("connections.toast.disconnecting", name = name)
}

/// Formats the "No active connection" warning toast for schema refresh.
pub(crate) fn connections_no_active_connection_message() -> String {
    dbflux_i18n::t!("connections.toast.no_active_connection")
}

/// Formats the "Refreshing schema..." toast shown while a schema reload is in flight.
pub(crate) fn connections_refreshing_schema_message() -> String {
    dbflux_i18n::t!("connections.toast.refreshing_schema")
}

/// Formats the error shown when a schema refresh fails.
pub(crate) fn connections_refresh_schema_failed_message(error: impl std::fmt::Display) -> String {
    dbflux_i18n::t!("connections.error.refresh_schema_failed", error = error)
}

/// Formats the "Open Script" file dialog title.
pub(crate) fn scripts_open_dialog_title() -> String {
    dbflux_i18n::t!("scripts.dialog.title")
}

/// Formats the SQL file-dialog filter label.
pub(crate) fn scripts_filter_sql_label() -> String {
    dbflux_i18n::t!("scripts.dialog.filter.sql")
}

/// Formats the MongoDB JavaScript file-dialog filter label.
pub(crate) fn scripts_filter_javascript_mongodb_label() -> String {
    dbflux_i18n::t!("scripts.dialog.filter.javascript_mongodb")
}

/// Formats the Redis file-dialog filter label.
pub(crate) fn scripts_filter_redis_label() -> String {
    dbflux_i18n::t!("scripts.dialog.filter.redis")
}

/// Formats the "All Files" file-dialog filter label.
pub(crate) fn scripts_filter_all_files_label() -> String {
    dbflux_i18n::t!("scripts.dialog.filter.all_files")
}

/// Formats the error shown when reading a script file from disk fails.
pub(crate) fn scripts_read_file_failed_message(
    path: impl std::fmt::Display,
    error: impl std::fmt::Display,
) -> String {
    dbflux_i18n::t!(
        "scripts.error.read_file_failed",
        path = path.to_string(),
        error = error
    )
}

/// Formats the "Focusing existing audit viewer" toast.
pub(crate) fn audit_focus_existing_viewer_message() -> String {
    dbflux_i18n::t!("audit.toast.focus_existing_viewer")
}

/// Formats the "Opened audit viewer" toast.
pub(crate) fn audit_opened_viewer_message() -> String {
    dbflux_i18n::t!("audit.toast.opened_viewer")
}

/// Formats the "Opened MCP approvals" toast.
pub(crate) fn audit_opened_mcp_approvals_message() -> String {
    dbflux_i18n::t!("audit.toast.opened_mcp_approvals")
}

/// Formats the "MCP governance state persisted" toast.
pub(crate) fn audit_mcp_governance_persisted_message() -> String {
    dbflux_i18n::t!("audit.toast.mcp_governance_persisted")
}

/// Formats the error shown when the audit viewer cannot open its repository.
pub(crate) fn audit_open_viewer_failed_message(error: impl std::fmt::Display) -> String {
    dbflux_i18n::t!("audit.error.open_viewer_failed", error = error)
}

/// Formats the error shown when persisting MCP governance state fails.
pub(crate) fn audit_persist_mcp_governance_failed_message(error: impl std::fmt::Display) -> String {
    dbflux_i18n::t!("audit.error.persist_mcp_governance_failed", error = error)
}

/// The document/resource kind behind a "No active connection for this X" toast.
///
/// Exhaustively matched by [`documents_no_active_connection_message`] so a new
/// variant without a matching catalog key fails to compile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NoActiveConnectionKind {
    Table,
    Collection,
    EventSource,
    KeyValueDb,
    ObjectStorageAccount,
    Bucket,
    Object,
}

/// Formats the "No active connection for this X" toast for the given resource kind.
pub(crate) fn documents_no_active_connection_message(kind: NoActiveConnectionKind) -> String {
    match kind {
        NoActiveConnectionKind::Table => {
            dbflux_i18n::t!("documents.toast.no_active_connection_for.table")
        }
        NoActiveConnectionKind::Collection => {
            dbflux_i18n::t!("documents.toast.no_active_connection_for.collection")
        }
        NoActiveConnectionKind::EventSource => {
            dbflux_i18n::t!("documents.toast.no_active_connection_for.event_source")
        }
        NoActiveConnectionKind::KeyValueDb => {
            dbflux_i18n::t!("documents.toast.no_active_connection_for.key_value_db")
        }
        NoActiveConnectionKind::ObjectStorageAccount => {
            dbflux_i18n::t!("documents.toast.no_active_connection_for.object_storage_account")
        }
        NoActiveConnectionKind::Bucket => {
            dbflux_i18n::t!("documents.toast.no_active_connection_for.bucket")
        }
        NoActiveConnectionKind::Object => {
            dbflux_i18n::t!("documents.toast.no_active_connection_for.object")
        }
    }
}

/// Formats the default title used for a tab whose real name could not be resolved.
pub(crate) fn documents_default_title() -> String {
    dbflux_i18n::t!("documents.default_title")
}

/// Formats the default file name used for a freshly created query tab.
pub(crate) fn documents_new_query_name() -> String {
    dbflux_i18n::t!("documents.new_query_name")
}

/// Formats the "Opened schema diff" toast.
pub(crate) fn documents_schema_diff_opened_message() -> String {
    dbflux_i18n::t!("documents.toast.schema_diff_opened")
}

/// Formats the error shown when writing a new query tab's initial script content fails.
pub(crate) fn documents_write_initial_script_failed_message(
    error: impl std::fmt::Display,
) -> String {
    dbflux_i18n::t!("documents.error.write_initial_script_failed", error = error)
}

/// Formats the error shown when the workspace session manifest fails to save.
pub(crate) fn documents_save_session_failed_message() -> String {
    dbflux_i18n::t!("documents.error.save_session_failed")
}

/// Formats the fallback profile name used when opening the login modal manually
/// with no active connection to name it after.
pub(crate) fn settings_default_connection_name() -> String {
    dbflux_i18n::t!("settings.action.default_connection_name")
}

#[cfg(test)]
mod tests {
    use super::{
        NoActiveConnectionKind, audit_focus_existing_viewer_message,
        audit_mcp_governance_persisted_message, audit_open_viewer_failed_message,
        audit_opened_mcp_approvals_message, audit_opened_viewer_message,
        audit_persist_mcp_governance_failed_message,
        charts_instance_overview_create_editable_failed_message,
        charts_instance_overview_created_editable_message, charts_instance_overview_editable_name,
        charts_instance_overview_no_dashboard_message, connections_disconnecting_message,
        connections_edit_window_title, connections_manager_window_title,
        connections_no_active_connection_message, connections_refresh_schema_failed_message,
        connections_refreshing_schema_message, documents_default_title, documents_new_query_name,
        documents_no_active_connection_message, documents_save_session_failed_message,
        documents_schema_diff_opened_message, documents_write_initial_script_failed_message,
        scripts_filter_all_files_label, scripts_filter_javascript_mongodb_label,
        scripts_filter_redis_label, scripts_filter_sql_label, scripts_open_dialog_title,
        scripts_read_file_failed_message, settings_default_connection_name, shutdown_phase_label,
        tasks_running_label, workspace_delete_connection_message, workspace_delete_folder_message,
        workspace_delete_selected_message, workspace_drop_object_message,
    };
    use dbflux_core::ShutdownPhase;

    const WORKSPACE_CATALOG_KEYS: &[&str] = &[
        "workspace.background_tasks",
        "workspace.empty_documents",
        "workspace.hint.new_query",
        "workspace.hint.command_palette",
        "workspace.hint.open",
        "workspace.hint.new_connection",
        "workspace.mcp_approvals",
        "workspace.event_streams",
        "workspace.action.delete",
        "workspace.action.drop",
        "workspace.action.delete_folder",
        "workspace.action.delete_connection",
        "workspace.action.cancel",
        "workspace.confirm.delete_selected",
        "workspace.confirm.drop_object",
        "workspace.confirm.delete_folder",
        "workspace.confirm.delete_connection",
        "workspace.default_object_type",
    ];

    const STATUS_BAR_AND_SHELL_CATALOG_KEYS: &[&str] = &[
        "status_bar.disconnected",
        "status_bar.tasks_label",
        "status_bar.tasks_running.one",
        "status_bar.tasks_running.many",
        "tasks_panel.empty",
        "tasks_panel.output_truncated",
        "shutdown.signal_sent",
        "shutdown.cancelling_tasks",
        "shutdown.closing_connections",
        "shutdown.flushing_logs",
        "shutdown.complete",
        "shutdown.failed",
    ];

    #[test]
    fn workspace_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for key in WORKSPACE_CATALOG_KEYS {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(
                    !value.is_empty(),
                    "key {key} resolved empty for locale {locale}"
                );
                assert_ne!(value, *key, "key {key} did not resolve for locale {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "key {key} fell back to the raw locale-qualified form for locale {locale}"
                );
            }
        }
    }

    #[test]
    fn status_bar_and_shell_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for key in STATUS_BAR_AND_SHELL_CATALOG_KEYS {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(
                    !value.is_empty(),
                    "key {key} resolved empty for locale {locale}"
                );
                assert_ne!(value, *key, "key {key} did not resolve for locale {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "key {key} fell back to the raw locale-qualified form for locale {locale}"
                );
            }
        }
    }

    #[test]
    fn workspace_background_tasks_differs_between_locales() {
        let english = dbflux_i18n::t!("workspace.background_tasks", locale = "en");
        let spanish = dbflux_i18n::t!("workspace.background_tasks", locale = "es");

        assert_eq!(english, "Background Tasks");
        assert_eq!(spanish, "Tareas en segundo plano");
        assert_ne!(english, spanish);
    }

    #[test]
    fn tasks_running_label_uses_singular_and_plural_forms() {
        assert!(tasks_running_label(1).contains('1'));
        assert!(tasks_running_label(0).contains('0'));
        assert!(tasks_running_label(3).contains('3'));
        assert_ne!(tasks_running_label(1), tasks_running_label(3));
        assert_ne!(tasks_running_label(1), tasks_running_label(0));
    }

    #[test]
    fn shutdown_phase_label_is_exhaustive_and_not_started_is_empty() {
        assert_eq!(shutdown_phase_label(ShutdownPhase::NotStarted), "");

        for phase in [
            ShutdownPhase::SignalSent,
            ShutdownPhase::CancellingTasks,
            ShutdownPhase::ClosingConnections,
            ShutdownPhase::FlushingLogs,
            ShutdownPhase::Complete,
            ShutdownPhase::Failed,
        ] {
            assert!(
                !shutdown_phase_label(phase).is_empty(),
                "{phase:?} resolved to an empty message"
            );
        }
    }

    #[test]
    fn workspace_delete_selected_message_embeds_count() {
        let message = workspace_delete_selected_message(3);

        assert!(message.contains('3'));
    }

    #[test]
    fn workspace_drop_object_message_falls_back_to_default_object_type() {
        let with_type = workspace_drop_object_message(Some("Table"), "users");
        let without_type = workspace_drop_object_message(None, "users");

        assert!(with_type.contains("Table"));
        assert!(with_type.contains("users"));
        assert!(without_type.contains("users"));
        assert_ne!(with_type, without_type);
    }

    #[test]
    fn workspace_delete_folder_message_embeds_name() {
        let message = workspace_delete_folder_message("scratch");

        assert!(message.contains("scratch"));
    }

    #[test]
    fn workspace_delete_connection_message_embeds_name() {
        let message = workspace_delete_connection_message("prod-db");

        assert!(message.contains("prod-db"));
    }

    const WORKSPACE_ACTIONS_CATALOG_KEYS: &[&str] = &[
        "connections.window.manager_title",
        "connections.window.edit_title",
        "connections.toast.disconnecting",
        "connections.toast.no_active_connection",
        "connections.toast.refreshing_schema",
        "connections.error.refresh_schema_failed",
        "scripts.dialog.title",
        "scripts.dialog.filter.sql",
        "scripts.dialog.filter.javascript_mongodb",
        "scripts.dialog.filter.redis",
        "scripts.dialog.filter.all_files",
        "scripts.error.read_file_failed",
        "audit.toast.focus_existing_viewer",
        "audit.toast.opened_viewer",
        "audit.toast.opened_mcp_approvals",
        "audit.toast.mcp_governance_persisted",
        "audit.error.open_viewer_failed",
        "audit.error.persist_mcp_governance_failed",
        "documents.toast.no_active_connection_for.table",
        "documents.toast.no_active_connection_for.collection",
        "documents.toast.no_active_connection_for.event_source",
        "documents.toast.no_active_connection_for.key_value_db",
        "documents.toast.no_active_connection_for.object_storage_account",
        "documents.toast.no_active_connection_for.bucket",
        "documents.toast.no_active_connection_for.object",
        "documents.default_title",
        "documents.new_query_name",
        "documents.toast.schema_diff_opened",
        "documents.error.write_initial_script_failed",
        "documents.error.save_session_failed",
        "settings.action.default_connection_name",
        "charts.instance_overview.toast.no_dashboard_defined",
        "charts.instance_overview.toast.created_editable",
        "charts.instance_overview.editable_name",
        "charts.instance_overview.error.create_editable_failed",
    ];

    #[test]
    fn workspace_actions_keys_resolve_in_both_locales() {
        for locale in ["en", "es"] {
            for key in WORKSPACE_ACTIONS_CATALOG_KEYS {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert!(
                    !value.is_empty(),
                    "key {key} resolved empty for locale {locale}"
                );
                assert_ne!(value, *key, "key {key} did not resolve for locale {locale}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "key {key} fell back to the raw locale-qualified form for locale {locale}"
                );
            }
        }
    }

    #[test]
    fn connections_manager_and_edit_window_titles_differ_between_locales() {
        let manager_en = dbflux_i18n::t!("connections.window.manager_title", locale = "en");
        let manager_es = dbflux_i18n::t!("connections.window.manager_title", locale = "es");
        assert_ne!(manager_en, manager_es);
        assert_eq!(manager_en, connections_manager_window_title());

        let edit_en = dbflux_i18n::t!("connections.window.edit_title", locale = "en");
        let edit_es = dbflux_i18n::t!("connections.window.edit_title", locale = "es");
        assert_ne!(edit_en, edit_es);
        assert_eq!(edit_en, connections_edit_window_title());
    }

    #[test]
    fn connections_disconnecting_message_embeds_name() {
        let message = connections_disconnecting_message("prod-db");
        assert!(message.contains("prod-db"));
    }

    #[test]
    fn connections_no_active_connection_and_refreshing_schema_messages_resolve() {
        assert!(!connections_no_active_connection_message().is_empty());
        assert!(!connections_refreshing_schema_message().is_empty());
    }

    #[test]
    fn connections_refresh_schema_failed_message_embeds_error() {
        let message = connections_refresh_schema_failed_message("driver timeout");
        assert!(message.contains("driver timeout"));
    }

    #[test]
    fn scripts_open_dialog_title_and_filters_resolve() {
        assert!(!scripts_open_dialog_title().is_empty());
        assert!(!scripts_filter_sql_label().is_empty());
        assert!(!scripts_filter_javascript_mongodb_label().is_empty());
        assert!(!scripts_filter_redis_label().is_empty());
        assert!(!scripts_filter_all_files_label().is_empty());
    }

    #[test]
    fn scripts_read_file_failed_message_embeds_path_and_error() {
        let message = scripts_read_file_failed_message("/tmp/query.sql", "permission denied");
        assert!(message.contains("/tmp/query.sql"));
        assert!(message.contains("permission denied"));
    }

    #[test]
    fn audit_toast_messages_resolve() {
        assert!(!audit_focus_existing_viewer_message().is_empty());
        assert!(!audit_opened_viewer_message().is_empty());
        assert!(!audit_opened_mcp_approvals_message().is_empty());
        assert!(!audit_mcp_governance_persisted_message().is_empty());
    }

    #[test]
    fn audit_error_messages_embed_error() {
        assert!(audit_open_viewer_failed_message("disk full").contains("disk full"));
        assert!(
            audit_persist_mcp_governance_failed_message("write failed").contains("write failed")
        );
    }

    #[test]
    fn documents_no_active_connection_message_is_exhaustive_and_distinct_per_kind() {
        let kinds = [
            NoActiveConnectionKind::Table,
            NoActiveConnectionKind::Collection,
            NoActiveConnectionKind::EventSource,
            NoActiveConnectionKind::KeyValueDb,
            NoActiveConnectionKind::ObjectStorageAccount,
            NoActiveConnectionKind::Bucket,
            NoActiveConnectionKind::Object,
        ];

        let mut messages = Vec::with_capacity(kinds.len());
        for kind in kinds {
            let message = documents_no_active_connection_message(kind);
            assert!(!message.is_empty(), "{kind:?} resolved to an empty message");
            messages.push(message);
        }

        let unique: std::collections::HashSet<_> = messages.iter().collect();
        assert_eq!(
            unique.len(),
            messages.len(),
            "every NoActiveConnectionKind must resolve to a distinct message"
        );
    }

    #[test]
    fn documents_default_title_and_new_query_name_resolve() {
        assert!(!documents_default_title().is_empty());
        assert!(!documents_new_query_name().is_empty());
    }

    #[test]
    fn documents_schema_diff_opened_message_resolves() {
        assert!(!documents_schema_diff_opened_message().is_empty());
    }

    #[test]
    fn documents_error_messages_resolve() {
        assert!(documents_write_initial_script_failed_message("disk full").contains("disk full"));
        assert!(!documents_save_session_failed_message().is_empty());
    }

    #[test]
    fn settings_default_connection_name_resolves() {
        assert!(!settings_default_connection_name().is_empty());
    }

    #[test]
    fn charts_instance_overview_messages_resolve() {
        assert!(!charts_instance_overview_no_dashboard_message().is_empty());
        assert!(!charts_instance_overview_created_editable_message().is_empty());
        assert!(
            charts_instance_overview_create_editable_failed_message("disk full")
                .contains("disk full")
        );
    }

    #[test]
    fn charts_instance_overview_editable_name_embeds_source_title() {
        let name = charts_instance_overview_editable_name("Instance Overview");
        assert!(name.contains("Instance Overview"));
    }
}
