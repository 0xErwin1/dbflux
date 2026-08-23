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

#[cfg(test)]
mod tests {
    use super::{
        shutdown_phase_label, tasks_running_label, workspace_delete_connection_message,
        workspace_delete_folder_message, workspace_delete_selected_message,
        workspace_drop_object_message,
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
}
