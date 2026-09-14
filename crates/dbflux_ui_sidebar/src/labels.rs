//! Translated label helpers for sidebar chrome (tabs, footer, tree folders).

use dbflux_app::ExternalDriverStage;
use dbflux_core::{DatabaseCategory, PipelineState, RelationKind, SchemaObjectKind};

/// Translated label for a schema-tree container folder, e.g. `"Tables (12)"`.
pub(crate) fn container_folder_label(category: DatabaseCategory, count: usize) -> String {
    match category {
        DatabaseCategory::Relational => {
            dbflux_i18n::t!("sidebar.tree.container.relational", count = count)
        }
        DatabaseCategory::Document => {
            dbflux_i18n::t!("sidebar.tree.container.document", count = count)
        }
        DatabaseCategory::KeyValue => {
            dbflux_i18n::t!("sidebar.tree.container.key_value", count = count)
        }
        DatabaseCategory::Graph => dbflux_i18n::t!("sidebar.tree.container.graph", count = count),
        DatabaseCategory::TimeSeries => {
            dbflux_i18n::t!("sidebar.tree.container.time_series", count = count)
        }
        DatabaseCategory::WideColumn => {
            dbflux_i18n::t!("sidebar.tree.container.wide_column", count = count)
        }
        DatabaseCategory::LogStream => {
            dbflux_i18n::t!("sidebar.tree.container.log_stream", count = count)
        }
        DatabaseCategory::ObjectStorage => {
            dbflux_i18n::t!("sidebar.tree.container.object_storage", count = count)
        }
    }
}

/// Translated footer summary of connected vs. idle connections, e.g.
/// `"2 connected · 5 idle"`.
pub(crate) fn footer_counts_label(connected: usize, idle: usize) -> String {
    dbflux_i18n::t!(
        "sidebar.status.connection_summary",
        connected = connected,
        idle = idle
    )
}

/// Translated page indicator for the collection child picker, e.g.
/// `"Page 1/3 (1-50)"`. `page` and `pages` are 1-based, `from`/`to` are the
/// 1-based inclusive row range shown on the current page.
pub(crate) fn page_label(page: usize, pages: usize, from: usize, to: usize) -> String {
    if pages == 0 {
        return dbflux_i18n::t!("sidebar.overlay.child_picker.page_label_empty");
    }

    dbflux_i18n::t!(
        "sidebar.overlay.child_picker.page_label",
        current = page,
        total = pages,
        start = from,
        end = to
    )
}

/// Translated child-picker modal title, e.g. `"Event streams: orders"`.
pub(crate) fn child_picker_title(collection: &str) -> String {
    dbflux_i18n::t!("sidebar.overlay.child_picker.title", name = collection)
}

/// Translated toast headline reporting a connection profile was updated,
/// e.g. `"'prod-db' updated"`.
pub(crate) fn profile_updated_label(name: &str) -> String {
    dbflux_i18n::t!("sidebar.toast.edit_reconnect_updated", name = name)
}

/// Translated label for the Export Table(s) context menu item, e.g.
/// `"Export Table…"` for a single table or `"Export 3 Tables…"` for many.
pub(crate) fn export_tables_label(count: usize) -> String {
    if count > 1 {
        dbflux_i18n::t!("sidebar.menu.export_tables_many", count = count)
    } else {
        dbflux_i18n::t!("sidebar.menu.export_table")
    }
}

/// Translated label for the Migrate Table(s) context menu item, e.g.
/// `"Migrate Table…"` for a single table or `"Migrate 3 Tables…"` for many.
pub(crate) fn migrate_tables_label(count: usize) -> String {
    if count > 1 {
        dbflux_i18n::t!("sidebar.menu.migrate_tables_many", count = count)
    } else {
        dbflux_i18n::t!("sidebar.menu.migrate_table")
    }
}

/// Translated label for the batch/single Delete context menu item, e.g.
/// `"Delete"` for a single item or `"Delete 3 items"` for a multi-selection.
pub(crate) fn delete_items_label(count: usize) -> String {
    if count > 1 {
        dbflux_i18n::t!("sidebar.menu.delete_count", count = count)
    } else {
        dbflux_i18n::t!("sidebar.menu.delete")
    }
}

/// Translated label for a connecting profile row, e.g. `"Connecting to
/// prod-db…"`.
pub(crate) fn profile_connecting_label(name: &str) -> String {
    dbflux_i18n::t!("sidebar.tree.status.profile_connecting", name = name)
}

/// Translated label for a database node still loading its schema, e.g.
/// `"orders (loading…)"`.
pub(crate) fn node_loading_label(name: &str) -> String {
    dbflux_i18n::t!("sidebar.tree.status.database_loading", name = name)
}

/// Translated label for a retryable fetch error sentinel row, e.g.
/// `"Error: access denied — click to retry"`.
pub(crate) fn error_retry_label(error: &str) -> String {
    dbflux_i18n::t!("sidebar.tree.status.error_retry", error = error)
}

/// Translated label for the table-dependents folder, e.g. `"Used by 1
/// object"` or `"Used by 3 objects"`.
pub(crate) fn used_by_label(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!("sidebar.tree.status.used_by_objects.one")
    } else {
        dbflux_i18n::t!("sidebar.tree.status.used_by_objects.many", count = count)
    }
}

/// Translated kind suffix for a table-dependents child row, e.g. `"orders_v
/// (View)"`. `Trigger` intentionally stays untranslated in every locale.
pub(crate) fn dependent_kind_label(kind: &RelationKind) -> String {
    match kind {
        RelationKind::View => dbflux_i18n::t!("sidebar.tree.status.dependent_kind.view"),
        RelationKind::MaterializedView => {
            dbflux_i18n::t!("sidebar.tree.status.dependent_kind.materialized_view")
        }
        RelationKind::ForeignKeyChild => {
            dbflux_i18n::t!("sidebar.tree.status.dependent_kind.foreign_key_child")
        }
        RelationKind::Trigger => dbflux_i18n::t!("sidebar.tree.status.dependent_kind.trigger"),
    }
}

/// Translated label for a collection's Fields folder, e.g. `"Fields (5)"`.
pub(crate) fn fields_folder_label(count: usize) -> String {
    dbflux_i18n::t!("sidebar.tree.folder.fields", count = count)
}

/// Translated label for an Indexes folder (collection, table, or schema
/// level), e.g. `"Indexes (2)"`.
pub(crate) fn indexes_folder_label(count: usize) -> String {
    dbflux_i18n::t!("sidebar.tree.folder.indexes", count = count)
}

/// Translated label for an Indexes folder while its contents are still
/// loading, before a count is known.
pub(crate) fn indexes_folder_label_plain() -> String {
    dbflux_i18n::t!("sidebar.tree.folder.indexes_plain")
}

/// Translated label for a Foreign Keys folder (table or schema level), e.g.
/// `"Foreign Keys (1)"`.
pub(crate) fn foreign_keys_folder_label(count: usize) -> String {
    dbflux_i18n::t!("sidebar.tree.folder.foreign_keys", count = count)
}

/// Translated label for a Foreign Keys folder while its contents are still
/// loading, before a count is known.
pub(crate) fn foreign_keys_folder_label_plain() -> String {
    dbflux_i18n::t!("sidebar.tree.folder.foreign_keys_plain")
}

/// Translated label for a schema-level Routines folder, e.g. `"Routines
/// (4)"`.
pub(crate) fn routines_folder_label(count: usize) -> String {
    dbflux_i18n::t!("sidebar.tree.folder.routines", count = count)
}

/// Translated label for a Routines folder while its contents are still
/// loading, before a count is known.
pub(crate) fn routines_folder_label_plain() -> String {
    dbflux_i18n::t!("sidebar.tree.folder.routines_plain")
}

/// Translated label for a table's Columns folder, e.g. `"Columns (6)"`.
pub(crate) fn columns_folder_label(count: usize) -> String {
    dbflux_i18n::t!("sidebar.tree.folder.columns", count = count)
}

/// Translated label for a table's Constraints folder, e.g. `"Constraints
/// (2)"`.
pub(crate) fn constraints_folder_label(count: usize) -> String {
    dbflux_i18n::t!("sidebar.tree.folder.constraints", count = count)
}

/// Translated label for a schema-level Data Types folder, e.g. `"Data Types
/// (3)"`.
pub(crate) fn data_types_folder_label(count: usize) -> String {
    dbflux_i18n::t!("sidebar.tree.folder.data_types", count = count)
}

/// Translated label for a Data Types folder while its contents are still
/// loading, before a count is known.
pub(crate) fn data_types_folder_label_plain() -> String {
    dbflux_i18n::t!("sidebar.tree.folder.data_types_plain")
}

/// Translated label for a schema-level Views folder, e.g. `"Views (2)"`.
pub(crate) fn views_folder_label(count: usize) -> String {
    dbflux_i18n::t!("sidebar.tree.folder.views", count = count)
}

/// Translated label for a table's Storage folder (driver-supplied storage
/// hints such as distribution and sort keys), e.g. `"Storage (3)"`.
pub(crate) fn storage_folder_label(count: usize) -> String {
    dbflux_i18n::t!("sidebar.tree.folder.storage", count = count)
}

/// Translated label for the Dashboards sidebar folder. Also used as the
/// fallback label for the remote-dashboards folder when the driver does not
/// supply its own `DashboardSource::container_label`.
pub(crate) fn dashboards_folder_label() -> String {
    dbflux_i18n::t!("sidebar.tree.folder.dashboards")
}

/// Translated label for a remote-dashboards folder base label, suffixed with
/// the cached listing count once it resolves (the driver-supplied base
/// followed by `(N)`). `base` stays untranslated.
pub(crate) fn remote_dashboards_count_label(base: &str, count: usize) -> String {
    dbflux_i18n::t!(
        "sidebar.tree.status.remote_listing_count",
        label = base,
        count = count
    )
}

/// Translated label for a retryable remote-dashboards fetch error, e.g.
/// `"Error: access denied — collapse and expand to retry"`.
pub(crate) fn remote_dashboards_error_label(error: &str) -> String {
    dbflux_i18n::t!(
        "sidebar.tree.status.remote_dashboards_error_retry",
        error = error
    )
}

/// Translated placeholder for a remote-dashboards folder whose listing
/// resolved empty.
pub(crate) fn remote_dashboards_empty_label() -> String {
    dbflux_i18n::t!("sidebar.tree.empty.remote_dashboards")
}

/// Translated label for the Saved Charts sidebar folder.
pub(crate) fn saved_charts_folder_label() -> String {
    dbflux_i18n::t!("sidebar.tree.folder.saved_charts")
}

/// Translated placeholder for an empty Saved Charts folder.
pub(crate) fn no_saved_charts_yet_label() -> String {
    dbflux_i18n::t!("sidebar.tree.empty.saved_charts")
}

/// Translated fallback title for a saved chart persisted with a blank name.
pub(crate) fn untitled_chart_label() -> String {
    dbflux_i18n::t!("sidebar.tree.node.untitled_chart")
}

/// Translated placeholder for an empty Dashboards folder, e.g. `"No
/// dashboards yet — right-click to create"`. `can_import` selects the
/// variant that also mentions importing, gated on `DASHBOARD_IMPORT`.
pub(crate) fn no_dashboards_yet_label(can_import: bool) -> String {
    if can_import {
        dbflux_i18n::t!("sidebar.tree.empty.dashboards_with_import")
    } else {
        dbflux_i18n::t!("sidebar.tree.empty.dashboards")
    }
}

/// Translated label for the Instance Metrics sidebar folder.
pub(crate) fn instance_metrics_folder_label() -> String {
    dbflux_i18n::t!("sidebar.tree.folder.instance_metrics")
}

/// Translated label for the Instance Inspectors sidebar folder.
pub(crate) fn instance_inspectors_folder_label() -> String {
    dbflux_i18n::t!("sidebar.tree.folder.instance_inspectors")
}

/// Translated placeholder for an Instance Metrics folder whose probe
/// resolved with no metrics.
pub(crate) fn no_metrics_available_label() -> String {
    dbflux_i18n::t!("sidebar.tree.empty.metrics")
}

/// Translated placeholder for an Instance Inspectors folder whose probe
/// resolved with no inspectors.
pub(crate) fn no_inspectors_available_label() -> String {
    dbflux_i18n::t!("sidebar.tree.empty.inspectors")
}

/// Translated label for the Instance Overview sidebar leaf.
pub(crate) fn instance_overview_label() -> String {
    dbflux_i18n::t!("sidebar.tree.node.instance_overview")
}

/// Translated label for a database-level Metrics folder (metric catalog
/// browsing, e.g. CloudWatch namespaces).
pub(crate) fn metrics_folder_label() -> String {
    dbflux_i18n::t!("sidebar.tree.folder.metrics")
}

/// Translated task-panel label for an in-flight connect attempt, e.g.
/// `"Connecting to prod-db"`.
pub(crate) fn connecting_task_label(name: &str) -> String {
    dbflux_i18n::t!("sidebar.task.connecting", name = name)
}

/// Translated task-panel label for an in-flight disconnect, e.g.
/// `"Disconnecting prod-db"`.
pub(crate) fn disconnecting_task_label(name: &str) -> String {
    dbflux_i18n::t!("sidebar.task.disconnecting", name = name)
}

/// Translated task failure message when a pre-connect hook is cancelled.
pub(crate) fn connection_hook_cancelled_task_label() -> String {
    dbflux_i18n::t!("sidebar.task.connection_hook_cancelled")
}

/// Translated task failure message when a post-connect hook is cancelled.
pub(crate) fn post_connect_hook_cancelled_task_label() -> String {
    dbflux_i18n::t!("sidebar.task.post_connect_hook_cancelled")
}

/// Translated task failure message when a pre-disconnect hook is cancelled.
pub(crate) fn disconnect_hook_cancelled_task_label() -> String {
    dbflux_i18n::t!("sidebar.task.disconnect_hook_cancelled")
}

/// Translated task failure message when a post-disconnect hook is cancelled.
pub(crate) fn post_disconnect_hook_cancelled_task_label() -> String {
    dbflux_i18n::t!("sidebar.task.post_disconnect_hook_cancelled")
}

/// Translated toast shown when a connect/disconnect request is rejected
/// because an operation is already pending for the same profile.
pub(crate) fn connection_already_pending_toast_label() -> String {
    dbflux_i18n::t!("sidebar.toast.connection_already_pending")
}

/// Translated toast shown when a pending-operation slot was claimed by
/// another thread between the check and the reservation.
pub(crate) fn operation_started_elsewhere_toast_label() -> String {
    dbflux_i18n::t!("sidebar.toast.operation_started_elsewhere")
}

/// Translated toast shown when the background task limit blocks a new
/// connect or disconnect request.
pub(crate) fn background_task_limit_toast_label() -> String {
    dbflux_i18n::t!("sidebar.toast.background_task_limit")
}

/// Translated toast shown when a pre-connect hook cancels the connect flow.
pub(crate) fn connection_cancelled_by_hook_toast_label() -> String {
    dbflux_i18n::t!("sidebar.toast.connection_cancelled_by_hook")
}

/// Translated toast shown when a post-connect hook cancels the connect flow.
pub(crate) fn connection_cancelled_by_post_connect_hook_toast_label() -> String {
    dbflux_i18n::t!("sidebar.toast.connection_cancelled_by_post_connect_hook")
}

/// Translated toast shown when a pre-disconnect hook cancels the disconnect
/// flow.
pub(crate) fn disconnect_cancelled_by_hook_toast_label() -> String {
    dbflux_i18n::t!("sidebar.toast.disconnect_cancelled_by_hook")
}

/// Translated toast shown when the disconnect itself completed but the
/// post-disconnect hook was cancelled.
pub(crate) fn disconnected_hook_cancelled_toast_label() -> String {
    dbflux_i18n::t!("sidebar.toast.disconnected_hook_cancelled")
}

/// Translated error message for editing a connection profile that no longer
/// exists.
pub(crate) fn profile_not_found_label() -> String {
    dbflux_i18n::t!("sidebar.toast.profile_not_found")
}

/// Translated toast reporting a successful connect, e.g. `"Connected to
/// prod-db"`, optionally noting how many hook warnings were logged.
pub(crate) fn connected_toast_label(name: &str, warning_count: usize) -> String {
    match warning_count {
        0 => dbflux_i18n::t!("sidebar.toast.connected.plain", name = name),
        1 => dbflux_i18n::t!("sidebar.toast.connected.one", name = name),
        _ => dbflux_i18n::t!(
            "sidebar.toast.connected.many",
            name = name,
            count = warning_count
        ),
    }
}

/// Translated toast reporting a successful disconnect, e.g. `"Disconnected
/// from prod-db"`, optionally noting how many hook warnings were logged.
pub(crate) fn disconnected_toast_label(name: &str, warning_count: usize) -> String {
    match warning_count {
        0 => dbflux_i18n::t!("sidebar.toast.disconnected.plain", name = name),
        1 => dbflux_i18n::t!("sidebar.toast.disconnected.one", name = name),
        _ => dbflux_i18n::t!(
            "sidebar.toast.disconnected.many",
            name = name,
            count = warning_count
        ),
    }
}

/// Translated toast reporting a disconnect that completed despite a
/// post-disconnect hook error, e.g. `"Disconnected from prod-db, but the
/// hook timed out"`.
pub(crate) fn disconnected_hook_error_toast_label(name: &str, error: &str) -> String {
    dbflux_i18n::t!(
        "sidebar.toast.disconnected_hook_error",
        name = name,
        error = error
    )
}

/// Translated toast for an external (RPC-backed) driver that failed to
/// become available, with wording specific to which stage failed.
pub(crate) fn external_driver_unavailable_label(
    stage: &ExternalDriverStage,
    driver_id: &str,
    socket_id: &str,
    summary: &str,
) -> String {
    let key = match stage {
        ExternalDriverStage::Config => "sidebar.toast.external_driver_unavailable.config",
        ExternalDriverStage::Launch => "sidebar.toast.external_driver_unavailable.launch",
        ExternalDriverStage::Probe => "sidebar.toast.external_driver_unavailable.probe",
    };

    dbflux_i18n::t!(
        key,
        driver_id = driver_id,
        socket_id = socket_id,
        summary = summary
    )
}

/// Translated task-panel label for an in-flight pipeline connect attempt,
/// e.g. `"Connecting to prod-db (pipeline)"`.
pub(crate) fn pipeline_connecting_task_label(name: &str) -> String {
    dbflux_i18n::t!("sidebar.task.pipeline.connecting", name = name)
}

/// Translated task-detail line appended when a pipeline connect attempt is
/// cancelled.
pub(crate) fn pipeline_cancelled_detail_label() -> String {
    dbflux_i18n::t!("sidebar.task.pipeline.cancelled")
}

/// Translated task-detail line appended when a pipeline connect attempt
/// fails, e.g. `"Pipeline failed: connection refused"`.
pub(crate) fn pipeline_failed_detail_label(error: &str) -> String {
    dbflux_i18n::t!("sidebar.task.pipeline.failed", error = error)
}

/// Translated task-detail line appended when a pipeline connect attempt
/// completes successfully.
pub(crate) fn pipeline_completed_detail_label() -> String {
    dbflux_i18n::t!("sidebar.task.pipeline.completed")
}

/// Translated task-panel label for a background schema fetch that loads
/// event streams for a collection, e.g. `"Loading event streams: orders"`.
pub(crate) fn loading_event_streams_task_label(name: &str) -> String {
    dbflux_i18n::t!("sidebar.task.loading_event_streams", name = name)
}

/// Translated toast reported when loading a table's schema details fails,
/// e.g. `"Failed to load orders schema: connection reset"`.
pub(crate) fn table_load_failed_label(name: &str, error: &str) -> String {
    dbflux_i18n::t!(
        "sidebar.toast.load_failed.table",
        name = name,
        error = error
    )
}

/// Translated toast reported when loading a collection's event streams
/// fails, e.g. `"Failed to load event streams for orders: connection
/// reset"`.
pub(crate) fn collection_load_failed_label(name: &str, error: &str) -> String {
    dbflux_i18n::t!(
        "sidebar.toast.load_failed.collection",
        name = name,
        error = error
    )
}

/// Translated label for the batch/single delete confirmation item count,
/// e.g. `"1 item"` or `"3 items"`.
pub(crate) fn items_label(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!("sidebar.confirm.items.one")
    } else {
        dbflux_i18n::t!("sidebar.confirm.items.many", count = count)
    }
}

/// Translated task-panel label for a background DDL drop, e.g. `"Dropping
/// table orders"`.
pub(crate) fn dropping_task_label(kind: &SchemaObjectKind, name: &str) -> String {
    let key = match kind {
        SchemaObjectKind::Table => "sidebar.task.dropping.table",
        SchemaObjectKind::View => "sidebar.task.dropping.view",
        SchemaObjectKind::Collection => "sidebar.task.dropping.collection",
        SchemaObjectKind::Database => "sidebar.task.dropping.database",
    };

    dbflux_i18n::t!(key, name = name)
}

/// Translated toast/task-error message for a schema drop cancelled by the
/// user or a hook.
pub(crate) fn schema_drop_cancelled_label() -> String {
    dbflux_i18n::t!("sidebar.toast.schema_drop_cancelled")
}

/// Translated toast reported when a schema drop fails, e.g. `"Failed to
/// drop: permission denied"`.
pub(crate) fn drop_failed_label(error: &str) -> String {
    dbflux_i18n::t!("sidebar.toast.drop_failed", error = error)
}

/// Translated toast reported when revealing a script file/folder in the
/// system file manager fails.
pub(crate) fn reveal_failed_label(error: &str) -> String {
    dbflux_i18n::t!("sidebar.toast.reveal_failed", error = error)
}

/// Translated title for the rfd "Import Script" file picker dialog.
pub(crate) fn import_script_dialog_title() -> String {
    dbflux_i18n::t!("sidebar.dialog.import_script_title")
}

/// Translated filter label for the rfd "Import Script" file picker dialog.
pub(crate) fn script_files_filter_label() -> String {
    dbflux_i18n::t!("sidebar.dialog.script_files_filter")
}

/// Translated warning shown when an Export/Migrate Tables action skipped
/// tables outside the active profile/database, e.g. `"3 tables outside the
/// active profile/database were skipped"`.
pub(crate) fn skipped_tables_label(count: usize) -> String {
    if count == 1 {
        dbflux_i18n::t!("sidebar.overlay.skipped_tables.one")
    } else {
        dbflux_i18n::t!("sidebar.overlay.skipped_tables.many", count = count)
    }
}

/// Translated toast reported when capturing a schema snapshot fails during
/// a pipeline connect.
pub(crate) fn schema_snapshot_failed_label(error: &str) -> String {
    dbflux_i18n::t!("sidebar.toast.schema_snapshot_failed", error = error)
}

/// Translated `UserFacingError` message for a failed schema-types fetch.
pub(crate) fn cannot_load_schema_types_label() -> String {
    dbflux_i18n::t!("sidebar.error.cannot_load_schema_types")
}

/// Translated `UserFacingError` message for a failed schema-indexes fetch.
pub(crate) fn cannot_load_schema_indexes_label() -> String {
    dbflux_i18n::t!("sidebar.error.cannot_load_schema_indexes")
}

/// Translated `UserFacingError` message for a failed foreign-keys fetch.
pub(crate) fn cannot_load_schema_foreign_keys_label() -> String {
    dbflux_i18n::t!("sidebar.error.cannot_load_schema_foreign_keys")
}

/// Translated `UserFacingError` message for a failed routines fetch.
pub(crate) fn cannot_load_schema_routines_label() -> String {
    dbflux_i18n::t!("sidebar.error.cannot_load_schema_routines")
}

/// Translated toast reported when loading a schema's data types fails.
pub(crate) fn data_types_load_failed_label(error: &str) -> String {
    dbflux_i18n::t!("sidebar.toast.load_failed.data_types", error = error)
}

/// Translated toast reported when loading a schema's indexes fails.
pub(crate) fn indexes_load_failed_label(error: &str) -> String {
    dbflux_i18n::t!("sidebar.toast.load_failed.indexes", error = error)
}

/// Translated toast reported when loading a schema's foreign keys fails.
pub(crate) fn foreign_keys_load_failed_label(error: &str) -> String {
    dbflux_i18n::t!("sidebar.toast.load_failed.foreign_keys", error = error)
}

/// Translated toast reported when loading a schema's routines fails.
pub(crate) fn routines_load_failed_label(error: &str) -> String {
    dbflux_i18n::t!("sidebar.toast.load_failed.routines", error = error)
}

/// Translated task-panel label for a background bucket listing, e.g.
/// `"Listing buckets: prod-db"`.
pub(crate) fn listing_buckets_task_label(name: &str) -> String {
    dbflux_i18n::t!("sidebar.task.listing_buckets", name = name)
}

/// Translated task-failure detail reported when listing buckets fails.
pub(crate) fn list_buckets_failed_label(error: &str) -> String {
    dbflux_i18n::t!("sidebar.toast.list_buckets_failed", error = error)
}

/// Translated toast reported when SQL/query code generation fails.
pub(crate) fn code_generation_failed_label(error: &str) -> String {
    dbflux_i18n::t!("sidebar.toast.code_generation_failed", error = error)
}

/// Translated toast shown while an event-stream child-picker fetch is still
/// loading in the background.
pub(crate) fn loading_event_streams_toast_label() -> String {
    dbflux_i18n::t!("sidebar.toast.loading_event_streams")
}

/// Translated task-panel label for a database schema fetch, e.g. `"Loading
/// schema: orders"`.
pub(crate) fn loading_database_schema_task_label(name: &str) -> String {
    dbflux_i18n::t!("sidebar.task.loading_database_schema", name = name)
}

/// Translated toast reported when loading a database's schema fails.
pub(crate) fn load_schema_failed_label(error: &str) -> String {
    dbflux_i18n::t!("sidebar.toast.load_schema_failed", error = error)
}

/// Translated task-panel label for a per-database connection switch, e.g.
/// `"Connecting to database: orders"`.
pub(crate) fn connecting_to_database_task_label(name: &str) -> String {
    dbflux_i18n::t!("sidebar.task.connecting_to_database", name = name)
}

/// Translated toast reported when switching the active database fails.
pub(crate) fn connect_database_failed_label(error: &str) -> String {
    dbflux_i18n::t!("sidebar.toast.connect_database_failed", error = error)
}

/// Translated task-panel label for a database schema refresh, e.g.
/// `"Refreshing database: orders"`.
pub(crate) fn refreshing_database_task_label(name: &str) -> String {
    dbflux_i18n::t!("sidebar.task.refreshing_database", name = name)
}

/// Translated toast reported when a database schema refresh fails.
pub(crate) fn refresh_database_failed_label(error: &str) -> String {
    dbflux_i18n::t!("sidebar.toast.refresh_database_failed", error = error)
}

/// Translated toast shown when a database refresh is rejected because one is
/// already running for that database.
pub(crate) fn database_refresh_pending_label() -> String {
    dbflux_i18n::t!("sidebar.toast.database_refresh_pending")
}

/// Translated task-panel label for a schema-object refresh, e.g.
/// `"Refreshing schema object: orders"`.
pub(crate) fn refreshing_schema_object_task_label(name: &str) -> String {
    dbflux_i18n::t!("sidebar.task.refreshing_schema_object", name = name)
}

/// Translated toast shown when preparing a schema-object refresh fails.
pub(crate) fn prepare_schema_object_refresh_failed_label() -> String {
    dbflux_i18n::t!("sidebar.toast.prepare_schema_object_refresh_failed")
}

/// Translated toast reported when a schema-object refresh fails.
pub(crate) fn refresh_schema_object_failed_label(error: &str) -> String {
    dbflux_i18n::t!("sidebar.toast.refresh_schema_object_failed", error = error)
}

/// Translated task-panel label for the current pipeline stage, shown as a
/// subtask under the pipeline connect task. Returns `None` for stages with
/// no user-facing subtask (idle and terminal states).
pub(crate) fn pipeline_stage_label(state: &PipelineState) -> Option<String> {
    match state {
        PipelineState::Idle => None,
        PipelineState::Authenticating { provider_name } => Some(dbflux_i18n::t!(
            "sidebar.task.pipeline.stage.authenticating",
            provider = provider_name
        )),
        PipelineState::WaitingForLogin { provider_name, .. } => Some(dbflux_i18n::t!(
            "sidebar.task.pipeline.stage.waiting_for_login",
            provider = provider_name
        )),
        PipelineState::ResolvingValues { total, resolved } => Some(dbflux_i18n::t!(
            "sidebar.task.pipeline.stage.resolving_values",
            resolved = resolved,
            total = total
        )),
        PipelineState::OpeningAccess { method_label } => Some(dbflux_i18n::t!(
            "sidebar.task.pipeline.stage.opening_access",
            method = method_label
        )),
        PipelineState::Connecting { driver_name } => Some(dbflux_i18n::t!(
            "sidebar.task.pipeline.stage.connecting",
            driver = driver_name
        )),
        PipelineState::FetchingSchema => Some(dbflux_i18n::t!(
            "sidebar.task.pipeline.stage.fetching_schema"
        )),
        PipelineState::Connected | PipelineState::Failed { .. } | PipelineState::Cancelled => None,
    }
}

#[cfg(test)]
mod tests {
    use dbflux_core::DatabaseCategory;

    const ALL_CATEGORIES: [DatabaseCategory; 8] = [
        DatabaseCategory::Relational,
        DatabaseCategory::Document,
        DatabaseCategory::KeyValue,
        DatabaseCategory::Graph,
        DatabaseCategory::TimeSeries,
        DatabaseCategory::WideColumn,
        DatabaseCategory::LogStream,
        DatabaseCategory::ObjectStorage,
    ];

    const CONTAINER_KEYS: [&str; 8] = [
        "sidebar.tree.container.relational",
        "sidebar.tree.container.document",
        "sidebar.tree.container.key_value",
        "sidebar.tree.container.graph",
        "sidebar.tree.container.time_series",
        "sidebar.tree.container.wide_column",
        "sidebar.tree.container.log_stream",
        "sidebar.tree.container.object_storage",
    ];

    const SLICE_KEYS: [&str; 10] = [
        "sidebar.tabs.connections",
        "sidebar.tabs.scripts",
        "sidebar.confirm.delete_hint",
        "sidebar.empty.connections_title",
        "sidebar.empty.connections_hint",
        "sidebar.empty.scripts_title",
        "sidebar.empty.scripts_hint",
        "sidebar.status.connection_summary",
        "sidebar.tree.container.relational",
        "sidebar.tree.container.log_stream",
    ];

    const OVERLAY_KEYS: [&str; 23] = [
        "sidebar.filter.connections_placeholder",
        "sidebar.filter.scripts_placeholder",
        "sidebar.filter.stream_placeholder",
        "sidebar.overlay.add_folder",
        "sidebar.overlay.add_connection",
        "sidebar.overlay.add_script_file",
        "sidebar.overlay.add_script_folder",
        "sidebar.overlay.import_file",
        "sidebar.overlay.child_picker.title",
        "sidebar.overlay.child_picker.column_name",
        "sidebar.overlay.child_picker.column_last_event",
        "sidebar.overlay.child_picker.empty",
        "sidebar.overlay.child_picker.prev",
        "sidebar.overlay.child_picker.next",
        "sidebar.overlay.child_picker.page_label",
        "sidebar.overlay.child_picker.page_label_empty",
        "sidebar.overlay.child_picker.unsupported",
        "sidebar.toast.edit_reconnect_updated",
        "sidebar.toast.edit_reconnect_body",
        "sidebar.toast.edit_reconnect_now",
        "sidebar.toast.edit_reconnect_later",
        "sidebar.status.profile_fallback_name",
        "sidebar.tree.status.loading",
    ];

    #[test]
    fn container_folder_label_matches_container_name_for_every_category() {
        for category in ALL_CATEGORIES {
            let label = super::container_folder_label(category, 3);
            assert_eq!(label, format!("{} (3)", category.container_name()));
        }
    }

    #[test]
    fn container_folder_label_uses_the_given_count() {
        let label = super::container_folder_label(DatabaseCategory::Document, 7);
        assert_eq!(label, "Collections (7)");
    }

    #[test]
    fn footer_counts_label_reports_connected_and_idle_counts() {
        let label = super::footer_counts_label(2, 5);
        assert!(label.contains("2 connected"));
        assert!(label.contains("5 idle"));
    }

    #[test]
    fn footer_counts_label_reports_zero_counts() {
        let label = super::footer_counts_label(0, 0);
        assert!(label.contains("0 connected"));
        assert!(label.contains("0 idle"));
    }

    #[test]
    fn slice_translation_keys_resolve_in_every_shipped_locale() {
        for key in SLICE_KEYS {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert_ne!(value, key, "missing translation for {locale}.{key}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "translation fell back to the miss sentinel for {locale}.{key}"
                );
            }
        }
    }

    #[test]
    fn sidebar_tabs_connections_diverges_between_locales() {
        let english = dbflux_i18n::t!("sidebar.tabs.connections", locale = "en");
        let spanish = dbflux_i18n::t!("sidebar.tabs.connections", locale = "es");

        assert_eq!(english, "CONNECTIONS");
        assert_eq!(spanish, "CONEXIONES");
        assert_ne!(english, spanish);
    }

    #[test]
    fn sidebar_tree_container_relational_diverges_between_locales() {
        let english = dbflux_i18n::t!("sidebar.tree.container.relational", locale = "en");
        let spanish = dbflux_i18n::t!("sidebar.tree.container.relational", locale = "es");

        assert_ne!(english, spanish);
    }

    #[test]
    fn container_keys_cover_every_database_category() {
        for (category, key) in ALL_CATEGORIES.iter().zip(CONTAINER_KEYS.iter()) {
            let expected = super::container_folder_label(*category, 1);
            let translated = dbflux_i18n::t!(key, count = 1);

            assert_eq!(expected, translated);
        }
    }

    #[test]
    fn overlay_keys_resolve_in_both_locales() {
        for key in OVERLAY_KEYS {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert_ne!(value, key, "missing translation for {locale}.{key}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "translation fell back to the miss sentinel for {locale}.{key}"
                );
            }
        }
    }

    #[test]
    fn overlay_prev_differs_between_locales() {
        let english = dbflux_i18n::t!("sidebar.overlay.child_picker.prev", locale = "en");
        let spanish = dbflux_i18n::t!("sidebar.overlay.child_picker.prev", locale = "es");

        assert_eq!(english, "Prev");
        assert_eq!(spanish, "Anterior");
        assert_ne!(english, spanish);
    }

    #[test]
    fn page_label_reports_current_page_and_visible_range() {
        let label = super::page_label(1, 3, 1, 50);

        assert!(label.contains('1'));
        assert!(label.contains('3'));
        assert!(label.contains("50"));
        assert_eq!(
            label,
            dbflux_i18n::t!(
                "sidebar.overlay.child_picker.page_label",
                current = 1,
                total = 3,
                start = 1,
                end = 50
            )
        );
    }

    #[test]
    fn page_label_falls_back_to_empty_variant_when_there_are_no_pages() {
        let label = super::page_label(0, 0, 0, 0);

        assert_eq!(
            label,
            dbflux_i18n::t!("sidebar.overlay.child_picker.page_label_empty")
        );
    }

    #[test]
    fn child_picker_title_includes_the_collection_name() {
        let title = super::child_picker_title("orders");

        assert!(title.contains("orders"));
        assert_eq!(
            title,
            dbflux_i18n::t!("sidebar.overlay.child_picker.title", name = "orders")
        );
    }

    #[test]
    fn profile_updated_label_includes_the_profile_name() {
        let label = super::profile_updated_label("prod-db");

        assert!(label.contains("prod-db"));
        assert_eq!(
            label,
            dbflux_i18n::t!("sidebar.toast.edit_reconnect_updated", name = "prod-db")
        );
    }

    #[test]
    fn export_tables_label_one_vs_many() {
        let singular = super::export_tables_label(1);
        let plural = super::export_tables_label(3);

        assert_eq!(singular, dbflux_i18n::t!("sidebar.menu.export_table"));
        assert_eq!(
            plural,
            dbflux_i18n::t!("sidebar.menu.export_tables_many", count = 3)
        );
        assert!(plural.contains('3'));
        assert_ne!(singular, plural);
    }

    #[test]
    fn delete_items_label_one_vs_many() {
        let singular = super::delete_items_label(1);
        let plural = super::delete_items_label(3);

        assert_eq!(singular, dbflux_i18n::t!("sidebar.menu.delete"));
        assert_eq!(
            plural,
            dbflux_i18n::t!("sidebar.menu.delete_count", count = 3)
        );
        assert!(plural.contains('3'));
        assert_ne!(singular, plural);
    }

    #[test]
    fn migrate_tables_label_one_vs_many() {
        let singular = super::migrate_tables_label(1);
        let plural = super::migrate_tables_label(3);

        assert_eq!(singular, dbflux_i18n::t!("sidebar.menu.migrate_table"));
        assert_eq!(
            plural,
            dbflux_i18n::t!("sidebar.menu.migrate_tables_many", count = 3)
        );
        assert!(plural.contains('3'));
        assert_ne!(singular, plural);
    }

    #[test]
    fn profile_connecting_label_includes_the_profile_name() {
        let label = super::profile_connecting_label("prod-db");

        assert!(label.contains("prod-db"));
        assert_eq!(
            label,
            dbflux_i18n::t!("sidebar.tree.status.profile_connecting", name = "prod-db")
        );
    }

    #[test]
    fn node_loading_label_includes_the_database_name() {
        let label = super::node_loading_label("orders");

        assert!(label.contains("orders"));
        assert_eq!(
            label,
            dbflux_i18n::t!("sidebar.tree.status.database_loading", name = "orders")
        );
    }

    #[test]
    fn error_retry_label_includes_the_error_message() {
        let label = super::error_retry_label("access denied");

        assert!(label.contains("access denied"));
        assert_eq!(
            label,
            dbflux_i18n::t!("sidebar.tree.status.error_retry", error = "access denied")
        );
    }

    #[test]
    fn used_by_label_one_vs_many() {
        let singular = super::used_by_label(1);
        let plural = super::used_by_label(3);

        assert_eq!(
            singular,
            dbflux_i18n::t!("sidebar.tree.status.used_by_objects.one")
        );
        assert_eq!(
            plural,
            dbflux_i18n::t!("sidebar.tree.status.used_by_objects.many", count = 3)
        );
        assert!(plural.contains('3'));
        assert_ne!(singular, plural);
    }

    #[test]
    fn used_by_label_diverges_between_locales() {
        let english_template =
            dbflux_i18n::t!("sidebar.tree.status.used_by_objects.many", locale = "en");
        let spanish_template =
            dbflux_i18n::t!("sidebar.tree.status.used_by_objects.many", locale = "es");

        assert_ne!(english_template, spanish_template);
    }

    const C2_KEYS: [&str; 13] = [
        "sidebar.tree.folder.dashboards",
        "sidebar.tree.folder.saved_charts",
        "sidebar.tree.folder.instance_metrics",
        "sidebar.tree.folder.instance_inspectors",
        "sidebar.tree.folder.metrics",
        "sidebar.tree.node.instance_overview",
        "sidebar.tree.node.untitled_chart",
        "sidebar.tree.empty.dashboards",
        "sidebar.tree.empty.dashboards_with_import",
        "sidebar.tree.empty.saved_charts",
        "sidebar.tree.empty.metrics",
        "sidebar.tree.empty.inspectors",
        "sidebar.tree.empty.remote_dashboards",
    ];

    #[test]
    fn c2_keys_resolve_in_both_locales() {
        for key in C2_KEYS {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert_ne!(value, key, "missing translation for {locale}.{key}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "translation fell back to the miss sentinel for {locale}.{key}"
                );
            }
        }
    }

    #[test]
    fn saved_charts_folder_label_diverges_between_locales() {
        let english = dbflux_i18n::t!("sidebar.tree.folder.saved_charts", locale = "en");
        let spanish = dbflux_i18n::t!("sidebar.tree.folder.saved_charts", locale = "es");

        assert_ne!(english, spanish);
    }

    #[test]
    fn remote_dashboards_count_label_includes_base_and_count() {
        let label = super::remote_dashboards_count_label("CloudWatch Dashboards", 3);

        assert!(label.contains("CloudWatch Dashboards"));
        assert!(label.contains('3'));
        assert_eq!(
            label,
            dbflux_i18n::t!(
                "sidebar.tree.status.remote_listing_count",
                label = "CloudWatch Dashboards",
                count = 3
            )
        );
    }

    #[test]
    fn remote_dashboards_error_label_includes_the_error_message() {
        let label = super::remote_dashboards_error_label("access denied");

        assert!(label.contains("access denied"));
        assert_eq!(
            label,
            dbflux_i18n::t!(
                "sidebar.tree.status.remote_dashboards_error_retry",
                error = "access denied"
            )
        );
    }

    #[test]
    fn no_dashboards_yet_label_differs_with_and_without_import() {
        let with_import = super::no_dashboards_yet_label(true);
        let without_import = super::no_dashboards_yet_label(false);

        assert_eq!(
            with_import,
            dbflux_i18n::t!("sidebar.tree.empty.dashboards_with_import")
        );
        assert_eq!(
            without_import,
            dbflux_i18n::t!("sidebar.tree.empty.dashboards")
        );
        assert_ne!(with_import, without_import);
    }

    #[test]
    fn dependent_kind_label_covers_every_relation_kind() {
        use dbflux_core::RelationKind;

        assert_eq!(
            super::dependent_kind_label(&RelationKind::View),
            dbflux_i18n::t!("sidebar.tree.status.dependent_kind.view")
        );
        assert_eq!(
            super::dependent_kind_label(&RelationKind::MaterializedView),
            dbflux_i18n::t!("sidebar.tree.status.dependent_kind.materialized_view")
        );
        assert_eq!(
            super::dependent_kind_label(&RelationKind::ForeignKeyChild),
            dbflux_i18n::t!("sidebar.tree.status.dependent_kind.foreign_key_child")
        );
        assert_eq!(
            super::dependent_kind_label(&RelationKind::Trigger),
            dbflux_i18n::t!("sidebar.tree.status.dependent_kind.trigger")
        );
    }

    const D1A_KEYS: [&str; 18] = [
        "sidebar.task.connecting",
        "sidebar.task.disconnecting",
        "sidebar.task.connection_hook_cancelled",
        "sidebar.task.post_connect_hook_cancelled",
        "sidebar.task.disconnect_hook_cancelled",
        "sidebar.task.post_disconnect_hook_cancelled",
        "sidebar.toast.connection_already_pending",
        "sidebar.toast.operation_started_elsewhere",
        "sidebar.toast.background_task_limit",
        "sidebar.toast.connection_cancelled_by_hook",
        "sidebar.toast.connection_cancelled_by_post_connect_hook",
        "sidebar.toast.disconnect_cancelled_by_hook",
        "sidebar.toast.disconnected_hook_cancelled",
        "sidebar.toast.profile_not_found",
        "sidebar.toast.connected.plain",
        "sidebar.toast.connected.one",
        "sidebar.toast.connected.many",
        "sidebar.toast.disconnected_hook_error",
    ];

    #[test]
    fn d1a_keys_resolve_in_both_locales() {
        for key in D1A_KEYS {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert_ne!(value, key, "missing translation for {locale}.{key}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "translation fell back to the miss sentinel for {locale}.{key}"
                );
            }
        }
    }

    #[test]
    fn disconnected_toast_keys_resolve_in_both_locales() {
        for key in [
            "sidebar.toast.disconnected.plain",
            "sidebar.toast.disconnected.one",
            "sidebar.toast.disconnected.many",
        ] {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert_ne!(value, key, "missing translation for {locale}.{key}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "translation fell back to the miss sentinel for {locale}.{key}"
                );
            }
        }
    }

    #[test]
    fn external_driver_unavailable_keys_resolve_in_both_locales() {
        for key in [
            "sidebar.toast.external_driver_unavailable.config",
            "sidebar.toast.external_driver_unavailable.launch",
            "sidebar.toast.external_driver_unavailable.probe",
        ] {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert_ne!(value, key, "missing translation for {locale}.{key}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "translation fell back to the miss sentinel for {locale}.{key}"
                );
            }
        }
    }

    #[test]
    fn connecting_task_label_includes_the_profile_name() {
        let label = super::connecting_task_label("prod-db");

        assert!(label.contains("prod-db"));
        assert_eq!(
            label,
            dbflux_i18n::t!("sidebar.task.connecting", name = "prod-db")
        );
    }

    #[test]
    fn disconnecting_task_label_includes_the_profile_name() {
        let label = super::disconnecting_task_label("prod-db");

        assert!(label.contains("prod-db"));
        assert_eq!(
            label,
            dbflux_i18n::t!("sidebar.task.disconnecting", name = "prod-db")
        );
        assert_ne!(label, super::connecting_task_label("prod-db"));
    }

    #[test]
    fn connected_toast_label_plain_one_and_many_diverge() {
        let plain = super::connected_toast_label("prod-db", 0);
        let one = super::connected_toast_label("prod-db", 1);
        let many = super::connected_toast_label("prod-db", 3);

        assert_eq!(
            plain,
            dbflux_i18n::t!("sidebar.toast.connected.plain", name = "prod-db")
        );
        assert_eq!(
            one,
            dbflux_i18n::t!("sidebar.toast.connected.one", name = "prod-db")
        );
        assert_eq!(
            many,
            dbflux_i18n::t!("sidebar.toast.connected.many", name = "prod-db", count = 3)
        );
        assert!(many.contains('3'));
        assert_ne!(plain, one);
        assert_ne!(one, many);
        assert_ne!(plain, many);
    }

    #[test]
    fn disconnected_toast_label_plain_one_and_many_diverge() {
        let plain = super::disconnected_toast_label("prod-db", 0);
        let one = super::disconnected_toast_label("prod-db", 1);
        let many = super::disconnected_toast_label("prod-db", 2);

        assert_eq!(
            plain,
            dbflux_i18n::t!("sidebar.toast.disconnected.plain", name = "prod-db")
        );
        assert_eq!(
            one,
            dbflux_i18n::t!("sidebar.toast.disconnected.one", name = "prod-db")
        );
        assert_eq!(
            many,
            dbflux_i18n::t!(
                "sidebar.toast.disconnected.many",
                name = "prod-db",
                count = 2
            )
        );
        assert!(many.contains('2'));
        assert_ne!(plain, one);
        assert_ne!(one, many);
    }

    #[test]
    fn disconnected_hook_error_toast_label_includes_name_and_error() {
        let label = super::disconnected_hook_error_toast_label("prod-db", "hook timed out");

        assert!(label.contains("prod-db"));
        assert!(label.contains("hook timed out"));
        assert_eq!(
            label,
            dbflux_i18n::t!(
                "sidebar.toast.disconnected_hook_error",
                name = "prod-db",
                error = "hook timed out"
            )
        );
    }

    #[test]
    fn external_driver_unavailable_label_uses_the_stage_specific_key() {
        use dbflux_app::ExternalDriverStage;

        let config = super::external_driver_unavailable_label(
            &ExternalDriverStage::Config,
            "rpc:missing.sock",
            "missing.sock",
            "bad config",
        );
        let launch = super::external_driver_unavailable_label(
            &ExternalDriverStage::Launch,
            "rpc:missing.sock",
            "missing.sock",
            "did not start",
        );
        let probe = super::external_driver_unavailable_label(
            &ExternalDriverStage::Probe,
            "rpc:missing.sock",
            "missing.sock",
            "probe failed",
        );

        assert!(config.contains("rpc:missing.sock"));
        assert!(config.contains("bad config"));
        assert!(launch.contains("did not start"));
        assert!(probe.contains("probe failed"));
        assert_ne!(config, launch);
        assert_ne!(launch, probe);
    }

    #[test]
    fn sidebar_task_connecting_diverges_between_locales() {
        let english = dbflux_i18n::t!("sidebar.task.connecting", locale = "en");
        let spanish = dbflux_i18n::t!("sidebar.task.connecting", locale = "es");

        assert_ne!(english, spanish);
    }

    #[test]
    fn sidebar_toast_connected_many_diverges_between_locales() {
        let english = dbflux_i18n::t!("sidebar.toast.connected.many", locale = "en");
        let spanish = dbflux_i18n::t!("sidebar.toast.connected.many", locale = "es");

        assert_ne!(english, spanish);
    }

    const D1B_KEYS: [&str; 10] = [
        "sidebar.task.pipeline.connecting",
        "sidebar.task.pipeline.cancelled",
        "sidebar.task.pipeline.failed",
        "sidebar.task.pipeline.completed",
        "sidebar.task.pipeline.stage.authenticating",
        "sidebar.task.pipeline.stage.waiting_for_login",
        "sidebar.task.pipeline.stage.resolving_values",
        "sidebar.task.pipeline.stage.opening_access",
        "sidebar.task.pipeline.stage.connecting",
        "sidebar.task.pipeline.stage.fetching_schema",
    ];

    #[test]
    fn d1b_keys_resolve_in_both_locales() {
        for key in D1B_KEYS {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert_ne!(value, key, "missing translation for {locale}.{key}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "translation fell back to the miss sentinel for {locale}.{key}"
                );
            }
        }
    }

    #[test]
    fn sidebar_task_pipeline_connecting_diverges_between_locales() {
        let english = dbflux_i18n::t!("sidebar.task.pipeline.connecting", locale = "en");
        let spanish = dbflux_i18n::t!("sidebar.task.pipeline.connecting", locale = "es");

        assert_ne!(english, spanish);
    }

    use dbflux_core::PipelineState;

    #[test]
    fn pipeline_stage_label_covers_all_variants() {
        assert_eq!(
            super::pipeline_stage_label(&PipelineState::Idle),
            None,
            "idle has no subtask label"
        );
        assert_eq!(
            super::pipeline_stage_label(&PipelineState::Connected),
            None,
            "connected has no subtask label"
        );
        assert_eq!(
            super::pipeline_stage_label(&PipelineState::Cancelled),
            None,
            "cancelled has no subtask label"
        );
        assert_eq!(
            super::pipeline_stage_label(&PipelineState::Failed {
                stage: "driver_connect".to_string(),
                error: "boom".to_string(),
            }),
            None,
            "failed has no subtask label"
        );

        let authenticating = super::pipeline_stage_label(&PipelineState::Authenticating {
            provider_name: "aws-sso".to_string(),
        })
        .expect("authenticating has a subtask label");
        assert_eq!(
            authenticating,
            dbflux_i18n::t!(
                "sidebar.task.pipeline.stage.authenticating",
                provider = "aws-sso"
            )
        );

        let waiting_for_login = super::pipeline_stage_label(&PipelineState::WaitingForLogin {
            provider_name: "aws-sso".to_string(),
            verification_url: None,
        })
        .expect("waiting_for_login has a subtask label");
        assert_eq!(
            waiting_for_login,
            dbflux_i18n::t!(
                "sidebar.task.pipeline.stage.waiting_for_login",
                provider = "aws-sso"
            )
        );

        let resolving_values = super::pipeline_stage_label(&PipelineState::ResolvingValues {
            total: 3,
            resolved: 1,
        })
        .expect("resolving_values has a subtask label");
        assert_eq!(
            resolving_values,
            dbflux_i18n::t!(
                "sidebar.task.pipeline.stage.resolving_values",
                resolved = 1,
                total = 3
            )
        );

        let opening_access = super::pipeline_stage_label(&PipelineState::OpeningAccess {
            method_label: "SSH tunnel".to_string(),
        })
        .expect("opening_access has a subtask label");
        assert_eq!(
            opening_access,
            dbflux_i18n::t!(
                "sidebar.task.pipeline.stage.opening_access",
                method = "SSH tunnel"
            )
        );

        let connecting = super::pipeline_stage_label(&PipelineState::Connecting {
            driver_name: "PostgreSQL".to_string(),
        })
        .expect("connecting has a subtask label");
        assert_eq!(
            connecting,
            dbflux_i18n::t!(
                "sidebar.task.pipeline.stage.connecting",
                driver = "PostgreSQL"
            )
        );

        let fetching_schema = super::pipeline_stage_label(&PipelineState::FetchingSchema)
            .expect("fetching_schema has a subtask label");
        assert_eq!(
            fetching_schema,
            dbflux_i18n::t!("sidebar.task.pipeline.stage.fetching_schema")
        );
    }

    #[test]
    fn pipeline_failed_detail_label_includes_the_error() {
        let label = super::pipeline_failed_detail_label("connection refused");

        assert!(label.contains("connection refused"));
        assert_eq!(
            label,
            dbflux_i18n::t!("sidebar.task.pipeline.failed", error = "connection refused")
        );
    }

    const D2_KEYS: [&str; 15] = [
        "sidebar.task.loading_event_streams",
        "sidebar.toast.load_failed.table",
        "sidebar.toast.load_failed.collection",
        "sidebar.confirm.items.one",
        "sidebar.confirm.items.many",
        "sidebar.task.dropping.table",
        "sidebar.task.dropping.view",
        "sidebar.task.dropping.collection",
        "sidebar.task.dropping.database",
        "sidebar.toast.schema_drop_cancelled",
        "sidebar.toast.drop_failed",
        "sidebar.toast.reveal_failed",
        "sidebar.dialog.import_script_title",
        "sidebar.dialog.script_files_filter",
        "sidebar.toast.schema_snapshot_failed",
    ];

    #[test]
    fn d2_keys_resolve_in_both_locales() {
        for key in D2_KEYS {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert_ne!(value, key, "missing translation for {locale}.{key}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "translation fell back to the miss sentinel for {locale}.{key}"
                );
            }
        }
    }

    #[test]
    fn skipped_tables_label_keys_resolve_in_both_locales() {
        for key in [
            "sidebar.overlay.skipped_tables.one",
            "sidebar.overlay.skipped_tables.many",
        ] {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert_ne!(value, key, "missing translation for {locale}.{key}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "translation fell back to the miss sentinel for {locale}.{key}"
                );
            }
        }
    }

    #[test]
    fn sidebar_task_dropping_table_diverges_between_locales() {
        let english = dbflux_i18n::t!("sidebar.task.dropping.table", locale = "en");
        let spanish = dbflux_i18n::t!("sidebar.task.dropping.table", locale = "es");

        assert_ne!(english, spanish);
    }

    #[test]
    fn loading_event_streams_task_label_includes_the_collection_name() {
        let label = super::loading_event_streams_task_label("orders");

        assert!(label.contains("orders"));
        assert_eq!(
            label,
            dbflux_i18n::t!("sidebar.task.loading_event_streams", name = "orders")
        );
    }

    #[test]
    fn table_load_failed_label_includes_name_and_error() {
        let label = super::table_load_failed_label("orders", "connection reset");

        assert!(label.contains("orders"));
        assert!(label.contains("connection reset"));
        assert_eq!(
            label,
            dbflux_i18n::t!(
                "sidebar.toast.load_failed.table",
                name = "orders",
                error = "connection reset"
            )
        );
    }

    #[test]
    fn collection_load_failed_label_includes_name_and_error() {
        let label = super::collection_load_failed_label("orders", "connection reset");

        assert!(label.contains("orders"));
        assert!(label.contains("connection reset"));
        assert_ne!(
            label,
            super::table_load_failed_label("orders", "connection reset")
        );
    }

    #[test]
    fn items_label_one_vs_many() {
        let singular = super::items_label(1);
        let plural = super::items_label(3);

        assert_eq!(singular, dbflux_i18n::t!("sidebar.confirm.items.one"));
        assert_eq!(
            plural,
            dbflux_i18n::t!("sidebar.confirm.items.many", count = 3)
        );
        assert!(plural.contains('3'));
        assert_ne!(singular, plural);
    }

    #[test]
    fn dropping_task_label_covers_every_object_kind() {
        use dbflux_core::SchemaObjectKind;

        assert_eq!(
            super::dropping_task_label(&SchemaObjectKind::Table, "orders"),
            dbflux_i18n::t!("sidebar.task.dropping.table", name = "orders")
        );
        assert_eq!(
            super::dropping_task_label(&SchemaObjectKind::View, "orders_v"),
            dbflux_i18n::t!("sidebar.task.dropping.view", name = "orders_v")
        );
        assert_eq!(
            super::dropping_task_label(&SchemaObjectKind::Collection, "logs"),
            dbflux_i18n::t!("sidebar.task.dropping.collection", name = "logs")
        );
        assert_eq!(
            super::dropping_task_label(&SchemaObjectKind::Database, "analytics"),
            dbflux_i18n::t!("sidebar.task.dropping.database", name = "analytics")
        );
    }

    #[test]
    fn schema_drop_cancelled_label_is_stable() {
        assert_eq!(
            super::schema_drop_cancelled_label(),
            dbflux_i18n::t!("sidebar.toast.schema_drop_cancelled")
        );
    }

    #[test]
    fn drop_failed_label_includes_the_error() {
        let label = super::drop_failed_label("permission denied");

        assert!(label.contains("permission denied"));
        assert_eq!(
            label,
            dbflux_i18n::t!("sidebar.toast.drop_failed", error = "permission denied")
        );
    }

    #[test]
    fn reveal_failed_label_includes_the_error() {
        let label = super::reveal_failed_label("no such file");

        assert!(label.contains("no such file"));
        assert_eq!(
            label,
            dbflux_i18n::t!("sidebar.toast.reveal_failed", error = "no such file")
        );
    }

    #[test]
    fn skipped_tables_label_one_vs_many() {
        let singular = super::skipped_tables_label(1);
        let plural = super::skipped_tables_label(3);

        assert_eq!(
            singular,
            dbflux_i18n::t!("sidebar.overlay.skipped_tables.one")
        );
        assert_eq!(
            plural,
            dbflux_i18n::t!("sidebar.overlay.skipped_tables.many", count = 3)
        );
        assert!(plural.contains('3'));
        assert_ne!(singular, plural);
    }

    #[test]
    fn schema_snapshot_failed_label_includes_the_error() {
        let label = super::schema_snapshot_failed_label("disk full");

        assert!(label.contains("disk full"));
        assert_eq!(
            label,
            dbflux_i18n::t!("sidebar.toast.schema_snapshot_failed", error = "disk full")
        );
    }

    const E1_KEYS: [&str; 22] = [
        "sidebar.error.cannot_load_schema_types",
        "sidebar.error.cannot_load_schema_indexes",
        "sidebar.error.cannot_load_schema_foreign_keys",
        "sidebar.error.cannot_load_schema_routines",
        "sidebar.toast.load_failed.data_types",
        "sidebar.toast.load_failed.indexes",
        "sidebar.toast.load_failed.foreign_keys",
        "sidebar.toast.load_failed.routines",
        "sidebar.task.listing_buckets",
        "sidebar.toast.list_buckets_failed",
        "sidebar.toast.code_generation_failed",
        "sidebar.toast.loading_event_streams",
        "sidebar.task.loading_database_schema",
        "sidebar.toast.load_schema_failed",
        "sidebar.task.connecting_to_database",
        "sidebar.toast.connect_database_failed",
        "sidebar.task.refreshing_database",
        "sidebar.toast.refresh_database_failed",
        "sidebar.toast.database_refresh_pending",
        "sidebar.task.refreshing_schema_object",
        "sidebar.toast.prepare_schema_object_refresh_failed",
        "sidebar.toast.refresh_schema_object_failed",
    ];

    #[test]
    fn new_folder_default_name_resolves_in_both_locales() {
        let key = "sidebar.tree.folder.new_default";
        let english = dbflux_i18n::t!(key, locale = "en");
        let spanish = dbflux_i18n::t!(key, locale = "es");

        assert_eq!(english, "New Folder");
        assert_ne!(spanish, format!("es.{key}"));
        assert_ne!(english, spanish);
    }

    #[test]
    fn sidebar_e1_keys_resolve_in_both_locales() {
        for key in E1_KEYS {
            for locale in ["en", "es"] {
                let value = dbflux_i18n::t!(key, locale = locale);

                assert_ne!(value, key, "missing translation for {locale}.{key}");
                assert_ne!(
                    value,
                    format!("{locale}.{key}"),
                    "translation fell back to the miss sentinel for {locale}.{key}"
                );
            }
        }
    }

    #[test]
    fn sidebar_task_refreshing_database_diverges_between_locales() {
        let english = dbflux_i18n::t!("sidebar.task.refreshing_database", locale = "en");
        let spanish = dbflux_i18n::t!("sidebar.task.refreshing_database", locale = "es");

        assert_ne!(english, spanish);
    }

    #[test]
    fn cannot_load_schema_types_label_is_stable() {
        assert_eq!(
            super::cannot_load_schema_types_label(),
            dbflux_i18n::t!("sidebar.error.cannot_load_schema_types")
        );
    }

    #[test]
    fn data_types_load_failed_label_includes_the_error() {
        let label = super::data_types_load_failed_label("connection reset");

        assert!(label.contains("connection reset"));
        assert_eq!(
            label,
            dbflux_i18n::t!(
                "sidebar.toast.load_failed.data_types",
                error = "connection reset"
            )
        );
    }

    #[test]
    fn listing_buckets_task_label_includes_the_profile_name() {
        let label = super::listing_buckets_task_label("prod-db");

        assert!(label.contains("prod-db"));
        assert_eq!(
            label,
            dbflux_i18n::t!("sidebar.task.listing_buckets", name = "prod-db")
        );
    }

    #[test]
    fn refreshing_database_task_label_includes_the_database_name() {
        let label = super::refreshing_database_task_label("orders");

        assert!(label.contains("orders"));
        assert_eq!(
            label,
            dbflux_i18n::t!("sidebar.task.refreshing_database", name = "orders")
        );
    }

    #[test]
    fn database_refresh_pending_label_is_stable() {
        assert_eq!(
            super::database_refresh_pending_label(),
            dbflux_i18n::t!("sidebar.toast.database_refresh_pending")
        );
    }
}
