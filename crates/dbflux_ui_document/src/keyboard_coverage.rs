//! Keyboard coverage registries of the document surfaces.
//!
//! Each registry maps the id of every clickable element a surface draws to
//! the keyboard path that runs the same action (see
//! `dbflux_ui_base::keyboard_coverage`). The coverage tests beside each
//! surface render it and check the frame against its registry, so a new
//! `.id(..).on_click(..)` element fails until it is listed here.

use dbflux_app::keymap::{Command, ContextId};
use dbflux_ui_base::keyboard_coverage::{KeyboardPath, SurfaceRegistry};

/// The code editor's toolbar and context bar. The pane actions menu (`m` in
/// the context bar, Shift+F10 in the editor) lists the toolbar.
pub(crate) const CODE_EDITOR_CHROME: SurfaceRegistry = SurfaceRegistry {
    name: "code editor chrome",
    contexts: &[ContextId::Editor, ContextId::ContextBar, ContextId::Results],
    entries: &[
        ("run-query-btn", KeyboardPath::Command(Command::RunQuery)),
        (
            "run-in-new-tab-btn",
            KeyboardPath::Command(Command::RunQueryInNewTab),
        ),
        (
            "toolbar-save-btn",
            KeyboardPath::Command(Command::SaveQuery),
        ),
        ("toolbar-history-btn", KeyboardPath::Menu("history")),
        ("toolbar-explain-btn", KeyboardPath::Menu("explain")),
        ("toolbar-chart-btn", KeyboardPath::Menu("chart")),
        ("toolbar-format-btn", KeyboardPath::Menu("format")),
        ("sql-refresh-action", KeyboardPath::Menu("refresh")),
        ("sql-auto-refresh.*", KeyboardPath::Menu("auto-refresh")),
        (
            "exec-context-pane-actions",
            KeyboardPath::Command(Command::OpenPaneActions),
        ),
        (
            "close-result-tab-*",
            KeyboardPath::Command(Command::CloseResultTab),
        ),
        (
            "result-tab-*",
            KeyboardPath::Command(Command::NextResultTab),
        ),
        (
            "toggle-maximize-results",
            KeyboardPath::Command(Command::ToggleResults),
        ),
        (
            "hide-results-panel",
            KeyboardPath::Command(Command::ToggleEditor),
        ),
        // The mode bar of the results (Data, JSON, Chart): Shift+T shows the
        // next view, and the grid's Toolbar submenu lists them.
        (
            "seg-ctl-item-table",
            KeyboardPath::Command(Command::CycleResultView),
        ),
        (
            "seg-ctl-item-json",
            KeyboardPath::Command(Command::CycleResultView),
        ),
    ],
};

/// The data grid: the table, its toolbar and footer, its menus and its side
/// islands. The table menu (`m`) ends with a Toolbar submenu that lists the
/// toolbar buttons shown (`DataGridPanel::toolbar_actions`).
pub(crate) const DATA_GRID: SurfaceRegistry = SurfaceRegistry {
    name: "data grid",
    contexts: &[
        ContextId::Results,
        ContextId::DataTable,
        ContextId::ContextMenu,
        ContextId::Inspector,
    ],
    entries: &[
        ("cell-*", KeyboardPath::Command(Command::SelectNext)),
        // The table menu of the column carries Order ascending / descending.
        (
            "header-col-*",
            KeyboardPath::Command(Command::OpenContextMenu),
        ),
        // Every row of the table menu and its submenus.
        ("context-menu.*", KeyboardPath::Command(Command::MenuSelect)),
        ("export-menu.*", KeyboardPath::Command(Command::MenuSelect)),
        (
            "export-trigger",
            KeyboardPath::Command(Command::ExportResults),
        ),
        (
            "record-mode-toggle",
            KeyboardPath::Command(Command::ToggleRecordView),
        ),
        (
            "refresh-action",
            KeyboardPath::Command(Command::RefreshSchema),
        ),
        (
            "data-grid-auto-refresh.*",
            KeyboardPath::Menu("auto-refresh"),
        ),
        ("clear-filter", KeyboardPath::Command(Command::ClearFilter)),
        (
            "view-toggle-btn",
            KeyboardPath::Command(Command::CycleDocumentView),
        ),
        ("open-builder-btn", KeyboardPath::Menu("open-builder")),
        ("builder-notice-edit", KeyboardPath::Menu("open-builder")),
        ("builder-notice-reset", KeyboardPath::Menu("reset-builder")),
        ("toggle-maximize", KeyboardPath::Menu("maximize")),
        ("hide-panel", KeyboardPath::Menu("hide")),
        ("undo-btn", KeyboardPath::Command(Command::Undo)),
        ("redo-btn", KeyboardPath::Command(Command::Redo)),
        ("save-btn", KeyboardPath::Menu("save-changes")),
        ("revert-btn", KeyboardPath::Menu("revert-changes")),
        (
            "row-inspector-close",
            KeyboardPath::Command(Command::ToggleRowInspector),
        ),
        (
            "row-inspector-copy",
            KeyboardPath::Command(Command::ResultsCopyRow),
        ),
        (
            "row-inspector-edit",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "row-inspector-duplicate",
            KeyboardPath::Command(Command::ResultsDuplicateRow),
        ),
        (
            "row-inspector-delete",
            KeyboardPath::Command(Command::ResultsDeleteRow),
        ),
        ("row-inspector-pin", KeyboardPath::Menu("pin-row-inspector")),
        // The value panel's buttons are Toolbar entries of the table menu,
        // which `m` also opens from inside the panel.
        ("value-panel-format-*", KeyboardPath::Menu("value-format-*")),
        ("value-panel-wrap", KeyboardPath::Menu("value-wrap")),
        (
            "value-panel-format",
            KeyboardPath::Menu("value-pretty-print"),
        ),
        ("value-panel-compact", KeyboardPath::Menu("value-compact")),
        ("value-panel-revert", KeyboardPath::Menu("value-revert")),
        ("value-panel-save", KeyboardPath::Menu("value-save")),
        // The footer's view switch (Grid / JSON / Chart): Shift+T, and the
        // Toolbar submenu's Show entries.
        (
            "seg-ctl-item-result-view-*",
            KeyboardPath::Command(Command::CycleResultView),
        ),
        // The Tree / Table / JSON switch of a document collection.
        (
            "seg-ctl-item-tree",
            KeyboardPath::Command(Command::CycleDocumentView),
        ),
        (
            "seg-ctl-item-table",
            KeyboardPath::Command(Command::CycleDocumentView),
        ),
        (
            "seg-ctl-item-json",
            KeyboardPath::Command(Command::CycleDocumentView),
        ),
        // The Documents / Schema / Aggregate views of a document collection.
        (
            "seg-ctl-item-documents",
            KeyboardPath::Command(Command::NextResultTab),
        ),
        (
            "seg-ctl-item-schema",
            KeyboardPath::Command(Command::NextResultTab),
        ),
        (
            "seg-ctl-item-aggregate",
            KeyboardPath::Command(Command::NextResultTab),
        ),
        ("collection-find", KeyboardPath::Menu("find")),
        (
            "collection-builder-toggle",
            KeyboardPath::Menu("open-builder"),
        ),
    ],
};

/// The SQL query builder rail. Every control on a rail row is a field of
/// that row (`QueryBuilderPanel::rail_rows`): J/K reach the row, H/L the
/// field, Enter works it, Space toggles, A / Shift+A add, X removes. The
/// header buttons are entries of the rail menu (`m`).
pub(crate) const QUERY_BUILDER: SurfaceRegistry = SurfaceRegistry {
    name: "SQL query builder",
    contexts: &[ContextId::QueryBuilder, ContextId::ContextMenu],
    entries: &[
        ("qb-run", KeyboardPath::Command(Command::RunQuery)),
        ("qb-hdr-save", KeyboardPath::Command(Command::SaveQuery)),
        ("qb-hdr-reset", KeyboardPath::Menu("reset")),
        ("qb-hdr-close", KeyboardPath::Menu("close")),
        ("qb-open-editor", KeyboardPath::Menu("open-in-editor")),
        ("qb-mode-*", KeyboardPath::Command(Command::NextPanelTab)),
        ("qb-rail-menu.*", KeyboardPath::Command(Command::MenuSelect)),
        ("qb-grp-add-grp", KeyboardPath::Command(Command::AddGroup)),
        (
            "qb-join-grp-add-grp",
            KeyboardPath::Command(Command::AddGroup),
        ),
        (
            "qb-add-first-group",
            KeyboardPath::Command(Command::AddGroup),
        ),
        (
            "qb-having-add-first-group",
            KeyboardPath::Command(Command::AddGroup),
        ),
        ("qb-*-rm*", KeyboardPath::Command(Command::Delete)),
        ("qb-rm-*", KeyboardPath::Command(Command::Delete)),
        ("qb-*-add*", KeyboardPath::Command(Command::AddItem)),
        ("qb-add-*", KeyboardPath::Command(Command::AddItem)),
        (
            "qb-all-columns",
            KeyboardPath::Command(Command::ExpandCollapse),
        ),
        (
            "qb-col-toggle",
            KeyboardPath::Command(Command::ExpandCollapse),
        ),
        (
            "qb-sortkey-dir",
            KeyboardPath::Command(Command::ExpandCollapse),
        ),
        (
            "qb-assign-kind",
            KeyboardPath::Command(Command::ExpandCollapse),
        ),
        (
            "qb-exec-mode*",
            KeyboardPath::Command(Command::ExpandCollapse),
        ),
        (
            "seg-ctl-item-qb-*grp-op-*",
            KeyboardPath::Command(Command::ExpandCollapse),
        ),
        // Dropdowns, chips and the banner on a row: Enter on the field.
        ("qb-pred-cmp-dd*", KeyboardPath::Command(Command::Execute)),
        (
            "qb-having-pred-cmp-dd*",
            KeyboardPath::Command(Command::Execute),
        ),
        ("qb-agg-fn-dd*", KeyboardPath::Command(Command::Execute)),
        ("qb-join-kind-dd*", KeyboardPath::Command(Command::Execute)),
        ("qb-join-cond-op*", KeyboardPath::Command(Command::Execute)),
        ("qb-sort-column*", KeyboardPath::Command(Command::Execute)),
        ("qb-col-chip*", KeyboardPath::Command(Command::Execute)),
        (
            "qb-dismiss-fk-banner",
            KeyboardPath::Command(Command::Execute),
        ),
    ],
};

/// The document builder rail, built like the SQL builder rail.
pub(crate) const DOCUMENT_BUILDER: SurfaceRegistry = SurfaceRegistry {
    name: "document builder",
    contexts: &[ContextId::DocumentBuilder, ContextId::ContextMenu],
    entries: &[
        ("doc-builder-find", KeyboardPath::Command(Command::RunQuery)),
        (
            "doc-builder-run-pipeline",
            KeyboardPath::Command(Command::RunQuery),
        ),
        (
            "doc-builder-save",
            KeyboardPath::Command(Command::SaveQuery),
        ),
        ("doc-builder-close", KeyboardPath::Menu("close")),
        (
            "doc-builder-open-editor",
            KeyboardPath::Menu("open-in-editor"),
        ),
        ("doc-builder-saved-toggle", KeyboardPath::Menu("saved")),
        (
            "doc-builder-mode-*",
            KeyboardPath::Command(Command::NextPanelTab),
        ),
        (
            "doc-builder-rail-menu.*",
            KeyboardPath::Command(Command::MenuSelect),
        ),
        (
            "doc-builder-add-group-*",
            KeyboardPath::Command(Command::AddGroup),
        ),
        ("doc-builder-*add*", KeyboardPath::Command(Command::AddItem)),
        (
            "doc-builder-*remove*",
            KeyboardPath::Command(Command::Delete),
        ),
        // The AND / OR switch of a group and Include / Exclude of the
        // projection flip with Space on their row.
        (
            "segmented-doc-builder-combinator-*",
            KeyboardPath::Command(Command::ExpandCollapse),
        ),
        (
            "segmented-doc-builder-projection-mode-*",
            KeyboardPath::Command(Command::ExpandCollapse),
        ),
        (
            "doc-builder-sort-dir-*",
            KeyboardPath::Command(Command::ExpandCollapse),
        ),
        (
            "doc-builder-bool-*",
            KeyboardPath::Command(Command::ExpandCollapse),
        ),
        (
            "doc-builder-match-edit",
            KeyboardPath::Command(Command::ExpandCollapse),
        ),
        // The fields of a row: Enter on the field.
        (
            "doc-builder-field-*",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "doc-builder-operator-*",
            KeyboardPath::Command(Command::Execute),
        ),
        ("doc-builder-acc-*", KeyboardPath::Command(Command::Execute)),
        (
            "doc-builder-saved-*",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "doc-builder-conflict-*",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "doc-builder-use-oid-*",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "doc-builder-picker*",
            KeyboardPath::Command(Command::Execute),
        ),
    ],
};

/// A chart tab: its toolbar, axis bar and pickers. The pane actions menu
/// (`m`) lists the toolbar controls the chart keys do not reach.
pub(crate) const CHART: SurfaceRegistry = SurfaceRegistry {
    name: "chart",
    contexts: &[ContextId::Chart, ContextId::ContextMenu, ContextId::Modal],
    entries: &[
        // The name prompt of Save chart.
        ("confirm-save", KeyboardPath::Command(Command::Execute)),
        ("cancel-save", KeyboardPath::Command(Command::Cancel)),
        ("axis-pill-x", KeyboardPath::Menu("chart-axis-x")),
        ("axis-pill-y", KeyboardPath::Menu("chart-axis-y")),
        ("axis-pill-group", KeyboardPath::Menu("chart-axis-group")),
        ("axis-pill-agg", KeyboardPath::Menu("chart-axis-agg")),
        // The rows of an open axis picker: J and K move, Enter picks.
        ("axis-picker-*.*", KeyboardPath::Command(Command::Execute)),
        (
            "chart-toolbar-save",
            KeyboardPath::Command(Command::SaveQuery),
        ),
        ("chart-toolbar-stats", KeyboardPath::Menu("chart-stats")),
        (
            "chart-doc-refresh.*",
            KeyboardPath::Menu("chart-auto-refresh"),
        ),
        (
            "refresh-action",
            KeyboardPath::Command(Command::RefreshSchema),
        ),
        (
            "seg-ctl-item-chart-kind-*",
            KeyboardPath::Command(Command::NextPanelTab),
        ),
        (
            "seg-ctl-item-time-preset-*",
            KeyboardPath::Command(Command::NextTimeRange),
        ),
    ],
};

/// A dashboard tab: its header, panel chrome and the Configure popover. The
/// pane actions menu (`m`) lists the header controls; a panel's own actions
/// are dashboard keys on the selected panel.
pub(crate) const DASHBOARD: SurfaceRegistry = SurfaceRegistry {
    name: "dashboard",
    contexts: &[
        ContextId::Dashboard,
        ContextId::Chart,
        ContextId::ContextMenu,
        ContextId::Modal,
    ],
    entries: &[
        (
            "dash-add-panel-toolbar",
            KeyboardPath::Command(Command::AddItem),
        ),
        (
            "dashboard-refresh.*",
            KeyboardPath::Menu("dashboard-auto-refresh"),
        ),
        (
            "seg-ctl-item-dash-mode-*",
            KeyboardPath::Command(Command::NextPanelTab),
        ),
        (
            "dashboard-divider-*",
            KeyboardPath::Command(Command::ExpandCollapse),
        ),
        // The panel menu: its entries are the selected panel's pane actions.
        (
            "panel-kebab-*",
            KeyboardPath::Command(Command::OpenPaneActions),
        ),
        // The Configure popover (C), a dialog that takes the chart keys.
        ("configure-apply", KeyboardPath::Command(Command::Execute)),
        ("configure-cancel", KeyboardPath::Command(Command::Cancel)),
        (
            "configure-kind-*",
            KeyboardPath::Command(Command::NextPanelTab),
        ),
        // Stats of the opened chart panel (Enter, then M).
        ("configure-stats", KeyboardPath::Menu("chart-stats")),
        // A chart panel's axis pills: C opens the Configure popover, where H
        // and L open the same pickers.
        (
            "panel-card-*.axis-pill-*",
            KeyboardPath::Command(Command::ConfigurePanel),
        ),
        // A chart panel draws the chart's toolbar; the dashboard owns its
        // refresh (F5 on the entered panel), and the pane actions of the
        // opened panel (Enter, then M) list the panel's own interval.
        (
            "panel-card-*.chart-doc-refresh.*",
            KeyboardPath::Menu("chart-auto-refresh"),
        ),
    ],
};

/// The key-value browser: key list, toolbar and value panel. Its `m` menu
/// (`KeyValueDocument::build_key_menu_items`) lists the toolbar and value
/// panel buttons; menu entry ids are the `KvMenuAction` names.
pub(crate) const KEY_VALUE: SurfaceRegistry = SurfaceRegistry {
    name: "key-value browser",
    contexts: &[
        ContextId::KeyValue,
        ContextId::Results,
        ContextId::ContextMenu,
    ],
    entries: &[
        ("kv-key-row-*", KeyboardPath::Command(Command::SelectNext)),
        (
            "kv-folder-row-*",
            KeyboardPath::Command(Command::SelectNext),
        ),
        ("kv-new-key", KeyboardPath::Command(Command::ResultsAddRow)),
        ("kv-rename-key", KeyboardPath::Command(Command::Rename)),
        ("kv-delete-key", KeyboardPath::Command(Command::Delete)),
        (
            "refresh-action",
            KeyboardPath::Command(Command::RefreshSchema),
        ),
        ("kv-auto-refresh.*", KeyboardPath::Menu("AutoRefresh")),
        ("kv-copy-key", KeyboardPath::Menu("CopyKey")),
        ("kv-copy-command", KeyboardPath::Menu("CopyAsCommand")),
        ("kv-edit-value", KeyboardPath::Menu("EditValue")),
        // Double click edits the value, as the Edit button does.
        ("kv-value-text", KeyboardPath::Menu("EditValue")),
        ("kv-reload-value", KeyboardPath::Menu("ReloadValue")),
        (
            "segmented-key-list-layout-*",
            KeyboardPath::Menu("ToggleListLayout"),
        ),
        // The string value's toolbar: its View as switch and decompression
        // list are entries of the value menu (`m` in the value panel).
        ("kv-compression.*", KeyboardPath::Menu("Decompression")),
        ("seg-ctl-item-auto", KeyboardPath::Menu("ViewAs(*")),
        ("seg-ctl-item-text", KeyboardPath::Menu("ViewAs(*")),
        ("seg-ctl-item-json", KeyboardPath::Menu("ViewAs(*")),
        ("seg-ctl-item-hex", KeyboardPath::Menu("ViewAs(*")),
        ("seg-ctl-item-msgpack", KeyboardPath::Menu("ViewAs(*")),
    ],
};

/// The native command console docked under a document (`crate::console`).
/// Ctrl+` toggles it (bound globally, and on the key-value document itself);
/// with the document focused, Enter runs a command waiting for confirmation
/// and Escape drops it, as the Run anyway and Cancel buttons do.
pub(crate) const NATIVE_CONSOLE: SurfaceRegistry = SurfaceRegistry {
    name: "native console",
    contexts: &[
        ContextId::KeyValue,
        ContextId::Global,
        ContextId::Results,
        ContextId::TextInput,
    ],
    entries: &[
        (
            "native-console-header",
            KeyboardPath::Command(Command::ToggleConsole),
        ),
        (
            "native-console-run-anyway",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "native-console-cancel",
            KeyboardPath::Command(Command::Cancel),
        ),
    ],
};

/// The object browser of a bucket: its listing, toolbar and preview. The
/// row menu (`m`) lists the row's actions and the listing's buttons; with no
/// row selected the same buttons are the pane actions. Row menu entry ids
/// are the `ObjectMenuAction` names.
pub(crate) const OBJECT_BROWSER: SurfaceRegistry = SurfaceRegistry {
    name: "object browser",
    contexts: &[ContextId::Results, ContextId::ContextMenu],
    entries: &[
        ("object-row-*", KeyboardPath::Command(Command::SelectNext)),
        (
            "object-browser-context-menu.*",
            KeyboardPath::Command(Command::MenuSelect),
        ),
        (
            "object-browser-refresh",
            KeyboardPath::Command(Command::RefreshSchema),
        ),
        // A breadcrumb segment: H goes up one level.
        (
            "object-path-copy",
            KeyboardPath::Menu("object-browser-copy-path"),
        ),
        ("object-path-*", KeyboardPath::Command(Command::ColumnLeft)),
        (
            "object-browser-upload",
            KeyboardPath::Menu("object-browser-upload"),
        ),
        (
            "object-browser-new-folder",
            KeyboardPath::Menu("object-browser-new-folder"),
        ),
        (
            "object-browser-load-more",
            KeyboardPath::Menu("object-browser-load-more"),
        ),
        (
            "seg-ctl-item-object-browser-mode-*",
            KeyboardPath::Menu("object-browser-toggle-tree"),
        ),
    ],
};

/// The bucket list of an object store. Its pane actions (`m`) list Browse,
/// Calculate size, New bucket and Refresh.
pub(crate) const BUCKETS: SurfaceRegistry = SurfaceRegistry {
    name: "bucket list",
    contexts: &[ContextId::Results, ContextId::ContextMenu],
    entries: &[
        ("bucket-row-*", KeyboardPath::Command(Command::SelectNext)),
        ("buckets-browse", KeyboardPath::Command(Command::Execute)),
        ("buckets-new", KeyboardPath::Command(Command::ResultsAddRow)),
        (
            "buckets-refresh",
            KeyboardPath::Command(Command::RefreshSchema),
        ),
        (
            "buckets-calculate-size",
            KeyboardPath::Menu("buckets-calculate-size"),
        ),
    ],
};

/// An object editor tab. Its pane actions (`m`, once Escape leaves the
/// text) list Save, Discard, Find, the interpretation, Reload and Load
/// anyway.
pub(crate) const OBJECT_EDITOR: SurfaceRegistry = SurfaceRegistry {
    name: "object editor",
    contexts: &[
        ContextId::Results,
        ContextId::TextInput,
        ContextId::ContextMenu,
    ],
    entries: &[
        (
            "object-editor-save",
            KeyboardPath::Command(Command::SaveQuery),
        ),
        (
            "object-editor-discard",
            KeyboardPath::Menu("object-editor-discard"),
        ),
        (
            "object-editor-find",
            KeyboardPath::Menu("object-editor-find"),
        ),
    ],
};

/// A delimited file tab: the dialect toolbar, the table of the records
/// loaded so far, the control that loads the next page (`]`), the reload
/// (`f5`), the edit controls, and its dialogs. The pane actions list the
/// toolbar, insert above, add and rename a column, discard, the cancel of a
/// running load of the rest, and reload: each select entry opens its list
/// with the keyboard on it. Save is the table's save key. The rows stay in
/// file order, so the document does not sort and a column header click has
/// no action for a key to reach. The column prompt and the offer to load the
/// rest confirm with Enter and close with Escape, and the modal cell editor
/// saves with its save key and closes with Escape.
pub(crate) const DELIMITED: SurfaceRegistry = SurfaceRegistry {
    name: "delimited file",
    contexts: &[
        ContextId::Results,
        ContextId::DataTable,
        ContextId::Modal,
        ContextId::CellEditorModal,
    ],
    entries: &[
        ("cell-*", KeyboardPath::Command(Command::SelectNext)),
        (
            "header-col-*",
            KeyboardPath::MouseOnly(
                "a header click has no action in this document: rows stay in file order, \
                 and renaming the column is in the pane actions menu",
            ),
        ),
        (
            "delimited-load-more",
            KeyboardPath::Command(Command::ResultsNextPage),
        ),
        (
            "delimited-delimiter.*",
            KeyboardPath::Menu("delimited-delimiter"),
        ),
        ("delimited-quote.*", KeyboardPath::Menu("delimited-quote")),
        (
            "delimited-encoding.*",
            KeyboardPath::Menu("delimited-encoding"),
        ),
        ("delimited-header", KeyboardPath::Menu("delimited-header")),
        (
            "delimited-dialect-reset",
            KeyboardPath::Menu("delimited-dialect-reset"),
        ),
        ("delimited-save", KeyboardPath::Command(Command::SaveRow)),
        ("delimited-discard", KeyboardPath::Menu("delimited-discard")),
        (
            "delimited-insert-above",
            KeyboardPath::Menu("delimited-insert-above"),
        ),
        (
            "delimited-reload",
            KeyboardPath::Command(Command::RefreshSchema),
        ),
        (
            "delimited-add-column",
            KeyboardPath::Menu("delimited-add-column"),
        ),
        (
            "delimited-load-rest-cancel",
            KeyboardPath::Menu("delimited-load-rest-cancel"),
        ),
        (
            "delimited-column-name-confirm",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "delimited-column-name-cancel",
            KeyboardPath::Command(Command::Cancel),
        ),
        (
            "delimited-load-rest-confirm",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "delimited-load-rest-dismiss",
            KeyboardPath::Command(Command::Cancel),
        ),
        (
            "cell-editor-save",
            KeyboardPath::Command(Command::SaveQuery),
        ),
        ("cell-editor-cancel", KeyboardPath::Command(Command::Cancel)),
        ("modal-close", KeyboardPath::Command(Command::Cancel)),
    ],
};

/// The audit viewer: its toolbar ring (`f`), filters, event rows and their
/// expanded details, and the row menu (`m`).
pub(crate) const AUDIT: SurfaceRegistry = SurfaceRegistry {
    name: "audit viewer",
    contexts: &[ContextId::Audit, ContextId::ContextMenu],
    entries: &[
        (
            "audit-event-*",
            KeyboardPath::Command(Command::ExpandCollapse),
        ),
        ("audit-detail-copy-json", KeyboardPath::Menu("CopyJson")),
        (
            "audit-detail-filter-correlation",
            KeyboardPath::Menu("FilterByCorrelation"),
        ),
        (
            "audit-detail-open-approval",
            KeyboardPath::Menu("OpenApproval"),
        ),
        (
            "audit-export-trigger",
            KeyboardPath::Command(Command::ExportResults),
        ),
        (
            "seg-ctl-item-audit-view-*",
            KeyboardPath::Command(Command::NextPanelTab),
        ),
        (
            "refresh-action",
            KeyboardPath::Command(Command::RefreshSchema),
        ),
        // The toolbar ring (F, then H and L or the arrows, Enter or Space)
        // walks the time presets, the custom range, the timezone, the level,
        // category and outcome filters, the auto-refresh interval and Clear.
        (
            "seg-ctl-item-time-preset-*",
            KeyboardPath::Command(Command::FocusToolbar),
        ),
        (
            "audit-timestamp-mode.*",
            KeyboardPath::Command(Command::FocusToolbar),
        ),
        (
            "audit-level.*",
            KeyboardPath::Command(Command::FocusToolbar),
        ),
        (
            "audit-category.*",
            KeyboardPath::Command(Command::FocusToolbar),
        ),
        (
            "audit-outcome.*",
            KeyboardPath::Command(Command::FocusToolbar),
        ),
        (
            "audit-auto-refresh.*",
            KeyboardPath::Command(Command::FocusToolbar),
        ),
        (
            "audit-clear-btn",
            KeyboardPath::Command(Command::FocusToolbar),
        ),
    ],
};

/// The MCP approvals tab: the pending calls and the decision buttons.
#[cfg(feature = "mcp")]
pub(crate) const MCP_APPROVALS: SurfaceRegistry = SurfaceRegistry {
    name: "MCP approvals",
    contexts: &[ContextId::McpApprovals, ContextId::ContextMenu],
    entries: &[
        ("pending-*", KeyboardPath::Command(Command::SelectNext)),
        (
            "mcp-approval-approve",
            KeyboardPath::Command(Command::ApproveExecution),
        ),
        (
            "mcp-approval-reject",
            KeyboardPath::Command(Command::RejectExecution),
        ),
        (
            "mcp-approvals-refresh",
            KeyboardPath::Command(Command::RefreshSchema),
        ),
    ],
};

/// The schema diff tab. Every control is a field of a rail row
/// (`schema_diff::keyboard`): J and K reach the row, H and L the field,
/// Enter presses it and Space checks a change. The pane actions (`m`) list
/// Compute, Preview DDL, Apply and the modes.
pub(crate) const SCHEMA_DIFF: SurfaceRegistry = SurfaceRegistry {
    name: "schema diff",
    contexts: &[ContextId::Results, ContextId::ContextMenu],
    entries: &[
        ("mode-*", KeyboardPath::Command(Command::Execute)),
        ("ref-db-*", KeyboardPath::Command(Command::Execute)),
        ("ref-conn-*", KeyboardPath::Command(Command::Execute)),
        ("snap-*", KeyboardPath::Command(Command::Execute)),
        ("chk-*", KeyboardPath::Command(Command::ExpandCollapse)),
        (
            "compute-diff",
            KeyboardPath::Command(Command::RefreshSchema),
        ),
        ("preview-ddl", KeyboardPath::Menu("schema-diff-preview")),
        ("apply-ddl", KeyboardPath::Menu("schema-diff-apply")),
    ],
};

/// The migrate wizard: the step rail, each step's controls and the footer.
/// Alt+L / Alt+H continue and go back, and the pane actions (`m`) list the
/// footer buttons.
pub(crate) const MIGRATE_WIZARD: SurfaceRegistry = SurfaceRegistry {
    name: "migrate wizard",
    contexts: &[ContextId::MigrateWizard, ContextId::ContextMenu],
    entries: &[
        // The source and target trees: H and L switch tree, J and K move,
        // Enter chooses, Space checks a table.
        ("migrate-tree-*.*", KeyboardPath::Command(Command::Execute)),
        // Mapping rows: L moves to the Mode or Columns field, Enter opens it.
        ("migrate-mode-*.*", KeyboardPath::Command(Command::Execute)),
        (
            "migrate-transform-*",
            KeyboardPath::Command(Command::Execute),
        ),
        (
            "migrate-bulk-existing",
            KeyboardPath::Menu("migrate-set-all-existing"),
        ),
        (
            "migrate-bulk-truncate",
            KeyboardPath::Menu("migrate-set-all-truncate"),
        ),
        (
            "migrate-bulk-skip",
            KeyboardPath::Menu("migrate-set-all-skip"),
        ),
        (
            "migrate-wizard-continue",
            KeyboardPath::Command(Command::NextPanelTab),
        ),
        (
            "migrate-wizard-back",
            KeyboardPath::Command(Command::PrevPanelTab),
        ),
        (
            "migrate-wizard-cancel",
            KeyboardPath::Menu("migrate-cancel-run"),
        ),
        ("migrate-wizard-close", KeyboardPath::Menu("migrate-close")),
        // A finished step of the step rail: Alt+H steps back to it.
        (
            "wizard-rail-*",
            KeyboardPath::Command(Command::PrevPanelTab),
        ),
    ],
};
