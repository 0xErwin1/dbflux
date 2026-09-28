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
        // The Table / JSON switch of the results chrome sets the result view,
        // which no key switches (T only cycles a collection's views).
        (
            "seg-ctl-item-table",
            KeyboardPath::MouseOnly("gap: no key switches the Table / JSON result views"),
        ),
        (
            "seg-ctl-item-json",
            KeyboardPath::MouseOnly("gap: no key switches the Table / JSON result views"),
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
        (
            "row-inspector-pin",
            KeyboardPath::MouseOnly("gap: no key pins the row inspector to its row"),
        ),
        (
            "value-panel-*",
            KeyboardPath::MouseOnly(
                "gap: the value panel's format, wrap, transform, revert and save buttons have \
                 no key inside the island (Enter edits, Escape leaves)",
            ),
        ),
        (
            "seg-ctl-item-result-view-*",
            KeyboardPath::MouseOnly("gap: no key switches the Data / JSON / Chart result views"),
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
