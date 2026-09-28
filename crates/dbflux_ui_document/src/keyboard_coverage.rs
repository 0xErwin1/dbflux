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
