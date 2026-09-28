//! The default keymap: one layer of bindings per context.

use dbflux_app::keymap::{
    Command, ContextId, KeyChord, KeySequence, KeymapLayer, KeymapStack, Modifiers,
};
use std::sync::LazyLock;

pub(super) static DEFAULT_KEYMAP: LazyLock<KeymapStack> = LazyLock::new(|| {
    let mut stack = KeymapStack::new();

    stack.add_layer(global_layer());
    stack.add_layer(sidebar_layer());
    stack.add_layer(editor_layer());
    stack.add_layer(history_modal_layer());
    stack.add_layer(results_layer());
    stack.add_layer(background_tasks_layer());
    stack.add_layer(command_palette_layer());
    stack.add_layer(connection_manager_layer());
    stack.add_layer(text_input_layer());
    stack.add_layer(dropdown_layer());
    stack.add_layer(context_menu_layer());
    stack.add_layer(confirm_modal_layer());
    stack.add_layer(form_navigation_layer());
    stack.add_layer(context_bar_layer());
    stack.add_layer(audit_layer());
    stack.add_layer(event_streams_picker_layer());
    stack.add_layer(schema_viz_layer());
    stack.add_layer(document_tree_layer());
    stack.add_layer(data_table_layer());
    stack.add_layer(input_layer());
    stack.add_layer(modal_layer());
    stack.add_layer(sql_preview_modal_layer());
    stack.add_layer(cell_editor_modal_layer());
    stack.add_layer(document_preview_modal_layer());
    stack.add_layer(key_value_layer());
    stack.add_layer(settings_layer());
    stack.add_layer(inspector_layer());
    stack.add_layer(notifications_layer());
    stack.add_layer(builder_rail_layer(ContextId::QueryBuilder));
    stack.add_layer(builder_rail_layer(ContextId::DocumentBuilder));
    stack.add_layer(chart_layer());
    stack.add_layer(dashboard_layer());
    stack.add_layer(add_panel_picker_layer());
    stack.add_layer(mcp_approvals_layer());

    stack
});

/// A key sequence written as space-separated chords (`d d`).
fn sequence(text: &str) -> KeySequence {
    match KeySequence::parse(text) {
        Ok(keys) => keys,
        Err(error) => unreachable!("default key sequence `{text}` does not parse: {error}"),
    }
}

fn global_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::Global);

    // Command palette — Cmd+Shift+P on macOS, Ctrl+Shift+P elsewhere.
    layer.bind(
        KeyChord::new("p", Modifiers::primary_shift()),
        Command::ToggleCommandPalette,
    );

    // Connection Manager — Cmd+Shift+N on macOS, Ctrl+Shift+N elsewhere.
    layer.bind(
        KeyChord::new("n", Modifiers::primary_shift()),
        Command::OpenConnectionManager,
    );

    // Tab management — primary modifier (Cmd on macOS, Ctrl elsewhere).
    layer.bind(
        KeyChord::new("n", Modifiers::primary()),
        Command::NewQueryTab,
    );
    layer.bind(
        KeyChord::new("w", Modifiers::primary()),
        Command::CloseCurrentTab,
    );
    // Ctrl+Tab / Ctrl+Shift+Tab stay literal Ctrl on every platform — that is
    // the long-standing tabbed-UI idiom (browsers, terminals). Cmd+Tab on
    // macOS is the system app switcher and must not be shadowed.
    layer.bind(KeyChord::new("tab", Modifiers::ctrl()), Command::NextTab);
    layer.bind(
        KeyChord::new("tab", Modifiers::ctrl_shift()),
        Command::PrevTab,
    );
    // Move the active tab, as dragging it does; literal Ctrl like Ctrl+Tab.
    layer.bind(
        KeyChord::new("pageup", Modifiers::ctrl_shift()),
        Command::MoveTabLeft,
    );
    layer.bind(
        KeyChord::new("pagedown", Modifiers::ctrl_shift()),
        Command::MoveTabRight,
    );
    for i in 1..=9 {
        layer.bind(
            KeyChord::new(i.to_string(), Modifiers::primary()),
            Command::SwitchToTab(i),
        );
    }

    // File operations
    layer.bind(
        KeyChord::new("o", Modifiers::primary()),
        Command::OpenScriptFile,
    );

    // Query execution
    layer.bind(
        KeyChord::new("enter", Modifiers::primary()),
        Command::RunQuery,
    );
    layer.bind(
        KeyChord::new("enter", Modifiers::primary_shift()),
        Command::RunQueryInNewTab,
    );

    // Cancel / close modals
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);

    // Panel cycle (Tab/Shift+Tab)
    layer.bind(
        KeyChord::new("tab", Modifiers::none()),
        Command::CycleFocusForward,
    );
    layer.bind(
        KeyChord::new("tab", Modifiers::shift()),
        Command::CycleFocusBackward,
    );

    // Direct focus shortcuts — stay Ctrl+Shift+1..4 on every platform.
    // Cmd+Shift+3 and Cmd+Shift+4 are reserved by macOS for screenshots, so
    // switching the whole group to the primary modifier would silently break
    // two of the four bindings on Mac.
    //
    // GPUI normalizes Ctrl+Shift+digit at the platform layer (GitHub #65);
    // registering every binding natively applies the same normalization to
    // the binding and the incoming keystroke, so these match on every OS.
    layer.bind(
        KeyChord::new("1", Modifiers::ctrl_shift()),
        Command::FocusSidebar,
    );
    layer.bind(
        KeyChord::new("2", Modifiers::ctrl_shift()),
        Command::FocusEditor,
    );
    layer.bind(
        KeyChord::new("3", Modifiers::ctrl_shift()),
        Command::FocusResults,
    );
    layer.bind(
        KeyChord::new("4", Modifiers::ctrl_shift()),
        Command::FocusBackgroundTasks,
    );

    // Open audit viewer
    layer.bind(
        KeyChord::new("a", Modifiers::primary_shift()),
        Command::OpenAuditViewer,
    );

    // Toggle sidebar
    layer.bind(
        KeyChord::new("b", Modifiers::primary()),
        Command::ToggleSidebar,
    );

    // Tab context menu — stays Ctrl+M everywhere: Cmd+M is the system
    // "minimize window" shortcut on macOS.
    layer.bind(KeyChord::new("m", Modifiers::ctrl()), Command::OpenTabMenu);

    bind_workspace_commands(&mut layer);

    layer
}

/// Workspace commands that also have an activity rail entry, a status bar
/// chip or a title bar button. Every chord holds the primary modifier or
/// Ctrl, so it also works while a text field has focus, and none uses Alt:
/// Ctrl+Alt is AltGr on Windows and types characters on many layouts.
fn bind_workspace_commands(layer: &mut KeymapLayer) {
    layer.bind(
        KeyChord::new(",", Modifiers::primary()),
        Command::OpenSettings,
    );

    layer.bind(
        KeyChord::new("e", Modifiers::primary_shift()),
        Command::ToggleEditor,
    );
    layer.bind(
        KeyChord::new("r", Modifiers::primary_shift()),
        Command::ToggleResults,
    );
    layer.bind(
        KeyChord::new("t", Modifiers::primary_shift()),
        Command::ToggleTasks,
    );
    layer.bind(
        KeyChord::new("b", Modifiers::primary_shift()),
        Command::ToggleNotifications,
    );
    layer.bind(
        KeyChord::new("x", Modifiers::primary_shift()),
        Command::OpenLastErrorInAudit,
    );
    layer.bind(
        KeyChord::new("y", Modifiers::primary_shift()),
        Command::OpenToastActions,
    );

    layer.bind(
        KeyChord::new("l", Modifiers::primary_shift()),
        Command::OpenLoginModal,
    );
    layer.bind(
        KeyChord::new("o", Modifiers::primary_shift()),
        Command::OpenSsoWizard,
    );

    layer.bind(
        KeyChord::new("c", Modifiers::primary_shift()),
        Command::OpenSavedChart,
    );
    layer.bind(
        KeyChord::new("d", Modifiers::primary_shift()),
        Command::NewDashboard,
    );

    // The sidebar views continue the Ctrl+Shift+digit focus group, literal
    // Ctrl on every platform for the same reason.
    layer.bind(
        KeyChord::new("5", Modifiers::ctrl_shift()),
        Command::ShowConnectionsView,
    );
    layer.bind(
        KeyChord::new("6", Modifiers::ctrl_shift()),
        Command::ShowScriptsView,
    );
    layer.bind(
        KeyChord::new("7", Modifiers::ctrl_shift()),
        Command::ShowDashboardsView,
    );

    #[cfg(feature = "mcp")]
    {
        layer.bind(
            KeyChord::new("m", Modifiers::primary_shift()),
            Command::OpenMcpApprovals,
        );
        layer.bind(
            KeyChord::new("g", Modifiers::primary_shift()),
            Command::RefreshMcpGovernance,
        );
    }
}

fn sidebar_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::Sidebar);

    layer.bind(
        KeyChord::new("n", Modifiers::primary()),
        Command::NewQueryTab,
    );
    layer.bind(KeyChord::new("/", Modifiers::none()), Command::FocusSearch);
    layer.bind(
        KeyChord::new("q", Modifiers::none()),
        Command::SidebarNextTab,
    );
    layer.bind(
        KeyChord::new("e", Modifiers::none()),
        Command::SidebarNextTab,
    );

    // Panel navigation (Ctrl+hjkl)
    layer.bind(KeyChord::new("l", Modifiers::ctrl()), Command::FocusRight);

    // Tree collapse/expand
    layer.bind(KeyChord::new("h", Modifiers::none()), Command::ColumnLeft);
    layer.bind(KeyChord::new("l", Modifiers::none()), Command::ColumnRight);

    // List navigation
    layer.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);
    layer.bind(
        KeyChord::new("down", Modifiers::none()),
        Command::SelectNext,
    );
    layer.bind(KeyChord::new("k", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("up", Modifiers::none()), Command::SelectPrev);

    layer.bind(KeyChord::new("g", Modifiers::none()), Command::SelectFirst);
    layer.bind(
        KeyChord::new("home", Modifiers::none()),
        Command::SelectFirst,
    );
    layer.bind(KeyChord::new("g", Modifiers::shift()), Command::SelectLast);
    layer.bind(KeyChord::new("end", Modifiers::none()), Command::SelectLast);

    layer.bind(KeyChord::new("d", Modifiers::ctrl()), Command::PageDown);
    layer.bind(
        KeyChord::new("pagedown", Modifiers::none()),
        Command::PageDown,
    );
    layer.bind(KeyChord::new("u", Modifiers::ctrl()), Command::PageUp);
    layer.bind(KeyChord::new("pageup", Modifiers::none()), Command::PageUp);

    // Actions
    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);
    layer.bind(
        KeyChord::new("space", Modifiers::none()),
        Command::ExpandCollapse,
    );
    layer.bind(
        KeyChord::new("r", Modifiers::none()),
        Command::RefreshSchema,
    );
    layer.bind(
        KeyChord::new("c", Modifiers::none()),
        Command::OpenConnectionManager,
    );
    layer.bind(KeyChord::new("d", Modifiers::none()), Command::Disconnect);
    layer.bind(KeyChord::new("m", Modifiers::none()), Command::OpenItemMenu);

    // Multi-selection
    layer.bind(
        KeyChord::new("j", Modifiers::shift()),
        Command::ExtendSelectNext,
    );
    layer.bind(
        KeyChord::new("down", Modifiers::shift()),
        Command::ExtendSelectNext,
    );
    layer.bind(
        KeyChord::new("k", Modifiers::shift()),
        Command::ExtendSelectPrev,
    );
    layer.bind(
        KeyChord::new("up", Modifiers::shift()),
        Command::ExtendSelectPrev,
    );
    layer.bind(
        KeyChord::new("space", Modifiers::shift()),
        Command::ToggleSelection,
    );

    // Move selected items
    layer.bind(
        KeyChord::new("j", Modifiers::ctrl()),
        Command::MoveSelectedDown,
    );
    layer.bind(
        KeyChord::new("k", Modifiers::ctrl()),
        Command::MoveSelectedUp,
    );

    // Rename and delete
    layer.bind(KeyChord::new("r", Modifiers::shift()), Command::Rename);
    layer.bind(KeyChord::new("x", Modifiers::none()), Command::Delete);

    // Create folder
    layer.bind(
        KeyChord::new("n", Modifiers::shift()),
        Command::CreateFolder,
    );

    layer
}

fn editor_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::Editor);

    // Panel navigation (Ctrl+hjkl)
    layer.bind(KeyChord::new("h", Modifiers::ctrl()), Command::FocusLeft);
    layer.bind(KeyChord::new("j", Modifiers::ctrl()), Command::FocusDown);
    layer.bind(KeyChord::new("k", Modifiers::ctrl()), Command::FocusUp);

    // Enter focuses the SQL input
    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);

    // Query history / saved queries
    layer.bind(
        KeyChord::new("h", Modifiers::alt()),
        Command::ToggleHistoryDropdown,
    );
    layer.bind(
        KeyChord::new("p", Modifiers::primary()),
        Command::OpenSavedQueries,
    );
    layer.bind(KeyChord::new("s", Modifiers::primary()), Command::SaveQuery);
    layer.bind(
        KeyChord::new("s", Modifiers::primary_shift()),
        Command::SaveFileAs,
    );
    layer.bind(
        KeyChord::new("/", Modifiers::primary()),
        Command::ToggleComment,
    );

    // The toolbar menu from the text itself: Shift+F10 types nothing, so it
    // is free in every Vim mode and without Vim.
    layer.bind(
        KeyChord::new("f10", Modifiers::shift()),
        Command::OpenPaneActions,
    );

    layer
}

fn event_streams_picker_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::EventStreamsPicker);

    layer.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);
    layer.bind(
        KeyChord::new("down", Modifiers::none()),
        Command::SelectNext,
    );
    layer.bind(KeyChord::new("k", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("up", Modifiers::none()), Command::SelectPrev);

    layer.bind(KeyChord::new("g", Modifiers::none()), Command::SelectFirst);
    layer.bind(
        KeyChord::new("home", Modifiers::none()),
        Command::SelectFirst,
    );
    layer.bind(KeyChord::new("g", Modifiers::shift()), Command::SelectLast);
    layer.bind(KeyChord::new("end", Modifiers::none()), Command::SelectLast);

    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);
    layer.bind(KeyChord::new("/", Modifiers::none()), Command::FocusSearch);

    layer
}

fn history_modal_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::HistoryModal);

    layer.bind(KeyChord::new("j", Modifiers::ctrl()), Command::SelectNext);
    layer.bind(
        KeyChord::new("down", Modifiers::none()),
        Command::SelectNext,
    );
    layer.bind(KeyChord::new("k", Modifiers::ctrl()), Command::SelectPrev);
    layer.bind(KeyChord::new("up", Modifiers::none()), Command::SelectPrev);

    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);

    // Local mnemonics — Ctrl on every platform. Mapping these to the primary
    // modifier would clash with macOS conventions (Cmd+F = system Find,
    // Cmd+R = reload/run) without giving the user anything they didn't already
    // have via the standard Save command below.
    layer.bind(KeyChord::new("d", Modifiers::ctrl()), Command::Delete);
    layer.bind(
        KeyChord::new("f", Modifiers::ctrl()),
        Command::ToggleFavorite,
    );
    layer.bind(KeyChord::new("r", Modifiers::ctrl()), Command::Rename);
    // A typed character in the history's own search, rename and save fields.
    layer.bind_with_predicate(
        KeyChord::new("/", Modifiers::none()),
        Command::FocusSearch,
        "HistoryModal && !Input",
    );
    layer.bind(KeyChord::new("s", Modifiers::primary()), Command::SaveQuery);

    // Recent / Saved, with the Alt+H / Alt+L pair every in-pane tab strip
    // uses. The workspace keeps Ctrl+Tab for the document tabs. On macOS
    // Option+letter types a character, so there the fields keep it.
    let panel_tab_predicate = if cfg!(target_os = "macos") {
        "HistoryModal && !Input"
    } else {
        "HistoryModal"
    };
    layer.bind_with_predicate(
        KeyChord::new("l", Modifiers::alt()),
        Command::NextPanelTab,
        panel_tab_predicate,
    );
    layer.bind_with_predicate(
        KeyChord::new("h", Modifiers::alt()),
        Command::PrevPanelTab,
        panel_tab_predicate,
    );

    layer
}

fn results_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::Results);

    layer.bind(
        KeyChord::new("n", Modifiers::primary()),
        Command::NewQueryTab,
    );

    // Panel navigation (Ctrl+hjkl) — vim-style, literal Ctrl on every platform.
    layer.bind(KeyChord::new("h", Modifiers::ctrl()), Command::FocusLeft);
    layer.bind(KeyChord::new("j", Modifiers::ctrl()), Command::FocusToolbar);
    layer.bind(KeyChord::new("k", Modifiers::ctrl()), Command::FocusUp);
    layer.bind(KeyChord::new("l", Modifiers::ctrl()), Command::FocusRight);

    // Table navigation
    layer.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);
    layer.bind(
        KeyChord::new("down", Modifiers::none()),
        Command::SelectNext,
    );
    layer.bind(KeyChord::new("k", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("up", Modifiers::none()), Command::SelectPrev);

    layer.bind(KeyChord::new("h", Modifiers::none()), Command::ColumnLeft);
    layer.bind(
        KeyChord::new("left", Modifiers::none()),
        Command::ColumnLeft,
    );
    layer.bind(KeyChord::new("l", Modifiers::none()), Command::ColumnRight);
    layer.bind(
        KeyChord::new("right", Modifiers::none()),
        Command::ColumnRight,
    );

    layer.bind(KeyChord::new("g", Modifiers::none()), Command::SelectFirst);
    layer.bind(
        KeyChord::new("home", Modifiers::none()),
        Command::SelectFirst,
    );
    layer.bind(KeyChord::new("g", Modifiers::shift()), Command::SelectLast);
    layer.bind(KeyChord::new("end", Modifiers::none()), Command::SelectLast);

    layer.bind(KeyChord::new("d", Modifiers::ctrl()), Command::PageDown);
    layer.bind(
        KeyChord::new("pagedown", Modifiers::none()),
        Command::PageDown,
    );
    layer.bind(KeyChord::new("u", Modifiers::ctrl()), Command::PageUp);
    layer.bind(KeyChord::new("pageup", Modifiers::none()), Command::PageUp);

    // Pagination
    layer.bind(
        KeyChord::new("]", Modifiers::none()),
        Command::ResultsNextPage,
    );
    layer.bind(
        KeyChord::new("[", Modifiers::none()),
        Command::ResultsPrevPage,
    );

    // Result tabs of a query document: Alt with the in-pane h / l moves
    // between them, Alt+W closes the one shown (Ctrl+W closes the document).
    layer.bind(KeyChord::new("l", Modifiers::alt()), Command::NextResultTab);
    layer.bind(KeyChord::new("h", Modifiers::alt()), Command::PrevResultTab);
    layer.bind(
        KeyChord::new("w", Modifiers::alt()),
        Command::CloseResultTab,
    );

    // Refresh the focused document. `r` stays Rename in this layer.
    layer.bind(
        KeyChord::new("f5", Modifiers::none()),
        Command::RefreshSchema,
    );

    // Export
    layer.bind(
        KeyChord::new("e", Modifiers::primary()),
        Command::ExportResults,
    );

    // Execute (Enter to edit input in toolbar mode)
    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);

    // Expand/collapse — object browser preview/properties, per-document meaning
    // otherwise (matches the Sidebar layer's `space` binding).
    layer.bind(
        KeyChord::new("space", Modifiers::none()),
        Command::ExpandCollapse,
    );

    // Toolbar / filter focus
    layer.bind(KeyChord::new("f", Modifiers::none()), Command::FocusToolbar);
    layer.bind(KeyChord::new("f", Modifiers::shift()), Command::ClearFilter);
    layer.bind(KeyChord::new("/", Modifiers::none()), Command::FocusSearch);

    // CRUD operations
    layer.bind(KeyChord::new("x", Modifiers::none()), Command::Delete);
    layer.bind(KeyChord::new("r", Modifiers::none()), Command::Rename);
    layer.bind(
        KeyChord::new("o", Modifiers::none()),
        Command::ResultsAddRow,
    );
    layer.bind(
        KeyChord::new("y", Modifiers::none()),
        Command::ResultsCopyRow,
    );
    layer.bind(
        KeyChord::new("i", Modifiers::none()),
        Command::ToggleRecordView,
    );
    layer.bind(
        KeyChord::new("t", Modifiers::none()),
        Command::CycleDocumentView,
    );
    layer.bind(
        KeyChord::new("v", Modifiers::none()),
        Command::ToggleValuePanel,
    );
    layer.bind(
        KeyChord::new("space", Modifiers::ctrl()),
        Command::ToggleRowInspector,
    );

    // Copy selected cell(s) to clipboard — Cmd+C on macOS, Ctrl+C elsewhere.
    // GPUI reports cmd vs ctrl on separate modifier fields, so binding only
    // the platform-correct chord keeps Ctrl+C on macOS from triggering copy.
    layer.bind(
        KeyChord::new("c", Modifiers::primary()),
        Command::ResultsCopyCell,
    );

    // Toggle panel collapse
    layer.bind(KeyChord::new("z", Modifiers::none()), Command::TogglePanel);

    // Context menu
    layer.bind(
        KeyChord::new("m", Modifiers::none()),
        Command::OpenContextMenu,
    );
    layer.bind(
        KeyChord::new("f10", Modifiers::shift()),
        Command::OpenContextMenu,
    );

    layer
}

fn context_menu_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::ContextMenu);

    // Navigation
    layer.bind(KeyChord::new("j", Modifiers::none()), Command::MenuDown);
    layer.bind(KeyChord::new("down", Modifiers::none()), Command::MenuDown);
    layer.bind(KeyChord::new("k", Modifiers::none()), Command::MenuUp);
    layer.bind(KeyChord::new("up", Modifiers::none()), Command::MenuUp);

    // Select / Enter submenu
    layer.bind(
        KeyChord::new("enter", Modifiers::none()),
        Command::MenuSelect,
    );
    layer.bind(KeyChord::new("l", Modifiers::none()), Command::MenuSelect);
    layer.bind(
        KeyChord::new("right", Modifiers::none()),
        Command::MenuSelect,
    );

    // Back / Close
    layer.bind(
        KeyChord::new("escape", Modifiers::none()),
        Command::MenuBack,
    );
    layer.bind(KeyChord::new("h", Modifiers::none()), Command::MenuBack);
    layer.bind(KeyChord::new("left", Modifiers::none()), Command::MenuBack);

    layer
}

/// Confirm-only modals (dangerous query, script confirm, delete, unsaved
/// changes) capture the keyboard: Enter confirms and Escape cancels. The
/// context has no parent, so nothing else resolves while a confirm modal is up.
fn confirm_modal_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::ConfirmModal);

    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);

    layer
}

fn background_tasks_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::BackgroundTasks);

    layer.bind(
        KeyChord::new("n", Modifiers::primary()),
        Command::NewQueryTab,
    );

    // Panel navigation (Ctrl+hjkl) — vim-style, literal Ctrl on every platform.
    layer.bind(KeyChord::new("h", Modifiers::ctrl()), Command::FocusLeft);
    layer.bind(KeyChord::new("j", Modifiers::ctrl()), Command::FocusDown);
    layer.bind(KeyChord::new("k", Modifiers::ctrl()), Command::FocusUp);

    // List navigation
    layer.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);
    layer.bind(
        KeyChord::new("down", Modifiers::none()),
        Command::SelectNext,
    );
    layer.bind(KeyChord::new("k", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("up", Modifiers::none()), Command::SelectPrev);

    layer.bind(KeyChord::new("g", Modifiers::none()), Command::SelectFirst);
    layer.bind(
        KeyChord::new("home", Modifiers::none()),
        Command::SelectFirst,
    );
    layer.bind(KeyChord::new("g", Modifiers::shift()), Command::SelectLast);
    layer.bind(KeyChord::new("end", Modifiers::none()), Command::SelectLast);

    // The selected task: show its output, cancel it, dismiss it once finished.
    layer.bind(
        KeyChord::new("space", Modifiers::none()),
        Command::ExpandCollapse,
    );
    layer.bind(
        KeyChord::new("enter", Modifiers::none()),
        Command::ExpandCollapse,
    );
    layer.bind(KeyChord::new("c", Modifiers::none()), Command::CancelTask);
    layer.bind(KeyChord::new("x", Modifiers::none()), Command::Delete);
    layer.bind(
        KeyChord::new("x", Modifiers::shift()),
        Command::ClearFinishedTasks,
    );

    layer.bind(
        KeyChord::new("m", Modifiers::none()),
        Command::OpenPaneActions,
    );
    layer.bind(
        KeyChord::new("f10", Modifiers::shift()),
        Command::OpenPaneActions,
    );

    // Toggle panel collapse
    layer.bind(KeyChord::new("z", Modifiers::none()), Command::TogglePanel);

    layer
}

/// The open notifications popover. Its context has no parent, so the
/// panels behind it see none of these keys; the bell shortcut is bound here
/// again so it closes the popover it opened.
fn notifications_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::Notifications);

    layer.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);
    layer.bind(
        KeyChord::new("down", Modifiers::none()),
        Command::SelectNext,
    );
    layer.bind(KeyChord::new("k", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("up", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("g", Modifiers::none()), Command::SelectFirst);
    layer.bind(
        KeyChord::new("home", Modifiers::none()),
        Command::SelectFirst,
    );
    layer.bind(KeyChord::new("g", Modifiers::shift()), Command::SelectLast);
    layer.bind(KeyChord::new("end", Modifiers::none()), Command::SelectLast);

    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);
    layer.bind(
        KeyChord::new("r", Modifiers::none()),
        Command::MarkNotificationRead,
    );
    layer.bind(KeyChord::new("x", Modifiers::none()), Command::Delete);
    layer.bind(
        KeyChord::new("i", Modifiers::none()),
        Command::InstallUpdate,
    );
    layer.bind(
        KeyChord::new("r", Modifiers::shift()),
        Command::MarkAllNotificationsRead,
    );
    layer.bind(
        KeyChord::new("x", Modifiers::shift()),
        Command::ClearReadNotifications,
    );

    // The filter chips, with the Alt+H / Alt+L pair of in-pane tab strips.
    layer.bind(KeyChord::new("l", Modifiers::alt()), Command::NextPanelTab);
    layer.bind(KeyChord::new("h", Modifiers::alt()), Command::PrevPanelTab);

    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);
    layer.bind(
        KeyChord::new("b", Modifiers::primary_shift()),
        Command::ToggleNotifications,
    );

    layer
}

fn command_palette_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::CommandPalette);

    // The palette's search input must receive every unmodified letter, so this
    // layer binds no bare a-z chords; list navigation stays on the arrow keys
    // and on Ctrl+J / Ctrl+K.
    layer.bind(
        KeyChord::new("down", Modifiers::none()),
        Command::SelectNext,
    );
    layer.bind(KeyChord::new("up", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("j", Modifiers::ctrl()), Command::SelectNext);
    layer.bind(KeyChord::new("k", Modifiers::ctrl()), Command::SelectPrev);

    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);
    // Opens the chosen table or collection in another tab instead of the
    // one already showing it.
    layer.bind(
        KeyChord::new("enter", Modifiers::primary()),
        Command::RunQueryInNewTab,
    );
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);

    layer
}

fn connection_manager_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::ConnectionManager);

    // Bare printable keys stay out of the window's text fields, so typing
    // them inserts text instead of navigating. The arrows need no predicate:
    // a focused text field binds them itself at a deeper context.
    const OUTSIDE_TEXT_FIELDS: &str = "ConnectionManager && !Input";

    // Vertical navigation (j/k without Ctrl, plus arrow keys for the picker).
    layer.bind_with_predicate(
        KeyChord::new("j", Modifiers::none()),
        Command::SelectNext,
        OUTSIDE_TEXT_FIELDS,
    );
    layer.bind_with_predicate(
        KeyChord::new("k", Modifiers::none()),
        Command::SelectPrev,
        OUTSIDE_TEXT_FIELDS,
    );
    layer.bind(KeyChord::new("down", Modifiers::none()), Command::FocusDown);
    layer.bind(KeyChord::new("up", Modifiers::none()), Command::FocusUp);

    // Horizontal navigation within row (h/l without Ctrl, plus arrows).
    layer.bind_with_predicate(
        KeyChord::new("h", Modifiers::none()),
        Command::FocusLeft,
        OUTSIDE_TEXT_FIELDS,
    );
    layer.bind_with_predicate(
        KeyChord::new("l", Modifiers::none()),
        Command::FocusRight,
        OUTSIDE_TEXT_FIELDS,
    );
    layer.bind(KeyChord::new("left", Modifiers::none()), Command::FocusLeft);
    layer.bind(
        KeyChord::new("right", Modifiers::none()),
        Command::FocusRight,
    );

    // Tab switching (C-h/C-l)
    layer.bind(
        KeyChord::new("h", Modifiers::ctrl()),
        Command::CycleFocusBackward,
    );
    layer.bind(
        KeyChord::new("l", Modifiers::ctrl()),
        Command::CycleFocusForward,
    );

    // Filter focus shortcut used by the New-Connection picker.
    layer.bind_with_predicate(
        KeyChord::new("/", Modifiers::none()),
        Command::FocusSearch,
        OUTSIDE_TEXT_FIELDS,
    );

    // Save the connection from anywhere in the form, fields included.
    layer.bind(KeyChord::new("s", Modifiers::primary()), Command::SaveQuery);

    // Actions
    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);

    layer
}

/// Keys of a navigable form or list that is not a text field: the forms of
/// the key-value modals, and the navigation and sections of the settings
/// window, which translates each command to the key its sections handle.
fn form_navigation_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::FormNavigation);

    layer.bind(
        KeyChord::new("n", Modifiers::primary()),
        Command::NewQueryTab,
    );

    layer.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);
    layer.bind(
        KeyChord::new("down", Modifiers::none()),
        Command::SelectNext,
    );
    layer.bind(KeyChord::new("k", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("up", Modifiers::none()), Command::SelectPrev);
    // `h` / `l` move between panes (a form and its list), the arrows move
    // within a form row; forms without panes treat both the same.
    layer.bind(KeyChord::new("h", Modifiers::none()), Command::FocusLeft);
    layer.bind(
        KeyChord::new("left", Modifiers::none()),
        Command::ColumnLeft,
    );
    layer.bind(KeyChord::new("l", Modifiers::none()), Command::FocusRight);
    layer.bind(
        KeyChord::new("right", Modifiers::none()),
        Command::ColumnRight,
    );
    layer.bind(KeyChord::new("g", Modifiers::none()), Command::SelectFirst);
    layer.bind(KeyChord::new("g", Modifiers::shift()), Command::SelectLast);
    layer.bind(
        KeyChord::new("tab", Modifiers::none()),
        Command::CycleFocusForward,
    );
    layer.bind(
        KeyChord::new("tab", Modifiers::shift()),
        Command::CycleFocusBackward,
    );
    layer.bind(
        KeyChord::new("space", Modifiers::none()),
        Command::ExpandCollapse,
    );
    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);
    layer.bind(KeyChord::new("/", Modifiers::none()), Command::FocusSearch);

    layer
}

fn text_input_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::TextInput);

    layer.bind(
        KeyChord::new("n", Modifiers::primary()),
        Command::NewQueryTab,
    );

    // Save must keep working while a text buffer owns the keyboard: the S3
    // object editors report `ContextId::TextInput`, which has no parent
    // layer, so without these bindings Ctrl/Cmd+S is silently dropped.
    layer.bind(KeyChord::new("s", Modifiers::primary()), Command::SaveQuery);
    layer.bind(
        KeyChord::new("s", Modifiers::primary_shift()),
        Command::SaveFileAs,
    );

    // Escape exits text input mode
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);

    layer
}

fn context_bar_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::ContextBar);

    // Commands that should pass through to the workspace/document.
    layer.bind(
        KeyChord::new("n", Modifiers::primary()),
        Command::NewQueryTab,
    );
    layer.bind(
        KeyChord::new("enter", Modifiers::primary()),
        Command::RunQuery,
    );
    layer.bind(
        KeyChord::new("enter", Modifiers::primary_shift()),
        Command::RunQueryInNewTab,
    );
    layer.bind(
        KeyChord::new("w", Modifiers::primary()),
        Command::CloseCurrentTab,
    );
    layer.bind(KeyChord::new("s", Modifiers::primary()), Command::SaveQuery);
    layer.bind(
        KeyChord::new("s", Modifiers::primary_shift()),
        Command::SaveFileAs,
    );

    // Navigate between dropdowns
    layer.bind(KeyChord::new("h", Modifiers::none()), Command::FocusLeft);
    layer.bind(KeyChord::new("left", Modifiers::none()), Command::FocusLeft);
    layer.bind(KeyChord::new("l", Modifiers::none()), Command::FocusRight);
    layer.bind(
        KeyChord::new("right", Modifiers::none()),
        Command::FocusRight,
    );

    // Navigate items within an open dropdown
    layer.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);
    layer.bind(
        KeyChord::new("down", Modifiers::none()),
        Command::SelectNext,
    );
    layer.bind(KeyChord::new("k", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("up", Modifiers::none()), Command::SelectPrev);

    // Open/select dropdown
    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);

    // Return to editor
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);
    layer.bind(KeyChord::new("j", Modifiers::ctrl()), Command::FocusDown);

    // C-k stays in context bar (no-op)
    layer.bind(KeyChord::new("k", Modifiers::ctrl()), Command::FocusUp);

    // Ctrl+h/l also navigate between dropdowns
    layer.bind(KeyChord::new("h", Modifiers::ctrl()), Command::FocusLeft);
    layer.bind(KeyChord::new("l", Modifiers::ctrl()), Command::FocusRight);

    // The editor's toolbar, as a menu: the context bar is the editor's
    // chrome, where letters are free (the text area keeps them for typing).
    layer.bind(
        KeyChord::new("m", Modifiers::none()),
        Command::OpenPaneActions,
    );
    layer.bind(
        KeyChord::new("f10", Modifiers::shift()),
        Command::OpenPaneActions,
    );

    layer
}

fn audit_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::Audit);

    // Panel navigation (Ctrl+hjkl) — identical to Results layer.
    layer.bind(KeyChord::new("h", Modifiers::ctrl()), Command::FocusLeft);
    layer.bind(KeyChord::new("j", Modifiers::ctrl()), Command::FocusDown);
    layer.bind(KeyChord::new("k", Modifiers::ctrl()), Command::FocusUp);
    layer.bind(KeyChord::new("l", Modifiers::ctrl()), Command::FocusRight);

    // Focus the search/filter toolbar.
    layer.bind(KeyChord::new("f", Modifiers::none()), Command::FocusToolbar);
    layer.bind(KeyChord::new("/", Modifiers::none()), Command::FocusSearch);

    // Toolbar item navigation (h/l without ctrl) — only consumed by
    // dispatch_command when the filter bar is in Navigating mode.
    layer.bind(KeyChord::new("h", Modifiers::none()), Command::ColumnLeft);
    layer.bind(
        KeyChord::new("left", Modifiers::none()),
        Command::ColumnLeft,
    );
    layer.bind(KeyChord::new("l", Modifiers::none()), Command::ColumnRight);
    layer.bind(
        KeyChord::new("right", Modifiers::none()),
        Command::ColumnRight,
    );

    // Row navigation — same bindings as Results and Sidebar.
    layer.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);
    layer.bind(
        KeyChord::new("down", Modifiers::none()),
        Command::SelectNext,
    );
    layer.bind(KeyChord::new("k", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("up", Modifiers::none()), Command::SelectPrev);

    layer.bind(KeyChord::new("g", Modifiers::none()), Command::SelectFirst);
    layer.bind(
        KeyChord::new("home", Modifiers::none()),
        Command::SelectFirst,
    );
    layer.bind(KeyChord::new("g", Modifiers::shift()), Command::SelectLast);
    layer.bind(KeyChord::new("end", Modifiers::none()), Command::SelectLast);

    layer.bind(KeyChord::new("d", Modifiers::ctrl()), Command::PageDown);
    layer.bind(
        KeyChord::new("pagedown", Modifiers::none()),
        Command::PageDown,
    );
    layer.bind(KeyChord::new("u", Modifiers::ctrl()), Command::PageUp);
    layer.bind(KeyChord::new("pageup", Modifiers::none()), Command::PageUp);

    // Pagination between pages.
    layer.bind(
        KeyChord::new("]", Modifiers::none()),
        Command::ResultsNextPage,
    );
    layer.bind(
        KeyChord::new("[", Modifiers::none()),
        Command::ResultsPrevPage,
    );

    // Expand/collapse the selected row.
    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);
    layer.bind(
        KeyChord::new("space", Modifiers::none()),
        Command::ExpandCollapse,
    );

    // Context menu.
    layer.bind(
        KeyChord::new("m", Modifiers::none()),
        Command::OpenContextMenu,
    );
    layer.bind(
        KeyChord::new("f10", Modifiers::shift()),
        Command::OpenContextMenu,
    );

    // Refresh.
    layer.bind(
        KeyChord::new("r", Modifiers::none()),
        Command::RefreshSchema,
    );

    // Export menu (CSV / JSON), driven by the context menu keys once open.
    layer.bind(
        KeyChord::new("e", Modifiers::primary()),
        Command::ExportResults,
    );

    // Table / chart view of the internal audit log.
    layer.bind(KeyChord::new("l", Modifiers::alt()), Command::NextPanelTab);
    layer.bind(KeyChord::new("h", Modifiers::alt()), Command::PrevPanelTab);

    // Dismiss / exit toolbar navigation.
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);

    layer
}

/// The schema diagram resolves every keystroke through this layer from its own
/// key handler (see `SchemaVizDocument`), and swallows the keystroke whether
/// or not it resolves, so the workspace never takes focus away from the
/// diagram.
fn schema_viz_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::SchemaViz);

    // Zoom. `+` and `=` share a key on common layouts, so both zoom in with
    // or without Shift.
    for key in ["+", "="] {
        layer.bind(KeyChord::new(key, Modifiers::none()), Command::ZoomIn);
        layer.bind(KeyChord::new(key, Modifiers::shift()), Command::ZoomIn);
    }
    layer.bind(KeyChord::new("-", Modifiers::none()), Command::ZoomOut);

    // Layout
    layer.bind(
        KeyChord::new("r", Modifiers::none()),
        Command::LayoutLeftRight,
    );
    layer.bind(
        KeyChord::new("s", Modifiers::none()),
        Command::LayoutSnowflake,
    );
    layer.bind(
        KeyChord::new("c", Modifiers::none()),
        Command::LayoutCompact,
    );

    // Each direction pans with the bare key, selects the nearest table with
    // Shift, and moves the selected table with Alt.
    let directions = [
        (
            ["h", "left"],
            Command::PanLeft,
            Command::SelectTableLeft,
            Command::MoveTableLeft,
        ),
        (
            ["l", "right"],
            Command::PanRight,
            Command::SelectTableRight,
            Command::MoveTableRight,
        ),
        (
            ["k", "up"],
            Command::PanUp,
            Command::SelectTableUp,
            Command::MoveTableUp,
        ),
        (
            ["j", "down"],
            Command::PanDown,
            Command::SelectTableDown,
            Command::MoveTableDown,
        ),
    ];
    for (keys, pan, select_table, move_table) in directions {
        for key in keys {
            layer.bind(KeyChord::new(key, Modifiers::none()), pan);
            layer.bind(KeyChord::new(key, Modifiers::shift()), select_table);
            layer.bind(KeyChord::new(key, Modifiers::alt()), move_table);
        }
    }

    // Context menu
    layer.bind(
        KeyChord::new("m", Modifiers::none()),
        Command::OpenContextMenu,
    );

    // Close the context menu, or clear the table selection.
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);

    layer
}

/// Keys of the document tree (document databases and JSON values).
///
/// The tree handles these as its own GPUI actions inside its own key
/// context, which keeps the precedence it has always had over the window
/// root. The text inputs nested in it (search box, inline value editor)
/// keep their typed letters: only Escape and Ctrl+F reach the tree from
/// them.
fn document_tree_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::DocumentTree);

    // Cursor movement
    layer.bind(KeyChord::new("up", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("k", Modifiers::none()), Command::SelectPrev);
    layer.bind(
        KeyChord::new("down", Modifiers::none()),
        Command::SelectNext,
    );
    layer.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);

    // Collapse / go to parent, and expand / go to first child.
    layer.bind(
        KeyChord::new("left", Modifiers::none()),
        Command::ColumnLeft,
    );
    layer.bind(KeyChord::new("h", Modifiers::none()), Command::ColumnLeft);
    layer.bind(
        KeyChord::new("right", Modifiers::none()),
        Command::ColumnRight,
    );
    layer.bind(KeyChord::new("l", Modifiers::none()), Command::ColumnRight);

    layer.bind(
        KeyChord::new("home", Modifiers::none()),
        Command::SelectFirst,
    );
    layer.bind(KeyChord::new("g", Modifiers::none()), Command::SelectFirst);
    layer.bind(KeyChord::new("end", Modifiers::none()), Command::SelectLast);
    layer.bind(KeyChord::new("g", Modifiers::shift()), Command::SelectLast);

    layer.bind(KeyChord::new("pageup", Modifiers::none()), Command::PageUp);
    layer.bind(KeyChord::new("u", Modifiers::ctrl()), Command::PageUp);
    layer.bind(
        KeyChord::new("pagedown", Modifiers::none()),
        Command::PageDown,
    );
    layer.bind(KeyChord::new("d", Modifiers::ctrl()), Command::PageDown);

    // Node actions
    layer.bind(
        KeyChord::new("space", Modifiers::none()),
        Command::ExpandCollapse,
    );
    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);
    layer.bind(KeyChord::new("f2", Modifiers::none()), Command::Execute);
    layer.bind(
        KeyChord::new("e", Modifiers::none()),
        Command::PreviewDocument,
    );
    layer.bind(KeyChord::new("delete", Modifiers::none()), Command::Delete);
    layer.bind(sequence("d d"), Command::Delete);
    layer.bind(
        KeyChord::new("t", Modifiers::none()),
        Command::CycleDocumentView,
    );
    layer.bind(
        KeyChord::new("r", Modifiers::none()),
        Command::ToggleRawView,
    );

    // Search
    layer.bind_with_predicate(
        KeyChord::new("f", Modifiers::ctrl()),
        Command::FocusSearch,
        "DocumentTree",
    );
    layer.bind(KeyChord::new("/", Modifiers::none()), Command::FocusSearch);
    layer.bind(KeyChord::new("n", Modifiers::none()), Command::NextMatch);
    layer.bind(KeyChord::new("n", Modifiers::shift()), Command::PrevMatch);
    layer.bind_with_predicate(
        KeyChord::new("escape", Modifiers::none()),
        Command::Cancel,
        "DocumentTree",
    );

    layer
}

/// Keys of a dropdown or multi-select. A window root reports this context
/// while its owner drives an open dropdown (the key-value new key dialog);
/// a dropdown focused from the keyboard carries it on itself and answers the
/// keys directly, passing on the ones it has no use for.
fn dropdown_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::Dropdown);

    layer.bind(
        KeyChord::new("n", Modifiers::primary()),
        Command::NewQueryTab,
    );

    // Navigation within dropdown
    layer.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);
    layer.bind(
        KeyChord::new("down", Modifiers::none()),
        Command::SelectNext,
    );

    layer.bind(KeyChord::new("k", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("up", Modifiers::none()), Command::SelectPrev);

    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);
    // Opens a closed dropdown, confirms an open one, and toggles the
    // highlighted item of a multi-select.
    layer.bind(
        KeyChord::new("space", Modifiers::none()),
        Command::ExpandCollapse,
    );
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);
    layer.bind(KeyChord::new("s", Modifiers::none()), Command::SaveQuery);

    layer
}

/// Keys of the data table grid (results, table documents, audit rows).
///
/// The table handles these as its own GPUI actions in its `DataTable` key
/// context, below the window root, and never while an inline cell editor or
/// another input inside it has focus.
fn data_table_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::DataTable);

    // Navigation
    layer.bind(KeyChord::new("up", Modifiers::none()), Command::SelectPrev);
    layer.bind(
        KeyChord::new("down", Modifiers::none()),
        Command::SelectNext,
    );
    layer.bind(
        KeyChord::new("left", Modifiers::none()),
        Command::ColumnLeft,
    );
    layer.bind(
        KeyChord::new("right", Modifiers::none()),
        Command::ColumnRight,
    );
    layer.bind(KeyChord::new("k", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);
    layer.bind(KeyChord::new("h", Modifiers::none()), Command::ColumnLeft);
    layer.bind(KeyChord::new("l", Modifiers::none()), Command::ColumnRight);

    // Extend the selection
    layer.bind(
        KeyChord::new("up", Modifiers::shift()),
        Command::ExtendSelectPrev,
    );
    layer.bind(
        KeyChord::new("down", Modifiers::shift()),
        Command::ExtendSelectNext,
    );
    layer.bind(
        KeyChord::new("left", Modifiers::shift()),
        Command::ExtendSelectLeft,
    );
    layer.bind(
        KeyChord::new("right", Modifiers::shift()),
        Command::ExtendSelectRight,
    );

    // Row and table edges
    layer.bind(
        KeyChord::new("home", Modifiers::none()),
        Command::MoveToRowStart,
    );
    layer.bind(
        KeyChord::new("end", Modifiers::none()),
        Command::MoveToRowEnd,
    );
    layer.bind(
        KeyChord::new("home", Modifiers::ctrl()),
        Command::SelectFirst,
    );
    layer.bind(KeyChord::new("end", Modifiers::ctrl()), Command::SelectLast);
    layer.bind(
        KeyChord::new("home", Modifiers::shift()),
        Command::ExtendSelectRowStart,
    );
    layer.bind(
        KeyChord::new("end", Modifiers::shift()),
        Command::ExtendSelectRowEnd,
    );
    layer.bind(
        KeyChord::new("home", Modifiers::ctrl_shift()),
        Command::ExtendSelectFirst,
    );
    layer.bind(
        KeyChord::new("end", Modifiers::ctrl_shift()),
        Command::ExtendSelectLast,
    );

    // The system-standard commands use the primary modifier (Cmd on macOS,
    // Ctrl elsewhere); binding the literal Ctrl chord too would shadow the
    // editor's interrupt semantics on macOS.
    layer.bind(KeyChord::new("a", Modifiers::primary()), Command::SelectAll);
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);

    // Copy
    layer.bind(
        KeyChord::new("c", Modifiers::primary()),
        Command::ResultsCopyCell,
    );
    layer.bind(sequence("y y"), Command::ResultsCopyCell);
    layer.bind(sequence("shift+y shift+y"), Command::ResultsCopyRow);

    // Edit
    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);
    layer.bind(KeyChord::new("f2", Modifiers::none()), Command::Execute);
    layer.bind(
        KeyChord::new("enter", Modifiers::primary()),
        Command::SaveRow,
    );
    // Commit (Ctrl+S / Cmd+S) saves every staged edit, as Save does.
    layer.bind(KeyChord::new("s", Modifiers::primary()), Command::SaveRow);

    // Row operations (vim-style)
    layer.bind(sequence("d d"), Command::ResultsDeleteRow);
    layer.bind(
        KeyChord::new("delete", Modifiers::none()),
        Command::ResultsDeleteRow,
    );
    layer.bind(sequence("a a"), Command::ResultsAddRow);
    layer.bind(sequence("shift+a shift+a"), Command::ResultsDuplicateRow);
    // "N" for NULL, literal Ctrl on every platform: Cmd+N on macOS is New
    // Query Tab.
    layer.bind(
        KeyChord::new("n", Modifiers::ctrl()),
        Command::ResultsSetNull,
    );

    // Undo / redo: the standard primary chords plus the vim-style `u` and
    // `ctrl-r`, kept literal as familiar editor aliases.
    layer.bind(KeyChord::new("u", Modifiers::none()), Command::Undo);
    layer.bind(KeyChord::new("z", Modifiers::primary()), Command::Undo);
    layer.bind(KeyChord::new("r", Modifiers::ctrl()), Command::Redo);
    layer.bind(
        KeyChord::new("z", Modifiers::primary_shift()),
        Command::Redo,
    );

    // Document grids: `e` expands an object column in place, Backspace
    // leaves a nested value. Relational grids ignore both.
    layer.bind(
        KeyChord::new("e", Modifiers::none()),
        Command::ToggleColumnGroup,
    );
    layer.bind(
        KeyChord::new("backspace", Modifiers::none()),
        Command::StepOut,
    );

    layer
}

/// Keys of every text input and code editor buffer, on top of the editing
/// keys the input component binds itself.
fn input_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::Input);

    // Vim-style aliases for the input's own Down / Up, which also move
    // through an open completion menu. The code editor leaves them to the
    // Editor layer's pane navigation and steps its menus itself.
    layer.bind_with_predicate(
        KeyChord::new("j", Modifiers::ctrl()),
        Command::SelectNext,
        "Input && !CodeEditor",
    );
    layer.bind_with_predicate(
        KeyChord::new("k", Modifiers::ctrl()),
        Command::SelectPrev,
        "Input && !CodeEditor",
    );

    // The input component binds Ctrl+H to its replace panel at this depth.
    // Inside the code editor, and its find panel, it moves focus left like
    // everywhere else; replace moves to Ctrl+Shift+H (see `init_keymap`).
    layer.bind_with_predicate(
        KeyChord::new("h", Modifiers::ctrl()),
        Command::FocusLeft,
        "CodeEditor > Input",
    );
    layer.bind(
        KeyChord::new("space", Modifiers::ctrl()),
        Command::TriggerCompletion,
    );

    // The input binds the primary modifier + Enter to a newline; binding the
    // run chords here, at the input's own depth and after it, lets them run
    // the query instead.
    layer.bind(
        KeyChord::new("enter", Modifiers::primary()),
        Command::RunQuery,
    );
    layer.bind(
        KeyChord::new("enter", Modifiers::primary_shift()),
        Command::RunQueryInNewTab,
    );

    // The input binds only Ctrl+Y as redo on Linux and Windows (macOS
    // already has Cmd+Shift+Z); Ctrl+Shift+Z is the redo most editors pair
    // with Ctrl+Z.
    #[cfg(not(target_os = "macos"))]
    layer.bind(KeyChord::new("z", Modifiers::ctrl_shift()), Command::Redo);

    layer
}

/// Keys of every modal dialog: Escape cancels, Enter confirms, and a modal
/// with a scrolling body scrolls with the arrows, Page Up/Down, Home and
/// End. A modal left without a handler lets the key through to the context
/// below it.
fn modal_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::Modal);

    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);
    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);

    layer.bind(KeyChord::new("up", Modifiers::none()), Command::SelectPrev);
    layer.bind(
        KeyChord::new("down", Modifiers::none()),
        Command::SelectNext,
    );
    layer.bind(KeyChord::new("pageup", Modifiers::none()), Command::PageUp);
    layer.bind(
        KeyChord::new("pagedown", Modifiers::none()),
        Command::PageDown,
    );
    layer.bind(
        KeyChord::new("home", Modifiers::none()),
        Command::SelectFirst,
    );
    layer.bind(KeyChord::new("end", Modifiers::none()), Command::SelectLast);

    layer
}

/// The SQL preview and the dialogs that share its context: Escape closes,
/// Enter runs the primary action, j / k and the arrows scroll the content a
/// line, Page Up / Page Down a page, and the primary modifier + C copies the
/// preview. The letters stay with a focused text field inside the dialog.
fn sql_preview_modal_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::SqlPreviewModal);

    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);
    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);

    layer.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);
    layer.bind(
        KeyChord::new("down", Modifiers::none()),
        Command::SelectNext,
    );
    layer.bind(KeyChord::new("k", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("up", Modifiers::none()), Command::SelectPrev);
    layer.bind(
        KeyChord::new("pagedown", Modifiers::none()),
        Command::PageDown,
    );
    layer.bind(KeyChord::new("pageup", Modifiers::none()), Command::PageUp);

    layer.bind(
        KeyChord::new("c", Modifiers::primary()),
        Command::CopyPreview,
    );

    layer
}

/// The cell editor modal: Escape closes it (a focused editor only lets
/// Escape through when it has nothing of its own to cancel) and the primary
/// modifier + S saves the value.
fn cell_editor_modal_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::CellEditorModal);

    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);
    layer.bind(KeyChord::new("s", Modifiers::primary()), Command::SaveQuery);

    layer
}

/// The document preview modal, with the same keys as the cell editor.
fn document_preview_modal_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::DocumentPreviewModal);

    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);
    layer.bind(KeyChord::new("s", Modifiers::primary()), Command::SaveQuery);

    layer
}

/// Keys of a side panel the keyboard moved into from its document (the
/// value panel, row inspector, document panel and query builder): Ctrl+H or
/// Escape go back to the document, J and K scroll, Enter edits the value
/// panel's text.
fn inspector_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::Inspector);

    layer.bind(KeyChord::new("h", Modifiers::ctrl()), Command::FocusLeft);
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);
    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);

    layer.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);
    layer.bind(
        KeyChord::new("down", Modifiers::none()),
        Command::SelectNext,
    );
    layer.bind(KeyChord::new("k", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("up", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("d", Modifiers::ctrl()), Command::PageDown);
    layer.bind(
        KeyChord::new("pagedown", Modifiers::none()),
        Command::PageDown,
    );
    layer.bind(KeyChord::new("u", Modifiers::ctrl()), Command::PageUp);
    layer.bind(KeyChord::new("pageup", Modifiers::none()), Command::PageUp);
    layer.bind(KeyChord::new("g", Modifiers::none()), Command::SelectFirst);
    layer.bind(
        KeyChord::new("home", Modifiers::none()),
        Command::SelectFirst,
    );
    layer.bind(KeyChord::new("g", Modifiers::shift()), Command::SelectLast);
    layer.bind(KeyChord::new("end", Modifiers::none()), Command::SelectLast);

    layer
}

/// Keys of a query builder rail (SQL or document) once the keyboard moved
/// into it: J and K move between its rows, H and L between the fields of a
/// row, Enter or I work the field (type in it, open its list, press it),
/// Space toggles the row's switch, A and Shift+A add an entry or a group, X
/// or D remove the row, Shift+J / Shift+K move it (document sort keys), M
/// lists every action of the rail, Alt+H / Alt+L switch its mode. The
/// default predicate keeps these letters out of a text field or a dropdown
/// inside the rail; Escape, Ctrl+H and the chords work from those too.
fn builder_rail_layer(context: ContextId) -> KeymapLayer {
    let mut layer = KeymapLayer::new(context);

    let from_fields = match context {
        ContextId::QueryBuilder => "QueryBuilder && !Modal",
        _ => "DocumentBuilder && !Modal",
    };
    // On macOS Option+letter types a character, so there the fields keep it.
    let mode_predicate = if cfg!(target_os = "macos") {
        context.default_predicate()
    } else {
        from_fields
    };

    layer.bind_with_predicate(
        KeyChord::new("escape", Modifiers::none()),
        Command::Cancel,
        from_fields,
    );
    layer.bind_with_predicate(
        KeyChord::new("h", Modifiers::ctrl()),
        Command::FocusLeft,
        from_fields,
    );
    layer.bind_with_predicate(
        KeyChord::new("enter", Modifiers::primary()),
        Command::RunQuery,
        from_fields,
    );
    layer.bind_with_predicate(
        KeyChord::new("s", Modifiers::primary()),
        Command::SaveQuery,
        from_fields,
    );
    layer.bind_with_predicate(
        KeyChord::new("l", Modifiers::alt()),
        Command::NextPanelTab,
        mode_predicate,
    );
    layer.bind_with_predicate(
        KeyChord::new("h", Modifiers::alt()),
        Command::PrevPanelTab,
        mode_predicate,
    );

    layer.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);
    layer.bind(
        KeyChord::new("down", Modifiers::none()),
        Command::SelectNext,
    );
    layer.bind(KeyChord::new("k", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("up", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("g", Modifiers::none()), Command::SelectFirst);
    layer.bind(
        KeyChord::new("home", Modifiers::none()),
        Command::SelectFirst,
    );
    layer.bind(KeyChord::new("g", Modifiers::shift()), Command::SelectLast);
    layer.bind(KeyChord::new("end", Modifiers::none()), Command::SelectLast);
    layer.bind(KeyChord::new("d", Modifiers::ctrl()), Command::PageDown);
    layer.bind(
        KeyChord::new("pagedown", Modifiers::none()),
        Command::PageDown,
    );
    layer.bind(KeyChord::new("u", Modifiers::ctrl()), Command::PageUp);
    layer.bind(KeyChord::new("pageup", Modifiers::none()), Command::PageUp);

    layer.bind(KeyChord::new("h", Modifiers::none()), Command::ColumnLeft);
    layer.bind(
        KeyChord::new("left", Modifiers::none()),
        Command::ColumnLeft,
    );
    layer.bind(KeyChord::new("l", Modifiers::none()), Command::ColumnRight);
    layer.bind(
        KeyChord::new("right", Modifiers::none()),
        Command::ColumnRight,
    );

    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);
    layer.bind(KeyChord::new("i", Modifiers::none()), Command::Execute);
    layer.bind(
        KeyChord::new("space", Modifiers::none()),
        Command::ExpandCollapse,
    );
    layer.bind(KeyChord::new("a", Modifiers::none()), Command::AddItem);
    layer.bind(KeyChord::new("a", Modifiers::shift()), Command::AddGroup);
    layer.bind(KeyChord::new("x", Modifiers::none()), Command::Delete);
    layer.bind(KeyChord::new("d", Modifiers::none()), Command::Delete);
    layer.bind(
        KeyChord::new("k", Modifiers::shift()),
        Command::MoveSelectedUp,
    );
    layer.bind(
        KeyChord::new("j", Modifiers::shift()),
        Command::MoveSelectedDown,
    );

    layer.bind(
        KeyChord::new("m", Modifiers::none()),
        Command::OpenPaneActions,
    );
    layer.bind(
        KeyChord::new("f10", Modifiers::shift()),
        Command::OpenPaneActions,
    );

    layer
}

/// Keys a chart document and a dashboard share: Ctrl+H/J/K/L leave the
/// pane (also from a focused control inside it), [ and ] step the time range,
/// F5 re-runs, M and Shift+F10 list the pane's actions.
fn bind_chart_pane_keys(layer: &mut KeymapLayer, from_controls: &'static str) {
    for (key, command) in [
        ("h", Command::FocusLeft),
        ("j", Command::FocusDown),
        ("k", Command::FocusUp),
        ("l", Command::FocusRight),
    ] {
        layer.bind_with_predicate(
            KeyChord::new(key, Modifiers::ctrl()),
            command,
            from_controls,
        );
    }

    layer.bind(
        KeyChord::new("]", Modifiers::none()),
        Command::NextTimeRange,
    );
    layer.bind(
        KeyChord::new("[", Modifiers::none()),
        Command::PrevTimeRange,
    );
    layer.bind(
        KeyChord::new("f5", Modifiers::none()),
        Command::RefreshSchema,
    );
    layer.bind(
        KeyChord::new("m", Modifiers::none()),
        Command::OpenPaneActions,
    );
    layer.bind(
        KeyChord::new("f10", Modifiers::shift()),
        Command::OpenPaneActions,
    );
}

/// Keys of a chart document (and of a chart panel a dashboard entered, and
/// of a dashboard panel's Configure popover, which carries the context):
/// H and L move the highlighted point, J and K the series (or the rows of an
/// open axis picker), G and Shift+G jump to the first and last point, Enter
/// picks the picker row, Space toggles a Y column or hides the focused
/// series, Escape closes the picker or clears the point, Alt+H / Alt+L switch
/// the chart kind and Ctrl/Cmd+S saves.
fn chart_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::Chart);

    bind_chart_pane_keys(&mut layer, "Chart && !Modal");

    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);

    for (keys, command) in [
        (["h", "left"], Command::ColumnLeft),
        (["l", "right"], Command::ColumnRight),
        (["j", "down"], Command::SelectNext),
        (["k", "up"], Command::SelectPrev),
        (["g", "home"], Command::SelectFirst),
    ] {
        for key in keys {
            layer.bind(KeyChord::new(key, Modifiers::none()), command);
        }
    }
    layer.bind(KeyChord::new("g", Modifiers::shift()), Command::SelectLast);
    layer.bind(KeyChord::new("end", Modifiers::none()), Command::SelectLast);

    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);
    layer.bind(
        KeyChord::new("space", Modifiers::none()),
        Command::ExpandCollapse,
    );
    layer.bind(KeyChord::new("l", Modifiers::alt()), Command::NextPanelTab);
    layer.bind(KeyChord::new("h", Modifiers::alt()), Command::PrevPanelTab);
    layer.bind(KeyChord::new("s", Modifiers::primary()), Command::SaveQuery);

    layer
}

/// Keys of a dashboard's panel grid: hjkl and the arrows select a panel,
/// Enter or I open it (its chart or table takes the keys until Escape), C
/// configures it, R or F2 renames it, X or Delete removes it, Space folds a
/// divider's section, A adds a panel, Shift+hjkl moves the panel and
/// Alt+Shift+hjkl resizes it, Alt+H / Alt+L switch View and Edit.
fn dashboard_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::Dashboard);

    bind_chart_pane_keys(&mut layer, "Dashboard && !Modal");

    for (keys, command) in [
        (["h", "left"], Command::ColumnLeft),
        (["l", "right"], Command::ColumnRight),
        (["j", "down"], Command::SelectNext),
        (["k", "up"], Command::SelectPrev),
        (["g", "home"], Command::SelectFirst),
        (["enter", "i"], Command::Execute),
        (["r", "f2"], Command::Rename),
        (["x", "delete"], Command::Delete),
    ] {
        for key in keys {
            layer.bind(KeyChord::new(key, Modifiers::none()), command);
        }
    }
    layer.bind(KeyChord::new("g", Modifiers::shift()), Command::SelectLast);
    layer.bind(KeyChord::new("end", Modifiers::none()), Command::SelectLast);

    layer.bind(
        KeyChord::new("c", Modifiers::none()),
        Command::ConfigurePanel,
    );
    layer.bind(KeyChord::new("a", Modifiers::none()), Command::AddItem);
    layer.bind(
        KeyChord::new("space", Modifiers::none()),
        Command::ExpandCollapse,
    );
    layer.bind(KeyChord::new("l", Modifiers::alt()), Command::NextPanelTab);
    layer.bind(KeyChord::new("h", Modifiers::alt()), Command::PrevPanelTab);

    let alt_shift = Modifiers {
        alt: true,
        shift: true,
        ..Modifiers::none()
    };

    for (keys, move_command, resize_command) in [
        (
            ["h", "left"],
            Command::MovePanelLeft,
            Command::ResizePanelNarrower,
        ),
        (
            ["l", "right"],
            Command::MovePanelRight,
            Command::ResizePanelWider,
        ),
        (
            ["k", "up"],
            Command::MovePanelUp,
            Command::ResizePanelShorter,
        ),
        (
            ["j", "down"],
            Command::MovePanelDown,
            Command::ResizePanelTaller,
        ),
    ] {
        for key in keys {
            layer.bind(KeyChord::new(key, Modifiers::shift()), move_command);
            layer.bind(KeyChord::new(key, alt_shift), resize_command);
        }
    }

    layer
}

/// Keys of a dashboard's Add Panel dialog, carried by the dialog itself:
/// Alt+H / Alt+L switch its tabs (also from its text fields, except on macOS
/// where Option+letter types a character), and outside the text fields J / K
/// and G / Shift+G move through the focused list, H / L switch between the
/// metric tab's namespace and metric lists, Space toggles or picks the row
/// and / goes to the search. The arrows, Enter and Escape are the dialog's
/// own keys.
fn add_panel_picker_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::AddPanelPicker);

    let tab_predicate = if cfg!(target_os = "macos") {
        ContextId::AddPanelPicker.default_predicate()
    } else {
        "AddPanelPicker"
    };
    layer.bind_with_predicate(
        KeyChord::new("l", Modifiers::alt()),
        Command::NextPanelTab,
        tab_predicate,
    );
    layer.bind_with_predicate(
        KeyChord::new("h", Modifiers::alt()),
        Command::PrevPanelTab,
        tab_predicate,
    );

    layer.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);
    layer.bind(KeyChord::new("k", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("g", Modifiers::none()), Command::SelectFirst);
    layer.bind(KeyChord::new("g", Modifiers::shift()), Command::SelectLast);
    layer.bind(KeyChord::new("h", Modifiers::none()), Command::ColumnLeft);
    layer.bind(KeyChord::new("l", Modifiers::none()), Command::ColumnRight);
    layer.bind(
        KeyChord::new("space", Modifiers::none()),
        Command::ExpandCollapse,
    );
    layer.bind(KeyChord::new("/", Modifiers::none()), Command::FocusSearch);

    layer
}

/// Window-level keys of the settings window. Sections handle their own
/// navigation keys below these.
/// Keys of the MCP approvals document: J and K move over the pending calls,
/// A approves the selected one and R rejects it with the typed reason, Enter
/// or I types the reason, and Escape brings the keyboard back from it.
fn mcp_approvals_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::McpApprovals);

    // Panel navigation, also from the reason field.
    for (key, command) in [
        ("h", Command::FocusLeft),
        ("j", Command::FocusDown),
        ("k", Command::FocusUp),
        ("l", Command::FocusRight),
    ] {
        layer.bind_with_predicate(
            KeyChord::new(key, Modifiers::ctrl()),
            command,
            "McpApprovals && !Modal",
        );
    }

    layer.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);
    layer.bind(
        KeyChord::new("down", Modifiers::none()),
        Command::SelectNext,
    );
    layer.bind(KeyChord::new("k", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("up", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("g", Modifiers::none()), Command::SelectFirst);
    layer.bind(
        KeyChord::new("home", Modifiers::none()),
        Command::SelectFirst,
    );
    layer.bind(KeyChord::new("g", Modifiers::shift()), Command::SelectLast);
    layer.bind(KeyChord::new("end", Modifiers::none()), Command::SelectLast);

    #[cfg(feature = "mcp")]
    {
        layer.bind(
            KeyChord::new("a", Modifiers::none()),
            Command::ApproveExecution,
        );
        layer.bind(
            KeyChord::new("r", Modifiers::none()),
            Command::RejectExecution,
        );
    }

    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);
    layer.bind(KeyChord::new("i", Modifiers::none()), Command::Execute);
    layer.bind_with_predicate(
        KeyChord::new("escape", Modifiers::none()),
        Command::Cancel,
        "McpApprovals && !Modal",
    );

    layer.bind(
        KeyChord::new("f5", Modifiers::none()),
        Command::RefreshSchema,
    );
    layer.bind(
        KeyChord::new("m", Modifiers::none()),
        Command::OpenPaneActions,
    );
    layer.bind(
        KeyChord::new("f10", Modifiers::shift()),
        Command::OpenPaneActions,
    );

    layer
}

fn settings_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::Settings);

    layer.bind(KeyChord::new("w", Modifiers::ctrl()), Command::CloseWindow);
    layer.bind(KeyChord::new("q", Modifiers::ctrl()), Command::CloseWindow);
    layer.bind(KeyChord::new("s", Modifiers::ctrl()), Command::SaveQuery);
    layer.bind(KeyChord::new("h", Modifiers::ctrl()), Command::FocusLeft);
    layer.bind(KeyChord::new("l", Modifiers::ctrl()), Command::FocusRight);

    layer
}

/// Keys of the key-value document. The document handles them itself; the
/// list keys stay out of the text fields inside it, and the console toggle
/// also works from the console input.
fn key_value_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::KeyValue);

    layer.bind_with_predicate(
        KeyChord::new("`", Modifiers::ctrl()),
        Command::ToggleConsole,
        "KeyValueView",
    );
    layer.bind(KeyChord::new("j", Modifiers::ctrl()), Command::LoadMore);
    layer.bind(KeyChord::new("t", Modifiers::none()), Command::EditExpiry);

    // Alt+L / Alt+H step the key type filter, or the mode of the open expiry
    // editor, also from its duration field. On macOS Option with a letter
    // types a character, so there they stay out of text fields.
    let panel_tab_predicate = if cfg!(target_os = "macos") {
        "KeyValueView && !Input"
    } else {
        "KeyValueView"
    };
    layer.bind_with_predicate(
        KeyChord::new("l", Modifiers::alt()),
        Command::NextPanelTab,
        panel_tab_predicate,
    );
    layer.bind_with_predicate(
        KeyChord::new("h", Modifiers::alt()),
        Command::PrevPanelTab,
        panel_tab_predicate,
    );

    layer
}
