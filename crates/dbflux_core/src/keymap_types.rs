/// All possible commands that can be executed in the application.
///
/// Commands are the unified abstraction for user actions, whether triggered
/// by keyboard shortcuts, mouse clicks, or the command palette.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Command {
    // === Global ===
    ToggleCommandPalette,
    NewQueryTab,
    CloseCurrentTab,
    NextTab,
    PrevTab,
    SwitchToTab(usize),
    OpenTabMenu,
    /// Moves the active document tab one place to the left.
    MoveTabLeft,
    /// Moves the active document tab one place to the right.
    MoveTabRight,

    // === Focus Navigation ===
    FocusSidebar,
    FocusEditor,
    FocusResults,
    FocusBackgroundTasks,
    CycleFocusForward,
    CycleFocusBackward,
    FocusLeft,
    FocusRight,
    FocusUp,
    FocusDown,

    // === List Navigation ===
    SelectNext,
    SelectPrev,
    SelectFirst,
    SelectLast,
    PageDown,
    PageUp,
    /// Shows the next tab or filter of the focused panel (the query
    /// history's Recent and Saved, the notification filters), wrapping.
    NextPanelTab,
    /// Shows the previous tab or filter of the focused panel, wrapping.
    PrevPanelTab,

    // === Multi-selection ===
    ExtendSelectNext,
    ExtendSelectPrev,
    ToggleSelection,
    MoveSelectedUp,
    MoveSelectedDown,

    // === Column Navigation (Results) ===
    ColumnLeft,
    ColumnRight,

    // === Generic Actions ===
    Execute,
    Cancel,
    ExpandCollapse,
    Delete,
    Rename,
    FocusSearch,
    ToggleFavorite,

    // === Editor ===
    RunQuery,
    RunQueryInNewTab,
    /// Runs the editor's query under the driver's explain plan, as the
    /// toolbar's Explain button does.
    ExplainQuery,
    CancelQuery,
    ToggleHistoryDropdown,
    OpenSavedQueries,
    SaveQuery,
    SaveFileAs,
    OpenScriptFile,
    ToggleComment,

    // === Results ===
    ExportResults,
    /// Clears the WHERE filter of a table and reloads its rows.
    ClearFilter,
    /// Shows the next result tab of a query document, wrapping at the end.
    NextResultTab,
    /// Shows the previous result tab of a query document, wrapping at the start.
    PrevResultTab,
    /// Closes the result tab a query document shows.
    CloseResultTab,
    ResultsNextPage,
    ResultsPrevPage,
    FocusToolbar,
    TogglePanel,
    // Row operations (vim-style)
    ResultsDeleteRow,
    ResultsAddRow,
    /// Adds an entry to the list the cursor is in (a filter condition, a
    /// sort key, an assignment) in a side rail such as the query builders.
    AddItem,
    /// Adds a nested group to the filter group the cursor is in.
    AddGroup,
    ResultsDuplicateRow,
    ResultsCopyRow,
    ResultsCopyCell,
    ToggleRecordView,
    CycleDocumentView,
    /// Shows the next view of a result (Data, JSON, Chart and the others
    /// its shape offers), wrapping at the end.
    CycleResultView,
    ToggleValuePanel,
    ToggleRowInspector,
    ResultsSetNull,
    // Context menu
    OpenContextMenu,
    /// Opens the menu of the focused pane's actions (its toolbar buttons and
    /// other pointer-only controls).
    OpenPaneActions,
    MenuUp,
    MenuDown,
    MenuSelect,
    MenuBack,

    // === Sidebar ===
    SidebarNextTab,
    RefreshSchema,
    OpenConnectionManager,
    ExportConnections,
    Disconnect,
    OpenItemMenu,
    CreateFolder,

    // === View ===
    ToggleEditor,
    ToggleResults,
    ToggleTasks,
    /// Cancels the task selected in the background tasks panel.
    CancelTask,
    /// Removes every finished task from the background tasks panel.
    ClearFinishedTasks,
    ToggleSidebar,
    OpenSettings,
    OpenLoginModal,
    OpenSsoWizard,
    OpenAuditViewer,
    /// Open the audit viewer filtered to the most recent user-facing error,
    /// the same target as the error toast's "View in Audit".
    OpenLastErrorInAudit,
    /// Lists the buttons of the newest toast (its actions, the details toggle and Dismiss) in a keyboard menu.
    OpenToastActions,
    /// Open or close the title-bar notifications popover.
    ToggleNotifications,
    /// Marks the notification selected in the notifications center read.
    MarkNotificationRead,
    /// Marks every listed notification read.
    MarkAllNotificationsRead,
    /// Removes the read notifications, like Clear read in the notifications center.
    ClearReadNotifications,
    /// Downloads the update the notifications center lists, like its Install button.
    InstallUpdate,
    /// Show the Connections view of the sidebar, as its activity rail entry does.
    ShowConnectionsView,
    /// Show the Scripts view of the sidebar, as its activity rail entry does.
    ShowScriptsView,
    /// Show the Dashboards view of the sidebar, as its activity rail entry does.
    ShowDashboardsView,
    #[cfg(feature = "mcp")]
    OpenMcpApprovals,
    #[cfg(feature = "mcp")]
    RefreshMcpGovernance,
    /// Approves the pending MCP call selected in the approvals view.
    #[cfg(feature = "mcp")]
    ApproveExecution,
    /// Rejects the pending MCP call selected in the approvals view, with the
    /// typed reason.
    #[cfg(feature = "mcp")]
    RejectExecution,

    // === Charts / Dashboards ===
    /// Open the saved-chart fuzzy overlay (lists all SavedCharts for the current profile).
    OpenSavedChart,
    /// Open the "Import Dashboard from JSON" paste modal.
    ///
    /// Only available when the active connection has `DASHBOARD_IMPORT` capability.
    ImportDashboard,
    /// Open the "New dashboard…" creation modal (profile picker then name input).
    NewDashboard,
    /// Selects the next time-range preset of a chart or dashboard.
    NextTimeRange,
    /// Selects the previous time-range preset of a chart or dashboard.
    PrevTimeRange,
    /// Opens the settings of the selected dashboard panel.
    ConfigurePanel,
    /// Moves the selected dashboard panel one grid column left.
    MovePanelLeft,
    /// Moves the selected dashboard panel one grid column right.
    MovePanelRight,
    /// Moves the selected dashboard panel one grid row up.
    MovePanelUp,
    /// Moves the selected dashboard panel one grid row down.
    MovePanelDown,
    /// Makes the selected dashboard panel one grid column narrower.
    ResizePanelNarrower,
    /// Makes the selected dashboard panel one grid column wider.
    ResizePanelWider,
    /// Makes the selected dashboard panel one grid row shorter.
    ResizePanelShorter,
    /// Makes the selected dashboard panel one grid row taller.
    ResizePanelTaller,

    // === Document Tree ===
    PreviewDocument,
    ToggleRawView,
    NextMatch,
    PrevMatch,

    // === Schema Diagram ===
    ZoomIn,
    ZoomOut,
    PanLeft,
    PanRight,
    PanUp,
    PanDown,
    SelectTableLeft,
    SelectTableRight,
    SelectTableUp,
    SelectTableDown,
    MoveTableLeft,
    MoveTableRight,
    MoveTableUp,
    MoveTableDown,
    LayoutLeftRight,
    LayoutSnowflake,
    LayoutCompact,

    // === Grids, inputs and windows ===
    ExtendSelectLeft,
    ExtendSelectRight,
    MoveToRowStart,
    MoveToRowEnd,
    ExtendSelectRowStart,
    ExtendSelectRowEnd,
    ExtendSelectFirst,
    ExtendSelectLast,
    SelectAll,
    SaveRow,
    Undo,
    Redo,
    ToggleColumnGroup,
    StepOut,
    TriggerCompletion,
    CloseWindow,
    ToggleConsole,
    LoadMore,
    EditExpiry,
    CopyPreview,
    /// Imports entries from a file into the list that has the keyboard (the
    /// profile lists of the settings window).
    ImportItems,
    /// Imports connections from another client (DBeaver, Beekeeper Studio
    /// and the others DBFlux reads), as the connection manager's Import from
    /// another client button does.
    ImportFromClient,
    /// Puts the selected key binding back to its default keys.
    ResetBinding,
    /// Drops every key binding override.
    ResetAllBindings,
    /// Edits the context predicate of the selected key binding.
    EditBindingContext,
    /// Opens the context filter of the key bindings list.
    FilterByContext,
}

impl Command {
    /// Resolve a command enum from a command palette identifier.
    pub fn from_palette_id(command_id: &str) -> Option<Self> {
        match command_id {
            "new_query_tab" => Some(Command::NewQueryTab),
            "run_query" => Some(Command::RunQuery),
            "run_query_in_new_tab" => Some(Command::RunQueryInNewTab),
            "save_query" => Some(Command::SaveQuery),
            "toggle_comment" => Some(Command::ToggleComment),
            "open_history" => Some(Command::ToggleHistoryDropdown),
            "cancel_query" => Some(Command::CancelQuery),
            "close_tab" => Some(Command::CloseCurrentTab),
            "next_tab" => Some(Command::NextTab),
            "prev_tab" => Some(Command::PrevTab),
            "move_tab_left" => Some(Command::MoveTabLeft),
            "move_tab_right" => Some(Command::MoveTabRight),
            "export_results" => Some(Command::ExportResults),
            "open_connection_manager" => Some(Command::OpenConnectionManager),
            "export_connections" => Some(Command::ExportConnections),
            "disconnect" => Some(Command::Disconnect),
            "refresh_schema" => Some(Command::RefreshSchema),
            "focus_sidebar" => Some(Command::FocusSidebar),
            "focus_editor" => Some(Command::FocusEditor),
            "focus_results" => Some(Command::FocusResults),
            "focus_tasks" => Some(Command::FocusBackgroundTasks),
            "toggle_sidebar" => Some(Command::ToggleSidebar),
            "toggle_editor" => Some(Command::ToggleEditor),
            "toggle_results" => Some(Command::ToggleResults),
            "toggle_tasks" => Some(Command::ToggleTasks),
            "clear_finished_tasks" => Some(Command::ClearFinishedTasks),
            "open_settings" => Some(Command::OpenSettings),
            "open_login_modal" => Some(Command::OpenLoginModal),
            "open_sso_wizard" => Some(Command::OpenSsoWizard),
            "open_audit_viewer" => Some(Command::OpenAuditViewer),
            "open_last_error_in_audit" => Some(Command::OpenLastErrorInAudit),
            "open_toast_actions" => Some(Command::OpenToastActions),
            "toggle_notifications" => Some(Command::ToggleNotifications),
            "show_connections_view" => Some(Command::ShowConnectionsView),
            "show_scripts_view" => Some(Command::ShowScriptsView),
            "show_dashboards_view" => Some(Command::ShowDashboardsView),
            #[cfg(feature = "mcp")]
            "open_mcp_approvals" => Some(Command::OpenMcpApprovals),
            #[cfg(feature = "mcp")]
            "refresh_mcp_governance" => Some(Command::RefreshMcpGovernance),
            "open_saved_chart" => Some(Command::OpenSavedChart),
            "import_dashboard" => Some(Command::ImportDashboard),
            "new_dashboard" => Some(Command::NewDashboard),
            "open_pane_actions" => Some(Command::OpenPaneActions),
            _ => None,
        }
    }

    /// Returns the display name for this command (used in command palette).
    #[allow(dead_code)]
    pub fn display_name(&self) -> &'static str {
        match self {
            Command::ToggleCommandPalette => "Toggle command palette",
            Command::NewQueryTab => "New query tab",
            Command::CloseCurrentTab => "Close tab",
            Command::NextTab => "Next tab",
            Command::PrevTab => "Previous tab",
            Command::SwitchToTab(_) => "Switch to tab",
            Command::OpenTabMenu => "Open tab menu",
            Command::MoveTabLeft => "Move tab left",
            Command::MoveTabRight => "Move tab right",

            Command::FocusSidebar => "Focus sidebar",
            Command::FocusEditor => "Focus editor",
            Command::FocusResults => "Focus results",
            Command::FocusBackgroundTasks => "Focus background tasks",
            Command::CycleFocusForward => "Cycle focus forward",
            Command::CycleFocusBackward => "Cycle focus backward",
            Command::FocusLeft => "Focus left",
            Command::FocusRight => "Focus right",
            Command::FocusUp => "Focus up",
            Command::FocusDown => "Focus down",

            Command::SelectNext => "Select next",
            Command::SelectPrev => "Select previous",
            Command::SelectFirst => "Select first",
            Command::SelectLast => "Select last",
            Command::PageDown => "Page down",
            Command::PageUp => "Page up",
            Command::NextPanelTab => "Next panel tab",
            Command::PrevPanelTab => "Previous panel tab",

            Command::ExtendSelectNext => "Extend selection down",
            Command::ExtendSelectPrev => "Extend selection up",
            Command::ToggleSelection => "Toggle selection",
            Command::MoveSelectedUp => "Move selected up",
            Command::MoveSelectedDown => "Move selected down",

            Command::ColumnLeft => "Column left",
            Command::ColumnRight => "Column right",

            Command::Execute => "Execute",
            Command::Cancel => "Cancel",
            Command::ExpandCollapse => "Expand/collapse",
            Command::Delete => "Delete",
            Command::Rename => "Rename",
            Command::FocusSearch => "Focus search",
            Command::ToggleFavorite => "Toggle favorite",

            Command::RunQuery => "Run query",
            Command::RunQueryInNewTab => "Run query in new tab",
            Command::ExplainQuery => "Explain query",
            Command::CancelQuery => "Cancel query",
            Command::ToggleHistoryDropdown => "Toggle history dropdown",
            Command::OpenSavedQueries => "Open saved queries",
            Command::SaveQuery => "Save",
            Command::SaveFileAs => "Save file as…",
            Command::OpenScriptFile => "Open script file…",
            Command::ToggleComment => "Toggle line comment",

            Command::ExportResults => "Export results",
            Command::ClearFilter => "Clear filter",
            Command::NextResultTab => "Next result tab",
            Command::PrevResultTab => "Previous result tab",
            Command::CloseResultTab => "Close result tab",
            Command::ResultsNextPage => "Results next page",
            Command::ResultsPrevPage => "Results previous page",
            Command::FocusToolbar => "Focus toolbar",
            Command::TogglePanel => "Toggle panel",
            Command::ResultsDeleteRow => "Delete row",
            Command::ResultsAddRow => "Add row",
            Command::AddItem => "Add item",
            Command::AddGroup => "Add group",
            Command::ResultsDuplicateRow => "Duplicate row",
            Command::ResultsCopyRow => "Copy row",
            Command::ResultsCopyCell => "Copy cell",
            Command::ToggleRecordView => "Toggle record view",
            Command::CycleDocumentView => "Cycle tree / table / JSON",
            Command::CycleResultView => "Cycle result view",
            Command::ToggleValuePanel => "Toggle value panel",
            Command::ToggleRowInspector => "Toggle row inspector",
            Command::ResultsSetNull => "Set cell to NULL",
            Command::OpenContextMenu => "Open context menu",
            Command::OpenPaneActions => "Open pane actions",
            Command::MenuUp => "Menu up",
            Command::MenuDown => "Menu down",
            Command::MenuSelect => "Menu select",
            Command::MenuBack => "Menu back",

            Command::SidebarNextTab => "Sidebar next tab",
            Command::RefreshSchema => "Refresh schema",
            Command::OpenConnectionManager => "Open connection manager",
            Command::ExportConnections => "Export connections",
            Command::Disconnect => "Disconnect",
            Command::OpenItemMenu => "Open item menu",
            Command::CreateFolder => "Create folder",

            Command::ToggleEditor => "Toggle editor panel",
            Command::ToggleResults => "Toggle results panel",
            Command::ToggleTasks => "Toggle tasks panel",
            Command::CancelTask => "Cancel task",
            Command::ClearFinishedTasks => "Clear finished tasks",
            Command::ToggleSidebar => "Toggle sidebar",
            Command::OpenSettings => "Open settings",
            Command::OpenLoginModal => "Open auth profile login",
            Command::OpenSsoWizard => "Open AWS SSO wizard",
            Command::OpenAuditViewer => "Open audit viewer",
            Command::OpenLastErrorInAudit => "Open last error in audit",
            Command::OpenToastActions => "Open toast actions",
            Command::ToggleNotifications => "Toggle notifications",
            Command::MarkNotificationRead => "Mark notification read",
            Command::MarkAllNotificationsRead => "Mark all notifications read",
            Command::ClearReadNotifications => "Clear read notifications",
            Command::InstallUpdate => "Install update",
            Command::ShowConnectionsView => "Show connections view",
            Command::ShowScriptsView => "Show scripts view",
            Command::ShowDashboardsView => "Show dashboards view",
            #[cfg(feature = "mcp")]
            Command::OpenMcpApprovals => "Open MCP approvals",
            #[cfg(feature = "mcp")]
            Command::ApproveExecution => "Approve pending call",
            #[cfg(feature = "mcp")]
            Command::RejectExecution => "Reject pending call",
            #[cfg(feature = "mcp")]
            Command::RefreshMcpGovernance => "Refresh MCP governance",
            Command::OpenSavedChart => "Open chart…",
            Command::ImportDashboard => "Import dashboard from JSON…",
            Command::NewDashboard => "New dashboard…",
            Command::NextTimeRange => "Next time range",
            Command::PrevTimeRange => "Previous time range",
            Command::ConfigurePanel => "Configure panel",
            Command::MovePanelLeft => "Move panel left",
            Command::MovePanelRight => "Move panel right",
            Command::MovePanelUp => "Move panel up",
            Command::MovePanelDown => "Move panel down",
            Command::ResizePanelNarrower => "Make panel narrower",
            Command::ResizePanelWider => "Make panel wider",
            Command::ResizePanelShorter => "Make panel shorter",
            Command::ResizePanelTaller => "Make panel taller",

            Command::PreviewDocument => "Preview document",
            Command::ToggleRawView => "Toggle raw JSON view",
            Command::NextMatch => "Next match",
            Command::PrevMatch => "Previous match",
            Command::ZoomIn => "Zoom in",
            Command::ZoomOut => "Zoom out",
            Command::PanLeft => "Pan left",
            Command::PanRight => "Pan right",
            Command::PanUp => "Pan up",
            Command::PanDown => "Pan down",
            Command::SelectTableLeft => "Select table left",
            Command::SelectTableRight => "Select table right",
            Command::SelectTableUp => "Select table up",
            Command::SelectTableDown => "Select table down",
            Command::MoveTableLeft => "Move table left",
            Command::MoveTableRight => "Move table right",
            Command::MoveTableUp => "Move table up",
            Command::MoveTableDown => "Move table down",
            Command::LayoutLeftRight => "Left-right layout",
            Command::LayoutSnowflake => "Snowflake layout",
            Command::LayoutCompact => "Compact layout",
            Command::ExtendSelectLeft => "Extend selection left",
            Command::ExtendSelectRight => "Extend selection right",
            Command::MoveToRowStart => "Move to row start",
            Command::MoveToRowEnd => "Move to row end",
            Command::ExtendSelectRowStart => "Extend selection to row start",
            Command::ExtendSelectRowEnd => "Extend selection to row end",
            Command::ExtendSelectFirst => "Extend selection to first row",
            Command::ExtendSelectLast => "Extend selection to last row",
            Command::SelectAll => "Select all",
            Command::SaveRow => "Save row changes",
            Command::Undo => "Undo",
            Command::Redo => "Redo",
            Command::ToggleColumnGroup => "Expand or collapse column",
            Command::StepOut => "Step out of nested value",
            Command::TriggerCompletion => "Show completions",
            Command::CloseWindow => "Close window",
            Command::ToggleConsole => "Toggle console",
            Command::LoadMore => "Load more",
            Command::EditExpiry => "Edit expiry…",
            Command::CopyPreview => "Copy preview",
            Command::ImportItems => "Import…",
            Command::ImportFromClient => "Import from another client…",
            Command::ResetBinding => "Reset binding",
            Command::ResetAllBindings => "Reset all bindings",
            Command::EditBindingContext => "Edit binding context",
            Command::FilterByContext => "Filter by context",
        }
    }

    /// Returns a stable, locale-independent identifier for this command.
    ///
    /// Where the command is also addressable from the command palette (see
    /// [`Command::from_palette_id`]), this returns the exact same string so
    /// the settings translation catalog and the palette translation catalog
    /// can share one `<id>` namespace per command.
    pub fn id(&self) -> &'static str {
        match self {
            Command::ToggleCommandPalette => "toggle_command_palette",
            Command::NewQueryTab => "new_query_tab",
            Command::CloseCurrentTab => "close_tab",
            Command::NextTab => "next_tab",
            Command::PrevTab => "prev_tab",
            Command::SwitchToTab(_) => "switch_to_tab",
            Command::OpenTabMenu => "open_tab_menu",
            Command::MoveTabLeft => "move_tab_left",
            Command::MoveTabRight => "move_tab_right",

            Command::FocusSidebar => "focus_sidebar",
            Command::FocusEditor => "focus_editor",
            Command::FocusResults => "focus_results",
            Command::FocusBackgroundTasks => "focus_tasks",
            Command::CycleFocusForward => "cycle_focus_forward",
            Command::CycleFocusBackward => "cycle_focus_backward",
            Command::FocusLeft => "focus_left",
            Command::FocusRight => "focus_right",
            Command::FocusUp => "focus_up",
            Command::FocusDown => "focus_down",

            Command::SelectNext => "select_next",
            Command::SelectPrev => "select_prev",
            Command::SelectFirst => "select_first",
            Command::SelectLast => "select_last",
            Command::PageDown => "page_down",
            Command::PageUp => "page_up",
            Command::NextPanelTab => "next_panel_tab",
            Command::PrevPanelTab => "prev_panel_tab",

            Command::ExtendSelectNext => "extend_select_next",
            Command::ExtendSelectPrev => "extend_select_prev",
            Command::ToggleSelection => "toggle_selection",
            Command::MoveSelectedUp => "move_selected_up",
            Command::MoveSelectedDown => "move_selected_down",

            Command::ColumnLeft => "column_left",
            Command::ColumnRight => "column_right",

            Command::Execute => "execute",
            Command::Cancel => "cancel",
            Command::ExpandCollapse => "expand_collapse",
            Command::Delete => "delete",
            Command::Rename => "rename",
            Command::FocusSearch => "focus_search",
            Command::ToggleFavorite => "toggle_favorite",

            Command::RunQuery => "run_query",
            Command::RunQueryInNewTab => "run_query_in_new_tab",
            Command::ExplainQuery => "explain_query",
            Command::CancelQuery => "cancel_query",
            Command::ToggleHistoryDropdown => "open_history",
            Command::OpenSavedQueries => "open_saved_queries",
            Command::SaveQuery => "save_query",
            Command::SaveFileAs => "save_file_as",
            Command::OpenScriptFile => "open_script_file",
            Command::ToggleComment => "toggle_comment",

            Command::ExportResults => "export_results",
            Command::ClearFilter => "clear_filter",
            Command::NextResultTab => "next_result_tab",
            Command::PrevResultTab => "prev_result_tab",
            Command::CloseResultTab => "close_result_tab",
            Command::ResultsNextPage => "results_next_page",
            Command::ResultsPrevPage => "results_prev_page",
            Command::FocusToolbar => "focus_toolbar",
            Command::TogglePanel => "toggle_panel",
            Command::ResultsDeleteRow => "results_delete_row",
            Command::ResultsAddRow => "results_add_row",
            Command::AddItem => "add_item",
            Command::AddGroup => "add_group",
            Command::ResultsDuplicateRow => "results_duplicate_row",
            Command::ResultsCopyRow => "results_copy_row",
            Command::ResultsCopyCell => "results_copy_cell",
            Command::ToggleRecordView => "toggle_record_view",
            Command::CycleDocumentView => "cycle_document_view",
            Command::CycleResultView => "cycle_result_view",
            Command::ToggleValuePanel => "toggle_value_panel",
            Command::ToggleRowInspector => "toggle_row_inspector",
            Command::ResultsSetNull => "results_set_null",
            Command::OpenContextMenu => "open_context_menu",
            Command::OpenPaneActions => "open_pane_actions",
            Command::MenuUp => "menu_up",
            Command::MenuDown => "menu_down",
            Command::MenuSelect => "menu_select",
            Command::MenuBack => "menu_back",

            Command::SidebarNextTab => "sidebar_next_tab",
            Command::RefreshSchema => "refresh_schema",
            Command::OpenConnectionManager => "open_connection_manager",
            Command::ExportConnections => "export_connections",
            Command::Disconnect => "disconnect",
            Command::OpenItemMenu => "open_item_menu",
            Command::CreateFolder => "create_folder",

            Command::ToggleEditor => "toggle_editor",
            Command::ToggleResults => "toggle_results",
            Command::ToggleTasks => "toggle_tasks",
            Command::CancelTask => "cancel_task",
            Command::ClearFinishedTasks => "clear_finished_tasks",
            Command::ToggleSidebar => "toggle_sidebar",
            Command::OpenSettings => "open_settings",
            Command::OpenLoginModal => "open_login_modal",
            Command::OpenSsoWizard => "open_sso_wizard",
            Command::OpenAuditViewer => "open_audit_viewer",
            Command::OpenLastErrorInAudit => "open_last_error_in_audit",
            Command::OpenToastActions => "open_toast_actions",
            Command::ToggleNotifications => "toggle_notifications",
            Command::MarkNotificationRead => "mark_notification_read",
            Command::MarkAllNotificationsRead => "mark_all_notifications_read",
            Command::ClearReadNotifications => "clear_read_notifications",
            Command::InstallUpdate => "install_update",
            Command::ShowConnectionsView => "show_connections_view",
            Command::ShowScriptsView => "show_scripts_view",
            Command::ShowDashboardsView => "show_dashboards_view",
            #[cfg(feature = "mcp")]
            Command::OpenMcpApprovals => "open_mcp_approvals",
            #[cfg(feature = "mcp")]
            Command::ApproveExecution => "approve_execution",
            #[cfg(feature = "mcp")]
            Command::RejectExecution => "reject_execution",
            #[cfg(feature = "mcp")]
            Command::RefreshMcpGovernance => "refresh_mcp_governance",

            Command::OpenSavedChart => "open_saved_chart",
            Command::ImportDashboard => "import_dashboard",
            Command::NewDashboard => "new_dashboard",
            Command::NextTimeRange => "next_time_range",
            Command::PrevTimeRange => "prev_time_range",
            Command::ConfigurePanel => "configure_panel",
            Command::MovePanelLeft => "move_panel_left",
            Command::MovePanelRight => "move_panel_right",
            Command::MovePanelUp => "move_panel_up",
            Command::MovePanelDown => "move_panel_down",
            Command::ResizePanelNarrower => "resize_panel_narrower",
            Command::ResizePanelWider => "resize_panel_wider",
            Command::ResizePanelShorter => "resize_panel_shorter",
            Command::ResizePanelTaller => "resize_panel_taller",

            Command::PreviewDocument => "preview_document",
            Command::ToggleRawView => "toggle_raw_view",
            Command::NextMatch => "next_match",
            Command::PrevMatch => "prev_match",
            Command::ZoomIn => "zoom_in",
            Command::ZoomOut => "zoom_out",
            Command::PanLeft => "pan_left",
            Command::PanRight => "pan_right",
            Command::PanUp => "pan_up",
            Command::PanDown => "pan_down",
            Command::SelectTableLeft => "select_table_left",
            Command::SelectTableRight => "select_table_right",
            Command::SelectTableUp => "select_table_up",
            Command::SelectTableDown => "select_table_down",
            Command::MoveTableLeft => "move_table_left",
            Command::MoveTableRight => "move_table_right",
            Command::MoveTableUp => "move_table_up",
            Command::MoveTableDown => "move_table_down",
            Command::LayoutLeftRight => "layout_left_right",
            Command::LayoutSnowflake => "layout_snowflake",
            Command::LayoutCompact => "layout_compact",
            Command::ExtendSelectLeft => "extend_select_left",
            Command::ExtendSelectRight => "extend_select_right",
            Command::MoveToRowStart => "move_to_row_start",
            Command::MoveToRowEnd => "move_to_row_end",
            Command::ExtendSelectRowStart => "extend_select_row_start",
            Command::ExtendSelectRowEnd => "extend_select_row_end",
            Command::ExtendSelectFirst => "extend_select_first",
            Command::ExtendSelectLast => "extend_select_last",
            Command::SelectAll => "select_all",
            Command::SaveRow => "save_row",
            Command::Undo => "undo",
            Command::Redo => "redo",
            Command::ToggleColumnGroup => "toggle_column_group",
            Command::StepOut => "step_out",
            Command::TriggerCompletion => "trigger_completion",
            Command::CloseWindow => "close_window",
            Command::ToggleConsole => "toggle_console",
            Command::LoadMore => "load_more",
            Command::EditExpiry => "edit_expiry",
            Command::CopyPreview => "copy_preview",
            Command::ImportItems => "import_items",
            Command::ImportFromClient => "import_from_client",
            Command::ResetBinding => "reset_binding",
            Command::ResetAllBindings => "reset_all_bindings",
            Command::EditBindingContext => "edit_binding_context",
            Command::FilterByContext => "filter_by_context",
        }
    }

    /// Returns one instance of every [`Command`] variant, using a
    /// representative payload for variants that carry one.
    ///
    /// Intended for exhaustive coverage in tests (id uniqueness, translation
    /// coverage) across `dbflux_core` and downstream UI crates.
    /// Identifier that names this exact command, argument included, for
    /// data-carrying key actions: [`Command::id`] except for
    /// [`Command::SwitchToTab`], which appends its tab number
    /// (`switch_to_tab_3`).
    pub fn action_id(&self) -> std::borrow::Cow<'static, str> {
        match self {
            Command::SwitchToTab(index) => format!("switch_to_tab_{index}").into(),
            command => command.id().into(),
        }
    }

    /// The command named by `action_id`, the inverse of [`Command::action_id`].
    pub fn from_action_id(action_id: &str) -> Option<Self> {
        if let Some(index) = action_id.strip_prefix("switch_to_tab_") {
            return index.parse().ok().map(Command::SwitchToTab);
        }

        Self::all_variants()
            .into_iter()
            .filter(|command| !matches!(command, Command::SwitchToTab(_)))
            .find(|command| command.id() == action_id)
    }

    pub fn all_variants() -> Vec<Command> {
        #[cfg_attr(not(feature = "mcp"), allow(unused_mut))]
        let mut variants = vec![
            Command::ToggleCommandPalette,
            Command::NewQueryTab,
            Command::CloseCurrentTab,
            Command::NextTab,
            Command::PrevTab,
            Command::SwitchToTab(0),
            Command::OpenTabMenu,
            Command::MoveTabLeft,
            Command::MoveTabRight,
            Command::FocusSidebar,
            Command::FocusEditor,
            Command::FocusResults,
            Command::FocusBackgroundTasks,
            Command::CycleFocusForward,
            Command::CycleFocusBackward,
            Command::FocusLeft,
            Command::FocusRight,
            Command::FocusUp,
            Command::FocusDown,
            Command::SelectNext,
            Command::SelectPrev,
            Command::SelectFirst,
            Command::SelectLast,
            Command::PageDown,
            Command::PageUp,
            Command::NextPanelTab,
            Command::PrevPanelTab,
            Command::ExtendSelectNext,
            Command::ExtendSelectPrev,
            Command::ToggleSelection,
            Command::MoveSelectedUp,
            Command::MoveSelectedDown,
            Command::ColumnLeft,
            Command::ColumnRight,
            Command::Execute,
            Command::Cancel,
            Command::ExpandCollapse,
            Command::Delete,
            Command::Rename,
            Command::FocusSearch,
            Command::ToggleFavorite,
            Command::RunQuery,
            Command::RunQueryInNewTab,
            Command::ExplainQuery,
            Command::CancelQuery,
            Command::ToggleHistoryDropdown,
            Command::OpenSavedQueries,
            Command::SaveQuery,
            Command::SaveFileAs,
            Command::OpenScriptFile,
            Command::ToggleComment,
            Command::ExportResults,
            Command::ClearFilter,
            Command::NextResultTab,
            Command::PrevResultTab,
            Command::CloseResultTab,
            Command::ResultsNextPage,
            Command::ResultsPrevPage,
            Command::FocusToolbar,
            Command::TogglePanel,
            Command::ResultsDeleteRow,
            Command::ResultsAddRow,
            Command::AddItem,
            Command::AddGroup,
            Command::ResultsDuplicateRow,
            Command::ResultsCopyRow,
            Command::ResultsCopyCell,
            Command::ToggleRecordView,
            Command::CycleDocumentView,
            Command::CycleResultView,
            Command::ToggleValuePanel,
            Command::ToggleRowInspector,
            Command::ResultsSetNull,
            Command::OpenContextMenu,
            Command::OpenPaneActions,
            Command::MenuUp,
            Command::MenuDown,
            Command::MenuSelect,
            Command::MenuBack,
            Command::SidebarNextTab,
            Command::RefreshSchema,
            Command::OpenConnectionManager,
            Command::ExportConnections,
            Command::Disconnect,
            Command::OpenItemMenu,
            Command::CreateFolder,
            Command::ToggleEditor,
            Command::ToggleResults,
            Command::ToggleTasks,
            Command::CancelTask,
            Command::ClearFinishedTasks,
            Command::ToggleSidebar,
            Command::OpenSettings,
            Command::OpenLoginModal,
            Command::OpenSsoWizard,
            Command::OpenAuditViewer,
            Command::OpenLastErrorInAudit,
            Command::OpenToastActions,
            Command::ToggleNotifications,
            Command::MarkNotificationRead,
            Command::MarkAllNotificationsRead,
            Command::ClearReadNotifications,
            Command::InstallUpdate,
            Command::ShowConnectionsView,
            Command::ShowScriptsView,
            Command::ShowDashboardsView,
            Command::OpenSavedChart,
            Command::ImportDashboard,
            Command::NewDashboard,
            Command::NextTimeRange,
            Command::PrevTimeRange,
            Command::ConfigurePanel,
            Command::MovePanelLeft,
            Command::MovePanelRight,
            Command::MovePanelUp,
            Command::MovePanelDown,
            Command::ResizePanelNarrower,
            Command::ResizePanelWider,
            Command::ResizePanelShorter,
            Command::ResizePanelTaller,
            Command::PreviewDocument,
            Command::ToggleRawView,
            Command::NextMatch,
            Command::PrevMatch,
            Command::ZoomIn,
            Command::ZoomOut,
            Command::PanLeft,
            Command::PanRight,
            Command::PanUp,
            Command::PanDown,
            Command::SelectTableLeft,
            Command::SelectTableRight,
            Command::SelectTableUp,
            Command::SelectTableDown,
            Command::MoveTableLeft,
            Command::MoveTableRight,
            Command::MoveTableUp,
            Command::MoveTableDown,
            Command::LayoutLeftRight,
            Command::LayoutSnowflake,
            Command::LayoutCompact,
            Command::ExtendSelectLeft,
            Command::ExtendSelectRight,
            Command::MoveToRowStart,
            Command::MoveToRowEnd,
            Command::ExtendSelectRowStart,
            Command::ExtendSelectRowEnd,
            Command::ExtendSelectFirst,
            Command::ExtendSelectLast,
            Command::SelectAll,
            Command::SaveRow,
            Command::Undo,
            Command::Redo,
            Command::ToggleColumnGroup,
            Command::StepOut,
            Command::TriggerCompletion,
            Command::CloseWindow,
            Command::ToggleConsole,
            Command::LoadMore,
            Command::EditExpiry,
            Command::CopyPreview,
            Command::ImportItems,
            Command::ImportFromClient,
            Command::ResetBinding,
            Command::ResetAllBindings,
            Command::EditBindingContext,
            Command::FilterByContext,
        ];

        #[cfg(feature = "mcp")]
        {
            variants.push(Command::OpenMcpApprovals);
            variants.push(Command::RefreshMcpGovernance);
            variants.push(Command::ApproveExecution);
            variants.push(Command::RejectExecution);
        }

        variants
    }

    /// Returns the category for this command (used in command palette grouping).
    #[allow(dead_code)]
    pub fn category(&self) -> &'static str {
        match self {
            Command::ToggleCommandPalette
            | Command::NewQueryTab
            | Command::CloseCurrentTab
            | Command::NextTab
            | Command::PrevTab
            | Command::SwitchToTab(_)
            | Command::MoveTabLeft
            | Command::MoveTabRight
            | Command::OpenTabMenu => "Global",

            Command::FocusSidebar
            | Command::FocusEditor
            | Command::FocusResults
            | Command::FocusBackgroundTasks
            | Command::CycleFocusForward
            | Command::CycleFocusBackward
            | Command::FocusLeft
            | Command::FocusRight
            | Command::FocusUp
            | Command::FocusDown => "Focus",

            Command::SelectNext
            | Command::SelectPrev
            | Command::SelectFirst
            | Command::SelectLast
            | Command::PageDown
            | Command::PageUp
            | Command::NextPanelTab
            | Command::PrevPanelTab
            | Command::ExtendSelectNext
            | Command::ExtendSelectPrev
            | Command::ToggleSelection
            | Command::MoveSelectedUp
            | Command::MoveSelectedDown => "Navigation",

            Command::ColumnLeft | Command::ColumnRight => "Results",

            Command::Execute
            | Command::Cancel
            | Command::ExpandCollapse
            | Command::Delete
            | Command::Rename
            | Command::FocusSearch
            | Command::AddItem
            | Command::AddGroup
            | Command::ToggleFavorite => "Actions",

            Command::RunQuery
            | Command::RunQueryInNewTab
            | Command::ExplainQuery
            | Command::CancelQuery
            | Command::ToggleHistoryDropdown
            | Command::OpenSavedQueries
            | Command::SaveQuery
            | Command::SaveFileAs
            | Command::OpenScriptFile
            | Command::ToggleComment => "Editor",

            Command::ExportResults
            | Command::ClearFilter
            | Command::NextResultTab
            | Command::PrevResultTab
            | Command::CloseResultTab
            | Command::ResultsNextPage
            | Command::ResultsPrevPage
            | Command::FocusToolbar
            | Command::ResultsDeleteRow
            | Command::ResultsAddRow
            | Command::ResultsDuplicateRow
            | Command::ResultsCopyRow
            | Command::ResultsCopyCell
            | Command::ToggleRecordView
            | Command::CycleDocumentView
            | Command::CycleResultView
            | Command::ToggleValuePanel
            | Command::ToggleRowInspector
            | Command::ResultsSetNull
            | Command::OpenContextMenu
            | Command::MenuUp
            | Command::MenuDown
            | Command::MenuSelect
            | Command::MenuBack => "Results",

            Command::SidebarNextTab
            | Command::RefreshSchema
            | Command::OpenConnectionManager
            | Command::ExportConnections
            | Command::Disconnect
            | Command::OpenItemMenu
            | Command::CreateFolder => "Sidebar",

            Command::ToggleEditor
            | Command::ToggleResults
            | Command::ToggleTasks
            | Command::CancelTask
            | Command::ClearFinishedTasks
            | Command::ToggleSidebar
            | Command::TogglePanel
            | Command::OpenSettings
            | Command::OpenLoginModal
            | Command::OpenSsoWizard
            | Command::OpenAuditViewer
            | Command::OpenLastErrorInAudit
            | Command::OpenToastActions
            | Command::ToggleNotifications
            | Command::MarkNotificationRead
            | Command::MarkAllNotificationsRead
            | Command::ClearReadNotifications
            | Command::InstallUpdate
            | Command::ShowConnectionsView
            | Command::ShowScriptsView
            | Command::ShowDashboardsView => "View",

            #[cfg(feature = "mcp")]
            Command::OpenMcpApprovals | Command::RefreshMcpGovernance => "View",

            #[cfg(feature = "mcp")]
            Command::ApproveExecution | Command::RejectExecution => "Actions",

            Command::OpenSavedChart | Command::ImportDashboard | Command::NewDashboard => {
                "Dashboards"
            }

            Command::NextTimeRange | Command::PrevTimeRange => "View",

            Command::ConfigurePanel
            | Command::MovePanelLeft
            | Command::MovePanelRight
            | Command::MovePanelUp
            | Command::MovePanelDown
            | Command::ResizePanelNarrower
            | Command::ResizePanelWider
            | Command::ResizePanelShorter
            | Command::ResizePanelTaller => "Dashboards",

            Command::NextMatch | Command::PrevMatch => "Navigation",

            Command::PreviewDocument => "Actions",

            Command::ToggleRawView
            | Command::ZoomIn
            | Command::ZoomOut
            | Command::LayoutLeftRight
            | Command::LayoutSnowflake
            | Command::LayoutCompact => "View",

            Command::PanLeft
            | Command::PanRight
            | Command::PanUp
            | Command::PanDown
            | Command::SelectTableLeft
            | Command::SelectTableRight
            | Command::SelectTableUp
            | Command::SelectTableDown
            | Command::MoveTableLeft
            | Command::MoveTableRight
            | Command::MoveTableUp
            | Command::MoveTableDown => "Navigation",

            Command::ExtendSelectLeft
            | Command::ExtendSelectRight
            | Command::MoveToRowStart
            | Command::MoveToRowEnd
            | Command::ExtendSelectRowStart
            | Command::ExtendSelectRowEnd
            | Command::ExtendSelectFirst
            | Command::ExtendSelectLast => "Navigation",

            Command::SelectAll | Command::Undo | Command::Redo => "Actions",

            Command::SaveRow | Command::ToggleColumnGroup | Command::StepOut => "Results",

            Command::TriggerCompletion => "Editor",

            Command::CloseWindow => "Global",
            Command::ToggleConsole => "View",
            Command::LoadMore => "Navigation",
            Command::EditExpiry => "Actions",
            Command::CopyPreview => "Actions",
            Command::ImportItems
            | Command::ImportFromClient
            | Command::ResetBinding
            | Command::ResetAllBindings
            | Command::EditBindingContext
            | Command::FilterByContext => "Actions",
            Command::OpenPaneActions => "Actions",
        }
    }

    /// Returns true if this command is globally available (not context-specific).
    #[allow(dead_code)]
    pub fn is_global(&self) -> bool {
        matches!(
            self,
            Command::ToggleCommandPalette
                | Command::NewQueryTab
                | Command::OpenScriptFile
                | Command::CloseCurrentTab
                | Command::NextTab
                | Command::PrevTab
                | Command::MoveTabLeft
                | Command::MoveTabRight
                | Command::SwitchToTab(_)
                | Command::RunQuery
                | Command::Cancel
                | Command::FocusSidebar
                | Command::FocusEditor
                | Command::FocusResults
                | Command::FocusBackgroundTasks
                | Command::CycleFocusForward
                | Command::CycleFocusBackward
                | Command::FocusLeft
                | Command::FocusRight
                | Command::FocusUp
                | Command::FocusDown
                | Command::ToggleEditor
                | Command::ToggleResults
                | Command::ToggleTasks
                | Command::ToggleSidebar
                | Command::OpenLoginModal
                | Command::OpenSsoWizard
                | Command::OpenAuditViewer
                | Command::OpenLastErrorInAudit
                | Command::OpenToastActions
                | Command::ToggleNotifications
                | Command::ShowConnectionsView
                | Command::ShowScriptsView
                | Command::ShowDashboardsView
        ) || {
            #[cfg(feature = "mcp")]
            {
                matches!(
                    self,
                    Command::OpenMcpApprovals | Command::RefreshMcpGovernance
                )
            }
            #[cfg(not(feature = "mcp"))]
            {
                false
            }
        }
    }
}

/// Identifies the current UI context for keybinding resolution.
///
/// Different contexts have different keybindings. When a key is pressed,
/// the system first looks for a binding in the current context, then
/// falls back to the Global context if no match is found.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ContextId {
    /// Global context - keybindings available everywhere.
    #[default]
    Global,

    /// Schema tree navigation in the sidebar.
    Sidebar,

    /// SQL editor area.
    Editor,

    /// Results table area.
    Results,

    /// Background tasks panel.
    BackgroundTasks,

    /// Command palette modal (captures all input).
    CommandPalette,

    /// Connection manager modal (captures all input).
    ConnectionManager,

    /// History modal (captures all input).
    HistoryModal,

    /// Any text input is focused and receiving keyboard input.
    TextInput,

    /// A dropdown menu is open and receiving keyboard navigation.
    Dropdown,

    /// SQL preview modal is open (captures all input).
    SqlPreviewModal,

    /// Cell editor modal is open (edits one grid cell as JSON or long text).
    CellEditorModal,

    /// Document preview modal is open (views or edits a whole document).
    DocumentPreviewModal,

    /// Context menu is open and receiving keyboard navigation.
    ContextMenu,

    /// Confirmation modal is open (dangerous query, delete, etc.).
    ConfirmModal,

    /// A navigable form is in `Navigating` mode (j/k/h/l move focus ring).
    FormNavigation,

    /// Execution context bar (Connection/Database/Schema dropdowns).
    ContextBar,

    /// Audit event viewer row list.
    Audit,

    /// Event-stream picker modal (collection child picker).
    EventStreamsPicker,

    /// Schema visualization document.
    SchemaViz,

    /// Document tree view (document databases and JSON values).
    DocumentTree,

    /// Data table grid (results, table documents, audit rows).
    DataTable,

    /// Any focused text input or code editor buffer.
    Input,

    /// Any modal dialog.
    Modal,

    /// Key-value document (keys list, value panel, console).
    KeyValue,

    /// Settings window.
    Settings,

    /// A side panel beside a document that keyboard focus moved into: the
    /// value panel, row inspector, document panel or query builder rail.
    Inspector,

    /// The notifications center popover under the title-bar bell.
    Notifications,

    /// The SQL query builder rail, once the keyboard moved into it.
    QueryBuilder,

    /// The document query builder rail, once the keyboard moved into it.
    DocumentBuilder,

    /// A chart document, or a chart panel a dashboard has entered.
    Chart,

    /// A dashboard document's panel grid.
    Dashboard,

    /// The Add Panel dialog of a dashboard (its tabs and lists).
    AddPanelPicker,

    /// The MCP approvals document (pending calls list and decision).
    McpApprovals,

    /// The table migration wizard document (its steps and footer).
    MigrateWizard,

    /// An editor with Vim mode in Normal or Visual mode, where the leader
    /// key starts a sequence (`<leader> a`).
    VimNormal,
}

impl ContextId {
    /// Returns the parent context for fallback keybinding resolution.
    ///
    /// Modal contexts (CommandPalette, ConnectionManager) and input contexts
    /// (TextInput, Dropdown) have no parent because they capture keyboard input.
    pub fn parent(&self) -> Option<ContextId> {
        match self {
            ContextId::Global => None,
            ContextId::CommandPalette => None,
            ContextId::ConnectionManager => None,
            ContextId::HistoryModal => None,
            ContextId::TextInput => None,
            ContextId::Dropdown => None,
            ContextId::SqlPreviewModal => None,
            ContextId::CellEditorModal => None,
            ContextId::DocumentPreviewModal => None,
            ContextId::ContextMenu => None,
            ContextId::ConfirmModal => None,
            ContextId::FormNavigation => None,
            ContextId::ContextBar => None,
            ContextId::EventStreamsPicker => None,
            ContextId::Sidebar => Some(ContextId::Global),
            ContextId::Editor => Some(ContextId::Global),
            ContextId::Results => Some(ContextId::Global),
            ContextId::BackgroundTasks => Some(ContextId::Global),
            ContextId::Audit => Some(ContextId::Global),
            ContextId::SchemaViz => Some(ContextId::Global),
            ContextId::DocumentTree => Some(ContextId::Global),
            ContextId::DataTable => None,
            ContextId::Input => None,
            ContextId::Modal => None,
            ContextId::KeyValue => None,
            ContextId::Settings => None,
            ContextId::Inspector => Some(ContextId::Global),
            ContextId::Notifications => None,
            ContextId::QueryBuilder => Some(ContextId::Global),
            ContextId::DocumentBuilder => Some(ContextId::Global),
            ContextId::Chart => Some(ContextId::Global),
            ContextId::Dashboard => Some(ContextId::Global),
            ContextId::AddPanelPicker => None,
            ContextId::McpApprovals => Some(ContextId::Global),
            ContextId::MigrateWizard => Some(ContextId::Global),
            ContextId::VimNormal => None,
        }
    }

    /// Identifier a window root adds to its key context when its context keeps
    /// the global chords (see [`ContextId::inherits_global_chords`]).
    pub const GLOBAL_CHORDS_IDENTIFIER: &'static str = "GlobalChords";

    /// Whether this context keeps the global layer's chords, the global
    /// bindings whose first key holds Ctrl or Cmd, although it does not
    /// inherit the global layer.
    ///
    /// These contexts own the keyboard while text is typed outside a dialog:
    /// a text field and the execution context bar. Unmodified keys (letters,
    /// Tab, Escape, Enter, the arrows) stay with the field, while chords such
    /// as Ctrl+Tab or Ctrl+W still reach the workspace. Dialogs, menus,
    /// dropdowns and pickers do not keep them: the user closes those first.
    pub fn inherits_global_chords(&self) -> bool {
        matches!(self, ContextId::TextInput | ContextId::ContextBar)
    }

    /// The context predicate the global chords carry in the contexts that
    /// keep them. Like the global layer, it does not hold inside a modal.
    pub fn global_chords_predicate() -> &'static str {
        "GlobalChords && !Modal"
    }

    /// Returns true if this context captures all keyboard input (modals/inputs).
    #[allow(dead_code)]
    pub fn is_modal(&self) -> bool {
        matches!(
            self,
            ContextId::CommandPalette
                | ContextId::ConnectionManager
                | ContextId::HistoryModal
                | ContextId::TextInput
                | ContextId::Dropdown
                | ContextId::SqlPreviewModal
                | ContextId::CellEditorModal
                | ContextId::DocumentPreviewModal
                | ContextId::ContextMenu
                | ContextId::ConfirmModal
                | ContextId::FormNavigation
                | ContextId::ContextBar
                | ContextId::EventStreamsPicker
        )
    }

    /// Returns true if this context is the audit viewer context.
    pub fn is_audit(&self) -> bool {
        matches!(self, ContextId::Audit)
    }

    /// Returns a human-readable name for this context.
    #[allow(dead_code)]
    pub fn display_name(&self) -> &'static str {
        match self {
            ContextId::Global => "Global",
            ContextId::Sidebar => "Sidebar",
            ContextId::Editor => "Editor",
            ContextId::Results => "Results",
            ContextId::BackgroundTasks => "Background Tasks",
            ContextId::CommandPalette => "Command Palette",
            ContextId::ConnectionManager => "Connection Manager",
            ContextId::HistoryModal => "History",
            ContextId::TextInput => "Text Input",
            ContextId::Dropdown => "Dropdown",
            ContextId::SqlPreviewModal => "SQL Preview",
            ContextId::CellEditorModal => "Cell Editor",
            ContextId::DocumentPreviewModal => "Document Preview",
            ContextId::ContextMenu => "Context Menu",
            ContextId::ConfirmModal => "Confirm",
            ContextId::FormNavigation => "Form Navigation",
            ContextId::ContextBar => "Context Bar",
            ContextId::Audit => "Audit Viewer",
            ContextId::EventStreamsPicker => "Event Streams Picker",
            ContextId::SchemaViz => "Schema Viz",
            ContextId::DocumentTree => "Document Tree",
            ContextId::DataTable => "Data Table",
            ContextId::Input => "Text Field",
            ContextId::Modal => "Modal Dialog",
            ContextId::KeyValue => "Key-Value Browser",
            ContextId::Settings => "Settings Window",
            ContextId::Inspector => "Inspector",
            ContextId::Notifications => "Notifications",
            ContextId::QueryBuilder => "Query Builder",
            ContextId::DocumentBuilder => "Document Builder",
            ContextId::Chart => "Chart",
            ContextId::Dashboard => "Dashboard",
            ContextId::AddPanelPicker => "Add Panel Picker",
            ContextId::McpApprovals => "MCP Approvals",
            ContextId::MigrateWizard => "Migrate Wizard",
            ContextId::VimNormal => "Vim Normal",
        }
    }

    /// Returns a stable, locale-independent identifier for this context.
    pub fn id(&self) -> &'static str {
        match self {
            ContextId::Global => "global",
            ContextId::Sidebar => "sidebar",
            ContextId::Editor => "editor",
            ContextId::Results => "results",
            ContextId::BackgroundTasks => "background_tasks",
            ContextId::CommandPalette => "command_palette",
            ContextId::ConnectionManager => "connection_manager",
            ContextId::HistoryModal => "history_modal",
            ContextId::TextInput => "text_input",
            ContextId::Dropdown => "dropdown",
            ContextId::SqlPreviewModal => "sql_preview_modal",
            ContextId::CellEditorModal => "cell_editor_modal",
            ContextId::DocumentPreviewModal => "document_preview_modal",
            ContextId::ContextMenu => "context_menu",
            ContextId::ConfirmModal => "confirm_modal",
            ContextId::FormNavigation => "form_navigation",
            ContextId::ContextBar => "context_bar",
            ContextId::Audit => "audit",
            ContextId::EventStreamsPicker => "event_streams_picker",
            ContextId::SchemaViz => "schema_viz",
            ContextId::DocumentTree => "document_tree",
            ContextId::DataTable => "data_table",
            ContextId::Input => "input",
            ContextId::Modal => "modal",
            ContextId::KeyValue => "key_value",
            ContextId::Settings => "settings",
            ContextId::Inspector => "inspector",
            ContextId::Notifications => "notifications",
            ContextId::QueryBuilder => "query_builder",
            ContextId::DocumentBuilder => "document_builder",
            ContextId::Chart => "chart",
            ContextId::Dashboard => "dashboard",
            ContextId::AddPanelPicker => "add_panel_picker",
            ContextId::McpApprovals => "mcp_approvals",
            ContextId::MigrateWizard => "migrate_wizard",
            ContextId::VimNormal => "vim_normal",
        }
    }

    /// Returns all context variants in display order.
    pub fn all_variants() -> &'static [ContextId] {
        &[
            ContextId::Global,
            ContextId::Sidebar,
            ContextId::Editor,
            ContextId::Results,
            ContextId::BackgroundTasks,
            ContextId::CommandPalette,
            ContextId::ConnectionManager,
            ContextId::HistoryModal,
            ContextId::TextInput,
            ContextId::Dropdown,
            ContextId::SqlPreviewModal,
            ContextId::CellEditorModal,
            ContextId::DocumentPreviewModal,
            ContextId::ContextMenu,
            ContextId::ConfirmModal,
            ContextId::FormNavigation,
            ContextId::ContextBar,
            ContextId::Audit,
            ContextId::EventStreamsPicker,
            ContextId::SchemaViz,
            ContextId::DocumentTree,
            ContextId::DataTable,
            ContextId::Input,
            ContextId::Modal,
            ContextId::KeyValue,
            ContextId::Settings,
            ContextId::Inspector,
            ContextId::Notifications,
            ContextId::QueryBuilder,
            ContextId::DocumentBuilder,
            ContextId::Chart,
            ContextId::Dashboard,
            ContextId::AddPanelPicker,
            ContextId::McpApprovals,
            ContextId::MigrateWizard,
            ContextId::VimNormal,
        ]
    }

    /// Returns the GPUUI key_context string for this context.
    pub fn as_gpui_context(&self) -> &'static str {
        match self {
            ContextId::Global => "Global",
            ContextId::Sidebar => "Sidebar",
            ContextId::Editor => "Editor",
            ContextId::Results => "Results",
            ContextId::BackgroundTasks => "BackgroundTasks",
            ContextId::CommandPalette => "CommandPalette",
            ContextId::ConnectionManager => "ConnectionManager",
            ContextId::HistoryModal => "HistoryModal",
            ContextId::TextInput => "TextInput",
            ContextId::Dropdown => "Dropdown",
            ContextId::SqlPreviewModal => "SqlPreviewModal",
            ContextId::CellEditorModal => "CellEditorModal",
            ContextId::DocumentPreviewModal => "DocumentPreviewModal",
            ContextId::ContextMenu => "ContextMenu",
            ContextId::ConfirmModal => "ConfirmModal",
            ContextId::FormNavigation => "FormNavigation",
            ContextId::ContextBar => "ContextBar",
            ContextId::Audit => "Audit",
            ContextId::EventStreamsPicker => "EventStreamsPicker",
            ContextId::SchemaViz => "SchemaViz",
            ContextId::DocumentTree => "DocumentTree",
            ContextId::DataTable => "DataTable",
            ContextId::Input => "Input",
            ContextId::Modal => "Modal",
            ContextId::KeyValue => "KeyValueView",
            ContextId::Settings => "Settings",
            ContextId::Inspector => "Inspector",
            ContextId::Notifications => "Notifications",
            ContextId::QueryBuilder => "QueryBuilder",
            ContextId::DocumentBuilder => "DocumentBuilder",
            ContextId::Chart => "Chart",
            ContextId::Dashboard => "Dashboard",
            ContextId::AddPanelPicker => "AddPanelPicker",
            ContextId::McpApprovals => "McpApprovals",
            ContextId::MigrateWizard => "MigrateWizard",
            ContextId::VimNormal => "VimNormal",
        }
    }

    /// The context predicate the default bindings of this context use, in
    /// GPUI's key context predicate language.
    ///
    /// A window root adds the identifier of the context that owns the
    /// keyboard, plus `Global` when that context inherits the global
    /// bindings. The contexts that inherit them also require `!Modal`: while
    /// focus is inside a modal dialog, the panels behind it do not see the
    /// keys. A few contexts belong to an element instead (the data table,
    /// text inputs, modals, the document tree and the modal editors) and
    /// match that element's own key context, which sits deeper than the
    /// window root and therefore takes precedence over it.
    pub fn default_predicate(&self) -> &'static str {
        match self {
            ContextId::Global => "Global && !Modal",
            ContextId::Sidebar => "Sidebar && !Modal",
            ContextId::Editor => "Editor && !Modal",
            ContextId::Results => "Results && !Modal",
            ContextId::BackgroundTasks => "BackgroundTasks && !Modal",
            ContextId::Audit => "Audit && !Modal",
            ContextId::SchemaViz => "SchemaViz && !Modal",
            ContextId::Inspector => "Inspector && !Modal",
            ContextId::QueryBuilder => "QueryBuilder && !Input && !Dropdown && !Modal",
            ContextId::DocumentBuilder => "DocumentBuilder && !Input && !Dropdown && !Modal",
            // A dialog can carry the Chart context (a dashboard panel's
            // Configure popover) to take the chart keys.
            ContextId::Chart => "Chart && !Input && !Dropdown",
            ContextId::Dashboard => "Dashboard && !Input && !Dropdown && !Modal",
            ContextId::AddPanelPicker => "AddPanelPicker && !Input",
            ContextId::McpApprovals => "McpApprovals && !Input && !Modal",
            ContextId::MigrateWizard => "MigrateWizard && !Input && !Dropdown && !Modal",
            ContextId::DataTable => "DataTable && !Input",
            ContextId::KeyValue => "KeyValueView && !Input",
            ContextId::FormNavigation => "FormNavigation && !Input",
            ContextId::SqlPreviewModal => "SqlPreviewModal && !Input",
            // A leader binding dispatches to the workspace, which must not act
            // on the document behind a dialog's editor.
            ContextId::VimNormal => "VimNormal && !Modal",
            context => context.as_gpui_context(),
        }
    }

    /// Whether this context's identifier is set by one element on itself
    /// rather than by a window root for the context owning the keyboard.
    pub fn is_element_context(&self) -> bool {
        matches!(
            self,
            ContextId::DocumentTree
                | ContextId::DataTable
                | ContextId::Input
                | ContextId::Modal
                | ContextId::KeyValue
                | ContextId::SqlPreviewModal
                | ContextId::CellEditorModal
                | ContextId::DocumentPreviewModal
                | ContextId::VimNormal
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{Command, ContextId};

    #[test]
    fn command_display_names_are_stable() {
        assert_eq!(
            Command::ToggleHistoryDropdown.display_name(),
            "Toggle history dropdown"
        );
        assert_eq!(
            Command::OpenSavedQueries.display_name(),
            "Open saved queries"
        );
        assert_eq!(Command::SaveQuery.display_name(), "Save");
    }

    #[test]
    fn history_modal_is_modal() {
        assert!(ContextId::HistoryModal.is_modal());
        assert_eq!(ContextId::HistoryModal.parent(), None);
    }

    #[test]
    fn command_ids_are_unique() {
        let ids: Vec<&str> = Command::all_variants().iter().map(Command::id).collect();
        let unique: std::collections::HashSet<&str> = ids.iter().copied().collect();
        assert_eq!(ids.len(), unique.len(), "duplicate command ids: {ids:?}");
    }

    #[test]
    fn command_ids_round_trip_through_from_palette_id() {
        const PALETTE_IDS: &[&str] = &[
            "new_query_tab",
            "run_query",
            "run_query_in_new_tab",
            "save_query",
            "toggle_comment",
            "open_history",
            "cancel_query",
            "close_tab",
            "next_tab",
            "prev_tab",
            "export_results",
            "open_connection_manager",
            "export_connections",
            "disconnect",
            "refresh_schema",
            "focus_sidebar",
            "focus_editor",
            "focus_results",
            "focus_tasks",
            "toggle_sidebar",
            "toggle_editor",
            "toggle_results",
            "toggle_tasks",
            "open_settings",
            "open_login_modal",
            "open_sso_wizard",
            "open_audit_viewer",
            "open_saved_chart",
            "import_dashboard",
            "new_dashboard",
        ];

        for palette_id in PALETTE_IDS {
            let command = Command::from_palette_id(palette_id)
                .unwrap_or_else(|| panic!("from_palette_id lost mapping for {palette_id}"));
            assert_eq!(
                command.id(),
                *palette_id,
                "Command::id() must reuse the palette id for {palette_id}"
            );
        }
    }

    #[test]
    fn action_ids_round_trip_every_command() {
        for command in Command::all_variants() {
            assert_eq!(
                Command::from_action_id(&command.action_id()),
                Some(command),
                "{command:?} must round-trip through its action id"
            );
        }

        assert_eq!(Command::SwitchToTab(7).action_id(), "switch_to_tab_7");
        assert_eq!(
            Command::from_action_id("switch_to_tab_7"),
            Some(Command::SwitchToTab(7))
        );
        assert_eq!(Command::from_action_id("switch_to_tab"), None);
        assert_eq!(Command::from_action_id("no_such_command"), None);
    }

    #[test]
    fn element_contexts_match_their_own_predicate() {
        for context in ContextId::all_variants() {
            assert!(
                context
                    .default_predicate()
                    .starts_with(context.as_gpui_context()),
                "{context:?} default predicate must name its own identifier"
            );
        }
    }

    #[test]
    fn only_text_entry_contexts_outside_dialogs_keep_the_global_chords() {
        let keeping: Vec<ContextId> = ContextId::all_variants()
            .iter()
            .copied()
            .filter(ContextId::inherits_global_chords)
            .collect();

        assert_eq!(keeping, vec![ContextId::TextInput, ContextId::ContextBar]);

        for context in keeping {
            assert_eq!(
                context.parent(),
                None,
                "{context:?} keeps the chords without inheriting the global layer"
            );
        }

        assert!(
            ContextId::global_chords_predicate().starts_with(ContextId::GLOBAL_CHORDS_IDENTIFIER)
        );
    }

    #[test]
    fn new_shell_commands_have_palette_ids() {
        for command in [
            Command::OpenLastErrorInAudit,
            Command::OpenToastActions,
            Command::ToggleNotifications,
            Command::ShowConnectionsView,
            Command::ShowScriptsView,
            Command::ShowDashboardsView,
        ] {
            assert_eq!(Command::from_palette_id(command.id()), Some(command));
            assert!(command.is_global(), "{command:?} is a workspace command");
        }
    }

    #[test]
    fn context_ids_are_unique() {
        let ids: Vec<&str> = ContextId::all_variants()
            .iter()
            .map(ContextId::id)
            .collect();
        let unique: std::collections::HashSet<&str> = ids.iter().copied().collect();
        assert_eq!(ids.len(), unique.len(), "duplicate context ids: {ids:?}");
    }
}
