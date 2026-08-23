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
    CancelQuery,
    ToggleHistoryDropdown,
    OpenSavedQueries,
    SaveQuery,
    SaveFileAs,
    OpenScriptFile,

    // === Results ===
    ExportResults,
    ResultsNextPage,
    ResultsPrevPage,
    FocusToolbar,
    TogglePanel,
    // Row operations (vim-style)
    ResultsDeleteRow,
    ResultsAddRow,
    ResultsDuplicateRow,
    ResultsCopyRow,
    ResultsCopyCell,
    ResultsSetNull,
    // Context menu
    OpenContextMenu,
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
    ToggleSidebar,
    OpenSettings,
    OpenLoginModal,
    OpenSsoWizard,
    OpenAuditViewer,
    #[cfg(feature = "mcp")]
    OpenMcpApprovals,
    #[cfg(feature = "mcp")]
    RefreshMcpGovernance,

    // === Charts / Dashboards ===
    /// Open the saved-chart fuzzy overlay (lists all SavedCharts for the current profile).
    OpenSavedChart,
    /// Open the "Import Dashboard from JSON" paste modal.
    ///
    /// Only available when the active connection has `DASHBOARD_IMPORT` capability.
    ImportDashboard,
    /// Open the "New Dashboard..." creation modal (profile picker then name input).
    NewDashboard,
}

impl Command {
    /// Resolve a command enum from a command palette identifier.
    pub fn from_palette_id(command_id: &str) -> Option<Self> {
        match command_id {
            "new_query_tab" => Some(Command::NewQueryTab),
            "run_query" => Some(Command::RunQuery),
            "run_query_in_new_tab" => Some(Command::RunQueryInNewTab),
            "save_query" => Some(Command::SaveQuery),
            "open_history" => Some(Command::ToggleHistoryDropdown),
            "cancel_query" => Some(Command::CancelQuery),
            "close_tab" => Some(Command::CloseCurrentTab),
            "next_tab" => Some(Command::NextTab),
            "prev_tab" => Some(Command::PrevTab),
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
            "open_settings" => Some(Command::OpenSettings),
            "open_login_modal" => Some(Command::OpenLoginModal),
            "open_sso_wizard" => Some(Command::OpenSsoWizard),
            "open_audit_viewer" => Some(Command::OpenAuditViewer),
            #[cfg(feature = "mcp")]
            "open_mcp_approvals" => Some(Command::OpenMcpApprovals),
            #[cfg(feature = "mcp")]
            "refresh_mcp_governance" => Some(Command::RefreshMcpGovernance),
            "open_saved_chart" => Some(Command::OpenSavedChart),
            "import_dashboard" => Some(Command::ImportDashboard),
            "new_dashboard" => Some(Command::NewDashboard),
            _ => None,
        }
    }

    /// Returns the display name for this command (used in command palette).
    #[allow(dead_code)]
    pub fn display_name(&self) -> &'static str {
        match self {
            Command::ToggleCommandPalette => "Toggle Command Palette",
            Command::NewQueryTab => "New Query Tab",
            Command::CloseCurrentTab => "Close Current Tab",
            Command::NextTab => "Next Tab",
            Command::PrevTab => "Previous Tab",
            Command::SwitchToTab(_) => "Switch to Tab",
            Command::OpenTabMenu => "Open Tab Menu",

            Command::FocusSidebar => "Focus Sidebar",
            Command::FocusEditor => "Focus Editor",
            Command::FocusResults => "Focus Results",
            Command::FocusBackgroundTasks => "Focus Background Tasks",
            Command::CycleFocusForward => "Cycle Focus Forward",
            Command::CycleFocusBackward => "Cycle Focus Backward",
            Command::FocusLeft => "Focus Left",
            Command::FocusRight => "Focus Right",
            Command::FocusUp => "Focus Up",
            Command::FocusDown => "Focus Down",

            Command::SelectNext => "Select Next",
            Command::SelectPrev => "Select Previous",
            Command::SelectFirst => "Select First",
            Command::SelectLast => "Select Last",
            Command::PageDown => "Page Down",
            Command::PageUp => "Page Up",

            Command::ExtendSelectNext => "Extend Selection Down",
            Command::ExtendSelectPrev => "Extend Selection Up",
            Command::ToggleSelection => "Toggle Selection",
            Command::MoveSelectedUp => "Move Selected Up",
            Command::MoveSelectedDown => "Move Selected Down",

            Command::ColumnLeft => "Column Left",
            Command::ColumnRight => "Column Right",

            Command::Execute => "Execute",
            Command::Cancel => "Cancel",
            Command::ExpandCollapse => "Expand/Collapse",
            Command::Delete => "Delete",
            Command::Rename => "Rename",
            Command::FocusSearch => "Focus Search",
            Command::ToggleFavorite => "Toggle Favorite",

            Command::RunQuery => "Run Query",
            Command::RunQueryInNewTab => "Run Query in New Tab",
            Command::CancelQuery => "Cancel Query",
            Command::ToggleHistoryDropdown => "Toggle History Dropdown",
            Command::OpenSavedQueries => "Open Saved Queries",
            Command::SaveQuery => "Save",
            Command::SaveFileAs => "Save File As",
            Command::OpenScriptFile => "Open Script File",

            Command::ExportResults => "Export Results",
            Command::ResultsNextPage => "Results Next Page",
            Command::ResultsPrevPage => "Results Previous Page",
            Command::FocusToolbar => "Focus Toolbar",
            Command::TogglePanel => "Toggle Panel",
            Command::ResultsDeleteRow => "Delete Row",
            Command::ResultsAddRow => "Add Row",
            Command::ResultsDuplicateRow => "Duplicate Row",
            Command::ResultsCopyRow => "Copy Row",
            Command::ResultsCopyCell => "Copy Cell",
            Command::ResultsSetNull => "Set Cell to NULL",
            Command::OpenContextMenu => "Open Context Menu",
            Command::MenuUp => "Menu Up",
            Command::MenuDown => "Menu Down",
            Command::MenuSelect => "Menu Select",
            Command::MenuBack => "Menu Back",

            Command::SidebarNextTab => "Sidebar Next Tab",
            Command::RefreshSchema => "Refresh Schema",
            Command::OpenConnectionManager => "Open Connection Manager",
            Command::ExportConnections => "Export Connections…",
            Command::Disconnect => "Disconnect",
            Command::OpenItemMenu => "Open Item Menu",
            Command::CreateFolder => "Create Folder",

            Command::ToggleEditor => "Toggle Editor Panel",
            Command::ToggleResults => "Toggle Results Panel",
            Command::ToggleTasks => "Toggle Tasks Panel",
            Command::ToggleSidebar => "Toggle Sidebar",
            Command::OpenSettings => "Open Settings",
            Command::OpenLoginModal => "Open Auth Profile Login",
            Command::OpenSsoWizard => "Open AWS SSO Wizard",
            Command::OpenAuditViewer => "Open Audit Viewer",
            #[cfg(feature = "mcp")]
            Command::OpenMcpApprovals => "Open MCP Approvals",
            #[cfg(feature = "mcp")]
            Command::RefreshMcpGovernance => "Refresh MCP Governance",
            Command::OpenSavedChart => "Open Chart...",
            Command::ImportDashboard => "Import Dashboard from JSON...",
            Command::NewDashboard => "New Dashboard...",
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
            Command::CancelQuery => "cancel_query",
            Command::ToggleHistoryDropdown => "open_history",
            Command::OpenSavedQueries => "open_saved_queries",
            Command::SaveQuery => "save_query",
            Command::SaveFileAs => "save_file_as",
            Command::OpenScriptFile => "open_script_file",

            Command::ExportResults => "export_results",
            Command::ResultsNextPage => "results_next_page",
            Command::ResultsPrevPage => "results_prev_page",
            Command::FocusToolbar => "focus_toolbar",
            Command::TogglePanel => "toggle_panel",
            Command::ResultsDeleteRow => "results_delete_row",
            Command::ResultsAddRow => "results_add_row",
            Command::ResultsDuplicateRow => "results_duplicate_row",
            Command::ResultsCopyRow => "results_copy_row",
            Command::ResultsCopyCell => "results_copy_cell",
            Command::ResultsSetNull => "results_set_null",
            Command::OpenContextMenu => "open_context_menu",
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
            Command::ToggleSidebar => "toggle_sidebar",
            Command::OpenSettings => "open_settings",
            Command::OpenLoginModal => "open_login_modal",
            Command::OpenSsoWizard => "open_sso_wizard",
            Command::OpenAuditViewer => "open_audit_viewer",
            #[cfg(feature = "mcp")]
            Command::OpenMcpApprovals => "open_mcp_approvals",
            #[cfg(feature = "mcp")]
            Command::RefreshMcpGovernance => "refresh_mcp_governance",

            Command::OpenSavedChart => "open_saved_chart",
            Command::ImportDashboard => "import_dashboard",
            Command::NewDashboard => "new_dashboard",
        }
    }

    /// Returns one instance of every [`Command`] variant, using a
    /// representative payload for variants that carry one.
    ///
    /// Intended for exhaustive coverage in tests (id uniqueness, translation
    /// coverage) across `dbflux_core` and downstream UI crates.
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
            Command::CancelQuery,
            Command::ToggleHistoryDropdown,
            Command::OpenSavedQueries,
            Command::SaveQuery,
            Command::SaveFileAs,
            Command::OpenScriptFile,
            Command::ExportResults,
            Command::ResultsNextPage,
            Command::ResultsPrevPage,
            Command::FocusToolbar,
            Command::TogglePanel,
            Command::ResultsDeleteRow,
            Command::ResultsAddRow,
            Command::ResultsDuplicateRow,
            Command::ResultsCopyRow,
            Command::ResultsCopyCell,
            Command::ResultsSetNull,
            Command::OpenContextMenu,
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
            Command::ToggleSidebar,
            Command::OpenSettings,
            Command::OpenLoginModal,
            Command::OpenSsoWizard,
            Command::OpenAuditViewer,
            Command::OpenSavedChart,
            Command::ImportDashboard,
            Command::NewDashboard,
        ];

        #[cfg(feature = "mcp")]
        {
            variants.push(Command::OpenMcpApprovals);
            variants.push(Command::RefreshMcpGovernance);
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
            | Command::ToggleFavorite => "Actions",

            Command::RunQuery
            | Command::RunQueryInNewTab
            | Command::CancelQuery
            | Command::ToggleHistoryDropdown
            | Command::OpenSavedQueries
            | Command::SaveQuery
            | Command::SaveFileAs
            | Command::OpenScriptFile => "Editor",

            Command::ExportResults
            | Command::ResultsNextPage
            | Command::ResultsPrevPage
            | Command::FocusToolbar
            | Command::ResultsDeleteRow
            | Command::ResultsAddRow
            | Command::ResultsDuplicateRow
            | Command::ResultsCopyRow
            | Command::ResultsCopyCell
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
            | Command::ToggleSidebar
            | Command::TogglePanel
            | Command::OpenSettings
            | Command::OpenLoginModal
            | Command::OpenSsoWizard
            | Command::OpenAuditViewer => "View",

            #[cfg(feature = "mcp")]
            Command::OpenMcpApprovals | Command::RefreshMcpGovernance => "View",

            Command::OpenSavedChart | Command::ImportDashboard | Command::NewDashboard => {
                "Dashboards"
            }
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
        }
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
            ContextId::ContextMenu => "Context Menu",
            ContextId::ConfirmModal => "Confirm",
            ContextId::FormNavigation => "Form Navigation",
            ContextId::ContextBar => "Context Bar",
            ContextId::Audit => "Audit Viewer",
            ContextId::EventStreamsPicker => "Event Streams Picker",
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
            ContextId::ContextMenu => "context_menu",
            ContextId::ConfirmModal => "confirm_modal",
            ContextId::FormNavigation => "form_navigation",
            ContextId::ContextBar => "context_bar",
            ContextId::Audit => "audit",
            ContextId::EventStreamsPicker => "event_streams_picker",
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
            ContextId::ContextMenu,
            ContextId::ConfirmModal,
            ContextId::FormNavigation,
            ContextId::ContextBar,
            ContextId::Audit,
            ContextId::EventStreamsPicker,
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
            ContextId::ContextMenu => "ContextMenu",
            ContextId::ConfirmModal => "ConfirmModal",
            ContextId::FormNavigation => "FormNavigation",
            ContextId::ContextBar => "ContextBar",
            ContextId::Audit => "Audit",
            ContextId::EventStreamsPicker => "EventStreamsPicker",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Command, ContextId};

    #[test]
    fn command_display_names_are_stable() {
        assert_eq!(
            Command::ToggleHistoryDropdown.display_name(),
            "Toggle History Dropdown"
        );
        assert_eq!(
            Command::OpenSavedQueries.display_name(),
            "Open Saved Queries"
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
    fn context_ids_are_unique() {
        let ids: Vec<&str> = ContextId::all_variants()
            .iter()
            .map(ContextId::id)
            .collect();
        let unique: std::collections::HashSet<&str> = ids.iter().copied().collect();
        assert_eq!(ids.len(), unique.len(), "duplicate context ids: {ids:?}");
    }
}
