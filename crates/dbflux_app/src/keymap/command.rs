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
            Command::ResultsSetNull => "Set Cell to NULL",
            Command::OpenContextMenu => "Open Context Menu",
            Command::MenuUp => "Menu Up",
            Command::MenuDown => "Menu Down",
            Command::MenuSelect => "Menu Select",
            Command::MenuBack => "Menu Back",

            Command::SidebarNextTab => "Sidebar Next Tab",
            Command::RefreshSchema => "Refresh Schema",
            Command::OpenConnectionManager => "Open Connection Manager",
            Command::Disconnect => "Disconnect",
            Command::OpenItemMenu => "Open Item Menu",
            Command::CreateFolder => "Create Folder",

            Command::ToggleEditor => "Toggle Editor Panel",
            Command::ToggleResults => "Toggle Results Panel",
            Command::ToggleTasks => "Toggle Tasks Panel",
            Command::ToggleSidebar => "Toggle Sidebar",
            Command::OpenSettings => "Open Settings",
            Command::OpenLoginModal => "Open Login Modal",
            Command::OpenSsoWizard => "Open AWS SSO Wizard",
            Command::OpenAuditViewer => "Open Audit Viewer",
            #[cfg(feature = "mcp")]
            Command::OpenMcpApprovals => "Open MCP Approvals",
            #[cfg(feature = "mcp")]
            Command::RefreshMcpGovernance => "Refresh MCP Governance",
        }
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
            | Command::ResultsSetNull
            | Command::OpenContextMenu
            | Command::MenuUp
            | Command::MenuDown
            | Command::MenuSelect
            | Command::MenuBack => "Results",

            Command::SidebarNextTab
            | Command::RefreshSchema
            | Command::OpenConnectionManager
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

#[cfg(test)]
mod tests {
    use super::Command;

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
}
