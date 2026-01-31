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

    // === Results ===
    ExportResults,
    ResultsNextPage,
    ResultsPrevPage,
    FocusToolbar,
    TogglePanel,

    // === Sidebar ===
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
}

impl Command {
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
            Command::SaveQuery => "Save Query",

            Command::ExportResults => "Export Results",
            Command::ResultsNextPage => "Results Next Page",
            Command::ResultsPrevPage => "Results Previous Page",
            Command::FocusToolbar => "Focus Toolbar",
            Command::TogglePanel => "Toggle Panel",

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
            | Command::SwitchToTab(_) => "Global",

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
            | Command::SaveQuery => "Editor",

            Command::ExportResults
            | Command::ResultsNextPage
            | Command::ResultsPrevPage
            | Command::FocusToolbar => "Results",

            Command::RefreshSchema
            | Command::OpenConnectionManager
            | Command::Disconnect
            | Command::OpenItemMenu
            | Command::CreateFolder => "Connections",

            Command::ToggleEditor
            | Command::ToggleResults
            | Command::ToggleTasks
            | Command::ToggleSidebar
            | Command::TogglePanel
            | Command::OpenSettings => "View",
        }
    }

    /// Returns true if this command is globally available (not context-specific).
    #[allow(dead_code)]
    pub fn is_global(&self) -> bool {
        matches!(
            self,
            Command::ToggleCommandPalette
                | Command::NewQueryTab
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
        )
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
        assert_eq!(Command::SaveQuery.display_name(), "Save Query");
    }
}
