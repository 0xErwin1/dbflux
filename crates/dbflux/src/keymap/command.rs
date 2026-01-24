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
    FocusHistory,
    CycleFocusForward,
    CycleFocusBackward,
    FocusLeft,
    FocusRight,
    FocusUp,
    FocusDown,

    // === List Navigation (Sidebar, Results, History) ===
    SelectNext,
    SelectPrev,
    SelectFirst,
    SelectLast,
    PageDown,
    PageUp,

    // === Column Navigation (Results) ===
    ColumnLeft,
    ColumnRight,

    // === Generic Actions ===
    Execute,
    Cancel,
    ExpandCollapse,
    Delete,

    // === Editor ===
    RunQuery,
    CancelQuery,

    // === Results ===
    ExportResults,
    ResultsNextPage,
    ResultsPrevPage,

    // === Sidebar ===
    RefreshSchema,
    OpenConnectionManager,
    Disconnect,

    // === History ===
    ToggleFavorite,
    LoadQuery,
    DeleteHistoryEntry,

    // === View ===
    ToggleEditor,
    ToggleResults,
    ToggleTasks,
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
            Command::FocusHistory => "Focus History",
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

            Command::ColumnLeft => "Column Left",
            Command::ColumnRight => "Column Right",

            Command::Execute => "Execute",
            Command::Cancel => "Cancel",
            Command::ExpandCollapse => "Expand/Collapse",
            Command::Delete => "Delete",

            Command::RunQuery => "Run Query",
            Command::CancelQuery => "Cancel Query",

            Command::ExportResults => "Export Results",
            Command::ResultsNextPage => "Results Next Page",
            Command::ResultsPrevPage => "Results Previous Page",

            Command::RefreshSchema => "Refresh Schema",
            Command::OpenConnectionManager => "Open Connection Manager",
            Command::Disconnect => "Disconnect",

            Command::ToggleFavorite => "Toggle Favorite",
            Command::LoadQuery => "Load Query",
            Command::DeleteHistoryEntry => "Delete History Entry",

            Command::ToggleEditor => "Toggle Editor Panel",
            Command::ToggleResults => "Toggle Results Panel",
            Command::ToggleTasks => "Toggle Tasks Panel",
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
            | Command::FocusHistory
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
            | Command::PageUp => "Navigation",

            Command::ColumnLeft | Command::ColumnRight => "Results",

            Command::Execute | Command::Cancel | Command::ExpandCollapse | Command::Delete => {
                "Actions"
            }

            Command::RunQuery | Command::CancelQuery => "Editor",

            Command::ExportResults | Command::ResultsNextPage | Command::ResultsPrevPage => {
                "Results"
            }

            Command::RefreshSchema | Command::OpenConnectionManager | Command::Disconnect => {
                "Connections"
            }

            Command::ToggleFavorite | Command::LoadQuery | Command::DeleteHistoryEntry => "History",

            Command::ToggleEditor | Command::ToggleResults | Command::ToggleTasks => "View",
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
                | Command::FocusHistory
                | Command::CycleFocusForward
                | Command::CycleFocusBackward
                | Command::FocusLeft
                | Command::FocusRight
                | Command::FocusUp
                | Command::FocusDown
                | Command::ToggleEditor
                | Command::ToggleResults
                | Command::ToggleTasks
        )
    }
}
