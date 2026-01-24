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

    /// Query history panel.
    History,

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

    /// Any text input is focused and receiving keyboard input.
    TextInput,

    /// A dropdown menu is open and receiving keyboard navigation.
    Dropdown,
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
            ContextId::TextInput => None,
            ContextId::Dropdown => None,
            ContextId::Sidebar => Some(ContextId::Global),
            ContextId::History => Some(ContextId::Global),
            ContextId::Editor => Some(ContextId::Global),
            ContextId::Results => Some(ContextId::Global),
            ContextId::BackgroundTasks => Some(ContextId::Global),
        }
    }

    /// Returns true if this context captures all keyboard input (modals/inputs).
    #[allow(dead_code)]
    pub fn is_modal(&self) -> bool {
        matches!(
            self,
            ContextId::CommandPalette
                | ContextId::ConnectionManager
                | ContextId::TextInput
                | ContextId::Dropdown
        )
    }

    /// Returns a human-readable name for this context.
    #[allow(dead_code)]
    pub fn display_name(&self) -> &'static str {
        match self {
            ContextId::Global => "Global",
            ContextId::Sidebar => "Sidebar",
            ContextId::History => "History",
            ContextId::Editor => "Editor",
            ContextId::Results => "Results",
            ContextId::BackgroundTasks => "Background Tasks",
            ContextId::CommandPalette => "Command Palette",
            ContextId::ConnectionManager => "Connection Manager",
            ContextId::TextInput => "Text Input",
            ContextId::Dropdown => "Dropdown",
        }
    }

    /// Returns the GPUI key_context string for this context.
    pub fn as_gpui_context(&self) -> &'static str {
        match self {
            ContextId::Global => "Global",
            ContextId::Sidebar => "Sidebar",
            ContextId::History => "History",
            ContextId::Editor => "Editor",
            ContextId::Results => "Results",
            ContextId::BackgroundTasks => "BackgroundTasks",
            ContextId::CommandPalette => "CommandPalette",
            ContextId::ConnectionManager => "ConnectionManager",
            ContextId::TextInput => "TextInput",
            ContextId::Dropdown => "Dropdown",
        }
    }
}
