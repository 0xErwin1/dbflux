use super::{Command, ContextId, KeyChord, KeymapLayer, KeymapStack, Modifiers};

/// Creates a KeymapStack with all default keybindings.
pub fn default_keymap() -> KeymapStack {
    let mut stack = KeymapStack::new();

    stack.add_layer(global_layer());
    stack.add_layer(sidebar_layer());
    stack.add_layer(history_layer());
    stack.add_layer(editor_layer());
    stack.add_layer(results_layer());
    stack.add_layer(background_tasks_layer());
    stack.add_layer(command_palette_layer());
    stack.add_layer(connection_manager_layer());
    stack.add_layer(text_input_layer());
    stack.add_layer(dropdown_layer());

    stack
}

fn global_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::Global);

    // Command palette
    layer.bind(
        KeyChord::new("p", Modifiers::ctrl_shift()),
        Command::ToggleCommandPalette,
    );

    // Tab management
    layer.bind(KeyChord::new("n", Modifiers::ctrl()), Command::NewQueryTab);
    layer.bind(
        KeyChord::new("w", Modifiers::ctrl()),
        Command::CloseCurrentTab,
    );
    layer.bind(KeyChord::new("tab", Modifiers::ctrl()), Command::NextTab);
    layer.bind(
        KeyChord::new("tab", Modifiers::ctrl_shift()),
        Command::PrevTab,
    );
    for i in 1..=9 {
        layer.bind(
            KeyChord::new(i.to_string(), Modifiers::ctrl()),
            Command::SwitchToTab(i),
        );
    }

    // Query execution
    layer.bind(KeyChord::new("enter", Modifiers::ctrl()), Command::RunQuery);

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

    // Direct focus shortcuts
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
        Command::FocusHistory,
    );

    layer
}

fn sidebar_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::Sidebar);

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

    layer
}

fn history_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::History);

    // Panel navigation (Ctrl+hjkl)
    layer.bind(KeyChord::new("l", Modifiers::ctrl()), Command::FocusRight);

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
    layer.bind(
        KeyChord::new("enter", Modifiers::none()),
        Command::LoadQuery,
    );
    layer.bind(
        KeyChord::new("f", Modifiers::none()),
        Command::ToggleFavorite,
    );
    layer.bind(
        KeyChord::new("delete", Modifiers::none()),
        Command::DeleteHistoryEntry,
    );
    layer.bind(
        KeyChord::new("x", Modifiers::none()),
        Command::DeleteHistoryEntry,
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

    layer
}

fn results_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::Results);

    // Panel navigation (Ctrl+hjkl)
    layer.bind(KeyChord::new("h", Modifiers::ctrl()), Command::FocusLeft);
    layer.bind(KeyChord::new("j", Modifiers::ctrl()), Command::FocusDown);
    layer.bind(KeyChord::new("k", Modifiers::ctrl()), Command::FocusUp);

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

    // Export
    layer.bind(
        KeyChord::new("e", Modifiers::ctrl()),
        Command::ExportResults,
    );

    layer
}

fn background_tasks_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::BackgroundTasks);

    // Panel navigation (Ctrl+hjkl)
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

    layer
}

fn command_palette_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::CommandPalette);

    layer.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);
    layer.bind(
        KeyChord::new("down", Modifiers::none()),
        Command::SelectNext,
    );

    layer.bind(KeyChord::new("k", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("up", Modifiers::none()), Command::SelectPrev);

    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);

    layer
}

fn connection_manager_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::ConnectionManager);

    // Vertical navigation (j/k without Ctrl)
    layer.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);
    layer.bind(KeyChord::new("k", Modifiers::none()), Command::SelectPrev);

    // Horizontal navigation within row (h/l without Ctrl)
    layer.bind(KeyChord::new("h", Modifiers::none()), Command::FocusLeft);
    layer.bind(KeyChord::new("l", Modifiers::none()), Command::FocusRight);

    // Tab switching (C-h/C-l)
    layer.bind(
        KeyChord::new("h", Modifiers::ctrl()),
        Command::CycleFocusBackward,
    );
    layer.bind(
        KeyChord::new("l", Modifiers::ctrl()),
        Command::CycleFocusForward,
    );

    // Actions
    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);

    layer
}

fn text_input_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::TextInput);

    // Escape exits text input mode
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);

    layer
}

fn dropdown_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::Dropdown);

    // Navigation within dropdown
    layer.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);
    layer.bind(
        KeyChord::new("down", Modifiers::none()),
        Command::SelectNext,
    );

    layer.bind(KeyChord::new("k", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("up", Modifiers::none()), Command::SelectPrev);

    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);

    layer
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_keymap_resolves_global() {
        let keymap = default_keymap();

        let chord = KeyChord::new("p", Modifiers::ctrl_shift());
        assert_eq!(
            keymap.resolve(ContextId::Global, &chord),
            Some(Command::ToggleCommandPalette)
        );
    }

    #[test]
    fn test_sidebar_vim_navigation() {
        let keymap = default_keymap();

        let j = KeyChord::new("j", Modifiers::none());
        let k = KeyChord::new("k", Modifiers::none());

        assert_eq!(
            keymap.resolve(ContextId::Sidebar, &j),
            Some(Command::SelectNext)
        );
        assert_eq!(
            keymap.resolve(ContextId::Sidebar, &k),
            Some(Command::SelectPrev)
        );
    }

    #[test]
    fn test_global_fallback_from_sidebar() {
        let keymap = default_keymap();

        let ctrl_enter = KeyChord::new("enter", Modifiers::ctrl());
        assert_eq!(
            keymap.resolve(ContextId::Sidebar, &ctrl_enter),
            Some(Command::RunQuery)
        );
    }

    #[test]
    fn test_command_palette_no_fallback() {
        let keymap = default_keymap();

        let ctrl_enter = KeyChord::new("enter", Modifiers::ctrl());
        assert_eq!(keymap.resolve(ContextId::CommandPalette, &ctrl_enter), None);
    }
}
