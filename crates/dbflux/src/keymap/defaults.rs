use std::sync::LazyLock;

use super::{Command, ContextId, KeyChord, KeymapLayer, KeymapStack, Modifiers};

static DEFAULT_KEYMAP: LazyLock<KeymapStack> = LazyLock::new(|| {
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
    stack.add_layer(form_navigation_layer());

    stack
});

/// Returns a reference to the default KeymapStack with all default keybindings.
pub fn default_keymap() -> &'static KeymapStack {
    &DEFAULT_KEYMAP
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
    layer.bind(
        KeyChord::new("enter", Modifiers::ctrl_shift()),
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
        Command::FocusBackgroundTasks,
    );

    // Toggle sidebar (Ctrl+B)
    layer.bind(
        KeyChord::new("b", Modifiers::ctrl()),
        Command::ToggleSidebar,
    );

    layer
}

fn sidebar_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::Sidebar);

    layer.bind(KeyChord::new("n", Modifiers::ctrl()), Command::NewQueryTab);

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
        KeyChord::new("p", Modifiers::ctrl()),
        Command::OpenSavedQueries,
    );
    layer.bind(KeyChord::new("s", Modifiers::ctrl()), Command::SaveQuery);

    // Toggle panel collapse
    layer.bind(KeyChord::new("z", Modifiers::none()), Command::TogglePanel);

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

    layer.bind(KeyChord::new("d", Modifiers::ctrl()), Command::Delete);
    layer.bind(
        KeyChord::new("f", Modifiers::ctrl()),
        Command::ToggleFavorite,
    );
    layer.bind(KeyChord::new("r", Modifiers::ctrl()), Command::Rename);
    layer.bind(KeyChord::new("/", Modifiers::none()), Command::FocusSearch);
    layer.bind(KeyChord::new("s", Modifiers::ctrl()), Command::SaveQuery);

    layer
}

fn results_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::Results);

    layer.bind(KeyChord::new("n", Modifiers::ctrl()), Command::NewQueryTab);

    // Panel navigation (Ctrl+hjkl)
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

    // Export
    layer.bind(
        KeyChord::new("e", Modifiers::ctrl()),
        Command::ExportResults,
    );

    // Execute (Enter to edit input in toolbar mode)
    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);

    // Toolbar / filter focus
    layer.bind(KeyChord::new("f", Modifiers::none()), Command::FocusToolbar);
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

fn background_tasks_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::BackgroundTasks);

    layer.bind(KeyChord::new("n", Modifiers::ctrl()), Command::NewQueryTab);

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

    // Toggle panel collapse
    layer.bind(KeyChord::new("z", Modifiers::none()), Command::TogglePanel);

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
    layer.bind(KeyChord::new("s", Modifiers::none()), Command::SaveQuery);

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

fn form_navigation_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::FormNavigation);

    layer.bind(KeyChord::new("n", Modifiers::ctrl()), Command::NewQueryTab);

    layer.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);
    layer.bind(KeyChord::new("k", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("h", Modifiers::none()), Command::FocusLeft);
    layer.bind(KeyChord::new("l", Modifiers::none()), Command::FocusRight);
    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);

    layer
}

fn text_input_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::TextInput);

    layer.bind(KeyChord::new("n", Modifiers::ctrl()), Command::NewQueryTab);

    // Escape exits text input mode
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);

    layer
}

fn dropdown_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::Dropdown);

    layer.bind(KeyChord::new("n", Modifiers::ctrl()), Command::NewQueryTab);

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
    layer.bind(KeyChord::new("s", Modifiers::none()), Command::SaveQuery);

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
    fn test_editor_history_bindings() {
        let keymap = default_keymap();

        let alt_h = KeyChord::new("h", Modifiers::alt());
        let ctrl_p = KeyChord::new("p", Modifiers::ctrl());
        let ctrl_s = KeyChord::new("s", Modifiers::ctrl());

        assert_eq!(
            keymap.resolve(ContextId::Editor, &alt_h),
            Some(Command::ToggleHistoryDropdown)
        );
        assert_eq!(
            keymap.resolve(ContextId::Editor, &ctrl_p),
            Some(Command::OpenSavedQueries)
        );
        assert_eq!(
            keymap.resolve(ContextId::Editor, &ctrl_s),
            Some(Command::SaveQuery)
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
    fn test_ctrl_n_available_in_sidebar_and_text_input() {
        let keymap = default_keymap();

        let ctrl_n = KeyChord::new("n", Modifiers::ctrl());

        assert_eq!(
            keymap.resolve(ContextId::Sidebar, &ctrl_n),
            Some(Command::NewQueryTab)
        );
        assert_eq!(
            keymap.resolve(ContextId::TextInput, &ctrl_n),
            Some(Command::NewQueryTab)
        );
    }

    #[test]
    fn test_command_palette_no_fallback() {
        let keymap = default_keymap();

        let ctrl_enter = KeyChord::new("enter", Modifiers::ctrl());
        assert_eq!(keymap.resolve(ContextId::CommandPalette, &ctrl_enter), None);
    }
}
