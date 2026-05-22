//! Keymap helpers that depend on both GPUI and `dbflux_app::keymap`.
//!
//! These helpers live in `dbflux_ui_base` rather than `dbflux_components`
//! because they require `dbflux_app::keymap` types, which `dbflux_components`
//! intentionally does not depend on.

use dbflux_app::keymap::{KeyChord, KeymapStack, Modifiers};
use dbflux_app::keymap::{Command, ContextId, KeymapLayer};
use gpui::Keystroke;
use std::sync::LazyLock;

// ============================================================================
// GPUI keystroke conversion helpers
// ============================================================================

/// Creates a [`KeyChord`] from a GPUI [`Keystroke`].
#[allow(dead_code)]
pub fn key_chord_from_gpui(keystroke: &Keystroke) -> KeyChord {
    KeyChord {
        key: keystroke.key.clone(),
        modifiers: modifiers_from_gpui(&keystroke.modifiers),
    }
}

/// Creates [`Modifiers`] from a GPUI [`gpui::Modifiers`] struct.
#[allow(dead_code)]
pub fn modifiers_from_gpui(mods: &gpui::Modifiers) -> Modifiers {
    Modifiers {
        ctrl: mods.control,
        alt: mods.alt,
        shift: mods.shift,
        platform: mods.platform,
    }
}

// ============================================================================
// Default keymap
// ============================================================================

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
    stack.add_layer(context_bar_layer());
    stack.add_layer(audit_layer());
    stack.add_layer(event_streams_picker_layer());

    stack
});

/// Returns a reference to the default [`KeymapStack`] with all default keybindings.
pub fn default_keymap() -> &'static KeymapStack {
    &DEFAULT_KEYMAP
}

fn global_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::Global);

    // Command palette — Cmd+Shift+P on macOS, Ctrl+Shift+P elsewhere.
    layer.bind(
        KeyChord::new("p", Modifiers::primary_shift()),
        Command::ToggleCommandPalette,
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
    // IMPORTANT — these four entries are retained as a no-op label source only.
    //
    // (a) Actual keystroke dispatch is owned by `workspace_keybindings()` in
    //     `actions.rs`, registered via `cx.bind_keys` as native GPUI bindings.
    //     GPUI normalizes Ctrl+Shift+digit chords at the platform layer before
    //     KeymapStack sees them (see GitHub #65), so these structural matchers
    //     never fire at runtime.
    // (b) These entries serve one live purpose: supplying the shortcut label in
    //     the command palette. `shortcut_for_command` reads KeymapStack (not the
    //     GPUI keymap), so removing these entries would drop the "Ctrl+Shift+N"
    //     hints from the palette. Do not remove them until the command palette is
    //     updated to read GPUI native bindings.
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

    layer
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
    layer.bind(KeyChord::new("/", Modifiers::none()), Command::FocusSearch);
    layer.bind(KeyChord::new("s", Modifiers::primary()), Command::SaveQuery);

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

    // Export
    layer.bind(
        KeyChord::new("e", Modifiers::primary()),
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

    // Vertical navigation (j/k without Ctrl, plus arrow keys for the picker).
    layer.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);
    layer.bind(KeyChord::new("k", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("down", Modifiers::none()), Command::FocusDown);
    layer.bind(KeyChord::new("up", Modifiers::none()), Command::FocusUp);

    // Horizontal navigation within row (h/l without Ctrl, plus arrows).
    layer.bind(KeyChord::new("h", Modifiers::none()), Command::FocusLeft);
    layer.bind(KeyChord::new("l", Modifiers::none()), Command::FocusRight);
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
    layer.bind(KeyChord::new("/", Modifiers::none()), Command::FocusSearch);

    // Actions
    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);

    layer
}

fn form_navigation_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::FormNavigation);

    layer.bind(
        KeyChord::new("n", Modifiers::primary()),
        Command::NewQueryTab,
    );

    layer.bind(KeyChord::new("j", Modifiers::none()), Command::SelectNext);
    layer.bind(KeyChord::new("k", Modifiers::none()), Command::SelectPrev);
    layer.bind(KeyChord::new("h", Modifiers::none()), Command::ColumnLeft);
    layer.bind(KeyChord::new("l", Modifiers::none()), Command::ColumnRight);
    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);

    layer
}

fn text_input_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::TextInput);

    layer.bind(
        KeyChord::new("n", Modifiers::primary()),
        Command::NewQueryTab,
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

    // Dismiss / exit toolbar navigation.
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);

    layer
}

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
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);
    layer.bind(KeyChord::new("s", Modifiers::none()), Command::SaveQuery);

    layer
}
