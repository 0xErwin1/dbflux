//! Keymap helpers that depend on both GPUI and `dbflux_app::keymap`.
//!
//! These helpers live in `dbflux_ui_base` rather than `dbflux_components`
//! because they require `dbflux_app::keymap` types, which `dbflux_components`
//! intentionally does not depend on.

use dbflux_app::keymap::{Command, ContextId, KeymapLayer};
use dbflux_app::keymap::{KeyChord, KeymapStack, Modifiers};
use dbflux_components::components::document_tree;
use gpui::{Action, App, DummyKeyboardMapper, KeyBinding, KeyBindingContextPredicate, Keystroke};
use std::rc::Rc;
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
    stack.add_layer(confirm_modal_layer());
    stack.add_layer(form_navigation_layer());
    stack.add_layer(context_bar_layer());
    stack.add_layer(audit_layer());
    stack.add_layer(event_streams_picker_layer());
    stack.add_layer(schema_viz_layer());
    stack.add_layer(document_tree_layer());

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
    layer.bind(
        KeyChord::new("/", Modifiers::primary()),
        Command::ToggleComment,
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

    // Expand/collapse — object browser preview/properties, per-document meaning
    // otherwise (matches the Sidebar layer's `space` binding).
    layer.bind(
        KeyChord::new("space", Modifiers::none()),
        Command::ExpandCollapse,
    );

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
    layer.bind(
        KeyChord::new("i", Modifiers::none()),
        Command::ToggleRecordView,
    );
    layer.bind(
        KeyChord::new("v", Modifiers::none()),
        Command::ToggleValuePanel,
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

    // Toggle panel collapse
    layer.bind(KeyChord::new("z", Modifiers::none()), Command::TogglePanel);

    layer
}

fn command_palette_layer() -> KeymapLayer {
    let mut layer = KeymapLayer::new(ContextId::CommandPalette);

    // The palette's search input must receive every unmodified letter, so this
    // layer binds no bare a-z chords; list navigation stays on the arrow keys.
    layer.bind(
        KeyChord::new("down", Modifiers::none()),
        Command::SelectNext,
    );
    layer.bind(KeyChord::new("up", Modifiers::none()), Command::SelectPrev);

    layer.bind(KeyChord::new("enter", Modifiers::none()), Command::Execute);
    layer.bind(KeyChord::new("escape", Modifiers::none()), Command::Cancel);

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
/// The tree dispatches these as native GPUI actions inside its own key
/// context, generated from this layer by [`document_tree_keybindings`]. The
/// `d d` delete sequence is not listed: a [`KeyChord`] is a single
/// keystroke, so that sequence stays a native binding in the tree component.
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
    layer.bind(
        KeyChord::new("r", Modifiers::none()),
        Command::ToggleRawView,
    );

    // Search
    layer.bind(KeyChord::new("f", Modifiers::ctrl()), Command::FocusSearch);
    layer.bind(KeyChord::new("/", Modifiers::none()), Command::FocusSearch);
    layer.bind(KeyChord::new("n", Modifiers::none()), Command::NextMatch);
    layer.bind(KeyChord::new("n", Modifiers::shift()), Command::PrevMatch);
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

// ============================================================================
// Document tree native bindings
// ============================================================================

/// Registers every document tree keybinding.
///
/// Call once at startup, in place of calling the tree component's own `init`.
pub fn init_document_tree_keybindings(cx: &mut App) {
    document_tree::init(cx);
    cx.bind_keys(document_tree_keybindings());
}

/// Native GPUI bindings for the document tree, generated from the keymap's
/// `DocumentTree` layer.
///
/// The tree lives in `dbflux_components`, which cannot depend on the keymap.
/// It handles its keys as GPUI actions in its own key context, which keeps
/// the precedence it has always had over the workspace keymap and under the
/// text inputs nested in it (search box, inline value editor). Generating the
/// bindings from the layer keeps that dispatch while making the layer the
/// single source of the tree's keys.
pub fn document_tree_keybindings() -> Vec<KeyBinding> {
    let context: Rc<KeyBindingContextPredicate> =
        match KeyBindingContextPredicate::parse(document_tree::CONTEXT) {
            Ok(predicate) => predicate.into(),
            Err(error) => {
                log::error!("Invalid document tree key context: {error}");
                return Vec::new();
            }
        };

    default_keymap()
        .bindings_for_context(ContextId::DocumentTree)
        .into_iter()
        .filter(|(_, _, source)| *source == ContextId::DocumentTree)
        .filter_map(|(chord, command, _)| {
            let action = document_tree_action(command)?;
            let keystroke = gpui_keystroke(&chord);

            match KeyBinding::load(
                &keystroke,
                action,
                Some(context.clone()),
                false,
                None,
                &DummyKeyboardMapper,
            ) {
                Ok(binding) => Some(binding),
                Err(error) => {
                    log::error!("Invalid document tree keystroke `{keystroke}`: {error}");
                    None
                }
            }
        })
        .collect()
}

/// Maps a keymap command to the document tree action that performs it.
fn document_tree_action(command: Command) -> Option<Box<dyn Action>> {
    use document_tree::actions;

    let action: Box<dyn Action> = match command {
        Command::SelectPrev => Box::new(actions::MoveUp),
        Command::SelectNext => Box::new(actions::MoveDown),
        Command::ColumnLeft => Box::new(actions::MoveLeft),
        Command::ColumnRight => Box::new(actions::MoveRight),
        Command::SelectFirst => Box::new(actions::MoveToTop),
        Command::SelectLast => Box::new(actions::MoveToBottom),
        Command::PageUp => Box::new(actions::PageUp),
        Command::PageDown => Box::new(actions::PageDown),
        Command::ExpandCollapse => Box::new(actions::ToggleExpand),
        Command::Execute => Box::new(actions::StartEdit),
        Command::PreviewDocument => Box::new(actions::OpenPreview),
        Command::Delete => Box::new(actions::DeleteDocument),
        Command::ToggleRawView => Box::new(actions::ToggleViewMode),
        Command::FocusSearch => Box::new(actions::OpenSearch),
        Command::NextMatch => Box::new(actions::NextMatch),
        Command::PrevMatch => Box::new(actions::PrevMatch),
        Command::Cancel => Box::new(actions::CloseSearch),
        _ => return None,
    };

    Some(action)
}

/// Formats a chord in GPUI keystroke syntax, for example `ctrl-shift-p`.
fn gpui_keystroke(chord: &KeyChord) -> String {
    let modifiers = &chord.modifiers;
    let mut parts: Vec<&str> = Vec::new();

    if modifiers.platform {
        parts.push("cmd");
    }
    if modifiers.ctrl {
        parts.push("ctrl");
    }
    if modifiers.alt {
        parts.push("alt");
    }
    if modifiers.shift {
        parts.push("shift");
    }
    parts.push(&chord.key);

    parts.join("-")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_keymap_resolves_global() {
        let keymap = default_keymap();

        let chord = KeyChord::new("p", Modifiers::primary_shift());
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
        let primary_p = KeyChord::new("p", Modifiers::primary());
        let primary_s = KeyChord::new("s", Modifiers::primary());

        assert_eq!(
            keymap.resolve(ContextId::Editor, &alt_h),
            Some(Command::ToggleHistoryDropdown)
        );
        assert_eq!(
            keymap.resolve(ContextId::Editor, &primary_p),
            Some(Command::OpenSavedQueries)
        );
        assert_eq!(
            keymap.resolve(ContextId::Editor, &primary_s),
            Some(Command::SaveQuery)
        );
    }

    #[test]
    fn test_editor_toggle_comment_binding() {
        let keymap = default_keymap();

        let primary_slash = KeyChord::new("/", Modifiers::primary());
        assert_eq!(
            keymap.resolve(ContextId::Editor, &primary_slash),
            Some(Command::ToggleComment)
        );

        // An unmodified `/` stays with the text input: it must not be claimed
        // by the editor layer, or typing a slash in a query would toggle a
        // comment instead.
        assert_eq!(
            keymap.resolve(ContextId::Editor, &KeyChord::new("/", Modifiers::none())),
            None
        );
    }

    #[test]
    fn test_global_fallback_from_sidebar() {
        let keymap = default_keymap();

        let primary_enter = KeyChord::new("enter", Modifiers::primary());
        assert_eq!(
            keymap.resolve(ContextId::Sidebar, &primary_enter),
            Some(Command::RunQuery)
        );
    }

    #[test]
    fn test_primary_n_available_in_sidebar_and_text_input() {
        let keymap = default_keymap();

        let primary_n = KeyChord::new("n", Modifiers::primary());

        assert_eq!(
            keymap.resolve(ContextId::Sidebar, &primary_n),
            Some(Command::NewQueryTab)
        );
        assert_eq!(
            keymap.resolve(ContextId::TextInput, &primary_n),
            Some(Command::NewQueryTab)
        );
    }

    #[test]
    fn test_command_palette_no_fallback() {
        let keymap = default_keymap();

        let primary_enter = KeyChord::new("enter", Modifiers::primary());
        assert_eq!(
            keymap.resolve(ContextId::CommandPalette, &primary_enter),
            None
        );
    }

    /// On macOS the literal Ctrl+C must not trigger ResultsCopyCell — the
    /// platform convention is Cmd+C, and Ctrl+C is reserved for editor
    /// interrupt semantics. See the per-platform branch in `results_layer`.
    #[cfg(target_os = "macos")]
    #[test]
    fn macos_results_copy_uses_cmd_not_ctrl() {
        let keymap = default_keymap();
        let ctrl_c = KeyChord::new("c", Modifiers::ctrl());
        assert_eq!(keymap.resolve(ContextId::Results, &ctrl_c), None);

        let cmd_c = KeyChord {
            key: "c".to_string(),
            modifiers: Modifiers {
                platform: true,
                ..Modifiers::none()
            },
        };
        assert_eq!(
            keymap.resolve(ContextId::Results, &cmd_c),
            Some(Command::ResultsCopyCell)
        );
    }

    /// Regression test for the "missing letters in the SQL editor after
    /// Ctrl+Enter" bug: while focus is on the editor input, the Editor
    /// keymap layer must not consume plain ASCII letters — they have to fall
    /// through to gpui-component's `InputState` so they get inserted as text.
    #[test]
    fn editor_layer_does_not_steal_unmodified_letters() {
        let keymap = default_keymap();
        for letter in ['h', 'j', 'k', 'l', 'o', 'r', 's', 'v', 'x', 'y'] {
            let chord = KeyChord::new(letter.to_string(), Modifiers::none());
            assert_eq!(
                keymap.resolve(ContextId::Editor, &chord),
                None,
                "Editor layer must not bind unmodified `{letter}` — it would be \
                 swallowed by Workspace::on_key_down before the SQL input ever \
                 sees the keystroke",
            );
        }
    }

    /// Pairs with `editor_layer_does_not_steal_unmodified_letters`: these
    /// single-letter bindings are intentionally claimed by the Results layer,
    /// which is exactly why `CodeDocument::process_pending_result` must keep
    /// `focus_mode = Editor` while the input is focused — otherwise the same
    /// letters would get routed here and never reach the editor.
    #[test]
    fn results_layer_owns_navigation_and_crud_letters() {
        let keymap = default_keymap();
        let expectations: &[(char, Command)] = &[
            ('h', Command::ColumnLeft),
            ('l', Command::ColumnRight),
            ('j', Command::SelectNext),
            ('k', Command::SelectPrev),
            ('r', Command::Rename),
            ('o', Command::ResultsAddRow),
            ('i', Command::ToggleRecordView),
            ('v', Command::ToggleValuePanel),
            ('x', Command::Delete),
        ];
        for (letter, expected) in expectations {
            let chord = KeyChord::new(letter.to_string(), Modifiers::none());
            assert_eq!(
                keymap.resolve(ContextId::Results, &chord),
                Some(*expected),
                "Results layer must keep `{letter}` → {expected:?} so the \
                 typing-vs-grid focus invariant in CodeDocument stays meaningful",
            );
        }
    }

    /// `space` in the Results layer must resolve to `ExpandCollapse`, matching
    /// the Sidebar layer's binding, so the object browser's "Space
    /// preview"/"Space properties" footer hints are actually live.
    #[test]
    fn results_layer_binds_space_to_expand_collapse() {
        let keymap = default_keymap();
        let chord = KeyChord::new("space", Modifiers::none());
        assert_eq!(
            keymap.resolve(ContextId::Results, &chord),
            Some(Command::ExpandCollapse),
        );
    }

    /// The find shortcut must stay unbound in the text-input context (and in
    /// every context it inherits from).
    ///
    /// `gpui-component` owns `cmd-f` / `ctrl-f` inside its own `Input` key
    /// context, where it opens the code editor's find panel. The workspace
    /// only calls `stop_propagation` for chords this stack resolves, so
    /// binding the chord here would silently take find away from every code
    /// editor in the app.
    #[test]
    fn find_shortcut_stays_available_to_the_editor_component() {
        let keymap = default_keymap();

        for modifiers in [Modifiers::primary(), Modifiers::ctrl()] {
            let chord = KeyChord::new("f", modifiers);

            assert_eq!(keymap.resolve(ContextId::TextInput, &chord), None);
            assert_eq!(keymap.resolve(ContextId::Editor, &chord), None);
        }
    }

    /// In the results grid, Cmd+S and Cmd+Enter commit the edited row. The
    /// grid's own binding must win over the inherited script Save, and only
    /// there — an editor with focus still saves the script.
    /// Save must resolve while a text buffer owns the keyboard — the S3
    /// object editors report `ContextId::TextInput`, which inherits from no
    /// parent layer.
    #[test]
    fn text_input_layer_binds_save() {
        let keymap = default_keymap();

        assert_eq!(
            keymap.resolve(
                ContextId::TextInput,
                &KeyChord::new("s", Modifiers::primary())
            ),
            Some(Command::SaveQuery),
        );
        assert_eq!(
            keymap.resolve(
                ContextId::TextInput,
                &KeyChord::new("s", Modifiers::primary_shift())
            ),
            Some(Command::SaveFileAs),
        );
    }

    /// Every unmodified a-z keystroke must reach the palette's search input
    /// instead of being swallowed by the workspace keydown handler, so the
    /// CommandPalette layer must bind none of them. Navigation stays on the
    /// arrow keys; Enter confirms and Escape closes.
    #[test]
    fn command_palette_does_not_steal_unmodified_letters() {
        let keymap = default_keymap();
        for letter in 'a'..='z' {
            let chord = KeyChord::new(letter.to_string(), Modifiers::none());
            assert_eq!(
                keymap.resolve(ContextId::CommandPalette, &chord),
                None,
                "CommandPalette must not bind unmodified `{letter}` — it would be \
                 swallowed before the search input ever sees the keystroke",
            );
        }
    }

    /// Arrow navigation, Enter and Escape must keep resolving in the palette
    /// once the bare-letter bindings are gone.
    #[test]
    fn command_palette_keeps_navigation_and_dismiss_chords() {
        let keymap = default_keymap();

        assert_eq!(
            keymap.resolve(
                ContextId::CommandPalette,
                &KeyChord::new("down", Modifiers::none())
            ),
            Some(Command::SelectNext)
        );
        assert_eq!(
            keymap.resolve(
                ContextId::CommandPalette,
                &KeyChord::new("up", Modifiers::none())
            ),
            Some(Command::SelectPrev)
        );
        assert_eq!(
            keymap.resolve(
                ContextId::CommandPalette,
                &KeyChord::new("enter", Modifiers::none())
            ),
            Some(Command::Execute)
        );
        assert_eq!(
            keymap.resolve(
                ContextId::CommandPalette,
                &KeyChord::new("escape", Modifiers::none())
            ),
            Some(Command::Cancel)
        );
    }

    /// Confirm-only modals must own exactly two chords: Enter confirms and
    /// Escape cancels. The context has no parent, so nothing else (including
    /// the Global escape binding) may resolve while a confirm modal is up.
    #[test]
    fn confirm_modal_layer_binds_enter_and_escape_only() {
        let keymap = default_keymap();

        assert_eq!(
            keymap.resolve(
                ContextId::ConfirmModal,
                &KeyChord::new("enter", Modifiers::none())
            ),
            Some(Command::Execute),
        );
        assert_eq!(
            keymap.resolve(
                ContextId::ConfirmModal,
                &KeyChord::new("escape", Modifiers::none())
            ),
            Some(Command::Cancel),
        );

        // No fallthrough to document or global shortcuts.
        for chord in [
            KeyChord::new("p", Modifiers::primary_shift()),
            KeyChord::new("s", Modifiers::primary()),
            KeyChord::new("j", Modifiers::none()),
            KeyChord::new("h", Modifiers::ctrl()),
        ] {
            assert_eq!(
                keymap.resolve(ContextId::ConfirmModal, &chord),
                None,
                "ConfirmModal must not resolve {chord:?}",
            );
        }
    }

    /// Every key the schema diagram used to parse by hand in `on_key_down`
    /// must resolve, in the SchemaViz context, to the command that performs
    /// the same action.
    #[test]
    fn schema_viz_layer_keeps_the_diagram_keys() {
        let keymap = default_keymap();
        let mut expectations = vec![
            (KeyChord::new("+", Modifiers::none()), Command::ZoomIn),
            (KeyChord::new("+", Modifiers::shift()), Command::ZoomIn),
            (KeyChord::new("=", Modifiers::none()), Command::ZoomIn),
            (KeyChord::new("=", Modifiers::shift()), Command::ZoomIn),
            (KeyChord::new("-", Modifiers::none()), Command::ZoomOut),
            (
                KeyChord::new("r", Modifiers::none()),
                Command::LayoutLeftRight,
            ),
            (
                KeyChord::new("s", Modifiers::none()),
                Command::LayoutSnowflake,
            ),
            (
                KeyChord::new("c", Modifiers::none()),
                Command::LayoutCompact,
            ),
            (
                KeyChord::new("m", Modifiers::none()),
                Command::OpenContextMenu,
            ),
            (KeyChord::new("escape", Modifiers::none()), Command::Cancel),
        ];

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
                expectations.push((KeyChord::new(key, Modifiers::none()), pan));
                expectations.push((KeyChord::new(key, Modifiers::shift()), select_table));
                expectations.push((KeyChord::new(key, Modifiers::alt()), move_table));
            }
        }

        for (chord, expected) in expectations {
            assert_eq!(
                keymap.resolve(ContextId::SchemaViz, &chord),
                Some(expected),
                "SchemaViz must resolve {chord:?} to {expected:?}",
            );
        }

        // Shift+- types `_` and never zoomed out.
        assert_eq!(
            keymap.resolve(
                ContextId::SchemaViz,
                &KeyChord::new("-", Modifiers::shift())
            ),
            None
        );
    }

    /// While its context menu is open the diagram resolves keys in the
    /// ContextMenu context, which must keep the keys the diagram's menu used.
    #[test]
    fn context_menu_layer_keeps_the_diagram_menu_keys() {
        let keymap = default_keymap();
        let expectations = [
            ("up", Command::MenuUp),
            ("k", Command::MenuUp),
            ("down", Command::MenuDown),
            ("j", Command::MenuDown),
            ("right", Command::MenuSelect),
            ("enter", Command::MenuSelect),
            ("l", Command::MenuSelect),
            ("escape", Command::MenuBack),
            ("h", Command::MenuBack),
            ("left", Command::MenuBack),
        ];

        for (key, expected) in expectations {
            assert_eq!(
                keymap.resolve(
                    ContextId::ContextMenu,
                    &KeyChord::new(key, Modifiers::none())
                ),
                Some(expected),
                "ContextMenu must resolve `{key}` to {expected:?}",
            );
        }
    }

    /// The keys the document tree used to bind itself, and the action each
    /// one ran. `d d` is absent: a two-keystroke sequence is not a chord.
    fn former_document_tree_bindings() -> Vec<(&'static str, Box<dyn Action>)> {
        use document_tree::actions;

        vec![
            ("up", Box::new(actions::MoveUp)),
            ("k", Box::new(actions::MoveUp)),
            ("down", Box::new(actions::MoveDown)),
            ("j", Box::new(actions::MoveDown)),
            ("left", Box::new(actions::MoveLeft)),
            ("h", Box::new(actions::MoveLeft)),
            ("right", Box::new(actions::MoveRight)),
            ("l", Box::new(actions::MoveRight)),
            ("home", Box::new(actions::MoveToTop)),
            ("g", Box::new(actions::MoveToTop)),
            ("end", Box::new(actions::MoveToBottom)),
            ("shift-g", Box::new(actions::MoveToBottom)),
            ("pageup", Box::new(actions::PageUp)),
            ("ctrl-u", Box::new(actions::PageUp)),
            ("pagedown", Box::new(actions::PageDown)),
            ("ctrl-d", Box::new(actions::PageDown)),
            ("space", Box::new(actions::ToggleExpand)),
            ("enter", Box::new(actions::StartEdit)),
            ("f2", Box::new(actions::StartEdit)),
            ("e", Box::new(actions::OpenPreview)),
            ("delete", Box::new(actions::DeleteDocument)),
            ("r", Box::new(actions::ToggleViewMode)),
            ("ctrl-f", Box::new(actions::OpenSearch)),
            ("/", Box::new(actions::OpenSearch)),
            ("n", Box::new(actions::NextMatch)),
            ("shift-n", Box::new(actions::PrevMatch)),
            ("escape", Box::new(actions::CloseSearch)),
        ]
    }

    /// Each former tree key must resolve through the DocumentTree layer to a
    /// command whose generated native binding runs the same tree action.
    #[test]
    fn document_tree_bindings_run_the_same_actions_as_before() {
        use gpui::{KeyContext, Keymap};

        let mut native = Keymap::default();
        native.add_bindings(document_tree_keybindings());
        let context_stack = [KeyContext::parse(document_tree::CONTEXT).unwrap()];

        for (keystroke, expected) in former_document_tree_bindings() {
            let typed = [Keystroke::parse(keystroke).unwrap()];

            let chord = key_chord_from_gpui(&typed[0]);
            assert!(
                default_keymap()
                    .resolve(ContextId::DocumentTree, &chord)
                    .is_some(),
                "DocumentTree must resolve `{keystroke}`",
            );

            let (matches, _pending) = native.bindings_for_input(&typed, &context_stack);
            let top = matches
                .first()
                .unwrap_or_else(|| panic!("`{keystroke}` must have a native tree binding"));
            assert!(
                top.action().partial_eq(expected.as_ref()),
                "`{keystroke}` must run {}, got {}",
                expected.name(),
                top.action().name(),
            );
        }
    }

    /// Every binding of the DocumentTree layer must produce a native binding;
    /// a command without a tree action would be listed but never fire.
    #[test]
    fn every_document_tree_binding_has_a_native_binding() {
        let own_bindings = default_keymap()
            .bindings_for_context(ContextId::DocumentTree)
            .into_iter()
            .filter(|(_, _, source)| *source == ContextId::DocumentTree)
            .count();

        assert_eq!(document_tree_keybindings().len(), own_bindings);
        assert_eq!(own_bindings, former_document_tree_bindings().len());
    }

    /// The Keybindings viewer lists `bindings_for_context` for each entry of
    /// `ContextId::all_variants`, so both contexts must be listed there with
    /// their own bindings.
    #[test]
    fn keybindings_viewer_lists_the_tree_and_diagram_keys() {
        let keymap = default_keymap();

        for (context, chord, command) in [
            (
                ContextId::DocumentTree,
                KeyChord::new("g", Modifiers::shift()),
                Command::SelectLast,
            ),
            (
                ContextId::DocumentTree,
                KeyChord::new("n", Modifiers::none()),
                Command::NextMatch,
            ),
            (
                ContextId::SchemaViz,
                KeyChord::new("s", Modifiers::none()),
                Command::LayoutSnowflake,
            ),
            (
                ContextId::SchemaViz,
                KeyChord::new("h", Modifiers::alt()),
                Command::MoveTableLeft,
            ),
        ] {
            assert!(ContextId::all_variants().contains(&context));
            assert!(
                keymap
                    .bindings_for_context(context)
                    .contains(&(chord.clone(), command, context)),
                "the viewer must list {chord:?} -> {command:?} under {context:?}",
            );
        }
    }

    #[test]
    fn document_tree_key_context_matches_the_component() {
        assert_eq!(
            ContextId::DocumentTree.as_gpui_context(),
            document_tree::CONTEXT
        );
    }

    #[test]
    fn gpui_keystroke_formats_modifiers_before_the_key() {
        assert_eq!(
            gpui_keystroke(&KeyChord::new("g", Modifiers::shift())),
            "shift-g"
        );
        assert_eq!(
            gpui_keystroke(&KeyChord::new("u", Modifiers::ctrl())),
            "ctrl-u"
        );
        assert_eq!(
            gpui_keystroke(&KeyChord {
                key: "p".to_string(),
                modifiers: Modifiers {
                    platform: true,
                    alt: true,
                    ..Modifiers::none()
                },
            }),
            "cmd-alt-p"
        );
    }

    /// Key presses reach the tree through the generated bindings, and the
    /// `d d` sequence still reaches it through the component's own binding.
    #[gpui::test]
    fn document_tree_key_presses_run_the_tree_actions(cx: &mut gpui::TestAppContext) {
        use dbflux_components::components::document_tree::{
            DocumentTree, DocumentTreeEvent, DocumentTreeState, NodeId,
        };
        use dbflux_core::Value;
        use gpui::{AppContext as _, VisualTestContext};
        use std::cell::RefCell;

        cx.update(gpui_component::init);
        cx.update(dbflux_components::theme::init);
        cx.update(init_document_tree_keybindings);

        let state = cx.update(|cx| {
            cx.new(|cx| {
                let mut state = DocumentTreeState::new(cx);
                state.load_from_values(
                    vec![
                        ("first".to_string(), Value::Int(1)),
                        ("second".to_string(), Value::Int(2)),
                        ("third".to_string(), Value::Int(3)),
                    ],
                    cx,
                );
                state
            })
        });
        let (_tree, window) = cx.add_window_view({
            let state = state.clone();
            move |_, cx| DocumentTree::new("test-document-tree", state, cx)
        });

        let events: Rc<RefCell<Vec<DocumentTreeEvent>>> = Rc::default();
        window.update(|window, cx| {
            let events = events.clone();
            cx.subscribe(&state, move |_, event: &DocumentTreeEvent, _| {
                events.borrow_mut().push(event.clone());
            })
            .detach();
            state.update(cx, |state, cx| state.focus(window, cx));
        });
        window.run_until_parked();

        let cursor = |window: &mut VisualTestContext| {
            window.update(|_, cx| state.read(cx).cursor().cloned())
        };

        window.simulate_keystrokes("j");
        assert_eq!(cursor(window), Some(NodeId::root(1)), "j moves down");

        window.simulate_keystrokes("shift-g");
        assert_eq!(cursor(window), Some(NodeId::root(2)), "Shift+G moves last");

        window.simulate_keystrokes("g");
        assert_eq!(cursor(window), Some(NodeId::root(0)), "g moves first");

        window.simulate_keystrokes("down");
        assert_eq!(cursor(window), Some(NodeId::root(1)), "Down moves down");

        window.simulate_keystrokes("d d");
        assert!(
            events.borrow().iter().any(|event| matches!(
                event,
                DocumentTreeEvent::DeleteRequested(id) if *id == NodeId::root(1)
            )),
            "`d d` must request deleting the document under the cursor",
        );

        window.simulate_keystrokes("e");
        assert!(
            events.borrow().iter().any(|event| matches!(
                event,
                DocumentTreeEvent::DocumentPreviewRequested { doc_index: 1, .. }
            )),
            "`e` must request the document preview",
        );

        window.simulate_keystrokes("/");
        window.run_until_parked();
        assert!(
            window.update(|_, cx| state.read(cx).is_search_visible()),
            "`/` must open the search bar",
        );
    }
}
