use gpui::{KeyBinding, actions};

actions!(
    dbflux,
    [
        ToggleCommandPalette,
        NewQueryTab,
        CloseCurrentTab,
        NextTab,
        PrevTab,
        SwitchToTab1,
        SwitchToTab2,
        SwitchToTab3,
        SwitchToTab4,
        SwitchToTab5,
        SwitchToTab6,
        SwitchToTab7,
        SwitchToTab8,
        SwitchToTab9,
        FocusSidebar,
        FocusEditor,
        FocusResults,
        FocusBackgroundTasks,
        CycleFocusForward,
        CycleFocusBackward,
        RunQuery,
        RunQueryInNewTab,
        ExportResults,
        OpenConnectionManager,
        Disconnect,
        RefreshSchema,
        ToggleEditor,
        ToggleResults,
        ToggleTasks,
        ToggleSidebar,
        // List navigation
        SelectNext,
        SelectPrev,
        SelectFirst,
        SelectLast,
        Execute,
        ExpandCollapse,
        // Column navigation (Results)
        ColumnLeft,
        ColumnRight,
        // Directional panel navigation
        FocusLeft,
        FocusRight,
        FocusUp,
        FocusDown,
        // Saved queries / history actions
        Delete,
        ToggleFavorite,
        Rename,
        FocusSearch,
        SaveQuery,
        // Settings
        OpenSettings,
        // Item menu
        OpenItemMenu,
        // Results toolbar
        FocusToolbar,
        TogglePanel,
        // File operations
        OpenScriptFile,
        SaveFileAs,
    ]
);

/// GPUI native bindings for the four workspace pane-focus shortcuts.
///
/// ## Why this function exists (GitHub #65)
///
/// GPUI normalizes Ctrl+Shift+digit chords at the platform layer (via
/// `PlatformKeyboardMapper::map_key_equivalent`) before the custom `KeymapStack`
/// structural matcher ever sees them. On macOS, for example, Ctrl+Shift+2 is
/// delivered as key `"@"` with `shift = false`, so the literal
/// `KeyChord::new("2", Modifiers::ctrl_shift())` in `defaults.rs` never matches.
/// Registering the chords as native GPUI `KeyBinding`s (via `cx.bind_keys`) lets
/// GPUI apply the same per-platform/layout normalization to both the registered
/// chord and the incoming keystroke, so they always match regardless of OS.
///
/// ## Why `ctrl-shift` on every platform (no `#[cfg]`)
///
/// `Cmd+Shift+3` and `Cmd+Shift+4` are reserved by macOS for screenshot
/// operations. Switching to the primary modifier would silently break two of the
/// four bindings on macOS. The full group stays on `ctrl-shift` across all
/// platforms for consistency.
///
/// ## Scope: only these four digit chords
///
/// Only the four focus-pane shortcuts go through GPUI native bindings. All
/// letter-based and other chords remain in the `KeymapStack` system in
/// `defaults.rs` and are unaffected by this function.
pub fn workspace_keybindings() -> Vec<KeyBinding> {
    vec![
        KeyBinding::new("ctrl-shift-1", FocusSidebar, None),
        KeyBinding::new("ctrl-shift-2", FocusEditor, None),
        KeyBinding::new("ctrl-shift-3", FocusResults, None),
        KeyBinding::new("ctrl-shift-4", FocusBackgroundTasks, None),
    ]
}

/// Keybindings that shadow `gpui-component` input defaults inside the "Input"
/// context so that the primary modifier + Enter runs the active query instead
/// of inserting a newline.
///
/// `gpui-component` binds `secondary-enter` (== Ctrl+Enter on Linux/Windows,
/// Cmd+Enter on macOS) to its internal `Enter` action, which in multi-line
/// mode inserts a newline. Registering these bindings after
/// `gpui_component::init` makes them take precedence at the same context
/// depth.
///
/// We bind the platform-appropriate keystroke directly (`cmd-enter` on macOS,
/// `ctrl-enter` elsewhere) rather than the abstract `secondary-` form so the
/// macOS Ctrl+Enter stays free for editor interrupt semantics and matches the
/// Cmd convention used by `results_layer` for ResultsCopyCell.
pub fn input_context_keybindings() -> Vec<KeyBinding> {
    let ctx = Some("Input");
    #[cfg(target_os = "macos")]
    {
        vec![
            KeyBinding::new("cmd-enter", RunQuery, ctx),
            KeyBinding::new("cmd-shift-enter", RunQueryInNewTab, ctx),
        ]
    }
    #[cfg(not(target_os = "macos"))]
    {
        vec![
            KeyBinding::new("ctrl-enter", RunQuery, ctx),
            KeyBinding::new("ctrl-shift-enter", RunQueryInNewTab, ctx),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui::{KeyContext, Keymap, Keystroke};

    #[test]
    fn actions_module_compiles() {
        let _ = ToggleCommandPalette;
    }

    #[test]
    fn input_context_keybindings_shape() {
        let bindings = input_context_keybindings();
        assert_eq!(bindings.len(), 2);

        #[cfg(target_os = "macos")]
        let (run, run_new) = (
            Keystroke::parse("cmd-enter").unwrap(),
            Keystroke::parse("cmd-shift-enter").unwrap(),
        );
        #[cfg(not(target_os = "macos"))]
        let (run, run_new) = (
            Keystroke::parse("ctrl-enter").unwrap(),
            Keystroke::parse("ctrl-shift-enter").unwrap(),
        );

        assert!(
            bindings[0].match_keystrokes(&[run]) == Some(false)
                && bindings[0].action().partial_eq(&RunQuery),
        );
        assert!(
            bindings[1].match_keystrokes(&[run_new]) == Some(false)
                && bindings[1].action().partial_eq(&RunQueryInNewTab),
        );
    }

    // Regression test for the GPUI shift-digit normalization bug (GitHub #65):
    // GPUI normalizes Ctrl+Shift+digit chords at the platform layer before
    // KeymapStack sees them, so literal KeymapStack entries for ctrl-shift-1..4
    // never fire at runtime. `workspace_keybindings()` registers the same chords
    // as native GPUI KeyBindings so GPUI's internal normalization applies to both
    // the registered chord and the incoming keystroke, making them match.
    //
    // This test exercises the Keymap-level match path to catch any recurrence of
    // the normalization regression. Note: it proves binding-side normalization but
    // not the full platform-event path (no easy way to synthesize the
    // platform-mangled key in a unit test — see design notes for #65).
    #[test]
    fn workspace_keybindings_ctrl_shift_2_resolves_to_focus_editor() {
        let mut keymap = Keymap::default();
        keymap.add_bindings(workspace_keybindings());

        let typed = [Keystroke::parse("ctrl-shift-2").unwrap()];
        // Global context: workspace bindings use None (no context restriction).
        let context_stack: &[KeyContext] = &[];
        let (matches, _pending) = keymap.bindings_for_input(&typed, context_stack);

        let top = matches.first().expect(
            "ctrl-shift-2 should match at least one binding when registered via \
             workspace_keybindings()",
        );
        assert!(
            top.action().partial_eq(&FocusEditor),
            "FocusEditor must be the top-ranked match for ctrl-shift-2 (GitHub #65 regression guard)",
        );
    }

    // Regression test for the run-query / newline conflict:
    // `gpui-component` registers `secondary-enter` in the "Input" context
    // (== Ctrl+Enter on Linux/Windows, Cmd+Enter on macOS), which would
    // otherwise consume the run-query chord and insert a newline. Our binding
    // is registered afterwards at the same context depth so it must win.
    #[test]
    fn input_context_keybindings_override_secondary_enter() {
        gpui::actions!(dbflux_test_only, [PriorEnterBinding]);

        let mut keymap = Keymap::default();
        // Stand-in for the gpui-component binding registered during its init.
        keymap.add_bindings([KeyBinding::new(
            "secondary-enter",
            PriorEnterBinding,
            Some("Input"),
        )]);
        // Our override, registered later.
        keymap.add_bindings(input_context_keybindings());

        #[cfg(target_os = "macos")]
        let run_keystroke = "cmd-enter";
        #[cfg(not(target_os = "macos"))]
        let run_keystroke = "ctrl-enter";

        let typed = [Keystroke::parse(run_keystroke).unwrap()];
        let context_stack = [KeyContext::parse("Input").unwrap()];
        let (matches, _pending) = keymap.bindings_for_input(&typed, &context_stack);

        let top = matches.first().expect(
            "the primary-modifier+Enter chord should match at least one binding in the \
             Input context",
        );
        assert!(
            top.action().partial_eq(&RunQuery),
            "RunQuery must take precedence over the earlier secondary-enter binding",
        );
    }
}
