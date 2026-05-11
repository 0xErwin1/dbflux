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

/// Keybindings that shadow `gpui-component` input defaults inside the "Input"
/// context so that Ctrl+Enter / Ctrl+Shift+Enter run the active query instead of
/// inserting a newline.
///
/// `gpui-component` binds `secondary-enter` (== Ctrl+Enter on Linux/Windows,
/// Cmd+Enter on macOS) to its internal `Enter` action, which in multi-line mode
/// inserts a newline. Registering these bindings after `gpui_component::init`
/// makes them take precedence at the same context depth.
pub fn input_context_keybindings() -> Vec<KeyBinding> {
    let ctx = Some("Input");
    vec![
        KeyBinding::new("ctrl-enter", RunQuery, ctx),
        KeyBinding::new("ctrl-shift-enter", RunQueryInNewTab, ctx),
    ]
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

        let ctrl_enter = Keystroke::parse("ctrl-enter").unwrap();
        let ctrl_shift_enter = Keystroke::parse("ctrl-shift-enter").unwrap();

        assert!(
            bindings[0].match_keystrokes(&[ctrl_enter]) == Some(false)
                && bindings[0].action().partial_eq(&RunQuery),
        );
        assert!(
            bindings[1].match_keystrokes(&[ctrl_shift_enter]) == Some(false)
                && bindings[1].action().partial_eq(&RunQueryInNewTab),
        );
    }

    // Regression test for the Ctrl+Enter conflict on Linux/Windows:
    // `gpui-component` registers `secondary-enter` (== ctrl-enter on non-mac)
    // in the "Input" context, which would otherwise consume Ctrl+Enter and
    // insert a newline. Adding our binding afterwards at the same context
    // depth must win.
    //
    // On macOS `secondary` resolves to `cmd`, so the two keystrokes don't
    // collide and the override is unnecessary — the precedence claim here
    // doesn't apply, hence the cfg gate.
    #[cfg(not(target_os = "macos"))]
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

        let typed = [Keystroke::parse("ctrl-enter").unwrap()];
        let context_stack = [KeyContext::parse("Input").unwrap()];
        let (matches, _pending) = keymap.bindings_for_input(&typed, &context_stack);

        let top = matches
            .first()
            .expect("ctrl-enter should match at least one binding in the Input context");
        assert!(
            top.action().partial_eq(&RunQuery),
            "RunQuery must take precedence over the earlier secondary-enter binding",
        );
    }
}
