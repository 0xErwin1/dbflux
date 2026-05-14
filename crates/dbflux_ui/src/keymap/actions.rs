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
