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
fn primary_shift_n_opens_connection_manager_from_global_and_workspace_panels() {
    let keymap = default_keymap();
    let chord = KeyChord::new("n", Modifiers::primary_shift());

    for context in [
        ContextId::Global,
        ContextId::Sidebar,
        ContextId::Editor,
        ContextId::Results,
        ContextId::BackgroundTasks,
    ] {
        assert_eq!(
            keymap.resolve(context, &chord),
            Some(Command::OpenConnectionManager),
            "primary+shift+n must open the Connection Manager in {context:?}"
        );
    }
}

#[test]
fn primary_shift_n_is_not_bound_to_any_other_command() {
    let keymap = default_keymap();
    let chord = KeyChord::new("n", Modifiers::primary_shift());

    for context in ContextId::all_variants() {
        let resolved = keymap.resolve(*context, &chord);
        assert!(
            resolved.is_none() || resolved == Some(Command::OpenConnectionManager),
            "primary+shift+n resolves to {resolved:?} in {context:?}"
        );
    }
}

#[test]
fn chord_display_parts_follow_the_platform_modifier() {
    let chord = KeyChord::new("n", Modifiers::primary_shift());

    #[cfg(target_os = "macos")]
    let expected = ["Shift", "Cmd", "N"];
    #[cfg(not(target_os = "macos"))]
    let expected = ["Ctrl", "Shift", "N"];

    let parts = chord_display_parts(&chord);
    let parts: Vec<&str> = parts.iter().map(|part| part.as_ref()).collect();

    assert_eq!(parts, expected);
}

#[test]
fn chord_display_parts_keep_a_bare_letter_lowercase_and_uppercase_it_after_a_modifier() {
    let bare = chord_display_parts(&KeyChord::new("r", Modifiers::none()));
    let with_shift = chord_display_parts(&KeyChord::new("r", Modifiers::shift()));
    let with_ctrl = chord_display_parts(&KeyChord::new("c", Modifiers::ctrl()));

    assert_eq!(
        bare.iter().map(|part| part.as_ref()).collect::<Vec<_>>(),
        ["r"]
    );
    assert_eq!(with_shift.last().map(|part| part.as_ref()), Some("R"));
    assert_eq!(with_ctrl.last().map(|part| part.as_ref()), Some("C"));
}

#[test]
fn chord_display_parts_draw_enter_as_a_glyph_after_a_modifier() {
    let run = KeyChord::new("enter", Modifiers::primary());
    let parts = chord_display_parts(&run);

    assert_eq!(parts.last().map(|part| part.as_ref()), Some("\u{21b5}"));

    let confirm = KeyChord::new("enter", Modifiers::none());
    let parts = chord_display_parts(&confirm);
    let parts: Vec<&str> = parts.iter().map(|part| part.as_ref()).collect();

    assert_eq!(parts, ["Enter"]);
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

    let primary_n = KeyChord::new("n", Modifiers::primary());
    assert_eq!(keymap.resolve(ContextId::CommandPalette, &primary_n), None);
}

#[test]
fn command_palette_opens_in_a_new_tab_with_the_primary_enter_chord() {
    let keymap = default_keymap();

    let primary_enter = KeyChord::new("enter", Modifiers::primary());
    assert_eq!(
        keymap.resolve(ContextId::CommandPalette, &primary_enter),
        Some(Command::RunQueryInNewTab)
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

/// `F5` refreshes the focused document from the Results layer, and it is
/// the only refresh chord there: `r` keeps renaming, and documents render
/// their refresh hint from `shortcut_for_command`.
#[test]
fn results_layer_binds_f5_to_refresh() {
    let keymap = default_keymap();
    let f5 = KeyChord::new("f5", Modifiers::none());

    assert_eq!(
        keymap.resolve(ContextId::Results, &f5),
        Some(Command::RefreshSchema)
    );
    assert_eq!(
        keymap.resolve(ContextId::Results, &KeyChord::new("r", Modifiers::none())),
        Some(Command::Rename)
    );
    assert_eq!(
        keymap
            .shortcut_for_command(ContextId::Results, Command::RefreshSchema)
            .as_deref(),
        Some("f5")
    );
}

/// `F5` refreshes the focused document: only the Results, chart and
/// dashboard layers bind it, and always to `RefreshSchema`, so it cannot
/// shadow or be shadowed by another command.
#[test]
fn f5_is_bound_only_in_the_document_refresh_layers() {
    let keymap = default_keymap();
    let f5 = KeyChord::new("f5", Modifiers::none());
    let refresh_layers = [
        ContextId::Results,
        ContextId::Chart,
        ContextId::Dashboard,
        ContextId::McpApprovals,
    ];

    for context in ContextId::all_variants() {
        let bindings: Vec<Command> = keymap
            .bindings_for_context(*context)
            .into_iter()
            .filter(|(keys, _, owner)| *keys == KeySequence::from(f5.clone()) && owner == context)
            .map(|(_, command, _)| command)
            .collect();

        assert_eq!(
            !bindings.is_empty(),
            refresh_layers.contains(context),
            "unexpected F5 binding ownership in {context:?}"
        );
        assert!(
            bindings
                .iter()
                .all(|command| *command == Command::RefreshSchema),
            "F5 must refresh in {context:?}"
        );
    }
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

// ============================================================================
// Native bindings
// ============================================================================

fn native_keymap() -> gpui::Keymap {
    let mut keymap = gpui::Keymap::default();
    keymap.add_bindings(keymap_keybindings());
    keymap
}

fn parse_keys(text: &str) -> Vec<Keystroke> {
    text.split(' ')
        .map(|keystroke| Keystroke::parse(keystroke).expect("valid keystroke"))
        .collect()
}

fn element_stack(root: KeyContext, elements: &[&str]) -> Vec<KeyContext> {
    std::iter::once(root)
        .chain(
            elements
                .iter()
                .map(|element| KeyContext::parse(element).expect("valid key context")),
        )
        .collect()
}

/// The action the top-ranked binding of `keys` runs in `stack`.
fn top_action(keymap: &gpui::Keymap, keys: &str, stack: &[KeyContext]) -> Option<Box<dyn Action>> {
    let (matches, _pending) = keymap.bindings_for_input(&parse_keys(keys), stack);
    matches
        .first()
        .map(|binding| binding.action().boxed_clone())
}

fn runs_command(action: &dyn Action, command: Command) -> bool {
    action
        .as_any()
        .downcast_ref::<RunCommand>()
        .is_some_and(|run| run.command.as_ref() == command.action_id())
}

/// The key contexts a window root reports for `context`. The settings
/// window adds the active section and whether the section or its navigation
/// has the keyboard, which the section keys of the Settings layer match on.
fn roots_reported_for(context: ContextId) -> Vec<KeyContext> {
    if context != ContextId::Settings {
        return vec![root_key_context(WORKSPACE_KEY_CONTEXT, context, &[])];
    }

    ["proxies", "keybindings"]
        .into_iter()
        .map(|section| {
            root_key_context(
                WORKSPACE_KEY_CONTEXT,
                context,
                &[
                    ("section".into(), section.into()),
                    ("focus".into(), "section".into()),
                ],
            )
        })
        .collect()
}

/// Every default binding of a context a window root reports resolves, in
/// that root's key context, to the command the keymap stack resolves for it
/// (the context's own binding first, then the inherited one), so moving key
/// dispatch to GPUI keeps every default where it was.
#[test]
fn every_root_default_resolves_to_the_same_command_natively() {
    let keymap = native_keymap();

    for context in ContextId::all_variants() {
        if context.is_element_context() {
            continue;
        }

        let roots = roots_reported_for(*context);

        for (keys, command, _) in default_keymap().bindings_for_context(*context) {
            let typed = gpui_keystrokes(&keys);
            let actions: Vec<Box<dyn Action>> = roots
                .iter()
                .filter_map(|root| top_action(&keymap, &typed, std::slice::from_ref(root)))
                .collect();

            assert!(
                !actions.is_empty(),
                "`{typed}` must be bound in {context:?}"
            );
            assert!(
                actions
                    .iter()
                    .any(|action| runs_command(action.as_ref(), command)),
                "`{typed}` in {context:?} must run {command:?}, got {:?}",
                actions
                    .iter()
                    .map(|action| action.name())
                    .collect::<Vec<_>>()
            );
        }
    }
}

/// Global bindings reach every context that inherits them and none of the
/// contexts that capture the keyboard.
#[test]
fn global_bindings_stop_at_capturing_contexts_and_modals() {
    let keymap = native_keymap();
    let palette = gpui_keystrokes(
        default_keymap()
            .keys_for_command(ContextId::Global, Command::ToggleCommandPalette)
            .expect("the palette has a default shortcut"),
    );

    let editor = root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::Editor, &[]);
    assert!(
        top_action(&keymap, &palette, std::slice::from_ref(&editor))
            .is_some_and(|action| runs_command(action.as_ref(), Command::ToggleCommandPalette))
    );

    let confirm = root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::ConfirmModal, &[]);
    assert!(top_action(&keymap, &palette, &[confirm]).is_none());

    let inside_modal = element_stack(editor, &["Modal"]);
    assert!(
        top_action(&keymap, &palette, &inside_modal).is_none(),
        "the panels behind an open modal do not see the keys"
    );
}

/// The workspace commands that used to run only from the command palette,
/// and the keyboard shell commands, with the global keys they have.
fn workspace_command_chords() -> Vec<(Command, KeyChord)> {
    #[cfg_attr(not(feature = "mcp"), allow(unused_mut))]
    let mut chords = vec![
        (
            Command::OpenSettings,
            KeyChord::new(",", Modifiers::primary()),
        ),
        (
            Command::ToggleEditor,
            KeyChord::new("e", Modifiers::primary_shift()),
        ),
        (
            Command::ToggleResults,
            KeyChord::new("r", Modifiers::primary_shift()),
        ),
        (
            Command::ToggleTasks,
            KeyChord::new("t", Modifiers::primary_shift()),
        ),
        (
            Command::ToggleNotifications,
            KeyChord::new("b", Modifiers::primary_shift()),
        ),
        (
            Command::OpenLastErrorInAudit,
            KeyChord::new("x", Modifiers::primary_shift()),
        ),
        (
            Command::OpenToastActions,
            KeyChord::new("y", Modifiers::primary_shift()),
        ),
        (
            Command::MoveTabLeft,
            KeyChord::new("pageup", Modifiers::ctrl_shift()),
        ),
        (
            Command::MoveTabRight,
            KeyChord::new("pagedown", Modifiers::ctrl_shift()),
        ),
        (
            Command::OpenLoginModal,
            KeyChord::new("l", Modifiers::primary_shift()),
        ),
        (
            Command::OpenSsoWizard,
            KeyChord::new("o", Modifiers::primary_shift()),
        ),
        (
            Command::OpenSavedChart,
            KeyChord::new("c", Modifiers::primary_shift()),
        ),
        (
            Command::NewDashboard,
            KeyChord::new("d", Modifiers::primary_shift()),
        ),
        (
            Command::ShowConnectionsView,
            KeyChord::new("5", Modifiers::ctrl_shift()),
        ),
        (
            Command::ShowScriptsView,
            KeyChord::new("6", Modifiers::ctrl_shift()),
        ),
        (
            Command::ShowDashboardsView,
            KeyChord::new("7", Modifiers::ctrl_shift()),
        ),
    ];

    #[cfg(feature = "mcp")]
    chords.extend([
        (
            Command::OpenMcpApprovals,
            KeyChord::new("m", Modifiers::primary_shift()),
        ),
        (
            Command::RefreshMcpGovernance,
            KeyChord::new("g", Modifiers::primary_shift()),
        ),
    ]);

    chords
}

#[test]
fn workspace_commands_have_global_chords() {
    let keymap = effective_keymap();

    for (command, chord) in workspace_command_chords() {
        assert_eq!(
            keymap.keys_for_command(ContextId::Global, command),
            Some(&KeySequence::from(chord.clone())),
            "{command:?} must be bound to {chord} in the global layer"
        );

        for context in [
            ContextId::Sidebar,
            ContextId::Editor,
            ContextId::Results,
            ContextId::TextInput,
        ] {
            assert_eq!(
                keymap.resolve(context, &chord),
                Some(command),
                "{chord} must run {command:?} in {context:?}"
            );
        }
    }
}

/// The new global chords take no key another layer already binds, so no
/// panel shadows them and they shadow no panel. A layer without the global
/// parent may bind the chord to the same command (the notifications popover
/// closes with the shortcut that opened it).
#[test]
fn workspace_command_chords_are_bound_nowhere_else() {
    let keymap = default_keymap();

    for (command, chord) in workspace_command_chords() {
        let keys = KeySequence::from(chord.clone());

        for context in ContextId::all_variants() {
            let Some(layer) = keymap.layer(*context) else {
                continue;
            };

            if let Some(bound) = layer.get_sequence(&keys) {
                assert!(
                    bound == command,
                    "{chord} is also bound to {bound:?} in {context:?}"
                );
            }
        }
    }
}

/// While text is typed in a field or the context bar, the global chords
/// still reach the workspace, inside the field's own `Input` element too.
#[test]
fn global_chords_reach_text_entry_roots() {
    let keymap = native_keymap();

    for context in [ContextId::TextInput, ContextId::ContextBar] {
        let root = root_key_context(WORKSPACE_KEY_CONTEXT, context, &[]);
        let inside_input = element_stack(root.clone(), &["Input"]);

        for (keys, command) in [
            ("ctrl-shift-p", Command::ToggleCommandPalette),
            ("ctrl-tab", Command::NextTab),
            ("ctrl-shift-tab", Command::PrevTab),
            ("ctrl-w", Command::CloseCurrentTab),
            ("ctrl-3", Command::SwitchToTab(3)),
            ("ctrl-shift-2", Command::FocusEditor),
        ] {
            for stack in [std::slice::from_ref(&root), inside_input.as_slice()] {
                let action = top_action(&keymap, keys, stack)
                    .unwrap_or_else(|| panic!("`{keys}` must be bound in {context:?}"));

                assert!(
                    runs_command(action.as_ref(), command),
                    "`{keys}` in {context:?} must run {command:?}, got {}",
                    action.name()
                );
            }
        }
    }
}

/// Unmodified keys in a text root stay with the field: the global layer's
/// Tab cycle and Escape never take them from the text being typed.
#[test]
fn text_entry_roots_keep_unmodified_keys_from_the_global_layer() {
    let keymap = native_keymap();
    let root = root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::TextInput, &[]);
    let inside_input = element_stack(root.clone(), &["Input"]);

    for keys in ["j", "tab", "shift-tab", "enter", "down"] {
        let action = top_action(&keymap, keys, &inside_input);

        assert!(
            !action.as_ref().is_some_and(|action| {
                [
                    Command::CycleFocusForward,
                    Command::CycleFocusBackward,
                    Command::SelectNext,
                    Command::Execute,
                ]
                .into_iter()
                .any(|command| runs_command(action.as_ref(), command))
            }),
            "`{keys}` in a text field must not run a workspace command"
        );
    }

    let escape = top_action(&keymap, "escape", std::slice::from_ref(&root)).expect("bound");
    assert!(runs_command(escape.as_ref(), Command::Cancel));
}

/// Dialogs, menus, dropdowns and pickers own the keyboard until they close:
/// the global chords do not fire through them.
#[test]
fn global_chords_stop_at_dialogs_menus_and_pickers() {
    let keymap = native_keymap();

    for context in [
        ContextId::HistoryModal,
        ContextId::ConfirmModal,
        ContextId::Dropdown,
        ContextId::EventStreamsPicker,
        ContextId::SqlPreviewModal,
        ContextId::FormNavigation,
        ContextId::ContextMenu,
        ContextId::CommandPalette,
    ] {
        let root = root_key_context(WORKSPACE_KEY_CONTEXT, context, &[]);

        for keys in ["ctrl-tab", "ctrl-shift-p", "ctrl-1"] {
            assert!(
                top_action(&keymap, keys, std::slice::from_ref(&root)).is_none(),
                "`{keys}` must not fire while {context:?} owns the keyboard"
            );
        }
    }

    let text_root = root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::TextInput, &[]);
    let text_in_modal = element_stack(text_root, &["Modal", "Input"]);
    assert!(
        top_action(&keymap, "ctrl-tab", &text_in_modal).is_none(),
        "a text field inside a modal keeps the chords out"
    );
}

/// While an overlay owns the keyboard, the root drops the global chords even
/// for a text root, whether or not focus reached the overlay's `Modal`.
#[test]
fn overlay_roots_do_not_keep_the_global_chords() {
    let keymap = native_keymap();
    let overlay = overlay_root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::TextInput, &[]);

    assert!(overlay.contains("TextInput"));
    assert!(!overlay.contains(ContextId::GLOBAL_CHORDS_IDENTIFIER));
    assert!(top_action(&keymap, "ctrl-tab", &[overlay]).is_none());
}

/// The copies of the global chords follow the user's overrides: a rebound
/// chord moves, a shortcut moved to a bare key or to a predicate of the
/// user's own stays out of the text roots.
#[test]
fn global_chords_in_text_roots_follow_the_overrides() {
    let next_tab = BindingSlot::new(
        ContextId::Global,
        Command::NextTab,
        KeyChord::new("tab", Modifiers::ctrl()),
    );
    let close_tab = BindingSlot::new(
        ContextId::Global,
        Command::CloseCurrentTab,
        KeyChord::new("w", Modifiers::primary()),
    );
    let palette = BindingSlot::new(
        ContextId::Global,
        Command::ToggleCommandPalette,
        KeyChord::new("p", Modifiers::primary_shift()),
    );

    let mut overrides = KeymapOverrides::new();
    overrides.set(
        next_tab,
        Some(KeySequence::from(KeyChord::new(
            "j",
            Modifiers::ctrl_shift(),
        ))),
    );
    overrides.set(
        close_tab,
        Some(KeySequence::from(KeyChord::new("f4", Modifiers::none()))),
    );
    overrides.set_predicate(palette, Some("Editor && !Modal".to_string()));

    let mut keymap = gpui::Keymap::default();
    keymap.add_bindings(native_bindings(
        &overrides.effective_bindings(default_keymap()),
    ));

    let root = root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::TextInput, &[]);
    let stack = std::slice::from_ref(&root);

    let rebound = top_action(&keymap, "ctrl-shift-j", stack).expect("the rebound chord");
    assert!(runs_command(rebound.as_ref(), Command::NextTab));
    assert!(top_action(&keymap, "ctrl-tab", stack).is_none());
    assert!(top_action(&keymap, "f4", stack).is_none());
    assert!(top_action(&keymap, "ctrl-shift-p", stack).is_none());
}

/// GPUI normalizes Ctrl+Shift+digit at the platform layer (GitHub #65); as
/// native bindings the focus chords go through the same normalization.
#[test]
fn ctrl_shift_digit_focus_chords_are_native_bindings() {
    let keymap = native_keymap();
    let root = root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::Sidebar, &[]);

    let action = top_action(&keymap, "ctrl-shift-2", &[root]).expect("bound");
    assert!(runs_command(action.as_ref(), Command::FocusEditor));
}

/// The keys the document tree bound itself before the keymap owned them,
/// and the action each one ran.
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
        ("d d", Box::new(actions::DeleteDocument)),
        ("t", Box::new(actions::CycleDataView)),
        ("r", Box::new(actions::ToggleViewMode)),
        ("ctrl-f", Box::new(actions::OpenSearch)),
        ("/", Box::new(actions::OpenSearch)),
        ("n", Box::new(actions::NextMatch)),
        ("shift-n", Box::new(actions::PrevMatch)),
        ("escape", Box::new(actions::CloseSearch)),
    ]
}

/// The keys the data table bound itself before the keymap owned them, and
/// the action each one ran.
fn former_data_table_bindings() -> Vec<(&'static str, Box<dyn Action>)> {
    use data_table::actions;

    #[cfg(target_os = "macos")]
    let primary = "cmd";
    #[cfg(not(target_os = "macos"))]
    let primary = "ctrl";

    let with_primary =
        |key: &str| -> &'static str { Box::leak(format!("{primary}-{key}").into_boxed_str()) };

    vec![
        ("up", Box::new(actions::MoveUp)),
        ("down", Box::new(actions::MoveDown)),
        ("left", Box::new(actions::MoveLeft)),
        ("right", Box::new(actions::MoveRight)),
        ("k", Box::new(actions::MoveUp)),
        ("j", Box::new(actions::MoveDown)),
        ("h", Box::new(actions::MoveLeft)),
        ("l", Box::new(actions::MoveRight)),
        ("shift-up", Box::new(actions::SelectUp)),
        ("shift-down", Box::new(actions::SelectDown)),
        ("shift-left", Box::new(actions::SelectLeft)),
        ("shift-right", Box::new(actions::SelectRight)),
        ("home", Box::new(actions::MoveToLineStart)),
        ("end", Box::new(actions::MoveToLineEnd)),
        ("ctrl-home", Box::new(actions::MoveToTop)),
        ("ctrl-end", Box::new(actions::MoveToBottom)),
        ("shift-home", Box::new(actions::SelectToLineStart)),
        ("shift-end", Box::new(actions::SelectToLineEnd)),
        ("ctrl-shift-home", Box::new(actions::SelectToTop)),
        ("ctrl-shift-end", Box::new(actions::SelectToBottom)),
        (with_primary("a"), Box::new(actions::SelectAll)),
        ("escape", Box::new(actions::ClearSelection)),
        (with_primary("c"), Box::new(actions::Copy)),
        ("y y", Box::new(actions::Copy)),
        ("shift-y shift-y", Box::new(actions::CopyRow)),
        ("enter", Box::new(actions::StartEdit)),
        ("f2", Box::new(actions::StartEdit)),
        (with_primary("enter"), Box::new(actions::SaveRow)),
        (with_primary("s"), Box::new(actions::SaveRow)),
        ("d d", Box::new(actions::DeleteRow)),
        ("delete", Box::new(actions::DeleteRow)),
        ("a a", Box::new(actions::AddRow)),
        ("shift-a shift-a", Box::new(actions::DuplicateRow)),
        ("ctrl-n", Box::new(actions::SetNull)),
        ("u", Box::new(actions::Undo)),
        (with_primary("z"), Box::new(actions::Undo)),
        ("ctrl-r", Box::new(actions::Redo)),
        (with_primary("shift-z"), Box::new(actions::Redo)),
        ("e", Box::new(actions::ToggleColumnGroup)),
        ("backspace", Box::new(actions::StepOut)),
    ]
}

fn assert_element_bindings(
    stack: &[KeyContext],
    bindings: Vec<(&'static str, Box<dyn Action>)>,
    element: &str,
) {
    let keymap = native_keymap();

    for (keys, expected) in bindings {
        let action = top_action(&keymap, keys, stack)
            .unwrap_or_else(|| panic!("`{keys}` must have a native {element} binding"));

        assert!(
            action.partial_eq(expected.as_ref()),
            "`{keys}` in the {element} must run {}, got {}",
            expected.name(),
            action.name(),
        );
    }
}

/// Each former tree key reaches the same tree action through the keymap,
/// with the tree focused under the Results panel, and so does `d d`.
#[test]
fn document_tree_bindings_run_the_same_actions_as_before() {
    let stack = element_stack(
        root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::Results, &[]),
        &[document_tree::CONTEXT],
    );

    assert_element_bindings(&stack, former_document_tree_bindings(), "document tree");
}

/// Each former table key reaches the same table action through the keymap,
/// and the Results panel's own single keys do not take the table's
/// sequences (`y y`, `d d`) away.
#[test]
fn data_table_bindings_run_the_same_actions_as_before() {
    let stack = element_stack(
        root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::Results, &[]),
        &[data_table::CONTEXT],
    );

    assert_element_bindings(&stack, former_data_table_bindings(), "data table");

    let keymap = native_keymap();
    let (_, pending) = keymap.bindings_for_input(&parse_keys("y"), &stack);
    assert!(pending, "`y` waits for the second `y` of the table's copy");
}

/// The table's keys stay with an inline editor or any input nested in it.
#[test]
fn data_table_bindings_leave_nested_inputs_alone() {
    let keymap = native_keymap();
    let stack = element_stack(
        root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::TextInput, &[]),
        &[data_table::CONTEXT, "Input"],
    );

    assert!(top_action(&keymap, "j", &stack).is_none());
}

/// Every binding of an element context has an element action: a command
/// without one would be listed in the settings but never run.
#[test]
fn every_element_binding_has_an_element_action() {
    for context in ContextId::all_variants()
        .iter()
        .filter(|context| context.is_element_context())
    {
        let Some(layer) = default_keymap().layer(*context) else {
            continue;
        };

        for (keys, command) in layer.ordered_bindings() {
            let passes_through_root = (*context == ContextId::Input
                && matches!(
                    command,
                    Command::RunQuery | Command::RunQueryInNewTab | Command::FocusLeft
                ))
                || RUN_COMMAND_ELEMENT_CONTEXTS.contains(context);

            assert!(
                passes_through_root || element_action(*context, command).is_some(),
                "{context:?} binds {keys} to {command:?}, which it cannot run"
            );
        }
    }
}

#[test]
fn input_bindings_run_the_input_actions() {
    let stack = element_stack(
        root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::TextInput, &[]),
        &["Input"],
    );

    let mut expected: Vec<(&'static str, Box<dyn Action>)> = vec![
        ("ctrl-j", Box::new(InputMoveDown)),
        ("ctrl-k", Box::new(InputMoveUp)),
        ("ctrl-space", Box::new(TriggerCompletion)),
    ];
    #[cfg(not(target_os = "macos"))]
    expected.push(("ctrl-shift-z", Box::new(gpui_component::input::Redo)));

    assert_element_bindings(&stack, expected, "input");
}

/// Inside the code editor, including the text fields of its find panel,
/// Ctrl+h / Ctrl+j / Ctrl+k move focus between panes. Every other text field
/// keeps Ctrl+j / Ctrl+k as its own Down / Up.
#[test]
fn code_editor_inputs_move_focus_with_ctrl_h_j_k() {
    let keymap = native_keymap();
    let root = root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::Editor, &[]);

    for elements in [
        &["CodeEditor", "Input"][..],
        &["CodeEditor", "Input", "SearchPanel", "Input"][..],
    ] {
        let stack = element_stack(root.clone(), elements);

        for (keys, command) in [
            ("ctrl-h", Command::FocusLeft),
            ("ctrl-j", Command::FocusDown),
            ("ctrl-k", Command::FocusUp),
        ] {
            let action = top_action(&keymap, keys, &stack).expect("bound");
            assert!(
                runs_command(action.as_ref(), command),
                "`{keys}` in {elements:?} must run {command:?}, got {}",
                action.name()
            );
        }
    }

    let plain_input = element_stack(root, &["Input"]);
    assert_element_bindings(
        &plain_input,
        vec![
            ("ctrl-j", Box::new(InputMoveDown)),
            ("ctrl-k", Box::new(InputMoveUp)),
        ],
        "input outside the code editor",
    );
}

/// The input component binds the primary modifier + Enter to a newline; the
/// keymap's run chord is registered later at the same depth and wins.
#[test]
fn run_query_wins_over_the_inputs_own_primary_enter() {
    gpui::actions!(dbflux_test_only, [PriorEnterBinding]);

    let mut keymap = gpui::Keymap::default();
    keymap.add_bindings([KeyBinding::new(
        "secondary-enter",
        PriorEnterBinding,
        Some("Input"),
    )]);
    keymap.add_bindings(keymap_keybindings());

    #[cfg(target_os = "macos")]
    let run = "cmd-enter";
    #[cfg(not(target_os = "macos"))]
    let run = "ctrl-enter";

    let stack = element_stack(
        root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::Editor, &[]),
        &["CodeEditor", "Input"],
    );
    let action = top_action(&keymap, run, &stack).expect("bound");
    assert!(runs_command(action.as_ref(), Command::RunQuery));
}

#[test]
fn modal_keys_run_the_modal_actions() {
    let stack = element_stack(
        root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::Editor, &[]),
        &["Modal"],
    );

    assert_element_bindings(
        &stack,
        vec![
            ("escape", Box::new(component_actions::Cancel)),
            ("enter", Box::new(component_actions::Execute)),
            ("down", Box::new(component_actions::ScrollDown)),
            ("pagedown", Box::new(component_actions::ScrollPageDown)),
            ("end", Box::new(component_actions::ScrollToBottom)),
        ],
        "modal",
    );
}

#[test]
fn modal_editors_cancel_and_save() {
    #[cfg(target_os = "macos")]
    let save = "cmd-s";
    #[cfg(not(target_os = "macos"))]
    let save = "ctrl-s";

    for editor in ["Modal CellEditorModal", "Modal DocumentPreviewModal"] {
        let stack = element_stack(
            root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::Results, &[]),
            &[editor],
        );

        assert_element_bindings(
            &stack,
            vec![
                ("escape", Box::new(component_actions::Cancel)),
                (save, Box::new(component_actions::SaveEdit)),
            ],
            editor,
        );
    }
}

/// The SQL preview and the dialogs that share its context: Escape, Enter,
/// j / k and the paging keys run the modal actions, and the primary modifier
/// + C copies the preview. The letter keys stay with a focused text field.
#[test]
fn sql_preview_modal_keys_run_the_modal_actions() {
    #[cfg(target_os = "macos")]
    let copy = "cmd-c";
    #[cfg(not(target_os = "macos"))]
    let copy = "ctrl-c";

    let root = root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::SqlPreviewModal, &[]);
    let stack = element_stack(root.clone(), &["Modal SqlPreviewModal"]);

    assert_element_bindings(
        &stack,
        vec![
            ("escape", Box::new(component_actions::Cancel)),
            ("enter", Box::new(component_actions::Execute)),
            ("j", Box::new(component_actions::ScrollDown)),
            ("down", Box::new(component_actions::ScrollDown)),
            ("k", Box::new(component_actions::ScrollUp)),
            ("up", Box::new(component_actions::ScrollUp)),
            ("pagedown", Box::new(component_actions::ScrollPageDown)),
            ("pageup", Box::new(component_actions::ScrollPageUp)),
            (copy, Box::new(crate::sql_preview_modal::CopyPreview)),
        ],
        "SQL preview",
    );

    let keymap = native_keymap();
    let text_field = element_stack(root, &["Modal SqlPreviewModal", "Input"]);
    for keys in ["j", "k"] {
        assert!(
            top_action(&keymap, keys, &text_field).is_none(),
            "`{keys}` must stay with a text field inside the SQL preview"
        );
    }
}

#[test]
fn command_palette_navigates_with_arrows_and_ctrl_j_k() {
    let keymap = native_keymap();
    let stack = element_stack(
        root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::CommandPalette, &[]),
        &["CommandPalette", "Input"],
    );

    for (keys, command) in [
        ("down", Command::SelectNext),
        ("ctrl-j", Command::SelectNext),
        ("up", Command::SelectPrev),
        ("ctrl-k", Command::SelectPrev),
    ] {
        let action = top_action(&keymap, keys, &stack).expect("bound");
        assert!(
            runs_command(action.as_ref(), command)
                || (keys.starts_with("ctrl") && action.name().contains("Move")),
            "`{keys}` must navigate the palette, got {}",
            action.name()
        );
    }
}

// ============================================================================
// Predicates, sequences and overlap
// ============================================================================

#[test]
fn root_key_context_adds_global_only_to_inheriting_contexts() {
    let editor = root_key_context(
        WORKSPACE_KEY_CONTEXT,
        ContextId::Editor,
        &[(VIM_MODE_KEY.into(), "normal".into())],
    );
    assert!(editor.contains("Workspace"));
    assert!(editor.contains("Editor"));
    assert!(editor.contains("Global"));
    assert_eq!(
        editor.get(VIM_MODE_KEY).map(|value| value.as_ref()),
        Some("normal")
    );

    let palette = root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::CommandPalette, &[]);
    assert!(!palette.contains("Global"));
    assert!(!palette.contains(ContextId::GLOBAL_CHORDS_IDENTIFIER));

    let text = root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::TextInput, &[]);
    assert!(!text.contains("Global"));
    assert!(text.contains(ContextId::GLOBAL_CHORDS_IDENTIFIER));
}

#[test]
fn predicates_validate_and_flag_unknown_names() {
    assert_eq!(
        validate_predicate("Editor && vim_mode == normal"),
        Ok(vec![])
    );
    assert_eq!(validate_predicate("Workspace > Input"), Ok(vec![]));
    assert_eq!(
        validate_predicate("Editr || Results"),
        Ok(vec!["Editr".to_string()])
    );
    assert!(validate_predicate("Editor &&").is_err());
    assert!(validate_predicate("   ").is_err());
}

#[test]
fn every_default_predicate_is_valid_and_known() {
    for context in ContextId::all_variants() {
        assert_eq!(
            validate_predicate(context.default_predicate()),
            Ok(vec![]),
            "{context:?}"
        );
    }
}

#[test]
fn gpui_overlap_follows_the_context_tree_and_vim_modes() {
    let overlap = GpuiPredicateOverlap::new();

    assert!(overlap.overlaps(
        ContextId::Global.default_predicate(),
        ContextId::Editor.default_predicate()
    ));
    assert!(!overlap.overlaps(
        ContextId::Sidebar.default_predicate(),
        ContextId::Editor.default_predicate()
    ));
    assert!(!overlap.overlaps(
        ContextId::Global.default_predicate(),
        ContextId::CommandPalette.default_predicate()
    ));
    assert!(overlap.overlaps("Editor && vim_mode == normal", "Editor"));
    assert!(!overlap.overlaps(
        "Editor && vim_mode == normal",
        "Editor && vim_mode == insert"
    ));
    assert!(overlap.overlaps(
        ContextId::DataTable.default_predicate(),
        ContextId::Results.default_predicate()
    ));
}

#[test]
fn printable_keys_in_text_contexts_are_flagged() {
    let j = KeySequence::parse("j").expect("valid");
    let ctrl_j = KeySequence::parse("ctrl+j").expect("valid");

    assert!(binds_typed_text(&j, ContextId::Editor.default_predicate()));
    assert!(binds_typed_text(&j, "Input"));
    assert!(!binds_typed_text(&j, "Editor && vim_mode == normal"));
    assert!(!binds_typed_text(&ctrl_j, "Editor"));
    assert!(!binds_typed_text(
        &j,
        ContextId::Sidebar.default_predicate()
    ));
}

#[test]
fn key_sequences_format_for_gpui_and_labels() {
    let keys = KeySequence::parse("ctrl+k ctrl+s").expect("valid");

    assert_eq!(gpui_keystrokes(&keys), "ctrl-k ctrl-s");
    assert_eq!(key_sequence_label(&keys), "Ctrl K  Ctrl S");
    assert_eq!(
        gpui_keystrokes(&KeySequence::from(KeyChord {
            key: "p".to_string(),
            modifiers: Modifiers {
                platform: true,
                alt: true,
                ..Modifiers::none()
            },
        })),
        "cmd-alt-p"
    );
}

/// A user binding with a Vim-mode predicate and a key sequence becomes a
/// native binding that waits for its second key and fires only in that mode.
#[test]
fn user_sequences_with_predicates_become_native_bindings() {
    let slot = BindingSlot::new(
        ContextId::Global,
        Command::RunQuery,
        KeyChord::new("enter", Modifiers::primary()),
    );
    let mut overrides = KeymapOverrides::new();
    overrides.set(
        slot.clone(),
        Some(KeySequence::parse("space r").expect("valid")),
    );
    overrides.set_predicate(slot, Some("Editor && vim_mode == normal".to_string()));

    let binding = overrides
        .effective_bindings(default_keymap())
        .into_iter()
        .find(|binding| binding.slot.command == Command::RunQuery && binding.is_user)
        .expect("the user binding");
    let native = native_binding(&binding).expect("valid binding");

    let mut keymap = gpui::Keymap::default();
    keymap.add_bindings([native]);

    let normal = root_key_context(
        WORKSPACE_KEY_CONTEXT,
        ContextId::Editor,
        &[(VIM_MODE_KEY.into(), "normal".into())],
    );
    let insert = root_key_context(
        WORKSPACE_KEY_CONTEXT,
        ContextId::Editor,
        &[(VIM_MODE_KEY.into(), "insert".into())],
    );

    let (_, pending) =
        keymap.bindings_for_input(&parse_keys("space"), std::slice::from_ref(&normal));
    assert!(pending, "`space` waits for `r` in Normal mode");

    let action = top_action(&keymap, "space r", std::slice::from_ref(&normal)).expect("bound");
    assert!(runs_command(action.as_ref(), Command::RunQuery));

    assert!(top_action(&keymap, "space r", &[insert]).is_none());
}

// ============================================================================
// Live key presses
// ============================================================================

/// Key presses reach the tree through the generated bindings, `d d`
/// included.
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
    cx.update(init_keymap);

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

    let cursor =
        |window: &mut VisualTestContext| window.update(|_, cx| state.read(cx).cursor().cloned());

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

/// The search field types the letters the tree binds, Enter hands the
/// keyboard back to the tree with the matches kept, and Escape closes the
/// search from the field.
#[gpui::test]
fn document_tree_search_field_types_the_tree_letters(cx: &mut gpui::TestAppContext) {
    use dbflux_components::components::document_tree::{DocumentTree, DocumentTreeState, NodeId};
    use dbflux_core::Value;
    use gpui::{AppContext as _, VisualTestContext};

    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);
    cx.update(init_keymap);

    let state = cx.update(|cx| {
        cx.new(|cx| {
            let mut state = DocumentTreeState::new(cx);
            state.load_from_values(
                vec![
                    ("jgt".to_string(), Value::Int(1)),
                    ("other".to_string(), Value::Int(2)),
                    ("third".to_string(), Value::Int(3)),
                ],
                cx,
            );
            state
        })
    });
    let (_tree, window) = cx.add_window_view({
        let state = state.clone();
        move |_, cx| DocumentTree::new("test-document-tree-search", state, cx)
    });

    window.update(|window, cx| state.update(cx, |state, cx| state.focus(window, cx)));
    window.run_until_parked();

    let cursor =
        |window: &mut VisualTestContext| window.update(|_, cx| state.read(cx).cursor().cloned());
    let query = |window: &mut VisualTestContext| {
        window.update(|_, cx| state.read(cx).search_query().map(str::to_string))
    };

    window.simulate_keystrokes("/");
    window.run_until_parked();

    window.simulate_keystrokes("j g t");
    window.run_until_parked();
    assert_eq!(
        query(window).as_deref(),
        Some("jgt"),
        "letters type in the search"
    );
    assert_eq!(
        cursor(window),
        Some(NodeId::root(0)),
        "typing leaves the cursor on the match"
    );

    window.simulate_keystrokes("enter");
    window.run_until_parked();
    assert!(
        window.update(|_, cx| state.read(cx).is_search_visible()),
        "Enter keeps the search open",
    );

    window.simulate_keystrokes("j");
    window.run_until_parked();
    assert_eq!(
        cursor(window),
        Some(NodeId::root(1)),
        "after Enter, j moves the tree cursor"
    );
    assert_eq!(query(window).as_deref(), Some("jgt"), "the query is kept");

    window.simulate_keystrokes("n");
    window.run_until_parked();
    assert_eq!(
        cursor(window),
        Some(NodeId::root(0)),
        "n jumps to the match"
    );

    window.simulate_keystrokes("/");
    window.run_until_parked();
    window.simulate_keystrokes("escape");
    window.run_until_parked();
    assert!(
        !window.update(|_, cx| state.read(cx).is_search_visible()),
        "Escape in the search field closes the search",
    );
}

/// A rebinding reaches the effective keymap and the generated native
/// bindings at once, a key sequence works, and resetting it restores the
/// default key. This is the only test that changes the process-wide
/// overrides; it touches a binding no other test relies on and restores the
/// defaults at the end.
#[gpui::test]
fn overrides_rebind_native_document_tree_keys_live(cx: &mut gpui::TestAppContext) {
    use dbflux_components::components::document_tree::{
        DocumentTree, DocumentTreeEvent, DocumentTreeState,
    };
    use dbflux_core::Value;
    use gpui::AppContext as _;
    use std::cell::RefCell;

    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);
    cx.update(init_keymap);

    let state = cx.update(|cx| {
        cx.new(|cx| {
            let mut state = DocumentTreeState::new(cx);
            state.load_from_values(vec![("first".to_string(), Value::Int(1))], cx);
            state
        })
    });
    let (_tree, window) = cx.add_window_view({
        let state = state.clone();
        move |_, cx| DocumentTree::new("test-document-tree-overrides", state, cx)
    });

    let cycles = Rc::new(RefCell::new(0usize));
    window.update(|window, cx| {
        let cycles = cycles.clone();
        cx.subscribe(&state, move |_, event: &DocumentTreeEvent, _| {
            if matches!(event, DocumentTreeEvent::CycleDataViewRequested) {
                *cycles.borrow_mut() += 1;
            }
        })
        .detach();
        state.update(cx, |state, cx| state.focus(window, cx));
    });
    window.run_until_parked();

    // The tree's letters stay out of its text fields, so the slot carries
    // that predicate rather than the context's own.
    let slot = BindingSlot::new(
        ContextId::DocumentTree,
        Command::CycleDocumentView,
        KeyChord::new("t", Modifiers::none()),
    )
    .with_predicate("DocumentTree && !Input");
    let rebound = KeyChord::new("t", Modifiers::alt());

    let mut overrides = KeymapOverrides::new();
    overrides.set(slot.clone(), Some(KeySequence::from(rebound.clone())));
    window.update(|_, cx| apply_keymap_overrides(overrides.clone(), cx));

    assert_eq!(keymap_overrides(), overrides);
    assert_eq!(
        effective_keymap().resolve(ContextId::DocumentTree, &rebound),
        Some(Command::CycleDocumentView)
    );
    assert_eq!(
        default_keymap().resolve(ContextId::DocumentTree, &rebound),
        None,
        "the defaults stay untouched"
    );

    window.simulate_keystrokes("t");
    assert_eq!(*cycles.borrow(), 0, "the old key no longer cycles the view");

    window.simulate_keystrokes("alt-t");
    assert_eq!(*cycles.borrow(), 1, "the new key cycles the view");

    overrides.set(
        slot.clone(),
        Some(KeySequence::parse("ctrl+k t").expect("valid")),
    );
    window.update(|_, cx| apply_keymap_overrides(overrides.clone(), cx));

    window.simulate_keystrokes("ctrl-k t");
    assert_eq!(*cycles.borrow(), 2, "a key sequence cycles the view");

    overrides.reset(&slot);
    window.update(|_, cx| apply_keymap_overrides(overrides, cx));

    window.simulate_keystrokes("t");
    assert_eq!(*cycles.borrow(), 3, "the reset restores the default key");
}

/// A dropdown or multi-select focused from the keyboard carries the
/// `Dropdown` key context itself, so its keys win over the pane around it
/// (here the code editor's context bar, which binds the same letters) and
/// reach the control as keymap commands.
#[test]
fn a_focused_dropdown_takes_its_keys_before_the_pane() {
    let keymap = native_keymap();
    let stack = element_stack(
        root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::ContextBar, &[]),
        &["Dropdown"],
    );

    for (keys, command) in [
        ("j", Command::SelectNext),
        ("down", Command::SelectNext),
        ("k", Command::SelectPrev),
        ("up", Command::SelectPrev),
        ("enter", Command::Execute),
        ("space", Command::ExpandCollapse),
        ("escape", Command::Cancel),
    ] {
        let action = top_action(&keymap, keys, &stack).expect("bound");
        assert!(
            runs_command(action.as_ref(), command),
            "`{keys}` in a focused dropdown must run {command:?}"
        );
    }
}

/// `m` opens the pane-actions menu in the code editor's chrome (its context
/// bar), while the data table keeps `m` for its own context menu, which is
/// that pane's actions menu. The text area keeps `m` for typing.
#[test]
fn m_opens_the_pane_actions_in_the_editor_chrome_and_the_grid_menu_in_results() {
    let keymap = native_keymap();

    let context_bar = root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::ContextBar, &[]);
    for keys in ["m", "shift-f10"] {
        let action = top_action(&keymap, keys, std::slice::from_ref(&context_bar)).expect("bound");
        assert!(
            runs_command(action.as_ref(), Command::OpenPaneActions),
            "{keys}"
        );
    }

    let results = root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::Results, &[]);
    let action = top_action(&keymap, "m", std::slice::from_ref(&results)).expect("bound");
    assert!(runs_command(action.as_ref(), Command::OpenContextMenu));

    let typing = element_stack(
        root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::Editor, &[]),
        &[CODE_EDITOR_KEY_CONTEXT, "Input"],
    );
    assert!(top_action(&keymap, "m", &typing).is_none());
}

/// Shift+F10 opens the pane-actions menu from the code editor's text, in
/// every Vim mode and without Vim, so the toolbar menu does not require
/// leaving the editor. It types nothing, so it takes no text away.
#[test]
fn shift_f10_opens_the_pane_actions_from_the_code_editor_text() {
    let keymap = native_keymap();

    let mut roots = vec![root_key_context(
        WORKSPACE_KEY_CONTEXT,
        ContextId::Editor,
        &[],
    )];
    for mode in [
        "normal",
        "insert",
        "replace",
        "visual",
        "visual_line",
        "visual_block",
    ] {
        roots.push(root_key_context(
            WORKSPACE_KEY_CONTEXT,
            ContextId::Editor,
            &[(VIM_MODE_KEY.into(), mode.into())],
        ));
    }

    for root in roots {
        let typing = element_stack(root.clone(), &[CODE_EDITOR_KEY_CONTEXT, "Input"]);
        let action = top_action(&keymap, "shift-f10", &typing).expect("bound");
        assert!(
            runs_command(action.as_ref(), Command::OpenPaneActions),
            "{root:?}"
        );
    }
}

/// A query's result tabs are switched and closed from the results area, on
/// the grid itself too: `Alt+l` / `Alt+h` move to the next / previous tab and
/// `Alt+w` closes the one shown.
#[test]
fn alt_keys_switch_and_close_result_tabs_from_the_results() {
    let keymap = native_keymap();
    let results = root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::Results, &[]);
    let grid = element_stack(results.clone(), &[data_table::CONTEXT]);

    for stack in [vec![results], grid] {
        for (keys, command) in [
            ("alt-l", Command::NextResultTab),
            ("alt-h", Command::PrevResultTab),
            ("alt-w", Command::CloseResultTab),
        ] {
            let action = top_action(&keymap, keys, &stack).expect("bound");
            assert!(
                runs_command(action.as_ref(), command),
                "`{keys}` must run {command:?} in {stack:?}"
            );
        }
    }
}

/// `/` focuses the query history's search from its list, but stays a typed
/// character in the history's own fields (search, rename, save name), which
/// sit inside the history element.
#[test]
fn slash_in_the_query_history_fields_is_typed_text() {
    let keymap = native_keymap();
    let root = root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::HistoryModal, &[]);
    let list = element_stack(root, &["HistoryModal"]);

    let action = top_action(&keymap, "/", &list).expect("bound");
    assert!(runs_command(action.as_ref(), Command::FocusSearch));

    let mut field = list.clone();
    field.push(KeyContext::parse("Input").expect("valid key context"));
    assert!(top_action(&keymap, "/", &field).is_none());
}

/// Alt+L and Alt+H show the next and previous tab of the query history
/// (Recent, Saved) from its list. In its search, rename and save fields they
/// do too, except on macOS, where Option+letter types a character.
#[test]
fn alt_keys_switch_the_query_history_tabs() {
    let keymap = native_keymap();
    let root = root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::HistoryModal, &[]);
    let list = element_stack(root, &["HistoryModal"]);
    let mut field = list.clone();
    field.push(KeyContext::parse("Input").expect("valid key context"));

    for (keys, command) in [
        ("alt-l", Command::NextPanelTab),
        ("alt-h", Command::PrevPanelTab),
    ] {
        let action = top_action(&keymap, keys, &list).expect("bound");
        assert!(
            runs_command(action.as_ref(), command),
            "`{keys}` must run {command:?} in the history list"
        );

        let in_field = top_action(&keymap, keys, &field);
        if cfg!(target_os = "macos") {
            assert!(in_field.is_none(), "`{keys}` types a character on macOS");
        } else {
            let action = in_field.expect("bound in the history fields");
            assert!(runs_command(action.as_ref(), command));
        }
    }
}

/// Inside either query builder rail the in-pane keys move the cursor and act
/// on its row, while a text field or a dropdown inside the rail keeps its
/// letters; Escape, Ctrl+H, the run and save chords and the mode keys still
/// reach the rail from a text field.
#[test]
fn builder_rail_keys_drive_the_rail_and_leave_its_fields_their_letters() {
    let keymap = native_keymap();

    for context in [ContextId::QueryBuilder, ContextId::DocumentBuilder] {
        let root = root_key_context(WORKSPACE_KEY_CONTEXT, context, &[]);
        let rail = element_stack(root.clone(), &[]);
        let field = element_stack(root.clone(), &["Input"]);
        let dropdown = element_stack(root, &["Dropdown"]);

        for (keys, command) in [
            ("j", Command::SelectNext),
            ("k", Command::SelectPrev),
            ("g", Command::SelectFirst),
            ("shift-g", Command::SelectLast),
            ("h", Command::ColumnLeft),
            ("l", Command::ColumnRight),
            ("enter", Command::Execute),
            ("i", Command::Execute),
            ("space", Command::ExpandCollapse),
            ("a", Command::AddItem),
            ("shift-a", Command::AddGroup),
            ("x", Command::Delete),
            ("d", Command::Delete),
            ("shift-k", Command::MoveSelectedUp),
            ("shift-j", Command::MoveSelectedDown),
            ("m", Command::OpenPaneActions),
            ("shift-f10", Command::OpenPaneActions),
            ("alt-l", Command::NextPanelTab),
            ("alt-h", Command::PrevPanelTab),
            ("escape", Command::Cancel),
            ("ctrl-h", Command::FocusLeft),
        ] {
            let action = top_action(&keymap, keys, &rail)
                .unwrap_or_else(|| panic!("`{keys}` bound in {context:?}"));
            assert!(
                runs_command(action.as_ref(), command),
                "`{keys}` must run {command:?} in {context:?}"
            );
        }

        for keys in ["j", "a", "x", "l", "i", "m", "space"] {
            let in_field = top_action(&keymap, keys, &field);
            assert!(
                in_field
                    .as_ref()
                    .is_none_or(|action| !action.as_any().is::<RunCommand>()),
                "`{keys}` is typed text in a {context:?} field"
            );
            let in_dropdown = top_action(&keymap, keys, &dropdown);
            assert!(
                in_dropdown.as_ref().is_none_or(|action| {
                    !runs_command(action.as_ref(), Command::AddItem)
                        && !runs_command(action.as_ref(), Command::Delete)
                        && !runs_command(action.as_ref(), Command::ColumnRight)
                        && !runs_command(action.as_ref(), Command::OpenPaneActions)
                }),
                "`{keys}` does not act on the rail row from a focused dropdown"
            );
        }
    }
}

/// A chart document moves its highlighted point with H and L, the series
/// with J and K, switches the chart kind with Alt+H / Alt+L and the time
/// range with [ and ], and lists its toolbar on M. A focused dropdown in the
/// chart (refresh interval, custom range) keeps those letters.
#[test]
fn chart_keys_move_the_point_and_reach_the_toolbar() {
    let keymap = native_keymap();
    let root = root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::Chart, &[]);
    let chart = element_stack(root.clone(), &[]);
    let dropdown = element_stack(root, &["Dropdown"]);

    for (keys, command) in [
        ("h", Command::ColumnLeft),
        ("left", Command::ColumnLeft),
        ("l", Command::ColumnRight),
        ("right", Command::ColumnRight),
        ("j", Command::SelectNext),
        ("k", Command::SelectPrev),
        ("g", Command::SelectFirst),
        ("shift-g", Command::SelectLast),
        ("enter", Command::Execute),
        ("space", Command::ExpandCollapse),
        ("alt-l", Command::NextPanelTab),
        ("alt-h", Command::PrevPanelTab),
        ("]", Command::NextTimeRange),
        ("[", Command::PrevTimeRange),
        ("f5", Command::RefreshSchema),
        ("m", Command::OpenPaneActions),
        ("shift-f10", Command::OpenPaneActions),
        ("escape", Command::Cancel),
        ("ctrl-h", Command::FocusLeft),
        ("ctrl-l", Command::FocusRight),
    ] {
        let action = top_action(&keymap, keys, &chart)
            .unwrap_or_else(|| panic!("`{keys}` bound in the chart"));
        assert!(
            runs_command(action.as_ref(), command),
            "`{keys}` must run {command:?} in the chart"
        );
    }

    for keys in ["h", "l", "m", "]"] {
        let in_dropdown = top_action(&keymap, keys, &dropdown);
        assert!(
            in_dropdown.as_ref().is_none_or(|action| {
                !runs_command(action.as_ref(), Command::ColumnLeft)
                    && !runs_command(action.as_ref(), Command::ColumnRight)
                    && !runs_command(action.as_ref(), Command::OpenPaneActions)
                    && !runs_command(action.as_ref(), Command::NextTimeRange)
            }),
            "`{keys}` stays with a focused dropdown in the chart"
        );
    }
}

/// A dashboard selects panels with hjkl and the arrows, acts on the selected
/// one (open, configure, rename, remove, fold), moves it with Shift and
/// resizes it with Alt+Shift, and switches between View and Edit with
/// Alt+H / Alt+L.
#[test]
fn dashboard_keys_select_and_act_on_panels() {
    let keymap = native_keymap();
    let root = root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::Dashboard, &[]);
    let grid = element_stack(root.clone(), &[]);
    let field = element_stack(root, &["Input"]);

    for (keys, command) in [
        ("h", Command::ColumnLeft),
        ("left", Command::ColumnLeft),
        ("l", Command::ColumnRight),
        ("right", Command::ColumnRight),
        ("j", Command::SelectNext),
        ("down", Command::SelectNext),
        ("k", Command::SelectPrev),
        ("up", Command::SelectPrev),
        ("g", Command::SelectFirst),
        ("shift-g", Command::SelectLast),
        ("enter", Command::Execute),
        ("i", Command::Execute),
        ("c", Command::ConfigurePanel),
        ("r", Command::Rename),
        ("f2", Command::Rename),
        ("x", Command::Delete),
        ("delete", Command::Delete),
        ("a", Command::AddItem),
        ("space", Command::ExpandCollapse),
        ("shift-h", Command::MovePanelLeft),
        ("shift-l", Command::MovePanelRight),
        ("shift-k", Command::MovePanelUp),
        ("shift-j", Command::MovePanelDown),
        ("alt-shift-h", Command::ResizePanelNarrower),
        ("alt-shift-l", Command::ResizePanelWider),
        ("alt-shift-k", Command::ResizePanelShorter),
        ("alt-shift-j", Command::ResizePanelTaller),
        ("alt-l", Command::NextPanelTab),
        ("alt-h", Command::PrevPanelTab),
        ("]", Command::NextTimeRange),
        ("[", Command::PrevTimeRange),
        ("f5", Command::RefreshSchema),
        ("m", Command::OpenPaneActions),
        ("shift-f10", Command::OpenPaneActions),
        ("escape", Command::Cancel),
        ("ctrl-h", Command::FocusLeft),
    ] {
        let action = top_action(&keymap, keys, &grid)
            .unwrap_or_else(|| panic!("`{keys}` bound in the dashboard"));
        assert!(
            runs_command(action.as_ref(), command),
            "`{keys}` must run {command:?} in the dashboard"
        );
    }

    for keys in ["h", "x", "r", "a", "c"] {
        let in_field = top_action(&keymap, keys, &field);
        assert!(
            in_field
                .as_ref()
                .is_none_or(|action| !action.as_any().is::<RunCommand>()),
            "`{keys}` is typed text in a dashboard title field"
        );
    }
}

/// The MCP approvals keys run as commands in the view and leave the reason
/// field its letters, while Escape brings the keyboard back from the field.
#[test]
fn mcp_approvals_keys_move_and_decide() {
    let keymap = native_keymap();
    let root = root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::McpApprovals, &[]);
    let list = element_stack(root.clone(), &[]);
    let field = element_stack(root, &["Input"]);

    #[cfg_attr(not(feature = "mcp"), allow(unused_mut))]
    let mut expected = vec![
        ("j", Command::SelectNext),
        ("down", Command::SelectNext),
        ("k", Command::SelectPrev),
        ("up", Command::SelectPrev),
        ("g", Command::SelectFirst),
        ("shift-g", Command::SelectLast),
        ("enter", Command::Execute),
        ("i", Command::Execute),
        ("escape", Command::Cancel),
        ("f5", Command::RefreshSchema),
        ("m", Command::OpenPaneActions),
        ("shift-f10", Command::OpenPaneActions),
        ("ctrl-h", Command::FocusLeft),
    ];
    #[cfg(feature = "mcp")]
    expected.extend([
        ("a", Command::ApproveExecution),
        ("r", Command::RejectExecution),
    ]);

    for (keys, command) in expected {
        let action = top_action(&keymap, keys, &list)
            .unwrap_or_else(|| panic!("`{keys}` bound in the approvals"));
        assert!(
            runs_command(action.as_ref(), command),
            "`{keys}` must run {command:?} in the approvals"
        );
    }

    for keys in ["j", "k", "a", "r", "i", "g"] {
        let in_field = top_action(&keymap, keys, &field);
        assert!(
            in_field
                .as_ref()
                .is_none_or(|action| !action.as_any().is::<RunCommand>()),
            "`{keys}` is typed text in the reason field"
        );
    }

    for (keys, command) in [("escape", Command::Cancel), ("ctrl-h", Command::FocusLeft)] {
        let action = top_action(&keymap, keys, &field)
            .unwrap_or_else(|| panic!("`{keys}` bound in the reason field"));
        assert!(runs_command(action.as_ref(), command));
    }
}

/// A dashboard panel's Configure popover carries the Chart context on its
/// dialog, so the chart keys drive its axis pickers and chart type there,
/// while the dashboard's own panel keys and the pane focus chords do not
/// reach behind the dialog. Enter and Escape stay the dialog's own keys; the
/// popover hands them to an open picker first.
#[test]
fn the_configure_popover_takes_the_chart_keys() {
    let keymap = native_keymap();
    let root = root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::Dashboard, &[]);
    let popover = element_stack(root, &["Modal Chart"]);

    for (keys, command) in [
        ("h", Command::ColumnLeft),
        ("j", Command::SelectNext),
        ("alt-l", Command::NextPanelTab),
        ("space", Command::ExpandCollapse),
    ] {
        let action = top_action(&keymap, keys, &popover)
            .unwrap_or_else(|| panic!("`{keys}` bound in the popover"));
        assert!(
            runs_command(action.as_ref(), command),
            "`{keys}` must run {command:?} in the popover"
        );
    }

    for (keys, command) in [
        ("shift-j", Command::MovePanelDown),
        ("x", Command::Delete),
        ("ctrl-h", Command::FocusLeft),
    ] {
        let action = top_action(&keymap, keys, &popover);
        assert!(
            action
                .as_ref()
                .is_none_or(|action| !runs_command(action.as_ref(), command)),
            "`{keys}` does not reach behind the popover"
        );
    }
}

/// The Add Panel dialog carries its own context: Alt+H / Alt+L switch its
/// tabs from anywhere in it (the text fields too, except on macOS), and the
/// list letters work outside its text fields.
#[test]
fn add_panel_picker_keys_switch_tabs_and_move_the_lists() {
    let keymap = native_keymap();
    let root = root_key_context(WORKSPACE_KEY_CONTEXT, ContextId::TextInput, &[]);
    let list = element_stack(root.clone(), &["Modal AddPanelPicker"]);
    let field = element_stack(root, &["Modal AddPanelPicker", "Input"]);

    for (keys, command) in [
        ("j", Command::SelectNext),
        ("k", Command::SelectPrev),
        ("g", Command::SelectFirst),
        ("shift-g", Command::SelectLast),
        ("h", Command::ColumnLeft),
        ("l", Command::ColumnRight),
        ("space", Command::ExpandCollapse),
        ("/", Command::FocusSearch),
        ("alt-l", Command::NextPanelTab),
        ("alt-h", Command::PrevPanelTab),
    ] {
        let action = top_action(&keymap, keys, &list)
            .unwrap_or_else(|| panic!("`{keys}` bound in the Add Panel dialog"));
        assert!(
            runs_command(action.as_ref(), command),
            "`{keys}` must run {command:?} in the Add Panel dialog"
        );
    }

    for keys in ["j", "space", "/"] {
        let in_field = top_action(&keymap, keys, &field);
        assert!(
            in_field
                .as_ref()
                .is_none_or(|action| !action.as_any().is::<RunCommand>()),
            "`{keys}` is typed text in the dialog's fields"
        );
    }

    if !cfg!(target_os = "macos") {
        let action = top_action(&keymap, "alt-l", &field).expect("bound in the fields");
        assert!(runs_command(action.as_ref(), Command::NextPanelTab));
    }
}
