//! Keyboard tests of the settings window: the keymap's FormNavigation keys
//! reach the navigation and the sections as commands, and the segmented
//! fields answer Left and Right.

use super::hooks_section::{HookFocus, HookFormField};
use super::{ActiveSettingsSection, SettingsCoordinator, SettingsFocus, SettingsSectionId};
use dbflux_app::keymap::Command;
use dbflux_core::{HookExecutionMode, ThemeSetting};
use dbflux_storage::bootstrap::StorageRuntime;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::keymap::init_keymap;
use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
use gpui::{AppContext as _, Entity, KeyDownEvent, Keystroke, TestAppContext, VisualTestContext};
use std::cell::RefCell;
use std::rc::Rc;

fn open_settings(
    cx: &mut TestAppContext,
    section: SettingsSectionId,
) -> (Entity<SettingsCoordinator>, &mut VisualTestContext) {
    cx.update(gpui_component::init);
    cx.update(dbflux_components::theme::init);
    cx.update(init_keymap);
    cx.update(|cx| {
        let host = cx.new(|_| ToastHost::new());
        cx.set_global(ToastGlobal { host });
    });

    let app_state: Entity<AppStateEntity> = cx.update(|cx| {
        cx.new(|_| {
            let runtime = StorageRuntime::in_memory().expect("in-memory storage");
            AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
        })
    });

    let slot: Rc<RefCell<Option<Entity<SettingsCoordinator>>>> = Rc::default();
    let (_, window) = cx.add_window_view({
        let slot = slot.clone();
        move |window, cx| {
            let settings =
                cx.new(|cx| SettingsCoordinator::new_with_section(app_state, section, window, cx));
            slot.replace(Some(settings.clone()));
            gpui_component::Root::new(settings, window, cx)
        }
    });
    window.run_until_parked();

    let settings = slot.borrow().clone().expect("the settings window is built");
    (settings, window)
}

/// Moves the keyboard into the section content, as Ctrl+L does.
fn focus_content(settings: &Entity<SettingsCoordinator>, window: &mut VisualTestContext) {
    window.update(|window, cx| {
        settings.update(cx, |settings, cx| {
            settings.focus_handle.focus(window, cx);
            settings.handle_command(Command::FocusRight, window, cx);
        })
    });
    window.run_until_parked();
}

fn general_theme(
    settings: &Entity<SettingsCoordinator>,
    window: &mut VisualTestContext,
) -> ThemeSetting {
    window.update(|_, cx| match &settings.read(cx).active_section_entity {
        ActiveSettingsSection::General(section) => section.read(cx).gen_settings.theme,
        _ => unreachable!("the general section is open"),
    })
}

fn general_cursor(settings: &Entity<SettingsCoordinator>, window: &mut VisualTestContext) -> usize {
    window.update(|_, cx| match &settings.read(cx).active_section_entity {
        ActiveSettingsSection::General(section) => section.read(cx).gen_form_cursor,
        _ => unreachable!("the general section is open"),
    })
}

#[gpui::test]
fn ctrl_h_and_ctrl_l_move_between_navigation_and_section(cx: &mut TestAppContext) {
    let (settings, window) = open_settings(cx, SettingsSectionId::General);
    let area = |window: &mut VisualTestContext| window.update(|_, cx| settings.read(cx).focus_area);

    window.simulate_keystrokes("ctrl-l");
    assert!(
        area(window) == SettingsFocus::Content,
        "Ctrl+L enters the section"
    );

    window.simulate_keystrokes("ctrl-h");
    assert!(
        area(window) == SettingsFocus::Sidebar,
        "Ctrl+H returns to the navigation"
    );
}

/// The theme row is a segmented field: Left and Right pick the choice next
/// to the current one and stop at either end.
#[gpui::test]
fn arrows_move_the_choice_of_a_segmented_general_row(cx: &mut TestAppContext) {
    let (settings, window) = open_settings(cx, SettingsSectionId::General);
    focus_content(&settings, window);
    assert_eq!(
        general_cursor(&settings, window),
        0,
        "the cursor starts on Theme"
    );

    window.update(|_, cx| {
        let section = match &settings.read(cx).active_section_entity {
            ActiveSettingsSection::General(section) => section.clone(),
            _ => unreachable!("the general section is open"),
        };
        section.update(cx, |section, _| {
            section.gen_settings.theme = ThemeSetting::Dark
        });
    });

    window.simulate_keystrokes("left");
    let after_left = general_theme(&settings, window);
    window.simulate_keystrokes("right");
    let after_right = general_theme(&settings, window);

    assert_ne!(after_left, after_right, "Left and Right change the choice");
    assert_eq!(
        general_cursor(&settings, window),
        0,
        "the cursor stays on the row"
    );
}

/// A FormNavigation key reaches the section as its command; the same key
/// arriving without a binding (the user removed it) is ignored.
#[gpui::test]
fn form_navigation_keys_only_move_through_the_keymap(cx: &mut TestAppContext) {
    let (settings, window) = open_settings(cx, SettingsSectionId::General);
    focus_content(&settings, window);

    window.simulate_keystrokes("j");
    assert_eq!(general_cursor(&settings, window), 1, "`j` moves down");

    window.update(|window, cx| {
        settings.update(cx, |settings, cx| {
            let event = KeyDownEvent {
                keystroke: Keystroke::parse("j").expect("valid keystroke"),
                is_held: false,
                prefer_character_input: false,
            };
            settings.handle_key_event(&event, window, cx);
        })
    });
    assert_eq!(
        general_cursor(&settings, window),
        1,
        "an unbound `j` does not move the cursor"
    );
}

/// Execution mode is one stop of the hook form; Left and Right switch it
/// between blocking and detached.
#[gpui::test]
fn arrows_switch_the_hook_execution_mode(cx: &mut TestAppContext) {
    let (settings, window) = open_settings(cx, SettingsSectionId::Hooks);
    focus_content(&settings, window);

    let hooks = window.update(|_, cx| match &settings.read(cx).active_section_entity {
        ActiveSettingsSection::Hooks(section) => section.clone(),
        _ => unreachable!("the hooks section is open"),
    });
    window.update(|_, cx| {
        hooks.update(cx, |section, _| {
            section.hook_focus = HookFocus::Form;
            section.hook_form_field = HookFormField::ExecutionMode;
        })
    });

    let mode =
        |window: &mut VisualTestContext| window.update(|_, cx| hooks.read(cx).hook_execution_mode);

    window.simulate_keystrokes("right");
    assert_eq!(mode(window), HookExecutionMode::Detached);

    window.simulate_keystrokes("left");
    assert_eq!(mode(window), HookExecutionMode::Blocking);
}

/// Recording in the keybindings editor captures a key sequence and saves it
/// after a pause; the context editor saves a predicate that parses and keeps
/// one that does not open with its error. Both apply to the live keymap, so
/// the test restores the defaults at the end.
#[gpui::test]
fn the_keybindings_editor_records_sequences_and_edits_predicates(cx: &mut TestAppContext) {
    use dbflux_app::keymap::{BindingSlot, ContextId, KeyChord, KeySequence, Modifiers};
    use dbflux_ui_base::keymap::{effective_keymap, keymap_overrides};

    let (settings, window) = open_settings(cx, SettingsSectionId::Keybindings);
    focus_content(&settings, window);

    let keybindings = window.update(|_, cx| match &settings.read(cx).active_section_entity {
        ActiveSettingsSection::Keybindings(section) => section.clone(),
        _ => unreachable!("the keybindings section is open"),
    });

    let slot = BindingSlot::new(
        ContextId::Global,
        Command::OpenAuditViewer,
        KeyChord::new("a", Modifiers::primary_shift()),
    );

    window.update(|_, cx| {
        keybindings.update(cx, |section, cx| {
            section.start_recording(slot.clone(), ContextId::Global, cx)
        })
    });
    window.simulate_keystrokes("ctrl-k shift-a");
    window
        .executor()
        .advance_clock(std::time::Duration::from_secs(1));
    window.run_until_parked();

    let sequence = KeySequence::parse("ctrl+k shift+a").expect("valid sequence");
    assert_eq!(
        keymap_overrides().effective_keys(&slot),
        Some(sequence.clone()),
        "the pause saves the recorded sequence"
    );
    assert_eq!(
        effective_keymap().resolve_sequence(ContextId::Global, &sequence),
        Some(Command::OpenAuditViewer)
    );

    window.update(|window, cx| {
        keybindings.update(cx, |section, cx| {
            section.start_predicate_editing(slot.clone(), window, cx)
        })
    });
    window.run_until_parked();

    let set_predicate_text = |text: &str, window: &mut VisualTestContext| {
        window.update(|window, cx| {
            let input = keybindings
                .read(cx)
                .predicate_editing
                .as_ref()
                .expect("the context editor is open")
                .input
                .clone();
            input.update(cx, |state, cx| {
                state.set_value(text.to_string(), window, cx)
            });
        });
    };

    set_predicate_text("Editor &&", window);
    window.simulate_keystrokes("enter");
    assert!(
        window.update(|_, cx| {
            keybindings
                .read(cx)
                .predicate_editing
                .as_ref()
                .is_some_and(|editing| editing.error.is_some())
        }),
        "an invalid predicate keeps the editor open with its error"
    );

    set_predicate_text("Editor && vim_mode == normal", window);
    window.simulate_keystrokes("enter");
    assert_eq!(
        keymap_overrides().custom_predicate(&slot),
        Some("Editor && vim_mode == normal"),
        "a valid predicate is saved"
    );

    window.update(|_, cx| keybindings.update(cx, |section, cx| section.reset_all(cx)));
    assert!(keymap_overrides().is_empty(), "the defaults are back");
}

fn proxies_focus(
    settings: &Entity<SettingsCoordinator>,
    window: &mut VisualTestContext,
) -> super::proxies_section::ProxyFocus {
    window.update(|_, cx| match &settings.read(cx).active_section_entity {
        ActiveSettingsSection::Proxies(section) => section.read(cx).proxy_focus,
        _ => unreachable!("the proxies section is open"),
    })
}

/// The section keys (`n` new, `d` delete, `i` import) are keymap commands of
/// the Settings context: `n` opens a new profile form, the same key arriving
/// without its binding does nothing, and a rebound key takes over.
#[gpui::test]
fn section_keys_run_through_the_keymap_and_follow_a_rebind(cx: &mut TestAppContext) {
    use super::proxies_section::ProxyFocus;
    use dbflux_app::keymap::{BindingSlot, ContextId, KeyChord, KeySequence, Modifiers};
    use dbflux_ui_base::keymap::{apply_keymap_overrides, keymap_overrides};

    let (settings, window) = open_settings(cx, SettingsSectionId::Proxies);
    focus_content(&settings, window);
    assert_eq!(proxies_focus(&settings, window), ProxyFocus::ProfileList);

    window.update(|window, cx| {
        settings.update(cx, |settings, cx| {
            let event = KeyDownEvent {
                keystroke: Keystroke::parse("n").expect("valid keystroke"),
                is_held: false,
                prefer_character_input: false,
            };
            settings.handle_key_event(&event, window, cx);
        })
    });
    assert_eq!(
        proxies_focus(&settings, window),
        ProxyFocus::ProfileList,
        "an `n` that no binding turned into a command does nothing"
    );

    window.simulate_keystrokes("n");
    assert_eq!(
        proxies_focus(&settings, window),
        ProxyFocus::Form,
        "`n` opens the form of a new proxy"
    );

    window.simulate_keystrokes("escape");
    assert_eq!(proxies_focus(&settings, window), ProxyFocus::ProfileList);

    // The section keys carry their own predicate, and so does their slot.
    let slot = BindingSlot::new(
        ContextId::Settings,
        Command::AddItem,
        KeyChord::new("n", Modifiers::none()),
    )
    .with_predicate("Settings && focus == section && !Input");
    let mut overrides = keymap_overrides();
    overrides.set(slot, Some(KeySequence::parse("a").expect("valid sequence")));
    window.update(|_, cx| apply_keymap_overrides(overrides, cx));
    window.run_until_parked();

    window.simulate_keystrokes("n");
    assert_eq!(
        proxies_focus(&settings, window),
        ProxyFocus::ProfileList,
        "the old key no longer adds a proxy"
    );

    window.simulate_keystrokes("a");
    assert_eq!(
        proxies_focus(&settings, window),
        ProxyFocus::Form,
        "the new key adds one"
    );
}

/// In the key bindings editor `c` opens the context filter with keyboard
/// focus and Shift+R drops every override, as the Reset to defaults button.
#[gpui::test]
fn keybindings_keys_open_the_context_filter_and_reset_everything(cx: &mut TestAppContext) {
    use dbflux_app::keymap::{BindingSlot, ContextId, KeyChord, KeySequence, Modifiers};
    use dbflux_ui_base::keymap::{apply_keymap_overrides, keymap_overrides};

    let (settings, window) = open_settings(cx, SettingsSectionId::Keybindings);
    focus_content(&settings, window);

    let keybindings = window.update(|_, cx| match &settings.read(cx).active_section_entity {
        ActiveSettingsSection::Keybindings(section) => section.clone(),
        _ => unreachable!("the keybindings section is open"),
    });

    window.simulate_keystrokes("c");
    let (open, focused) = window.update(|window, cx| {
        let filter = keybindings.read(cx).context_filter.read(cx);
        (filter.is_open(), filter.is_focused(window))
    });
    assert!(open && focused, "`c` opens the context filter with focus");

    window.simulate_keystrokes("escape");
    window.run_until_parked();

    let slot = BindingSlot::new(
        ContextId::Global,
        Command::OpenAuditViewer,
        KeyChord::new("a", Modifiers::primary_shift()),
    );
    let mut overrides = keymap_overrides();
    overrides.set(
        slot,
        Some(KeySequence::parse("ctrl+k a").expect("valid sequence")),
    );
    window.update(|_, cx| {
        apply_keymap_overrides(overrides.clone(), cx);
        keybindings.update(cx, |section, _| section.overrides = overrides);
    });

    window.simulate_keystrokes("shift-r");
    assert!(
        keymap_overrides().is_empty(),
        "Shift+R restores the defaults"
    );
}

/// The About page's links take the keyboard: Down moves to View source and
/// Enter opens it.
#[gpui::test]
fn the_about_links_open_from_the_keyboard(cx: &mut TestAppContext) {
    let (settings, window) = open_settings(cx, SettingsSectionId::About);
    focus_content(&settings, window);

    window.simulate_keystrokes("j enter");

    assert_eq!(
        window.opened_url().as_deref(),
        Some(env!("CARGO_PKG_REPOSITORY"))
    );
}

/// Recording a Vim leader binding stores the leader key pressed first as
/// the leader itself, so the new keys keep following the leader when it
/// changes.
#[gpui::test]
fn recording_a_leader_binding_keeps_it_relative_to_the_leader(cx: &mut TestAppContext) {
    use dbflux_app::keymap::{BindingSlot, ContextId, KeyChord, KeySequence, Modifiers};
    use dbflux_ui_base::keymap::{
        apply_keymap_overrides, default_vim_leader, effective_keymap, keymap_overrides,
        set_vim_leader,
    };

    let (settings, window) = open_settings(cx, SettingsSectionId::Keybindings);
    let keybindings = window.update(|_, cx| match &settings.read(cx).active_section_entity {
        ActiveSettingsSection::Keybindings(section) => section.clone(),
        _ => unreachable!("the keybindings section is open"),
    });

    let leader_then = |key: &str| {
        KeySequence::new(vec![
            KeyChord::leader(),
            KeyChord::new(key, Modifiers::none()),
        ])
        .expect("two chords")
    };
    let slot = BindingSlot::new(
        ContextId::VimNormal,
        Command::OpenPaneActions,
        leader_then("a"),
    );

    window.update(|_, cx| {
        keybindings.update(cx, |section, cx| {
            section.start_recording(slot.clone(), ContextId::VimNormal, cx)
        })
    });
    window.simulate_keystrokes("space x");
    window
        .executor()
        .advance_clock(std::time::Duration::from_secs(2));
    window.run_until_parked();

    assert_eq!(
        keymap_overrides().effective_keys(&slot),
        Some(leader_then("x")),
        "Space pressed first is recorded as the leader"
    );

    window.update(|_, cx| set_vim_leader(KeyChord::new(",", Modifiers::none()), cx));
    assert_eq!(
        effective_keymap()
            .keys_for_command(ContextId::VimNormal, Command::OpenPaneActions)
            .map(KeySequence::to_storage_string)
            .as_deref(),
        Some(", x"),
        "the recorded binding follows the new leader"
    );

    window.update(|_, cx| {
        set_vim_leader(default_vim_leader(), cx);
        apply_keymap_overrides(dbflux_app::keymap::KeymapOverrides::new(), cx);
    });
}
