//! Keyboard access to the key-value document: its dialogs keep Tab, its
//! `m` menu lists the toolbar and value panel buttons, and Alt+L / Alt+H
//! step the type filter and the expiry editor's mode.

use super::KeyValueDocument;
use super::context_menu::KvMenuAction;
use crate::keyboard_test_support::{KeymapHost, host_document, init_keyboard_runtime};
use crate::new_key_modal::ModalFocus;
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::components::form_navigation::FormNavigation as _;
use dbflux_core::{KeyType, KeyValueFeatures};
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};

/// A key-value document with no connection, hosted as the workspace hosts
/// it, with the keyboard on the document.
fn open(
    cx: &mut TestAppContext,
) -> (
    Entity<KeymapHost<KeyValueDocument>>,
    Entity<KeyValueDocument>,
    &mut VisualTestContext,
) {
    init_keyboard_runtime(cx);
    let app_state: Entity<dbflux_ui_base::AppStateEntity> = cx.update(|cx| {
        cx.new(|_| {
            let runtime =
                dbflux_storage::bootstrap::StorageRuntime::in_memory().expect("in-memory storage");
            dbflux_ui_base::AppStateEntity::new_with_storage_runtime(runtime)
                .expect("test storage setup")
        })
    });

    let (host, window) = host_document(
        cx,
        move |window, cx| {
            cx.new(|cx| {
                KeyValueDocument::new(uuid::Uuid::nil(), "0".to_string(), app_state, window, cx)
            })
        },
        |document, cx| document.active_context(cx),
        KeyValueDocument::dispatch_command,
    );
    let document = window.update(|_, cx| host.read(cx).document.clone());

    window.update(|window, cx| document.update(cx, |doc, cx| doc.focus(window, cx)));
    window.run_until_parked();

    (host, document, window)
}

fn keys(window: &mut VisualTestContext, keystrokes: &str) {
    for keystroke in keystrokes.split(' ') {
        window.simulate_keystrokes(keystroke);
        window.run_until_parked();
    }
}

/// Tab and Shift+Tab move through the New key dialog's fields and stay in
/// it: the document answers them instead of the workspace pane cycle.
#[gpui::test]
fn tab_moves_through_the_new_key_dialog(cx: &mut TestAppContext) {
    let (host, document, window) = open(cx);

    keys(window, "o");
    let modal = window.update(|_, cx| document.read(cx).new_key_modal.clone());
    assert!(window.update(|_, cx| modal.read(cx).is_visible()));
    assert_eq!(
        window.update(|_, cx| document.read(cx).active_context(cx)),
        ContextId::FormNavigation
    );
    assert_eq!(
        window.update(|_, cx| modal.read(cx).form_focus()),
        ModalFocus::KeyName
    );

    keys(window, "tab");
    assert_eq!(
        window.update(|_, cx| modal.read(cx).form_focus()),
        ModalFocus::Value,
        "Tab moves to the next field"
    );

    keys(window, "shift-tab");
    assert_eq!(
        window.update(|_, cx| modal.read(cx).form_focus()),
        ModalFocus::KeyName,
        "Shift+Tab moves back"
    );

    let handled_by_document = window.update(|window, cx| {
        document.update(cx, |doc, cx| {
            doc.dispatch_command(Command::CycleFocusForward, window, cx)
        })
    });
    assert!(
        handled_by_document,
        "the document keeps Tab while the dialog is open"
    );
    assert!(window.update(|_, cx| modal.read(cx).is_visible()));
    assert!(
        window
            .update(|_, cx| host.read(cx).commands.clone())
            .contains(&Command::CycleFocusForward),
        "Tab resolves to the form's cycle command"
    );
}

/// The key menu ends with the toolbar's layout switch and auto-refresh
/// interval, and running the layout entry switches the key list.
#[gpui::test]
fn the_key_menu_lists_the_toolbar_actions(cx: &mut TestAppContext) {
    let (_host, document, window) = open(cx);

    let actions: Vec<KvMenuAction> = window.update(|_, cx| {
        document
            .read(cx)
            .build_key_menu_items(cx)
            .into_iter()
            .map(|item| item.action)
            .collect()
    });
    assert!(actions.contains(&KvMenuAction::ToggleListLayout));
    assert!(actions.contains(&KvMenuAction::AutoRefresh));
    assert!(
        !actions.contains(&KvMenuAction::BulkDelete),
        "bulk delete needs a connection that supports it"
    );

    let layout_before = window.update(|_, cx| document.read(cx).list_layout);
    let index = actions
        .iter()
        .position(|action| *action == KvMenuAction::ToggleListLayout)
        .expect("layout entry");

    keys(window, "m");
    for _ in 0..index {
        keys(window, "j");
    }
    keys(window, "enter");

    let layout_after = window.update(|_, cx| document.read(cx).list_layout);
    assert_ne!(layout_after, layout_before, "the entry switches the layout");
}

/// Alt+L and Alt+H step the key type filter, All first, wrapping.
#[gpui::test]
fn alt_keys_step_the_type_filter(cx: &mut TestAppContext) {
    let (_host, document, window) = open(cx);
    window.update(|_, cx| {
        document.update(cx, |doc, _| {
            doc.key_features = KeyValueFeatures::SCAN_TYPE_FILTER;
        })
    });

    keys(window, "alt-l");
    assert_eq!(
        window.update(|_, cx| document.read(cx).type_filter),
        Some(KeyType::String),
        "Alt+L moves from All to the first type"
    );

    keys(window, "alt-h alt-h");
    assert_ne!(
        window.update(|_, cx| document.read(cx).type_filter),
        Some(KeyType::String),
        "Alt+H moves back past All to the last type"
    );
    assert!(window.update(|_, cx| document.read(cx).type_filter.is_some()));
}

/// Enter in the empty console field answers a pending confirmation with
/// Run anyway, as Escape answers it with Cancel.
#[gpui::test]
fn enter_confirms_a_pending_console_command(cx: &mut TestAppContext) {
    let (_host, document, window) = open(cx);

    keys(window, "ctrl-`");
    window.update(|_, cx| {
        document.update(cx, |doc, _| {
            doc.console.pending = Some(super::console::PendingConsoleCommand {
                command: "FLUSHDB".to_string(),
                title: "Dangerous".to_string(),
                body: "Deletes every key".to_string(),
            });
        })
    });

    keys(window, "enter");
    assert!(
        window.update(|_, cx| document.read(cx).console.pending.is_none()),
        "Enter answers the confirmation"
    );
    let cancelled = dbflux_i18n::t!("document.key_value.console.cancelled");
    let was_cancelled = window.update(|_, cx| {
        document
            .read(cx)
            .console
            .transcript
            .iter()
            .any(|entry| entry.output.iter().any(|line| line.text == cancelled))
    });
    assert!(
        !was_cancelled,
        "Enter runs the command rather than cancelling it"
    );
}

/// The key list with keys, a selected string key and its value panel.
/// `the_key_menu_lists_the_toolbar_actions` proves `m` opens the menu.
#[gpui::test]
fn the_key_value_browser_is_covered(cx: &mut TestAppContext) {
    use crate::keyboard_coverage::KEY_VALUE;
    use dbflux_core::{KeyEntry, KeyGetResult, KeyLoadState, ValueRepr};
    use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};

    let (_host, document, window) = open(cx);
    window.update(|_, cx| {
        document.update(cx, |document, cx| {
            let mut entry = KeyEntry::new("user:1");
            entry.key_type = Some(KeyType::String);
            document.keys = vec![
                entry.clone(),
                KeyEntry::new("user:2"),
                KeyEntry::new("jobs"),
            ];
            document.rebuild_key_rows();
            document.selected_index = Some(0);
            document.selected_value = Some(KeyGetResult {
                entry,
                value: b"hello".to_vec(),
                repr: ValueRepr::Text,
                load_state: KeyLoadState::Loaded,
            });
            cx.notify();
        })
    });
    window.run_until_parked();

    let menu: Vec<String> = window.update(|_, cx| {
        let document = document.read(cx);
        document
            .build_key_menu_items(cx)
            .into_iter()
            .chain(document.build_value_menu_items(cx))
            .map(|item| format!("{:?}", item.action))
            .collect()
    });

    let capture = FrameCapture::observe(window);
    let checked = Coverage::new(KEY_VALUE)
        .with_menu_entries(menu)
        .assert_covered(&capture.frame(window));
    assert!(
        checked.iter().any(|id| id == "kv-reload-value"),
        "{checked:?}"
    );
}
