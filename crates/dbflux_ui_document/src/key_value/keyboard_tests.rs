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
